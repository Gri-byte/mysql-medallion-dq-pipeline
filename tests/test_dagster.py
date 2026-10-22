"""Dagster layer: assets, blocking asset checks, schedule. Runs against a real MySQL server.

Skipped automatically when Dagster is not installed (it is an optional extra:
`pip install -r requirements-orchestration.txt`).
"""
import json

import pytest

pytest.importorskip("dagster")

from dagster import AssetCheckSeverity, AssetKey, materialize  # noqa: E402

from orchestration.assets import (  # noqa: E402
    ALL_ASSETS,
    BRONZE_SPECS,
    GOLD_SPECS,
    SILVER_SPECS,
    _asset_check_results,
    _conclude,
    bronze_raw,
)
from orchestration.definitions import defs  # noqa: E402
from orchestration.resources import MySQLResource  # noqa: E402
from src.checks import bronze_checks, gold_checks, silver_checks  # noqa: E402
from src.config import Settings  # noqa: E402
from src.db import connect, fetch_all, scalar  # noqa: E402
from src.dq import Check, CheckResult  # noqa: E402
from src.run_pipeline import drop_databases, init_schemas, start_run  # noqa: E402

SOURCE = AssetKey("source_csv_files")
LAYERS = [AssetKey("bronze_raw"), AssetKey("silver_clean"), AssetKey("gold_star_schema")]


@pytest.fixture
def dag(make_settings, tmp_path, monkeypatch):
    settings = make_settings("dag")
    monkeypatch.setenv("DQ_DATA_DIR", str(tmp_path))
    drop_databases(settings)
    yield settings
    drop_databases(settings)


def _run(settings, *, bookings=800, guests=250, defect_rate=0.04, seed=21, selection=None):
    config = {"ops": {"source_csv_files": {"config": {
        "n_bookings": bookings, "n_guests": guests, "defect_rate": defect_rate, "seed": seed}}}}
    return materialize(
        ALL_ASSETS,
        resources={"db": MySQLResource(prefix=settings.prefix)},
        run_config=None if selection else config,
        selection=selection,
        raise_on_error=False,
    )


def _materialized(result):
    return {event.asset_key for event in result.get_asset_materialization_events()}


def _count(settings, db, table):
    conn = connect(settings)
    try:
        return int(scalar(conn, f"SELECT COUNT(*) FROM `{db}`.{table}"))
    finally:
        conn.close()


def _runs(settings):
    conn = connect(settings)
    try:
        return fetch_all(conn, f"SELECT run_id, status, failed_stage, message FROM `{settings.dq}`.pipeline_runs")
    finally:
        conn.close()


# --------------------------------------------------------------------------------- happy path
def test_full_run_materializes_every_layer_and_all_34_checks_pass(dag, tmp_path):
    result = _run(dag)
    assert result.success
    assert _materialized(result) == {SOURCE, *LAYERS}

    evaluations = result.get_asset_check_evaluations()
    assert len(evaluations) == 34
    assert all(e.passed for e in evaluations)

    # Same bookkeeping as the CLI, keyed by the Dagster run id.
    runs = _runs(dag)
    assert [(r[0], r[1], r[2]) for r in runs] == [(result.run_id, "SUCCESS", None)]
    conn = connect(dag)
    try:
        persisted = scalar(conn, f"SELECT COUNT(*) FROM `{dag.dq}`.check_results WHERE run_id = %s", (result.run_id,))
    finally:
        conn.close()
    assert persisted == 34


def test_silver_matches_the_injected_ground_truth_and_reports_metadata(dag, tmp_path):
    result = _run(dag)
    assert result.success
    manifest = json.loads((tmp_path / "defects_manifest.json").read_text(encoding="utf-8"))
    for table in ("properties", "guests", "bookings", "payments"):
        assert _count(dag, dag.silver, table) == manifest["tables"][table]["expected_valid_rows"], table

    silver_event = next(e for e in result.get_asset_materialization_events() if e.asset_key == AssetKey("silver_clean"))
    metadata = silver_event.materialization.metadata
    assert metadata["quarantined_rows"].value == _count(dag, dag.silver, "quarantine") > 0
    assert metadata["rows_bookings"].value == manifest["tables"]["bookings"]["expected_valid_rows"]


# --------------------------------------------------------------------------------- failure modes
def test_quality_failure_blocks_downstream_layers_and_is_recorded(dag):
    result = _run(dag, bookings=1000, guests=300, defect_rate=0.30, seed=5)
    assert not result.success
    # Bronze data is kept for diagnosis, but silver and gold are never built from it.
    assert _materialized(result) == {SOURCE, AssetKey("bronze_raw")}
    assert _count(dag, dag.silver, "bookings") == 0
    assert _count(dag, dag.gold, "fact_bookings") == 0

    failed = {e.check_name for e in result.get_asset_check_evaluations() if not e.passed}
    assert {"bookings_null_key_rate", "payments_null_key_rate"} <= failed

    (run_id, status, stage, message), = _runs(dag)
    assert (run_id, status, stage) == (result.run_id, "FAILED", "bronze")
    assert "bookings_null_key_rate" in message


def test_failed_run_does_not_overwrite_last_good_gold(dag):
    good = _run(dag, seed=1)
    assert good.success
    facts, kpis = _count(dag, dag.gold, "fact_bookings"), _count(dag, dag.gold, "agg_monthly_property_kpis")
    assert facts > 0

    bad = _run(dag, defect_rate=0.30, seed=2)
    assert not bad.success
    assert _count(dag, dag.gold, "fact_bookings") == facts
    assert _count(dag, dag.gold, "agg_monthly_property_kpis") == kpis
    assert sorted(r[1] for r in _runs(dag)) == ["FAILED", "SUCCESS"]


def test_run_with_only_some_layers_still_closes_its_bookkeeping(dag):
    assert _run(dag).success
    bronze_only = _run(dag, selection=[bronze_raw])
    assert bronze_only.success
    statuses = {r[0]: r[1] for r in _runs(dag)}
    assert statuses[bronze_only.run_id] == "SUCCESS", "bronze was the last layer of that run, so the run must be closed"


# --------------------------------------------------------------------------------- unit-level
def test_check_severity_is_translated_to_dagster_severity():
    warn = Check("silver", "w", "SELECT 1", severity="WARN", description="warn")
    error = Check("silver", "e", "SELECT 1", severity="ERROR", description="error")
    out = {
        r.check_name: r
        for r in _asset_check_results([CheckResult(warn, 2, 10, "FAIL"), CheckResult(error, 0, None, "PASS")])
    }
    assert out["w"].passed is False and out["w"].severity == AssetCheckSeverity.WARN
    assert out["e"].passed is True and out["e"].severity == AssetCheckSeverity.ERROR
    assert out["w"].metadata["failed_rows"].value == 2 and "total_rows" in out["w"].metadata
    assert "total_rows" not in out["e"].metadata


def test_warn_failure_does_not_mark_the_run_failed(dag):
    init_schemas(dag)
    conn = connect(dag)
    try:
        start_run(conn, dag, "run-warn")
        warn = CheckResult(Check("gold", "w", "SELECT 1", severity="WARN"), 3, None, "FAIL")
        _conclude(conn, dag, "run-warn", "gold", [warn], last=True)
        assert fetch_all(conn, f"SELECT status, failed_stage FROM `{dag.dq}`.pipeline_runs") == [("SUCCESS", None)]

        start_run(conn, dag, "run-error")
        error = CheckResult(Check("gold", "e", "SELECT 1", severity="ERROR"), 3, None, "FAIL")
        _conclude(conn, dag, "run-error", "gold", [error], last=True)
        rows = dict(fetch_all(conn, f"SELECT run_id, status FROM `{dag.dq}`.pipeline_runs"))
        assert rows == {"run-warn": "SUCCESS", "run-error": "FAILED"}
    finally:
        conn.close()


def test_start_run_is_idempotent(dag):
    init_schemas(dag)
    conn = connect(dag)
    try:
        for _ in range(3):
            start_run(conn, dag, "same-run")
        assert scalar(conn, f"SELECT COUNT(*) FROM `{dag.dq}`.pipeline_runs") == 1
    finally:
        conn.close()


# --------------------------------------------------------------------------------- definitions
def test_asset_checks_match_the_check_catalogue_one_to_one():
    """If someone adds a check in checks.py, Dagster must expose it (and vice versa)."""
    settings = Settings()
    for specs, checks in ((BRONZE_SPECS, bronze_checks), (SILVER_SPECS, silver_checks), (GOLD_SPECS, gold_checks)):
        assert [s.name for s in specs] == [c.name for c in checks(settings)]
        assert all(s.blocking for s in specs)
    assert len(BRONZE_SPECS) + len(SILVER_SPECS) + len(GOLD_SPECS) == 34


def test_definitions_expose_assets_job_and_daily_schedule():
    graph = defs.resolve_asset_graph()
    assert {k for k in graph.get_all_asset_keys()} == {SOURCE, *LAYERS}
    assert {k for k in graph.asset_check_keys} and len(graph.asset_check_keys) == 34

    schedule = defs.resolve_schedule_def("daily_medallion")
    assert schedule.cron_schedule == "0 6 * * *"
    assert schedule.execution_timezone == "America/Argentina/Buenos_Aires"
    assert schedule.job_name == "medallion_pipeline"
