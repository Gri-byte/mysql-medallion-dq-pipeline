"""End-to-end happy path on a real MySQL server.

The key idea: the generator KNOWS which defects it injected (defects_manifest.json), so every test
compares what the pipeline did against ground truth instead of against its own output.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.config import SQL_DIR
from src.db import connect, fetch_all, scalar, split_statements
from src.run_pipeline import drop_databases, run_pipeline

TABLES = ("properties", "guests", "bookings", "payments")
RAW = {"properties": "raw_properties", "guests": "raw_guests", "bookings": "raw_bookings", "payments": "raw_payments"}


@pytest.fixture(scope="module")
def e2e(base_settings, tmp_path_factory):
    import dataclasses

    settings = dataclasses.replace(base_settings, prefix=f"{base_settings.prefix}e2e_")
    data_dir = tmp_path_factory.mktemp("raw")
    result = run_pipeline(settings, data_dir, n_bookings=2000, n_guests=500, defect_rate=0.04, seed=7, reset=True)
    conn = connect(settings)
    yield SimpleNamespace(settings=settings, result=result, manifest=result.manifest, data_dir=data_dir, conn=conn)
    conn.close()
    drop_databases(settings)


def _count(conn, db, table):
    return int(scalar(conn, f"SELECT COUNT(*) FROM `{db}`.{table}"))


def _quarantine(conn, settings, table):
    rows = fetch_all(
        conn, f"SELECT reason, COUNT(*) FROM `{settings.silver}`.quarantine WHERE source_table = %s GROUP BY reason", (table,)
    )
    return {reason: int(n) for reason, n in rows}


def test_run_succeeded_and_is_recorded(e2e):
    assert e2e.result.status == "SUCCESS"
    status = scalar(e2e.conn, f"SELECT status FROM `{e2e.settings.dq}`.pipeline_runs WHERE run_id = %s", (e2e.result.run_id,))
    assert status == "SUCCESS"


@pytest.mark.parametrize("table", TABLES)
def test_bronze_keeps_every_source_row(e2e, table):
    assert _count(e2e.conn, e2e.settings.bronze, RAW[table]) == e2e.manifest["tables"][table]["rows_written"]


@pytest.mark.parametrize("table", TABLES)
def test_silver_row_count_matches_ground_truth(e2e, table):
    assert _count(e2e.conn, e2e.settings.silver, table) == e2e.manifest["tables"][table]["expected_valid_rows"]


@pytest.mark.parametrize("table", TABLES)
def test_quarantine_contains_exactly_the_injected_defects(e2e, table):
    assert _quarantine(e2e.conn, e2e.settings, table) == e2e.manifest["tables"][table]["defects"]


@pytest.mark.parametrize("table", TABLES)
def test_nothing_is_silently_lost(e2e, table):
    """bronze = silver + quarantine + duplicates removed."""
    s = e2e.settings
    bronze = _count(e2e.conn, s.bronze, RAW[table])
    silver = _count(e2e.conn, s.silver, table)
    quarantined = sum(_quarantine(e2e.conn, s, table).values())
    assert bronze - silver - quarantined == e2e.manifest["tables"][table]["duplicates"]


def test_quarantine_keeps_the_raw_record_for_diagnosis(e2e):
    raw = scalar(
        e2e.conn,
        f"SELECT raw_record FROM `{e2e.settings.silver}`.quarantine WHERE source_table = 'bookings' AND reason = 'INVALID_RATE' LIMIT 1",
    )
    assert raw and '"nightly_rate"' in raw


def test_recoverable_dirt_is_standardized_not_rejected(e2e):
    s, conn = e2e.settings, e2e.conn
    assert e2e.manifest["tables"]["guests"]["recoverable_dirty_rows"] > 0
    assert scalar(conn, f"SELECT COUNT(*) FROM `{s.silver}`.guests WHERE full_name <> TRIM(full_name) OR email <> LOWER(email) OR country <> UPPER(country)") == 0
    statuses = {r[0] for r in fetch_all(conn, f"SELECT DISTINCT status FROM `{s.silver}`.bookings")}
    currencies = {r[0] for r in fetch_all(conn, f"SELECT DISTINCT currency FROM `{s.silver}`.bookings")}
    assert statuses <= {"confirmed", "completed", "cancelled", "no_show"}
    assert currencies <= {"USD", "ARS", "EUR"}


def test_every_check_is_persisted_and_none_blocking_failed(e2e):
    s = e2e.settings
    rows = fetch_all(e2e.conn, f"SELECT layer, severity, status FROM `{s.dq}`.check_results WHERE run_id = %s", (e2e.result.run_id,))
    assert len(rows) == len(e2e.result.checks) > 25
    assert {layer for layer, _, _ in rows} == {"bronze", "silver", "gold"}
    assert not [r for r in rows if r[1] == "ERROR" and r[2] == "FAIL"]


def test_gold_reconciles_with_silver(e2e):
    s, conn = e2e.settings, e2e.conn
    assert _count(conn, s.gold, "fact_bookings") == _count(conn, s.silver, "bookings")
    fact_revenue = scalar(conn, f"SELECT SUM(net_value) FROM `{s.gold}`.fact_bookings")
    silver_revenue = scalar(
        conn,
        f"SELECT SUM(DATEDIFF(check_out, check_in) * nightly_rate) FROM `{s.silver}`.bookings WHERE status IN ('confirmed','completed')",
    )
    assert fact_revenue == silver_revenue
    kpi_revenue = scalar(conn, f"SELECT SUM(revenue) FROM `{s.gold}`.agg_monthly_property_kpis")
    assert kpi_revenue == fact_revenue


def test_revenue_is_never_mixed_across_currencies(e2e):
    s = e2e.settings
    rows = fetch_all(e2e.conn, f"SELECT DISTINCT currency FROM `{s.gold}`.agg_monthly_property_kpis")
    assert {r[0] for r in rows} <= {"USD", "ARS", "EUR"} and len(rows) > 1
    per_property = fetch_all(
        e2e.conn, f"SELECT property_key, COUNT(DISTINCT currency) FROM `{s.gold}`.fact_bookings GROUP BY property_key"
    )
    assert all(n == 1 for _, n in per_property)


def test_analytics_queries_run_and_return_rows(e2e):
    sql = (SQL_DIR / "analytics_queries.sql").read_text(encoding="utf-8").replace("hotel_", e2e.settings.prefix)
    statements = split_statements(sql)
    assert len(statements) == 7
    for stmt in statements:
        assert fetch_all(e2e.conn, stmt), f"query returned no rows:\n{stmt[:120]}"


def test_rerun_is_idempotent(e2e):
    """Running the pipeline again on the same files must not change any layer."""
    s, conn = e2e.settings, e2e.conn

    def snapshot():
        return {
            "silver": {t: _count(conn, s.silver, t) for t in (*TABLES, "quarantine")},
            "gold": {t: _count(conn, s.gold, t) for t in ("dim_property", "dim_guest", "dim_date", "fact_bookings", "agg_monthly_property_kpis")},
            "bronze": {t: _count(conn, s.bronze, RAW[t]) for t in TABLES},
        }

    before = snapshot()
    again = run_pipeline(s, e2e.data_dir, generate_data=False)
    assert again.status == "SUCCESS"
    assert snapshot() == before
