"""Negative tests: simulate failures and prove the pipeline stops, says why, and limits the damage.

This is the QA mindset applied to data: a pipeline that only works on good data is not reliable.
"""
from __future__ import annotations

import csv
import dataclasses

import pymysql
import pytest

from src.db import connect, fetch_all, scalar
from src.dq import Check, DataQualityError, evaluate, run_checks
from src.generate_data import generate
from src.ingest_bronze import SchemaContractError
from src.run_pipeline import drop_databases, init_schemas, run_pipeline


@pytest.fixture
def neg(make_settings):
    settings = make_settings("neg")
    yield settings
    drop_databases(settings)


def _count(settings, db, table):
    conn = connect(settings)
    try:
        return int(scalar(conn, f"SELECT COUNT(*) FROM `{db}`.{table}"))
    finally:
        conn.close()


# ----------------------------------------------------------------------------- quality gates
def test_bronze_gate_stops_extremely_dirty_input_first(neg, tmp_path):
    """With default thresholds, a 50% defect rate is caught at bronze (null business keys)."""
    with pytest.raises(DataQualityError) as exc:
        run_pipeline(neg, tmp_path, n_bookings=1000, n_guests=300, defect_rate=0.5, seed=1, reset=True)
    assert exc.value.layer == "bronze"
    assert "bookings_null_key_rate" in {f.check.name for f in exc.value.failures}
    assert _count(neg, neg.silver, "bookings") == 0, "silver must not be built after a bronze failure"


def test_excessive_defect_rate_trips_the_silver_quarantine_gate(neg, tmp_path):
    # Tolerate null keys at bronze so the batch reaches the silver volume-anomaly gate.
    lenient_bronze = dataclasses.replace(neg, max_null_key_rate=1.0)
    with pytest.raises(DataQualityError) as exc:
        run_pipeline(lenient_bronze, tmp_path, n_bookings=1000, n_guests=300, defect_rate=0.5, seed=1, reset=True)
    assert exc.value.layer == "silver"
    assert any(f.check.name.endswith("_quarantine_rate") for f in exc.value.failures)

    conn = connect(neg)
    try:
        status, stage = fetch_all(conn, f"SELECT status, failed_stage FROM `{neg.dq}`.pipeline_runs")[0]
        assert (status, stage) == ("FAILED", "silver")
        failed = fetch_all(conn, f"SELECT check_name FROM `{neg.dq}`.check_results WHERE status = 'FAIL' AND severity = 'ERROR'")
        assert failed, "the failed check must be persisted for later analysis"
    finally:
        conn.close()
    assert _count(neg, neg.gold, "fact_bookings") == 0, "gold must not be built from a batch that failed the gate"


def test_failed_batch_does_not_overwrite_last_good_gold(neg, tmp_path):
    good = run_pipeline(neg, tmp_path / "good", n_bookings=800, n_guests=250, defect_rate=0.04, seed=2, reset=True)
    assert good.status == "SUCCESS"
    facts_before = _count(neg, neg.gold, "fact_bookings")
    kpis_before = _count(neg, neg.gold, "agg_monthly_property_kpis")
    assert facts_before > 0

    lenient_bronze = dataclasses.replace(neg, max_null_key_rate=1.0)
    with pytest.raises(DataQualityError) as exc:
        run_pipeline(lenient_bronze, tmp_path / "bad", n_bookings=800, n_guests=250, defect_rate=0.5, seed=3)
    assert exc.value.layer == "silver"

    # The bad batch stays in bronze / silver / quarantine for diagnosis, but gold is untouched.
    assert _count(neg, neg.gold, "fact_bookings") == facts_before
    assert _count(neg, neg.gold, "agg_monthly_property_kpis") == kpis_before
    conn = connect(neg)
    try:
        statuses = [r[0] for r in fetch_all(conn, f"SELECT status FROM `{neg.dq}`.pipeline_runs")]
    finally:
        conn.close()
    assert sorted(statuses) == ["FAILED", "SUCCESS"]


# ----------------------------------------------------------------------------- source contract
def test_schema_drift_is_refused_before_bronze_is_touched(neg, tmp_path):
    generate(tmp_path, n_bookings=300, n_guests=100, seed=4)
    bookings = tmp_path / "bookings.csv"
    with bookings.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    drop = rows[0].index("currency")
    with bookings.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows([[v for i, v in enumerate(r) if i != drop] for r in rows])

    with pytest.raises(SchemaContractError, match="currency"):
        run_pipeline(neg, tmp_path, generate_data=False, reset=True)
    # Contracts are validated for ALL files first: not even the valid files were loaded.
    assert _count(neg, neg.bronze, "raw_properties") == 0
    assert _count(neg, neg.bronze, "raw_bookings") == 0


def test_unexpected_extra_column_is_also_drift(neg, tmp_path):
    generate(tmp_path, n_bookings=200, n_guests=80, seed=5)
    guests = tmp_path / "guests.csv"
    lines = guests.read_text(encoding="utf-8").splitlines()
    guests.write_text("\n".join([lines[0] + ",loyalty_tier"] + [ln + ",gold" for ln in lines[1:]]) + "\n", encoding="utf-8")
    with pytest.raises(SchemaContractError, match="loyalty_tier"):
        run_pipeline(neg, tmp_path, generate_data=False, reset=True)


def test_empty_source_file_blocks_at_bronze(neg, tmp_path):
    generate(tmp_path, n_bookings=200, n_guests=80, seed=6)
    header = (tmp_path / "guests.csv").read_text(encoding="utf-8").splitlines()[0]
    (tmp_path / "guests.csv").write_text(header + "\n", encoding="utf-8")  # header only, zero rows
    with pytest.raises(DataQualityError) as exc:
        run_pipeline(neg, tmp_path, generate_data=False, reset=True)
    assert exc.value.layer == "bronze"
    assert "guests_not_empty" in {f.check.name for f in exc.value.failures}
    assert _count(neg, neg.silver, "bookings") == 0, "silver must not be built after a bronze failure"


# ----------------------------------------------------------------------------- engine-level guards
@pytest.fixture
def built(make_settings, tmp_path_factory):
    settings = make_settings("guard")
    run_pipeline(settings, tmp_path_factory.mktemp("raw"), n_bookings=300, n_guests=100, seed=9, reset=True)
    yield settings
    drop_databases(settings)


@pytest.mark.parametrize(
    "sql",
    [
        # checkout before checkin
        "INSERT INTO {s}.bookings VALUES ('BX1','P001','G00001','standard','2025-05-10','2025-05-09',100,'USD','confirmed','2025-01-01 10:00:00')",
        # zero rate
        "INSERT INTO {s}.bookings VALUES ('BX2','P001','G00001','standard','2025-05-10','2025-05-12',0,'USD','confirmed','2025-01-01 10:00:00')",
        # unknown currency
        "INSERT INTO {s}.bookings VALUES ('BX3','P001','G00001','standard','2025-05-10','2025-05-12',50,'XXX','confirmed','2025-01-01 10:00:00')",
        # zero date
        "INSERT INTO {s}.bookings VALUES ('BX4','P001','G00001','standard','0000-00-00','2025-05-12',50,'USD','confirmed','2025-01-01 10:00:00')",
        # orphan property (foreign key)
        "INSERT INTO {s}.bookings VALUES ('BX5','P999','G00001','standard','2025-05-10','2025-05-12',50,'USD','confirmed','2025-01-01 10:00:00')",
        # orphan booking in payments (foreign key)
        "INSERT INTO {s}.payments VALUES ('PX1','B999999',10,'USD','card','2025-01-01 10:00:00')",
        # negative payment
        "INSERT INTO {s}.payments SELECT 'PX2', booking_id, -5, currency, 'card', '2025-01-01 10:00:00' FROM {s}.bookings LIMIT 1",
    ],
)
def test_silver_constraints_reject_bad_rows_even_if_the_pipeline_had_a_bug(built, sql):
    conn = connect(built)
    try:
        with pytest.raises(pymysql.MySQLError), conn.cursor() as cur:
            cur.execute(sql.format(s=f"`{built.silver}`"))
    finally:
        conn.close()


def test_duplicate_primary_key_is_rejected_by_the_engine(built):
    conn = connect(built)
    try:
        existing = scalar(conn, f"SELECT booking_id FROM `{built.silver}`.bookings LIMIT 1")
        with pytest.raises(pymysql.MySQLError), conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO `{built.silver}`.bookings SELECT * FROM `{built.silver}`.bookings WHERE booking_id = %s", (existing,)
            )
    finally:
        conn.close()


# ----------------------------------------------------------------------------- the DQ engine itself
def test_check_ratio_tolerance(base_settings):
    conn = connect(base_settings)
    try:
        tolerant = Check("silver", "tolerant", "SELECT 5", "SELECT 100", max_ratio=0.05)
        strict = Check("silver", "strict", "SELECT 5", "SELECT 100", max_ratio=0.04)
        clean = Check("silver", "clean", "SELECT 0")
        assert evaluate(conn, tolerant).status == "PASS"
        assert evaluate(conn, strict).status == "FAIL"
        assert evaluate(conn, clean).status == "PASS"
    finally:
        conn.close()


def test_warn_failures_do_not_block_but_error_failures_do(neg):
    init_schemas(neg, reset=True)
    conn = connect(neg)
    try:
        warn = Check("silver", "warn_check", "SELECT 3", severity="WARN")
        results = run_checks(conn, neg, "run-warn", [warn])
        assert results[0].status == "FAIL"  # recorded ...

        error = Check("silver", "error_check", "SELECT 3", severity="ERROR")
        with pytest.raises(DataQualityError):  # ... but only ERROR blocks
            run_checks(conn, neg, "run-error", [error])

        persisted = fetch_all(conn, f"SELECT check_name, status FROM `{neg.dq}`.check_results ORDER BY result_id")
        assert persisted == [("warn_check", "FAIL"), ("error_check", "FAIL")]
    finally:
        conn.close()
