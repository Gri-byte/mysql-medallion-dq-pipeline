"""Pipeline orchestrator.

    generate (optional) -> ingest bronze -> [bronze checks] -> silver -> [silver checks] -> gold -> [gold checks]

If a quality gate fails, the run stops: the next layer is NOT built, so gold keeps serving the last
good data instead of a corrupted refresh. Every run and every check result is recorded in <prefix>dq.

Usage:
    python -m src.run_pipeline --reset
    python -m src.run_pipeline --bookings 20000 --defect-rate 0.08
    python -m src.run_pipeline --no-generate --data-dir path/to/your/csvs
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from . import build_gold, ingest_bronze, transform_silver
from .checks import bronze_checks, gold_checks, silver_checks
from .config import DEFAULT_DATA_DIR, SQL_DIR, Settings
from .db import connect, run_sql_file
from .dq import CheckResult, DataQualityError, run_checks
from .generate_data import generate

log = logging.getLogger("pipeline")

DDL_FILES = ("00_schemas.sql", "01_bronze_ddl.sql", "02_silver_ddl.sql", "03_gold_ddl.sql", "04_dq_ddl.sql")


@dataclass
class PipelineResult:
    run_id: str
    status: str = "RUNNING"
    bronze_rows: dict = field(default_factory=dict)
    silver: dict = field(default_factory=dict)
    gold_rows: dict = field(default_factory=dict)
    checks: list[CheckResult] = field(default_factory=list)
    manifest: dict | None = None


def drop_databases(settings: Settings) -> None:
    conn = connect(settings)
    try:
        with conn.cursor() as cur:
            for db in (settings.bronze, settings.silver, settings.gold, settings.dq):
                cur.execute(f"DROP DATABASE IF EXISTS `{db}`")  # nosec B608 - validated identifier
    finally:
        conn.close()


def init_schemas(settings: Settings, reset: bool = False) -> None:
    if reset:
        drop_databases(settings)
    conn = connect(settings)
    try:
        for name in DDL_FILES:
            run_sql_file(conn, SQL_DIR / name, settings)
    finally:
        conn.close()


def _start_run(conn, settings: Settings, run_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO `{settings.dq}`.pipeline_runs (run_id, started_at, status) VALUES (%s, %s, 'RUNNING')",  # nosec B608
            (run_id, datetime.now().replace(microsecond=0)),
        )


def _finish_run(conn, settings: Settings, run_id: str, status: str, stage: str | None, message: str | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE `{settings.dq}`.pipeline_runs SET finished_at = %s, status = %s, failed_stage = %s, message = %s "  # nosec B608
            "WHERE run_id = %s",
            (datetime.now().replace(microsecond=0), status, stage, (message or "")[:500] or None, run_id),
        )


def run_pipeline(
    settings: Settings,
    data_dir: str | Path = DEFAULT_DATA_DIR,
    generate_data: bool = True,
    n_bookings: int = 8000,
    n_guests: int = 2000,
    defect_rate: float = 0.04,
    seed: int = 42,
    reset: bool = False,
) -> PipelineResult:
    """Run the whole pipeline. Raises DataQualityError / SchemaContractError when a gate fails."""
    data_dir = Path(data_dir)
    init_schemas(settings, reset=reset)
    run_id = datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    result = PipelineResult(run_id=run_id)

    conn = connect(settings)
    stage = "generate"
    try:
        _start_run(conn, settings, run_id)
        if generate_data:
            result.manifest = generate(data_dir, n_bookings, n_guests, defect_rate, seed)
            log.info("generated synthetic data in %s", data_dir)

        stage = "bronze"
        result.bronze_rows = ingest_bronze.run(conn, settings, data_dir, run_id)
        log.info("bronze loaded: %s", result.bronze_rows)
        result.checks += run_checks(conn, settings, run_id, bronze_checks(settings))

        stage = "silver"
        result.silver = transform_silver.run(conn, settings)
        log.info("silver built: %s", result.silver["counts"])
        result.checks += run_checks(conn, settings, run_id, silver_checks(settings))

        stage = "gold"
        result.gold_rows = build_gold.run(conn, settings)
        log.info("gold built: %s", result.gold_rows)
        result.checks += run_checks(conn, settings, run_id, gold_checks(settings))

        result.status = "SUCCESS"
        _finish_run(conn, settings, run_id, "SUCCESS", None, None)
        return result
    except Exception as exc:
        result.status = "FAILED"
        _finish_run(conn, settings, run_id, "FAILED", stage, str(exc))
        raise
    finally:
        conn.close()


def print_summary(result: PipelineResult) -> None:
    print(f"\nRun {result.run_id}: {result.status}")
    print(f"  bronze rows : {result.bronze_rows}")
    print(f"  silver rows : {result.silver.get('counts')}")
    print(f"  gold rows   : {result.gold_rows}")
    for table, reasons in result.silver.get("quarantine", {}).items():
        print(f"  quarantine {table:<10}: {reasons}")
    passed = sum(1 for r in result.checks if r.status == "PASS")
    print(f"  checks      : {passed}/{len(result.checks)} passed")
    for r in result.checks:
        if r.status == "FAIL":
            print(f"    {r.check.severity} {r.check.layer}.{r.check.name}: failed={r.failed_rows} total={r.total_rows}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="MySQL Medallion data-quality pipeline")
    ap.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    ap.add_argument("--no-generate", action="store_true", help="use the CSVs already in --data-dir")
    ap.add_argument("--bookings", type=int, default=8000)
    ap.add_argument("--guests", type=int, default=2000)
    ap.add_argument("--defect-rate", type=float, default=0.04)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--reset", action="store_true", help="drop and recreate all databases first")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(levelname)s %(message)s")
    settings = Settings.from_env()
    try:
        result = run_pipeline(
            settings, args.data_dir, generate_data=not args.no_generate, n_bookings=args.bookings,
            n_guests=args.guests, defect_rate=args.defect_rate, seed=args.seed, reset=args.reset,
        )
    except DataQualityError as exc:
        print(f"\nPIPELINE STOPPED BY QUALITY GATE\n  {exc}", file=sys.stderr)
        return 2
    except ingest_bronze.SchemaContractError as exc:
        print(f"\nPIPELINE STOPPED: SOURCE CONTRACT VIOLATED\n  {exc}", file=sys.stderr)
        return 3
    print_summary(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
