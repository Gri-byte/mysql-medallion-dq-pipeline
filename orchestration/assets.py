"""Dagster assets: one per layer, with the layer's data-quality checks attached as BLOCKING asset checks.

    source_csv_files -> bronze_raw -> silver_clean -> gold_star_schema

How a quality failure behaves (same contract as the CLI):
  * a failed ERROR check marks the check red, lets the layer's data stay in MySQL for diagnosis,
    but Dagster does NOT run the next asset and the run fails -> gold keeps the last good data
  * a failed WARN check is recorded and shown, but blocks nothing
Every check result is also persisted in <prefix>dq.check_results and the run in <prefix>dq.pipeline_runs,
keyed by the Dagster run id, so the SQL views keep working exactly as they do for the CLI.
"""
import os
from contextlib import contextmanager
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)

from src import build_gold, ingest_bronze, transform_silver
from src.checks import bronze_checks, gold_checks, silver_checks
from src.config import DEFAULT_DATA_DIR, Settings
from src.db import connect
from src.dq import Check, CheckResult, blocking_failures, evaluate_and_record
from src.generate_data import generate
from src.run_pipeline import finish_run, init_schemas, start_run

from .resources import MySQLResource

# The pipeline layers in execution order (the source asset is not a layer).
LAYER_KEYS = [AssetKey("bronze_raw"), AssetKey("silver_clean"), AssetKey("gold_star_schema")]


def data_dir() -> Path:
    """Where the source CSVs live. Override with DQ_DATA_DIR (e.g. a mounted volume)."""
    return Path(os.getenv("DQ_DATA_DIR", str(DEFAULT_DATA_DIR)))


def _specs(asset_name: str, checks: list[Check]) -> list[AssetCheckSpec]:
    # Names/descriptions do not depend on the database prefix, so a default Settings is enough here.
    return [AssetCheckSpec(name=c.name, asset=asset_name, blocking=True, description=c.description) for c in checks]


BRONZE_SPECS = _specs("bronze_raw", bronze_checks(Settings()))
SILVER_SPECS = _specs("silver_clean", silver_checks(Settings()))
GOLD_SPECS = _specs("gold_star_schema", gold_checks(Settings()))


def _asset_check_results(results: list[CheckResult]) -> list[AssetCheckResult]:
    out = []
    for r in results:
        metadata: dict = {"failed_rows": r.failed_rows, "tolerated_ratio": r.check.max_ratio}
        if r.total_rows is not None:
            metadata["total_rows"] = r.total_rows
        out.append(
            AssetCheckResult(
                check_name=r.check.name,
                passed=r.status == "PASS",
                severity=AssetCheckSeverity.ERROR if r.check.severity == "ERROR" else AssetCheckSeverity.WARN,
                description=r.check.description,
                metadata=metadata,
            )
        )
    return out


def _is_last_selected_layer(context: AssetExecutionContext, key: AssetKey) -> bool:
    """True when `key` is the final layer of THIS run (a run can select only some assets)."""
    # The (possibly subsetted) job of this run knows exactly which assets it executes. Do not use
    # context.run.asset_selection: it is an empty set for in-process runs.
    executable = context.job_def.asset_layer.executable_asset_keys
    layers = [k for k in LAYER_KEYS if k in executable]
    return bool(layers) and layers[-1] == key


@contextmanager
def _stage(context: AssetExecutionContext, db: MySQLResource, stage: str):
    """Open a connection, register the run, and record FAILED if the stage raises."""
    settings = db.settings()
    init_schemas(settings)  # idempotent: CREATE ... IF NOT EXISTS
    conn = connect(settings)
    run_id = context.run.run_id
    try:
        start_run(conn, settings, run_id)
        yield settings, conn, run_id
    except Exception as exc:
        finish_run(conn, settings, run_id, "FAILED", stage, str(exc))
        raise
    finally:
        conn.close()


def _conclude(conn, settings: Settings, run_id: str, stage: str, results: list[CheckResult], last: bool) -> None:
    """Close the run bookkeeping when this layer blocks the pipeline or is the last one to run."""
    blocking = blocking_failures(results)
    if blocking:
        names = ", ".join(r.check.name for r in blocking)
        finish_run(conn, settings, run_id, "FAILED", stage, f"Blocking checks failed: {names}")
    elif last:
        finish_run(conn, settings, run_id, "SUCCESS", None, None)


class SourceConfig(Config):
    """Synthetic-source knobs, editable in the Launchpad. Try defect_rate=0.30 to watch a gate stop the run."""

    n_bookings: int = 8000
    n_guests: int = 2000
    defect_rate: float = 0.04
    seed: int = 42


@asset(group_name="source", kinds={"python", "csv"})
def source_csv_files(config: SourceConfig) -> MaterializeResult:
    """Synthetic hotel CSVs with KNOWN injected defects (ground truth for the quality checks).

    In a real deployment this asset would be replaced by an external source (files landing from an
    upstream system) observed with a Dagster sensor; everything downstream stays the same.
    """
    manifest = generate(data_dir(), config.n_bookings, config.n_guests, config.defect_rate, config.seed)
    injected = sum(sum(t["defects"].values()) + t["duplicates"] for t in manifest["tables"].values())
    return MaterializeResult(
        metadata={
            "data_dir": MetadataValue.path(str(data_dir())),
            "injected_defects_and_duplicates": injected,
            "defect_rate": config.defect_rate,
            "manifest": MetadataValue.json(manifest),
        }
    )


@asset(group_name="bronze", deps=[source_csv_files], check_specs=BRONZE_SPECS, kinds={"mysql"})
def bronze_raw(context: AssetExecutionContext, db: MySQLResource) -> MaterializeResult:
    """BRONZE: raw text exactly as received + lineage columns. Refuses files that break the schema contract."""
    with _stage(context, db, "bronze") as (settings, conn, run_id):
        rows = ingest_bronze.run(conn, settings, data_dir(), run_id)
        results = evaluate_and_record(conn, settings, run_id, bronze_checks(settings))
        _conclude(conn, settings, run_id, "bronze", results, _is_last_selected_layer(context, AssetKey("bronze_raw")))
    return MaterializeResult(
        metadata={f"rows_{name}": n for name, n in rows.items()} | {"total_rows": sum(rows.values())},
        check_results=_asset_check_results(results),
    )


@asset(group_name="silver", deps=[bronze_raw], check_specs=SILVER_SPECS, kinds={"mysql"})
def silver_clean(context: AssetExecutionContext, db: MySQLResource) -> MaterializeResult:
    """SILVER: typed, validated, de-duplicated. Rejected rows land in the quarantine table with a reason."""
    with _stage(context, db, "silver") as (settings, conn, run_id):
        summary = transform_silver.run(conn, settings)
        results = evaluate_and_record(conn, settings, run_id, silver_checks(settings))
        _conclude(conn, settings, run_id, "silver", results, _is_last_selected_layer(context, AssetKey("silver_clean")))
    counts = summary["counts"]
    return MaterializeResult(
        metadata={f"rows_{name}": n for name, n in counts.items() if name != "quarantine"}
        | {"quarantined_rows": counts["quarantine"], "quarantine_by_reason": MetadataValue.json(summary["quarantine"])},
        check_results=_asset_check_results(results),
    )


@asset(group_name="gold", deps=[silver_clean], check_specs=GOLD_SPECS, kinds={"mysql"})
def gold_star_schema(context: AssetExecutionContext, db: MySQLResource) -> MaterializeResult:
    """GOLD: star schema (dim_property, dim_guest, dim_date, fact_bookings) + monthly KPI table."""
    with _stage(context, db, "gold") as (settings, conn, run_id):
        rows = build_gold.run(conn, settings)
        results = evaluate_and_record(conn, settings, run_id, gold_checks(settings))
        _conclude(conn, settings, run_id, "gold", results, _is_last_selected_layer(context, AssetKey("gold_star_schema")))
    return MaterializeResult(
        metadata={f"rows_{name}": n for name, n in rows.items()},
        check_results=_asset_check_results(results),
    )


ALL_ASSETS = [source_csv_files, bronze_raw, silver_clean, gold_star_schema]
