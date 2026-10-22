"""A tiny data-quality engine: declarative checks, persisted results, blocking gates.

A Check is a SQL query that returns the NUMBER OF FAILING ROWS (0 = healthy), optionally a second
query returning the population size so a tolerance ratio can be applied.

  severity ERROR -> a failure blocks the pipeline (raises DataQualityError)
  severity WARN  -> a failure is recorded and reported, but the pipeline continues

Every result is written to <prefix>dq.check_results so quality can be tracked over time.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from .config import Settings
from .db import scalar

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Check:
    layer: str
    name: str
    failed_sql: str                 # returns a single number: failing rows
    total_sql: str | None = None    # returns a single number: population (enables max_ratio)
    severity: str = "ERROR"
    max_ratio: float = 0.0          # tolerated failed/total ratio (0 = zero tolerance)
    description: str = ""


@dataclass
class CheckResult:
    check: Check
    failed_rows: int
    total_rows: int | None
    status: str                     # PASS / FAIL


class DataQualityError(Exception):
    """One or more ERROR-severity checks failed; the pipeline stops before the next layer."""

    def __init__(self, layer: str, failures: list[CheckResult]):
        self.layer = layer
        self.failures = failures
        detail = "; ".join(f"{r.check.name} (failed={r.failed_rows}, total={r.total_rows})" for r in failures)
        super().__init__(f"Data-quality gate failed at {layer}: {detail}")


def evaluate(conn, check: Check) -> CheckResult:
    failed = int(scalar(conn, check.failed_sql) or 0)
    total = int(scalar(conn, check.total_sql) or 0) if check.total_sql else None
    within_tolerance = bool(total) and failed / total <= check.max_ratio
    status = "PASS" if failed == 0 or within_tolerance else "FAIL"
    return CheckResult(check, failed, total, status)


def _persist(conn, settings: Settings, run_id: str, result: CheckResult) -> None:
    c = result.check
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO `{settings.dq}`.check_results "  # nosec B608 - validated identifier
            "(run_id, layer, check_name, severity, status, failed_rows, total_rows, max_ratio, description) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (run_id, c.layer, c.name, c.severity, result.status, result.failed_rows,
             result.total_rows, c.max_ratio, c.description[:300]),
        )


def blocking_failures(results: list[CheckResult]) -> list[CheckResult]:
    """Failed checks that must stop the pipeline (severity ERROR)."""
    return [r for r in results if r.status == "FAIL" and r.check.severity == "ERROR"]


def evaluate_and_record(conn, settings: Settings, run_id: str, checks: list[Check]) -> list[CheckResult]:
    """Run all checks of one layer and persist every result. Never raises on a failed check.

    Used directly by orchestrators (Dagster) that decide for themselves how a failure blocks
    downstream work; `run_checks` below adds the plain exception-based gate for the CLI.
    """
    results = []
    for check in checks:
        result = evaluate(conn, check)
        _persist(conn, settings, run_id, result)
        results.append(result)
        level = logging.INFO if result.status == "PASS" else (logging.ERROR if check.severity == "ERROR" else logging.WARNING)
        log.log(level, "[%s] %-48s %s (failed=%s total=%s)", check.layer, check.name, result.status,
                result.failed_rows, result.total_rows)
    return results


def run_checks(conn, settings: Settings, run_id: str, checks: list[Check]) -> list[CheckResult]:
    """Run all checks of one layer, persist every result, then enforce the gate."""
    results = evaluate_and_record(conn, settings, run_id, checks)
    blocking = blocking_failures(results)
    if blocking:
        raise DataQualityError(checks[0].layer if checks else "unknown", blocking)
    return results
