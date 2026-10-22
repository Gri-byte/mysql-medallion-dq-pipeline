"""SILVER -> GOLD: runs sql/20_gold_load.sql (star schema + KPI aggregate)."""
from __future__ import annotations

from .config import SQL_DIR, Settings
from .db import run_sql_file, scalar


def run(conn, settings: Settings) -> dict[str, int]:
    run_sql_file(conn, SQL_DIR / "20_gold_load.sql", settings)
    return {
        t: int(scalar(conn, f"SELECT COUNT(*) FROM `{settings.gold}`.{t}"))  # nosec B608 - validated identifiers
        for t in ("dim_property", "dim_guest", "dim_date", "fact_bookings", "agg_monthly_property_kpis")
    }
