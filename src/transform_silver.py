"""BRONZE -> SILVER: runs sql/10_silver_load.sql and reports what landed where."""
from __future__ import annotations

from .config import SQL_DIR, Settings
from .db import fetch_all, run_sql_file, scalar


def run(conn, settings: Settings) -> dict:
    # Validation is explicit in SQL (regex / range / referential checks), so malformed values are
    # classified and quarantined instead of aborting the whole batch. STRICT mode is relaxed for
    # this script only; the silver tables keep their PK / FK / CHECK constraints regardless.
    previous_mode = scalar(conn, "SELECT @@SESSION.sql_mode")
    relaxed = ",".join(m for m in previous_mode.split(",") if m not in ("STRICT_TRANS_TABLES", "STRICT_ALL_TABLES"))
    with conn.cursor() as cur:
        cur.execute("SET SESSION sql_mode = %s", (relaxed,))
    conn.select_db(settings.silver)  # temporary tables in the script need a default database
    try:
        run_sql_file(conn, SQL_DIR / "10_silver_load.sql", settings)
    finally:
        with conn.cursor() as cur:
            cur.execute("SET SESSION sql_mode = %s", (previous_mode,))
    return summarize(conn, settings)


def summarize(conn, settings: Settings) -> dict:
    counts = {t: scalar(conn, f"SELECT COUNT(*) FROM `{settings.silver}`.{t}")  # nosec B608
              for t in ("properties", "guests", "bookings", "payments", "quarantine")}
    reasons = fetch_all(
        conn,
        f"SELECT source_table, reason, COUNT(*) FROM `{settings.silver}`.quarantine "  # nosec B608
        "GROUP BY source_table, reason ORDER BY source_table, reason",
    )
    quarantine: dict[str, dict[str, int]] = {}
    for table, reason, n in reasons:
        quarantine.setdefault(table, {})[reason] = int(n)
    return {"counts": {k: int(v) for k, v in counts.items()}, "quarantine": quarantine}
