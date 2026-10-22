"""Declarative data-quality checks for each Medallion layer.

All SQL is built from validated database names (see Settings), never from user input.
"""
from __future__ import annotations

from .config import Settings
from .dq import Check

# table -> business key column in bronze
KEYS = {"properties": "property_id", "guests": "guest_id", "bookings": "booking_id", "payments": "payment_id"}
RAW = {"properties": "raw_properties", "guests": "raw_guests", "bookings": "raw_bookings", "payments": "raw_payments"}


def _reconciliation_sql(s: Settings, table: str) -> str:
    """bronze rows = silver rows + quarantined rows + duplicates removed. Returns the absolute gap."""
    key, raw = KEYS[table], RAW[table]
    b = f"`{s.bronze}`.{raw}"
    return (  # nosec B608
        "SELECT ABS("
        f"(SELECT COUNT(*) FROM {b})"
        f" - (SELECT COUNT(*) FROM `{s.silver}`.{table})"
        f" - (SELECT COUNT(*) FROM `{s.silver}`.quarantine WHERE source_table = '{table}')"
        f" - ((SELECT COUNT(*) FROM {b})"
        f"     - (SELECT COUNT(DISTINCT TRIM({key})) FROM {b} WHERE {key} IS NOT NULL AND TRIM({key}) <> '')"
        f"     - (SELECT COUNT(*) FROM {b} WHERE {key} IS NULL OR TRIM({key}) = '')))"
    )


def bronze_checks(s: Settings) -> list[Check]:
    checks: list[Check] = []
    for table, raw in RAW.items():
        b = f"`{s.bronze}`.{raw}"
        checks.append(Check(
            "bronze", f"{table}_not_empty",
            f"SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM {b}",  # nosec B608
            description=f"{raw} must contain rows after ingestion",
        ))
        checks.append(Check(
            "bronze", f"{table}_fresh",
            f"SELECT CASE WHEN MAX(_ingested_at) IS NULL OR MAX(_ingested_at) < NOW() - INTERVAL {int(s.freshness_hours)} HOUR "  # nosec B608
            f"THEN 1 ELSE 0 END FROM {b}",
            description=f"newest row ingested within {s.freshness_hours}h",
        ))
        key = KEYS[table]
        checks.append(Check(
            "bronze", f"{table}_null_key_rate",
            f"SELECT COUNT(*) FROM {b} WHERE {key} IS NULL OR TRIM({key}) = ''",  # nosec B608
            f"SELECT COUNT(*) FROM {b}",  # nosec B608
            max_ratio=s.max_null_key_rate,
            description=f"share of rows without a {key} stays under {s.max_null_key_rate:.0%}",
        ))
    return checks


def silver_checks(s: Settings) -> list[Check]:
    sv, br = f"`{s.silver}`", f"`{s.bronze}`"
    checks: list[Check] = []
    for table in KEYS:
        raw = RAW[table]
        checks.append(Check(
            "silver", f"{table}_reconciliation", _reconciliation_sql(s, table),
            description="bronze = silver + quarantine + duplicates (no row silently lost)",
        ))
        checks.append(Check(
            "silver", f"{table}_quarantine_rate",
            f"SELECT COUNT(*) FROM {sv}.quarantine WHERE source_table = '{table}'",  # nosec B608
            f"SELECT COUNT(*) FROM {br}.{raw}",  # nosec B608
            max_ratio=s.max_quarantine_rate,
            description=f"quarantined share stays under {s.max_quarantine_rate:.0%} (volume-anomaly gate)",
        ))
    checks += [
        Check("silver", "bookings_no_orphan_property",
              f"SELECT COUNT(*) FROM {sv}.bookings b LEFT JOIN {sv}.properties p ON p.property_id = b.property_id "  # nosec B608
              "WHERE p.property_id IS NULL",
              description="every booking points to an existing property"),
        Check("silver", "bookings_no_orphan_guest",
              f"SELECT COUNT(*) FROM {sv}.bookings b LEFT JOIN {sv}.guests g ON g.guest_id = b.guest_id "  # nosec B608
              "WHERE g.guest_id IS NULL",
              description="every booking points to an existing guest"),
        Check("silver", "payments_no_orphan_booking",
              f"SELECT COUNT(*) FROM {sv}.payments p LEFT JOIN {sv}.bookings b ON b.booking_id = p.booking_id "  # nosec B608
              "WHERE b.booking_id IS NULL",
              description="every payment points to an existing booking"),
        Check("silver", "bookings_dates_consistent",
              f"SELECT COUNT(*) FROM {sv}.bookings WHERE check_out <= check_in",  # nosec B608
              description="check_out is after check_in"),
        Check("silver", "bookings_stay_length_plausible",
              f"SELECT COUNT(*) FROM {sv}.bookings WHERE DATEDIFF(check_out, check_in) > 60",  # nosec B608
              severity="WARN", description="stays longer than 60 nights are suspicious"),
        Check("silver", "bookings_standardized_values",
              f"SELECT COUNT(*) FROM {sv}.bookings WHERE status <> LOWER(TRIM(status)) "  # nosec B608
              "OR currency <> UPPER(TRIM(currency)) OR room_type <> LOWER(TRIM(room_type))",
              description="status / currency / room_type are trimmed and consistently cased"),
        Check("silver", "guests_email_valid",
              f"SELECT COUNT(*) FROM {sv}.guests WHERE email NOT REGEXP '^[^@ ]+@[^@ ]+\\\\.[a-z]{{2,}}$'",  # nosec B608
              description="every silver email has a valid shape"),
        Check("silver", "payments_currency_matches_booking",
              f"SELECT COUNT(*) FROM {sv}.payments p JOIN {sv}.bookings b ON b.booking_id = p.booking_id "  # nosec B608
              "WHERE p.currency <> b.currency",
              description="a payment is made in the currency of its booking"),
    ]
    return checks


def gold_checks(s: Settings) -> list[Check]:
    sv, gd = f"`{s.silver}`", f"`{s.gold}`"
    return [
        Check("gold", "fact_rowcount_matches_silver",
              f"SELECT ABS((SELECT COUNT(*) FROM {gd}.fact_bookings) - (SELECT COUNT(*) FROM {sv}.bookings))",  # nosec B608
              description="no booking lost or duplicated on the way to gold"),
        Check("gold", "revenue_reconciles_with_silver",
              f"SELECT CASE WHEN ABS("  # nosec B608
              f"(SELECT COALESCE(SUM(net_value), 0) FROM {gd}.fact_bookings) - "
              f"(SELECT COALESCE(SUM(DATEDIFF(check_out, check_in) * nightly_rate), 0) FROM {sv}.bookings "
              "  WHERE status IN ('confirmed', 'completed'))) > 0.005 THEN 1 ELSE 0 END",
              description="total revenue in gold equals revenue recomputed from silver"),
        Check("gold", "kpi_revenue_matches_fact",
              f"SELECT CASE WHEN ABS("  # nosec B608
              f"(SELECT COALESCE(SUM(revenue), 0) FROM {gd}.agg_monthly_property_kpis) - "
              f"(SELECT COALESCE(SUM(net_value), 0) FROM {gd}.fact_bookings)) > 0.005 THEN 1 ELSE 0 END",
              description="the KPI aggregate does not drift from the fact table"),
        Check("gold", "fact_no_null_dimension_keys",
              f"SELECT COUNT(*) FROM {gd}.fact_bookings WHERE property_key IS NULL OR guest_key IS NULL "  # nosec B608
              "OR check_in_date_key IS NULL OR check_out_date_key IS NULL",
              description="every fact row is fully linked to its dimensions"),
        Check("gold", "fact_dates_exist_in_dim_date",
              f"SELECT COUNT(*) FROM {gd}.fact_bookings f LEFT JOIN {gd}.dim_date d ON d.date_key = f.check_in_date_key "  # nosec B608
              "WHERE d.date_key IS NULL",
              description="the calendar dimension covers every check-in date"),
        Check("gold", "fact_balance_not_negative",
              f"SELECT COUNT(*) FROM {gd}.fact_bookings WHERE balance_due < -0.005",  # nosec B608
              severity="WARN", description="guests are not charged more than the booking value"),
    ]
