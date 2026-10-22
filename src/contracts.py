"""Source-file contracts: the single source of truth for what each raw file must look like.

The generator writes these columns, the ingestion step refuses files that do not match them
(schema-drift protection), and the bronze DDL mirrors them.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceContract:
    name: str          # logical table name (properties, guests, ...)
    csv_file: str      # expected file name inside the data directory
    bronze_table: str  # raw table in the bronze database
    columns: tuple[str, ...]


CONTRACTS: dict[str, SourceContract] = {
    "properties": SourceContract(
        "properties", "properties.csv", "raw_properties",
        ("property_id", "name", "city", "stars", "total_rooms"),
    ),
    "guests": SourceContract(
        "guests", "guests.csv", "raw_guests",
        ("guest_id", "full_name", "email", "country", "created_at"),
    ),
    "bookings": SourceContract(
        "bookings", "bookings.csv", "raw_bookings",
        ("booking_id", "property_id", "guest_id", "room_type", "check_in", "check_out",
         "nightly_rate", "currency", "status", "booked_at"),
    ),
    "payments": SourceContract(
        "payments", "payments.csv", "raw_payments",
        ("payment_id", "booking_id", "amount", "currency", "method", "paid_at"),
    ),
}

# Order matters: parents before children (silver enforces foreign keys).
LOAD_ORDER = ("properties", "guests", "bookings", "payments")
