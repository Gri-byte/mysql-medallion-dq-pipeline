"""Synthetic hotel-booking data with KNOWN, INJECTED defects.

Why inject defects on purpose? Because a data-quality framework is only trustworthy if it
catches the problems you know are there. Every defect written to the CSVs is counted in
`defects_manifest.json`, and the tests assert that the pipeline quarantines exactly those rows.

Defect families:
  * rejected      -> must end up in silver.quarantine with a specific reason
  * duplicates    -> exact copies of valid rows; silver must keep one
  * recoverable   -> dirty-but-valid values (whitespace, casing); silver must standardize, not reject
"""
from __future__ import annotations

import csv
import json
import random
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from .contracts import CONTRACTS

REF_DATE = date(2026, 7, 1)  # fixed "today" so generated data never depends on the run date

# property_id, name, city, stars, rooms, currency, base nightly rate
PROPERTIES = [
    ("P001", "Hotel Lago Azul", "Bariloche", 4, 60, "USD", 140),
    ("P002", "Cabanas del Nahuel", "Bariloche", 3, 24, "ARS", 90000),
    ("P003", "Hotel Glaciar Sur", "El Calafate", 4, 48, "USD", 160),
    ("P004", "Posada del Fin del Mundo", "Ushuaia", 3, 20, "USD", 120),
    ("P005", "Hotel Andes Plaza", "Mendoza", 5, 90, "USD", 210),
    ("P006", "Casa de Vinas", "Mendoza", 4, 32, "EUR", 130),
    ("P007", "Hotel Quebrada", "Salta", 3, 40, "ARS", 70000),
    ("P008", "Gran Hotel Obelisco", "Buenos Aires", 5, 120, "USD", 230),
    ("P009", "Hotel Recoleta Suites", "Buenos Aires", 4, 70, "EUR", 150),
    ("P010", "Lodge Perito Moreno", "El Calafate", 5, 36, "USD", 280),
    ("P011", "Hostal Costa Atlantica", "Mar del Plata", 3, 45, "ARS", 60000),
    ("P012", "Hotel Cataratas", "Puerto Iguazu", 4, 80, "EUR", 110),
]
ROOM_TYPES = ["standard", "superior", "suite", "family"]
ROOM_FACTOR = {"standard": 1.0, "superior": 1.25, "suite": 1.7, "family": 1.45}
COUNTRIES = ["AR", "BR", "CL", "US", "ES", "DE", "FR", "UY", "MX", "CO"]
FIRST = ["Ana", "Luis", "Maria", "Juan", "Sofia", "Pedro", "Laura", "Diego", "Carla", "Mateo",
         "Valeria", "Nicolas", "Camila", "Tomas", "Julia", "Martin", "Lucia", "Pablo", "Elena", "Hugo"]
LAST = ["Gomez", "Perez", "Silva", "Rojas", "Diaz", "Lopez", "Fernandez", "Ruiz", "Torres", "Castro",
        "Vega", "Molina", "Suarez", "Romero", "Herrera", "Medina", "Navarro", "Flores", "Ortiz", "Paz"]

BOOKING_DEFECTS = (
    "MISSING_KEY", "INVALID_DATE", "CHECKOUT_BEFORE_CHECKIN", "INVALID_RATE",
    "INVALID_CURRENCY", "INVALID_STATUS", "ORPHAN_PROPERTY", "ORPHAN_GUEST",
)
PAYMENT_DEFECTS = ("MISSING_KEY", "INVALID_AMOUNT", "INVALID_METHOD", "ORPHAN_BOOKING", "CURRENCY_MISMATCH")
BAD_DATES = ["not-a-date", "31/12/2025", "2025-13-45", "2025-02-30", ""]


def _money(value: float | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _write_csv(path: Path, columns: tuple[str, ...], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(columns))
        writer.writeheader()
        for row in rows:
            writer.writerow({c: ("" if row.get(c) is None else row[c]) for c in columns})


def _pick(rng: random.Random, pool: list[int], k: int) -> list[int]:
    """Take k distinct indices out of `pool` (and remove them from it)."""
    if k > len(pool):
        raise ValueError("defect_rate too high for the number of rows")
    chosen = rng.sample(pool, k)
    taken = set(chosen)
    pool[:] = [i for i in pool if i not in taken]
    return chosen


def generate(
    out_dir: str | Path,
    n_bookings: int = 8000,
    n_guests: int = 2000,
    defect_rate: float = 0.04,
    seed: int = 42,
) -> dict:
    """Write the four source CSVs plus defects_manifest.json and return the manifest."""
    if not 0 <= defect_rate <= 0.8:
        raise ValueError("defect_rate must be between 0 and 0.8")
    rng = random.Random(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ properties (clean)
    prop_rows = [
        {"property_id": p[0], "name": p[1], "city": p[2], "stars": p[3], "total_rooms": p[4]}
        for p in PROPERTIES
    ]
    prop_ccy = {p[0]: p[5] for p in PROPERTIES}

    # ------------------------------------------------------------------ guests
    guests = []
    for i in range(1, n_guests + 1):
        first, last = rng.choice(FIRST), rng.choice(LAST)
        guests.append({
            "guest_id": f"G{i:05d}",
            "full_name": f"{first} {last}",
            "email": f"{first}.{last}{i}@example.com".lower(),
            "country": rng.choice(COUNTRIES),
            "created_at": (date(2023, 1, 1) + timedelta(days=rng.randint(0, 900))).isoformat(),
        })
    g_pool = list(range(n_guests))
    k_g_bad = max(1, int(n_guests * defect_rate)) if defect_rate > 0 else 0
    bad_email_idx = _pick(rng, g_pool, k_g_bad)
    dirty_g_idx = _pick(rng, g_pool, min(max(1, int(n_guests * 0.05)), len(g_pool)))
    dup_g_idx = _pick(rng, g_pool, min(k_g_bad, len(g_pool)))  # at extreme defect rates fewer rows remain to duplicate
    for i in bad_email_idx:
        guests[i]["email"] = rng.choice(["not-an-email", "john@@example.com", "missing-domain@", "two words@x.com"])
    for i in dirty_g_idx:  # recoverable: silver trims and lower-cases
        guests[i]["full_name"] = f"  {guests[i]['full_name']} "
        guests[i]["email"] = guests[i]["email"].upper()
        guests[i]["country"] = guests[i]["country"].lower()
    bad_guest_ids = {guests[i]["guest_id"] for i in bad_email_idx}
    usable_guest_ids = [g["guest_id"] for g in guests if g["guest_id"] not in bad_guest_ids]
    guest_rows = guests + [dict(guests[i]) for i in dup_g_idx]

    # ------------------------------------------------------------------ bookings (valid base)
    start, end = date(2024, 1, 1), date(2026, 6, 30)
    span = (end - start).days
    bookings = []
    for i in range(1, n_bookings + 1):
        prop = rng.choice(PROPERTIES)
        room = rng.choice(ROOM_TYPES)
        check_in = start + timedelta(days=rng.randint(0, span))
        nights = rng.randint(1, 14)
        check_out = check_in + timedelta(days=nights)
        season = 1.25 if check_in.month in (12, 1, 2, 7) else 1.0
        rate = _money(prop[6] * ROOM_FACTOR[room] * season * rng.uniform(0.9, 1.1))
        if check_out <= REF_DATE:
            status = rng.choices(["completed", "cancelled", "no_show"], [88, 9, 3])[0]
        else:
            status = rng.choices(["confirmed", "cancelled"], [90, 10])[0]
        booked_at = datetime.combine(check_in - timedelta(days=rng.randint(1, 120)), datetime.min.time()) \
            + timedelta(seconds=rng.randint(0, 86399))
        bookings.append({
            "booking_id": f"B{i:06d}",
            "property_id": prop[0],
            "guest_id": rng.choice(usable_guest_ids),
            "room_type": room,
            "check_in": check_in.isoformat(),
            "check_out": check_out.isoformat(),
            "nightly_rate": str(rate),
            "currency": prop_ccy[prop[0]],
            "status": status,
            "booked_at": booked_at.strftime("%Y-%m-%d %H:%M:%S"),
        })

    b_pool = list(range(n_bookings))
    per_type = max(1, int(n_bookings * defect_rate / len(BOOKING_DEFECTS))) if defect_rate > 0 else 0
    b_idx = {d: _pick(rng, b_pool, per_type) for d in BOOKING_DEFECTS}
    dirty_b_idx = _pick(rng, b_pool, max(1, int(n_bookings * 0.03)))
    dup_b_idx = _pick(rng, b_pool, per_type)

    defective_booking_ids: set[str] = set()
    for defect, idxs in b_idx.items():
        for i in idxs:
            row = bookings[i]
            if defect != "MISSING_KEY":
                defective_booking_ids.add(row["booking_id"])
            if defect == "MISSING_KEY":
                row["booking_id"] = ""
            elif defect == "INVALID_DATE":
                row[rng.choice(["check_in", "check_out"])] = rng.choice(BAD_DATES)
            elif defect == "CHECKOUT_BEFORE_CHECKIN":
                row["check_out"] = (date.fromisoformat(row["check_in"]) - timedelta(days=rng.randint(0, 3))).isoformat()
            elif defect == "INVALID_RATE":
                row["nightly_rate"] = rng.choice(["-50", "0", "abc", "-12.50"])
            elif defect == "INVALID_CURRENCY":
                row["currency"] = rng.choice(["XXX", "BTC", ""])
            elif defect == "INVALID_STATUS":
                row["status"] = rng.choice(["pending_review", "unknown", "???"])
            elif defect == "ORPHAN_PROPERTY":
                row["property_id"] = "P999"
            elif defect == "ORPHAN_GUEST":
                row["guest_id"] = "G99999"

    # Payments are generated from the valid bookings only (before dirtying the recoverable ones)
    payments = []
    pay_ctr = 0
    valid_for_payment = [b for k, b in enumerate(bookings)
                         if b["booking_id"] and b["booking_id"] not in defective_booking_ids]
    for b in valid_for_payment:
        if b["status"] == "cancelled":
            continue
        nights = (date.fromisoformat(b["check_out"]) - date.fromisoformat(b["check_in"])).days
        total = _money(Decimal(b["nightly_rate"]) * nights)
        booked = datetime.strptime(b["booked_at"], "%Y-%m-%d %H:%M:%S")
        deposit = _money(total * Decimal("0.30"))
        plan = [(deposit, booked + timedelta(hours=rng.randint(1, 48)))]
        if b["status"] == "completed":
            plan.append((total - deposit, datetime.combine(date.fromisoformat(b["check_in"]), datetime.min.time())
                         + timedelta(hours=rng.randint(8, 20))))
        for amount, paid_at in plan:
            pay_ctr += 1
            payments.append({
                "payment_id": f"PM{pay_ctr:07d}",
                "booking_id": b["booking_id"],
                "amount": str(amount),
                "currency": b["currency"],
                "method": rng.choices(["card", "transfer", "cash", "other"], [60, 25, 10, 5])[0],
                "paid_at": paid_at.strftime("%Y-%m-%d %H:%M:%S"),
            })

    for i in dirty_b_idx:  # recoverable dirt on valid rows
        row = bookings[i]
        row["status"] = f" {row['status'].upper()} "
        row["currency"] = row["currency"].lower()
        row["room_type"] = row["room_type"].upper()
    booking_rows = bookings + [dict(bookings[i]) for i in dup_b_idx]

    # ------------------------------------------------------------------ payment defects
    p_pool = list(range(len(payments)))
    per_type_p = max(1, int(len(payments) * defect_rate / len(PAYMENT_DEFECTS))) if defect_rate > 0 else 0
    p_idx = {d: _pick(rng, p_pool, per_type_p) for d in PAYMENT_DEFECTS}
    dup_p_idx = _pick(rng, p_pool, per_type_p)
    for defect, idxs in p_idx.items():
        for n, i in enumerate(idxs):
            row = payments[i]
            if defect == "MISSING_KEY":
                row["payment_id"] = ""
            elif defect == "INVALID_AMOUNT":
                row["amount"] = rng.choice(["-25.00", "0", "n/a"])
            elif defect == "INVALID_METHOD":
                row["method"] = "bitcoin"
            elif defect == "ORPHAN_BOOKING":
                row["booking_id"] = f"B9{n:05d}"
            elif defect == "CURRENCY_MISMATCH":
                row["currency"] = rng.choice([c for c in ("USD", "ARS", "EUR") if c != row["currency"]])
    payment_rows = payments + [dict(payments[i]) for i in dup_p_idx]

    # ------------------------------------------------------------------ write files + manifest
    _write_csv(out / CONTRACTS["properties"].csv_file, CONTRACTS["properties"].columns, prop_rows)
    _write_csv(out / CONTRACTS["guests"].csv_file, CONTRACTS["guests"].columns, guest_rows)
    _write_csv(out / CONTRACTS["bookings"].csv_file, CONTRACTS["bookings"].columns, booking_rows)
    _write_csv(out / CONTRACTS["payments"].csv_file, CONTRACTS["payments"].columns, payment_rows)

    def table_manifest(rows_written: int, duplicates: int, defects: dict[str, int], recoverable: int) -> dict:
        rejected = sum(defects.values())
        return {
            "rows_written": rows_written,
            "duplicates": duplicates,
            "recoverable_dirty_rows": recoverable,
            "defects": {k: v for k, v in defects.items() if v},
            "expected_valid_rows": rows_written - duplicates - rejected,
        }

    manifest = {
        "seed": seed,
        "defect_rate": defect_rate,
        "tables": {
            "properties": table_manifest(len(prop_rows), 0, {}, 0),
            "guests": table_manifest(len(guest_rows), len(dup_g_idx), {"INVALID_EMAIL": len(bad_email_idx)}, len(dirty_g_idx)),
            "bookings": table_manifest(
                len(booking_rows), len(dup_b_idx), {d: len(v) for d, v in b_idx.items()}, len(dirty_b_idx)
            ),
            "payments": table_manifest(len(payment_rows), len(dup_p_idx), {d: len(v) for d, v in p_idx.items()}, 0),
        },
    }
    (out / "defects_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


if __name__ == "__main__":  # pragma: no cover
    import argparse

    ap = argparse.ArgumentParser(description="Generate synthetic hotel data with injected defects")
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--bookings", type=int, default=8000)
    ap.add_argument("--guests", type=int, default=2000)
    ap.add_argument("--defect-rate", type=float, default=0.04)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    print(json.dumps(generate(args.out, args.bookings, args.guests, args.defect_rate, args.seed), indent=2))
