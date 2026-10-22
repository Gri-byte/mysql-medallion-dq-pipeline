"""CSV -> BRONZE. Loads files as-is (all text) and records lineage; rejects nothing.

Two protections live here:
  * schema contract: a file whose header does not match the contract is refused outright
  * idempotent reload: loading the same file name again replaces the rows of that file
"""
from __future__ import annotations

import csv
from pathlib import Path

from .config import Settings
from .contracts import CONTRACTS, LOAD_ORDER, SourceContract

CHUNK = 1000


class SchemaContractError(Exception):
    """The source file does not match the agreed column contract."""


def _read_rows(path: Path, contract: SourceContract) -> list[list[str | None]]:
    if not path.exists():
        raise SchemaContractError(f"Missing source file: {path}")
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if header is None:
            raise SchemaContractError(f"{path.name}: file is empty (no header)")
        header = [h.strip() for h in header]
        if set(header) != set(contract.columns) or len(header) != len(contract.columns):
            missing = sorted(set(contract.columns) - set(header))
            extra = sorted(set(header) - set(contract.columns))
            raise SchemaContractError(f"{path.name}: schema drift. missing={missing} unexpected={extra}")
        position = {name: header.index(name) for name in contract.columns}
        rows = []
        for line_no, raw in enumerate(reader, start=2):
            if len(raw) != len(header):
                raise SchemaContractError(f"{path.name}: line {line_no} has {len(raw)} fields, expected {len(header)}")
            rows.append([(raw[position[c]] if raw[position[c]] != "" else None) for c in contract.columns])
        return rows


def ingest_table(conn, settings: Settings, contract: SourceContract, data_dir: Path, batch_id: str) -> int:
    path = Path(data_dir) / contract.csv_file
    rows = _read_rows(path, contract)  # validate the whole file BEFORE touching the database
    table = f"`{settings.bronze}`.{contract.bronze_table}"
    cols = ", ".join(contract.columns)
    placeholders = ", ".join(["%s"] * (len(contract.columns) + 2))
    insert_sql = f"INSERT INTO {table} ({cols}, _batch_id, _source_file) VALUES ({placeholders})"  # nosec B608 - identifiers come from validated settings/contracts
    delete_sql = f"DELETE FROM {table} WHERE _source_file = %s"  # nosec B608

    conn.begin()
    try:
        with conn.cursor() as cur:
            cur.execute(delete_sql, (contract.csv_file,))
            for i in range(0, len(rows), CHUNK):
                chunk = [tuple(r) + (batch_id, contract.csv_file) for r in rows[i:i + CHUNK]]
                cur.executemany(insert_sql, chunk)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(rows)


def run(conn, settings: Settings, data_dir: Path, batch_id: str) -> dict[str, int]:
    """Ingest the four source files. Returns rows loaded per table."""
    # Validate every file's contract first so a bad file never leaves a half-loaded bronze.
    for name in LOAD_ORDER:
        _read_rows(Path(data_dir) / CONTRACTS[name].csv_file, CONTRACTS[name])
    return {name: ingest_table(conn, settings, CONTRACTS[name], Path(data_dir), batch_id) for name in LOAD_ORDER}
