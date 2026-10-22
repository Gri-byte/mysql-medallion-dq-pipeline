"""Small MySQL helper layer (PyMySQL)."""
from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pymysql

from .config import Settings


def connect(settings: Settings, database: str | None = None, autocommit: bool = True):
    return pymysql.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        database=database,
        charset="utf8mb4",
        autocommit=autocommit,
    )


def render_sql(text: str, settings: Settings) -> str:
    """Replace the ${LAYER} placeholders used in the .sql files with the real database names."""
    return (
        text.replace("${BRONZE}", settings.bronze)
        .replace("${SILVER}", settings.silver)
        .replace("${GOLD}", settings.gold)
        .replace("${DQ}", settings.dq)
    )


def split_statements(sql_text: str) -> list[str]:
    """Split a script into statements. Full-line '--' comments are dropped first.

    The scripts in this repo contain no semicolons inside string literals, which keeps this
    splitter simple and predictable.
    """
    lines = [ln for ln in sql_text.splitlines() if not ln.strip().startswith("--")]
    return [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]


def run_sql_file(conn, path: Path, settings: Settings) -> int:
    """Execute every statement of a .sql file. Returns the number of statements run."""
    statements = split_statements(render_sql(Path(path).read_text(encoding="utf-8"), settings))
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
    return len(statements)


def fetch_all(conn, sql: str, params: Sequence[Any] | None = None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def scalar(conn, sql: str, params: Sequence[Any] | None = None):
    rows = fetch_all(conn, sql, params)
    return rows[0][0] if rows else None
