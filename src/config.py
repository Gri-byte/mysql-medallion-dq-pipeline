"""Runtime settings, read from environment variables (or a local .env file)."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

try:  # python-dotenv is optional at runtime
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT / "sql"
DEFAULT_DATA_DIR = ROOT / "data" / "raw"

_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Settings:
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    # Every database name is "<prefix><layer>". Tests use their own prefix so they can
    # never touch the databases you use for real work.
    prefix: str = "hotel_"
    # Quality-gate thresholds
    max_quarantine_rate: float = 0.10   # reject the batch if more than 10% of a table is quarantined
    max_null_key_rate: float = 0.02
    freshness_hours: int = 24

    def __post_init__(self) -> None:
        # Database names are interpolated into SQL text, so only plain identifiers are allowed.
        if not _IDENTIFIER.match(self.prefix):
            raise ValueError(f"DB_PREFIX may only contain letters, digits and underscores: {self.prefix!r}")

    @property
    def bronze(self) -> str:
        return f"{self.prefix}bronze"

    @property
    def silver(self) -> str:
        return f"{self.prefix}silver"

    @property
    def gold(self) -> str:
        return f"{self.prefix}gold"

    @property
    def dq(self) -> str:
        return f"{self.prefix}dq"

    @classmethod
    def from_env(cls, **overrides) -> Settings:
        values = {
            "host": os.getenv("MYSQL_HOST", cls.host),
            "port": int(os.getenv("MYSQL_PORT", cls.port)),
            "user": os.getenv("MYSQL_USER", cls.user),
            "password": os.getenv("MYSQL_PASSWORD", cls.password),
            "prefix": os.getenv("DB_PREFIX", cls.prefix),
            "max_quarantine_rate": float(os.getenv("MAX_QUARANTINE_RATE", cls.max_quarantine_rate)),
        }
        values.update(overrides)
        return cls(**values)
