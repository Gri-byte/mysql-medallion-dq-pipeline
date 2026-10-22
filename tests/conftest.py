"""Shared fixtures. Tests run against a REAL MySQL server (not mocks), in databases that carry the
prefix `test_hotel_...`, and they drop those databases when finished.

Set REQUIRE_MYSQL=1 (as the CI workflow does) to fail instead of skip when no server is reachable.
"""
from __future__ import annotations

import dataclasses
import os

import pytest

from src.config import Settings
from src.db import connect


@pytest.fixture(scope="session")
def base_settings() -> Settings:
    settings = Settings.from_env(prefix="test_hotel_")
    try:
        connect(settings).close()
    except Exception as exc:  # noqa: BLE001
        message = f"MySQL is not reachable at {settings.host}:{settings.port} ({exc})"
        if os.getenv("REQUIRE_MYSQL") == "1":
            pytest.fail(message)
        pytest.skip(message)
    return settings


@pytest.fixture
def make_settings(base_settings):
    """Factory: isolated settings (own database names) per test module / test."""
    def _make(suffix: str) -> Settings:
        return dataclasses.replace(base_settings, prefix=f"{base_settings.prefix}{suffix}_")
    return _make
