from dagster import ConfigurableResource

from src.config import Settings
from src.db import connect


class MySQLResource(ConfigurableResource):
    """How Dagster reaches MySQL.

    Connection details (host, port, user, PASSWORD) are read from the environment / .env exactly as
    in the CLI, so no secret is ever stored in Dagster's run configuration or shown in its UI.
    Only non-secret knobs are configurable here; leave them unset to use the environment values.
    """

    prefix: str | None = None               # database name prefix (default: DB_PREFIX or "hotel_")
    max_quarantine_rate: float | None = None  # volume-anomaly gate (default: MAX_QUARANTINE_RATE or 0.10)

    def settings(self) -> Settings:
        overrides = {
            key: value
            for key, value in (("prefix", self.prefix), ("max_quarantine_rate", self.max_quarantine_rate))
            if value is not None
        }
        return Settings.from_env(**overrides)

    def connection(self):
        return connect(self.settings())
