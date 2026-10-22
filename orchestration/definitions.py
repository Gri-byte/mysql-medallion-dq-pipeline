"""Entry point Dagster loads (see workspace.yaml): assets + job + daily schedule + resources."""
from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from .assets import ALL_ASSETS
from .resources import MySQLResource

medallion_job = define_asset_job(
    name="medallion_pipeline",
    selection=AssetSelection.all(),
    description="source -> bronze -> silver -> gold, with blocking quality gates between layers.",
)

# Every day at 06:00 Argentina time. Runs only while the Dagster daemon is up (`dagster dev` starts it).
daily_medallion = ScheduleDefinition(
    name="daily_medallion",
    job=medallion_job,
    cron_schedule="0 6 * * *",
    execution_timezone="America/Argentina/Buenos_Aires",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(
    assets=ALL_ASSETS,
    jobs=[medallion_job],
    schedules=[daily_medallion],
    resources={"db": MySQLResource()},
)
