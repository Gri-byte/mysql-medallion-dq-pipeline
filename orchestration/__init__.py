"""Dagster orchestration for the MySQL Medallion pipeline.

The business logic (SQL, quality checks, quarantine) lives in `src/` and is unchanged: this package
only wraps it so the layers become Dagster assets, the 34 quality checks become asset checks, and the
pipeline can run on a schedule with history and a web UI.
"""
