# MySQL Medallion Data-Quality Pipeline

[![CI](https://github.com/Gri-byte/mysql-medallion-dq-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Gri-byte/mysql-medallion-dq-pipeline/actions/workflows/ci.yml)

A small but complete data-engineering project on **MySQL 8**: hotel booking data flows through a
**Medallion architecture** (bronze → silver → gold), with **data-quality gates between every layer**,
a **quarantine** for rejected rows, a **star schema** for analytics, and a test suite that proves the
pipeline fails safely.

> **The idea that drives the project:** a pipeline that only works on clean data is not reliable.
> The generator injects *known* defects on purpose, and the tests check that the pipeline caught
> **exactly** those rows — no more, no fewer — and that bad batches never reach gold.

## Architecture

```mermaid
flowchart LR
    CSV[(Source CSVs<br/>+ schema contract)] --> B

    subgraph MySQL
        B[BRONZE<br/>raw text + lineage]
        S[SILVER<br/>typed, validated,<br/>de-duplicated]
        Q[(quarantine<br/>rejected rows + reason)]
        G[GOLD<br/>star schema + KPIs]
        DQ[(dq schema<br/>run history + check results)]
    end

    B -->|gate 1: bronze checks| S
    S --> Q
    S -->|gate 2: silver checks| G
    G -->|gate 3: gold checks| OUT[Analytics / BI]
    B -.-> DQ
    S -.-> DQ
    G -.-> DQ
```

| Layer | Database | What it guarantees |
|---|---|---|
| **Bronze** | `hotel_bronze` | Raw data exactly as received (all text) + lineage (`_batch_id`, `_source_file`, `_ingested_at`). Nothing is rejected here, so bad data stays visible. |
| **Silver** | `hotel_silver` | Typed, trimmed, consistently cased, de-duplicated. Integrity enforced **by the engine** (PK / FK / CHECK). Rejected rows go to `quarantine` with a reason and the raw record. |
| **Gold** | `hotel_gold` | Star schema (`dim_property`, `dim_guest`, `dim_date`, `fact_bookings`) and a monthly KPI table (revenue, ADR, cancellation rate). Rebuilt only if silver passed its gate. |
| **DQ** | `hotel_dq` | `pipeline_runs` and `check_results`: quality is recorded run after run, not just printed in a log. |

## Data-quality controls

| Control | Where | Behaviour |
|---|---|---|
| **Schema contract** | ingestion | A file with missing / extra columns is refused *before* any table is touched. |
| **Idempotent loads** | ingestion | Re-ingesting a file replaces that file's rows; re-running the pipeline changes nothing. |
| **Quarantine** | silver | One reject reason per row, raw record kept for diagnosis (see catalogue below). |
| **De-duplication** | silver | Latest ingested row per business key wins. |
| **Standardization** | silver | `' COMPLETED '`, `usd`, `ANA@X.COM` are cleaned, not rejected. |
| **Reconciliation** | silver / gold | `bronze = silver + quarantine + duplicates` for every table; gold revenue must equal revenue recomputed from silver. |
| **Volume-anomaly gate** | silver | If more than 10 % of a table is quarantined, the batch is stopped (`MAX_QUARANTINE_RATE`). |
| **Engine-level guards** | silver | PK, FK and CHECK constraints: a bug in the transformation cannot write an impossible row. |
| **Severity levels** | all | `ERROR` blocks the pipeline, `WARN` is recorded but does not. |
| **Blast-radius control** | pipeline | A failed gate stops the run; **gold keeps the last good data**. |

### Reject-reason catalogue

| Table | Reasons |
|---|---|
| `bookings` | `MISSING_KEY`, `INVALID_DATE`, `CHECKOUT_BEFORE_CHECKIN`, `INVALID_RATE`, `INVALID_CURRENCY`, `INVALID_STATUS`, `ORPHAN_PROPERTY`, `ORPHAN_GUEST` |
| `guests` | `MISSING_KEY`, `MISSING_NAME`, `INVALID_EMAIL`, `INVALID_COUNTRY` |
| `payments` | `MISSING_KEY`, `INVALID_AMOUNT`, `INVALID_CURRENCY`, `INVALID_DATE`, `INVALID_METHOD`, `ORPHAN_BOOKING`, `CURRENCY_MISMATCH` |
| `properties` | `MISSING_KEY`, `MISSING_NAME`, `INVALID_STARS`, `INVALID_ROOMS` |

Orphan checks run against **silver**, so a booking that belongs to a quarantined guest is quarantined
too instead of silently becoming an orphan.

## Example run

Default run: 8,000 bookings, 2,000 guests, 4 % injected defects.

```
bronze rows : {'properties': 12, 'guests': 2080, 'bookings': 8040, 'payments': 13773}
silver rows : {'properties': 12, 'guests': 1920, 'bookings': 7680, 'payments': 13119, 'quarantine': 945}
gold rows   : {'dim_property': 12, 'dim_guest': 1920, 'dim_date': 926, 'fact_bookings': 7680, 'agg_monthly_property_kpis': 360}
quarantine bookings : CHECKOUT_BEFORE_CHECKIN 40, INVALID_CURRENCY 40, INVALID_DATE 40, INVALID_RATE 40,
                      INVALID_STATUS 40, MISSING_KEY 40, ORPHAN_GUEST 40, ORPHAN_PROPERTY 40
checks      : 34/34 passed   (12 bronze, 16 silver, 6 gold)
```

The bronze row counts are higher than the silver ones because the files contain duplicates
(80 guests, 40 bookings, 109 payments) that silver collapses to one row each.

## Quick start

Requirements: Python 3.10+, and a MySQL 8 server you can create databases on
(a local install, or `docker compose up -d` with the included `docker-compose.yml`).

```bash
git clone https://github.com/Gri-byte/mysql-medallion-dq-pipeline.git
cd mysql-medallion-dq-pipeline

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env               # Windows: copy .env.example .env  -> then edit MYSQL_PASSWORD
python -m src.run_pipeline --reset
```

Then open **DBeaver** (or any client), refresh the connection and you will see four databases:
`hotel_bronze`, `hotel_silver`, `hotel_gold`, `hotel_dq`. Open
[`sql/analytics_queries.sql`](sql/analytics_queries.sql) and run the queries.

```bash
python -m src.run_pipeline --bookings 20000 --defect-rate 0.08   # bigger / dirtier data
python -m src.run_pipeline --defect-rate 0.30                    # watch a quality gate stop the run (exit code 2)
python -m src.run_pipeline --no-generate --data-dir ./my_csvs    # your own files (must match src/contracts.py)
```

> `cryptography` is in `requirements.txt` because MySQL 8's default authentication
> (`caching_sha2_password`) needs it with PyMySQL.

## Tests

```bash
pytest -v
```

The tests run against a **real MySQL server** (not mocks) in databases prefixed `test_hotel_…`,
and drop them afterwards. Your `hotel_*` databases are never touched.

| Suite | What it proves |
|---|---|
| `test_generator.py` | The data generator is deterministic and its defect manifest adds up (no DB needed). |
| `test_pipeline_e2e.py` | Silver row counts and quarantine reasons match the **injected ground truth**; nothing is silently lost; dirt is standardized; gold reconciles with silver; revenue is never mixed across currencies; re-running is idempotent; every analytics query returns rows. |
| `test_failure_modes.py` | Excess defects stop the pipeline at the right layer; a failed batch does **not** overwrite last good gold; schema drift and empty files are refused; the engine rejects bad rows even if the pipeline had a bug; the DQ engine's tolerance and severity rules behave as documented. |

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs lint, the full suite and a demo run
against a `mysql:8.0` service container on every push and pull request.

## Project layout

```
sql/        00-04  DDL per layer        10_silver_load.sql   20_gold_load.sql   analytics_queries.sql
src/        contracts.py  config.py  db.py  generate_data.py  ingest_bronze.py
            transform_silver.py  build_gold.py  checks.py  dq.py  run_pipeline.py
tests/      generator, end-to-end and failure-mode suites
.github/    CI workflow + Dependabot
```

The transformations are **plain SQL files** (the logic a data engineer reviews), executed by thin
Python orchestration. Python handles files, run bookkeeping and the DQ engine.

## Design decisions and trade-offs

- **Full refresh, not incremental.** Silver and gold are rebuilt on every run, which makes the pipeline
  idempotent and easy to reason about. For tables in the hundreds of millions of rows you would move to
  incremental loads keyed on `_ingested_at` — a natural next step.
- **Validation is explicit, not left to the engine.** Casting bad text in a lenient session can silently
  produce `0000-00-00`; in strict mode it aborts the whole batch. Instead, rules are written in SQL
  (regex, ranges, joins) so each bad row gets a *reason*. Silver's CHECK constraints stay on as a
  second line of defence.
- **Calendar validity is checked by hand.** Engines disagree on impossible dates: MySQL 8 turns
  `'2025-02-30'` into `NULL`, MariaDB keeps it as-is. The pipeline validates
  `DAYOFMONTH(d) <= DAY(LAST_DAY(d))` explicitly so behaviour does not depend on the engine.
  (Found while developing on MariaDB 10.11; see the comment at the top of `10_silver_load.sql`.)
- **A failed gate leaves the bad batch in bronze / silver / quarantine for diagnosis**, but never builds
  gold from it. Dashboards keep serving the last good data.
- **Currencies are never summed together** (USD / ARS / EUR) because no FX rates are assumed. The KPI
  grain includes `currency`.
- **KPIs are attributed to the month of check-in.** Stays that cross a month boundary are not split, so
  no occupancy rate is published (it would be misleading); `room_nights_sold` and `ADR` are.
- **Synthetic data.** The data is generated (fixed seed, fixed "today" of 2026-07-01) so the project is
  reproducible and contains no personal information.

## Possible extensions

- Orchestrate with Dagster / Airflow and move the SQL transformations to dbt models + tests.
- Incremental silver / gold loads and Slowly Changing Dimensions (type 2) for `dim_property`.
- Expose `dq.check_results` in a dashboard (failed checks over time, quarantine rate by reason).
- Add a Great Expectations suite or contract tests for the source files.
