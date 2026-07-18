# Airbnb & Census ELT Warehouse

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-orange?logo=apache-airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Core%20%7C%20Cloud-FF694B?logo=dbt&logoColor=white)
![Postgres](https://img.shields.io/badge/PostgreSQL-13+-336791?logo=postgresql&logoColor=white)
![Metabase](https://img.shields.io/badge/Metabase-BI-509EE3?logo=metabase&logoColor=white)

An end-to-end ELT pipeline for analysing Sydney Airbnb listings alongside Australian Census demographic data. The project uses Airflow for orchestration, PostgreSQL as the warehouse, dbt to model the data through Bronze, Silver, and Gold layers, and Metabase for local BI dashboards.

This repository is maintained as a personal data engineering project: local-first, reproducible with Docker, and designed to grow into a portfolio-grade analytics platform with data quality checks, BI dashboards, and operational documentation.

![Header Image](https://www.realestate.com.au/news-image/w_1280,h_720/v1743109398/news-lifestyle-content-assets/wp-content/production/capi_66e50ad6861c43dbf0bfbe364f663d5f_e58997b3f701d49d4cb6291f6204b1e1.jpeg?_i=AA)

## What This Project Demonstrates

![Architecture Diagram](docs/assets/architecture_flow.png)

### Skills

| Area | What's applied here |
|---|---|
| **Orchestration (Airflow)** | Idempotent, chronologically-sequenced DAGs; TaskGroups; runtime file discovery; Airflow Variables/connections |
| **Data Modeling (dbt)** | Medallion architecture (bronze/silver/gold), SCD Type 2 dimension history via snapshots, star-schema fact/dimension design, surrogate-key resolution via validity-window joins |
| **SQL** | CTE-heavy analytical queries answering real business questions (revenue, demographics correlation, mortgage affordability) |
| **Data Quality & CI/CD** | dbt tests, pytest unit tests for ingestion logic, GitHub Actions CI running syntax checks, unit tests, SQL lint, and `dbt parse` on every push |
| **Python** | Dynamic, self-healing CSV-to-Postgres ingestion (schema inference, drift recovery) |
| **Infrastructure** | Docker Compose stack running Airflow, Postgres, dbt, and Metabase together for a reproducible local environment |
| **BI** | Metabase dashboards over the Gold layer |

### Engineering Decisions

The parts of this project that involved real debugging and trade-offs, not just wiring tools together:

- **Eliminated ~300 lines of duplicated ingestion logic** shared between the two Airflow DAGs — but the two copies had quietly diverged (one archived files inline, the other ran archiving as a separate, independently observable task, and only one had a `truncate` flag). Reconciled both into one shared [`dags/utils/helpers.py`](dags/utils/helpers.py) module without changing either DAG's behavior, backed by a [pytest suite](tests/test_dag_helpers.py).
- **Avoided a subtle SCD2 correctness bug.** Airflow's dynamic task mapping runs mapped instances without guaranteed order — using it for monthly file processing would silently break the chronological snapshot history the whole warehouse depends on (see [docs/2_architecture.md#28-why-sequential-dbt-runs-matter](docs/2_architecture.md#28-why-sequential-dbt-runs-matter)). Kept sequential processing rather than trading correctness for a nicer Airflow UI.
- **Debugged a real tooling incompatibility.** sqlfluff's dbt templater depends on internal dbt-core APIs that no longer exist in the project's pinned `dbt-core==1.7.19` — traced it to an `ImportError` several layers deep, then switched to sqlfluff's jinja templater with dbt builtins to get working, dependency-free SQL linting into CI.
- **Kept the test suite fast and CI-friendly** by lazily importing `PostgresHook` only inside the two functions that actually need a live Airflow connection, so ingestion-helper unit tests run under plain pytest without installing Airflow at all.

## Overview

The warehouse combines monthly Airbnb listing extracts with ABS Census G01/G02 data and NSW LGA mapping files. It produces a dimensional model and analytics marts for rental-market performance, host concentration, neighbourhood demand, and mortgage-affordability analysis.

Core design choices:

- **Medallion architecture:** raw Bronze ingestion, cleaned Silver models, and Gold star-schema/reporting marts.
- **Idempotent ingestion:** bootstrap loads recreate the Bronze schema, while monthly listing files are appended chronologically.
- **Slowly changing dimensions:** dbt snapshots track history for hosts, properties, neighbourhoods, and LGAs.
- **Sequential monthly processing:** Airflow triggers dbt after each monthly load so snapshot validity remains aligned with the source timeline.

## Repository Structure

```text
.
|-- .github/
|   `-- workflows/
|       `-- ci.yml                    # Lightweight CI checks
|-- dags/
|   |-- airbnb_census_pipeline.py     # Main Airflow pipeline for initial and monthly loads
|   |-- initial_bronze_load.py        # Standalone Bronze bootstrap DAG
|   `-- utils/
|       `-- helpers.py                # Shared ingestion helpers used by both DAGs
|-- dbt/
|   |-- models/
|   |   |-- bronze/                   # Source-facing raw models
|   |   |-- silver/                   # Cleaning, typing, deduplication, and bridge models
|   |   `-- gold/                     # Star schema and analytics marts
|   |-- snapshots/                    # SCD Type 2 snapshot definitions
|   `-- dbt_project.yml
|-- docker/                           # Local Airflow, dbt, and Postgres support files
|-- docs/
|   |-- 0_coding_standards.md
|   |-- 1_instructions.md
|   |-- 2_architecture.md
|   |-- 3_operations.md
|   |-- 4_roadmap.md
|   |-- 5_dashboard_guide.md
|   |-- 6_requirement_coverage.md
|   |-- 7_dbt_guide.md
|   |-- assets/
|   |   |-- architecture_flow.drawio
|   |   |-- architecture_flow.png
|   |   `-- screenshots/
|   `-- reports/
|       `-- airbnb_census_warehouse_report.pdf
|-- scripts/
|   |-- check_pipeline_outputs.sh     # Smoke test for latest local pipeline run
|   |-- run_quality_checks.sh         # Syntax, dbt parse, and optional Docker checks
|   `-- stage_source_data.sh          # Unpacks source ZIP files into local data folders
|-- sql/
|   |-- init_bronze_schema.sql        # Warehouse schema bootstrap DDL
|   `-- analysis_queries.sql          # Business analysis query pack
|-- tests/
|   `-- test_dag_helpers.py           # pytest coverage for dags/utils/helpers.py
|-- .sqlfluff                         # SQL lint rules (matches project style)
|-- docker-compose.yml
|-- requirements.txt
`-- README.md
```

## Setup

Follow [docs/0_coding_standards.md](docs/0_coding_standards.md) before making code changes.

For local setup and run instructions, see [docs/1_instructions.md](docs/1_instructions.md).

For the project roadmap, see [docs/4_roadmap.md](docs/4_roadmap.md).

For Metabase dashboard setup, see [docs/5_dashboard_guide.md](docs/5_dashboard_guide.md).

For dbt model flow and SCD2 details, see [docs/7_dbt_guide.md](docs/7_dbt_guide.md).

Prerequisites:

- Apache Airflow 2.x (local, Docker, or managed).
- PostgreSQL warehouse connection available to Airflow as `postgres`.
- dbt Core locally, or dbt Cloud if `DBT_RUN_MODE=cloud`.
- Raw data staged under a local data root with `airbnb`, `census/G01`, `census/G02`, and `mappings` subdirectories.
  - Default root is `/home/airflow/gcs/data` for compatibility.
  - Set `AIRFLOW_DATA_PATH` (env var or Airflow Variable) to run locally, e.g. `/opt/airbnb-data`.

Airflow variables used by the main pipeline:

- `RUN_INITIAL_LOAD`: set to `true` to run the bootstrap load before monthly processing. The legacy `RUN_PART1` variable is still supported as a fallback.
- `DBT_CLOUD_URL`: defaults to `cloud.getdbt.com`.
- `DBT_CLOUD_ACCOUNT_ID`
- `DBT_CLOUD_JOB_ID`
- `DBT_CLOUD_API_TOKEN`
- `DBT_CLOUD_WAIT_TIMEOUT_SEC`: defaults to `3600`.
- `AIRFLOW_DATA_PATH` (optional): filesystem root for input files and archives.
- `DBT_RUN_MODE`: use `local` for Docker/local dbt or `cloud` for dbt Cloud.
- `DBT_PROJECT_DIR`: local dbt project path when `DBT_RUN_MODE=local`.
- `DBT_PROFILES_DIR`: local dbt profile path when `DBT_RUN_MODE=local`.
- `DBT_LOCAL_COMMANDS`: local command sequence, defaults to `dbt build`.

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Quick Start

Runs entirely locally with Docker Compose (Airflow + PostgreSQL + dbt Core + Metabase, no Google Cloud required):

```bash
cp .env.example .env && echo "AIRFLOW_UID=$(id -u)" >> .env
./scripts/stage_source_data.sh "/path/to/source-zips" data
docker compose up airflow-init && docker compose up -d
```

Open Airflow at `http://localhost:8080` (`admin` / `admin`) and trigger `airbnb_census_monthly_pipeline` with `RUN_INITIAL_LOAD=true`. This loads the baseline, runs dbt, then processes each monthly file in chronological order.

Verify the run:

```bash
./scripts/check_pipeline_outputs.sh
```

For the full step-by-step walkthrough, dbt Cloud mode, and troubleshooting, see [docs/1_instructions.md](docs/1_instructions.md) and [docs/3_operations.md](docs/3_operations.md).

## Data Model

- **Bronze:** raw CSV-aligned models for Airbnb listings, Census G01/G02, NSW LGA codes, and suburb mappings.
- **Silver:** cleaned listing records, normalized dimensions, and suburb-to-LGA bridge logic.
- **Snapshots:** SCD Type 2 history for host, property, neighbourhood, and LGA entities.
- **Gold:** monthly listing fact table, conformed dimensions, Census reference models, and data marts for host, listing-neighbourhood, and property-type performance.

## Analysis Queries

Run `sql/analysis_queries.sql` against the populated warehouse to answer:

- Which LGAs have the strongest and weakest revenue per active listing, and how do their demographics differ?
- How does median age correlate with revenue per active listing?
- Which property configurations perform best in the top revenue neighbourhoods?
- Are multi-listing hosts concentrated within a single LGA or distributed across several LGAs?
- How often can single-listing hosts cover annualised median mortgage repayments from Airbnb revenue?

## Notes

The project keeps raw Bronze fields as text for resilient ingestion and performs type casting in dbt. This makes source-file drift easier to isolate while keeping analytics models strongly typed.
