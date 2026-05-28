# Roadmap

This roadmap turns the project from a working local warehouse into a stronger personal data engineering portfolio project.

## Current State

Completed:

- Docker-based local Airflow + PostgreSQL + dbt setup.
- Local source-file staging without Google Cloud Storage.
- Full Airflow pipeline run.
- Local dbt build with tests and snapshots.
- Smoke test for latest run and key warehouse outputs.
- Numbered project documentation.

## Next Priorities

### 1. Make The DAG More Observable

Goal: show each monthly load and dbt run clearly in Airflow.

Options:

- Use Airflow dynamic task mapping for monthly file processing.
- Keep runtime discovery but emit stronger per-month logs and summary metrics.

Recommended next step:

- Add a run summary table or task log summary first.
- Refactor to dynamic task mapping after the current baseline is fully documented.

### 2. Add A BI Layer

Goal: make the warehouse useful beyond SQL queries.

Candidates:

- Metabase in Docker Compose.
- Apache Superset in Docker Compose.

Recommended next step:

- Add Metabase first because it is quick to wire to PostgreSQL and strong for portfolio screenshots.

### 3. Add Data Quality Contracts

Goal: make quality expectations explicit.

Ideas:

- Add relationships tests from fact to dimensions.
- Add accepted-value tests for Boolean flags.
- Add monthly row count checks.
- Add source freshness checks where a source date is available.

### 4. Add CI

Goal: verify the project on every push.

Ideas:

- `dbt parse`
- SQL linting
- Python syntax check for DAGs
- optional Docker Compose smoke test on selected branches

### 5. Publish Portfolio Story

Goal: make the project easy to understand by recruiters or collaborators.

Deliverables:

- architecture diagram refresh
- dashboard screenshots
- final `PROJECT_SUMMARY.md`
- short LinkedIn/GitHub project writeup

## Suggested Immediate Sequence

1. Add Metabase to Docker Compose.
2. Build two or three dashboards over `analytics_gold`.
3. Add screenshots under `docs/assets/`.
4. Add a `PROJECT_SUMMARY.md`.
5. Refactor monthly processing to dynamic task mapping.
