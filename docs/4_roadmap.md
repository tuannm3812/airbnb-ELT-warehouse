# Roadmap

This roadmap turns the project from a working local warehouse into a stronger personal data engineering portfolio project.

## 4.1 Current State

Completed:

- Docker-based local Airflow + PostgreSQL + dbt setup.
- Metabase service added to the local Docker stack.
- Local source-file staging without Google Cloud Storage.
- Full Airflow pipeline run.
- Local dbt build with tests and snapshots.
- Smoke test for latest run and key warehouse outputs.
- Numbered project documentation.

## 4.2 Next Priorities

### 4.2.1 Build The BI Demo

Goal: make the warehouse useful beyond SQL queries.

Recommended next step:

- Build the first Metabase dashboard from `analytics_gold` marts.
- Save dashboard screenshots under `docs/assets/`.
- Add the strongest screenshots to the README and project summary.

### 4.2.2 Make The DAG More Observable

Goal: show each monthly load and dbt run clearly in Airflow.

Options:

- Use Airflow dynamic task mapping for monthly file processing.
- Keep runtime discovery but emit stronger per-month logs and summary metrics.

Recommended next step:

- Add a run summary table or task log summary first.
- Refactor to dynamic task mapping after the current baseline is fully documented.

### 4.2.3 Improve The BI Layer

Goal: turn the first dashboard into a stronger analytics product.

Ideas:

- Add dashboard filters for month, neighbourhood, property type, and room type.
- Add saved questions for supply, revenue, price, and host concentration.
- Compare Airbnb supply with Census demographic indicators.

Recommended next step:

- Keep Metabase first because it is quick to wire to PostgreSQL and strong for portfolio screenshots.

### 4.2.4 Add Data Quality Contracts

Goal: make quality expectations explicit.

Ideas:

- Add relationships tests from fact to dimensions.
- Add accepted-value tests for Boolean flags.
- Add monthly row count checks.
- Add source freshness checks where a source date is available.

### 4.2.5 Add CI

Goal: verify the project on every push.

Ideas:

- `dbt parse`
- SQL linting
- Python syntax check for DAGs
- optional Docker Compose smoke test on selected branches

### 4.2.6 Publish Portfolio Story

Goal: make the project easy to understand by recruiters or collaborators.

Deliverables:

- architecture diagram refresh
- dashboard screenshots
- final `PROJECT_SUMMARY.md`
- short LinkedIn/GitHub project writeup

## 4.3 Suggested Immediate Sequence

1. Build the first Metabase dashboard over `analytics_gold`.
2. Add screenshots under `docs/assets/`.
3. Add a `PROJECT_SUMMARY.md`.
4. Add more dbt tests for fact-to-dimension relationships.
5. Refactor monthly processing to dynamic task mapping.
