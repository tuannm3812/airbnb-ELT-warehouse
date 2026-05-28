# Coding Standards

These standards keep the Airflow + dbt warehouse predictable, reviewable, and easy to extend as a personal data engineering project.

## 0.1 General

- Keep changes focused and minimal.
- Prefer explicit names over abbreviations.
- Prefer environment variables or Airflow Variables over hard-coded values.
- Keep one concern per file where practical.
- Document assumptions when data behavior is approximate or inferred.

## 0.2 Python And Airflow

- Python version: 3.9+.
- Follow PEP 8 and keep line length around 100 characters.
- Use type hints for helper functions and task callables where practical.
- Use `logging` for operational messages.
- Keep constants in `UPPER_CASE` near the top of DAG files.
- Import only what is needed.
- Use Airflow Variables for environment-specific values.
- Raise meaningful errors at orchestration boundaries.

## 0.3 SQL And dbt

- One logical model equals one SQL file.
- Each model should start with a short header comment covering purpose, grain, and dependencies.
- Prefer readable CTE names.
- Avoid `SELECT *` in final model outputs.
- Explicitly cast types before metric calculations.
- Keep snapshots and marts aligned to the medallion layers: Bronze, Silver, Gold.
- Use SCD2 validity-window joins whenever a fact row must resolve historical dimension values.
- Do not snapshot facts.

## 0.4 Data Quality

- Add dbt tests for primary keys, foreign keys, and important business grain assumptions.
- Use `not_null`, `unique`, and `relationships` tests where appropriate.
- Record why schema drift is allowed when a source is intentionally flexible.
- Add smoke tests for pipeline outputs before claiming a run is healthy.

## 0.5 Repo Hygiene

- Use ASCII by default for code and docs.
- Keep dependency files minimal and purposeful.
- Do not commit raw data files.
- When adding new source inputs, update the DAG, dbt source docs, and run instructions together.

## 0.6 Review Checklist

- DAG runs are idempotent or clearly documented when they are not.
- Airflow task dependencies are explicit and monotonic.
- dbt model naming follows the existing `bronze`, `silver`, and `gold` structure.
- Fallback logic is logged.
- README and docs reflect the current way to run the project.
