# Coding Standards

These standards keep the Airflow + dbt warehouse predictable, reviewable, and
easy to extend as a personal data engineering project.

## 0.1 General

- Keep changes focused and minimal.
- Prefer explicit names over abbreviations.
- Prefer environment variables or Airflow Variables over hard-coded values.
- Keep one concern per file where practical.
- Document assumptions when data behavior is approximate or inferred.

## 0.2 Python And Airflow

- Python version: 3.9+.
- Follow PEP 8.
- Use 4 spaces for indentation.
- Keep lines to 79 characters or fewer where practical.
- Prefer f-strings and small utility functions when they improve readability.
- Use type hints for reusable functions when the type is clear.
- Use Google-style docstrings for reusable functions that carry project logic.
- Use `logging` for operational messages.
- Keep constants in `UPPER_CASE` near the top of DAG files.
- Group imports as standard library, third-party libraries, then local imports.
- Import only what is needed.
- Use Airflow Variables for environment-specific values.
- Raise meaningful errors at orchestration boundaries.

## 0.3 Notebook Style

There are no notebooks in the current production pipeline. For future analysis
or Kaggle-style notebooks, use this structure:

- Add a short purpose statement at the top.
- Add a clear configuration section near the top.
- Define and print a `NOTEBOOK_VERSION` string in the first code cell.
- Use explicit mode flags such as `RUN_FAST`, `FAST_SAMPLE_PLAYS`, and
  `DISTANCE_THRESHOLDS` when a notebook supports fast or sampled runs.
- Keep fixed input paths in one configuration cell. For Kaggle competition
  notebooks, use the relevant fixed `/kaggle/input/...` competition path.
- Add Markdown insight cells after important checks, plots, or metrics.
- Add artifact-writing cells for reusable outputs such as `submission.csv`.
- Prefer readable, self-contained notebook code over imports from local project
  modules when the notebook must run in Kaggle with only input data available.

## 0.4 SQL And dbt

- One logical model equals one SQL file.
- Each model should start with a short header comment covering purpose, grain,
  and dependencies.
- Prefer readable CTE names.
- Avoid `SELECT *` in final model outputs.
- Explicitly cast types before metric calculations.
- Keep snapshots and marts aligned to the medallion layers: Bronze, Silver, Gold.
- Use SCD2 validity-window joins whenever a fact row must resolve historical
  dimension values.
- Do not snapshot facts.

## 0.5 Data Quality

- Add dbt tests for primary keys, foreign keys, and important business grain
  assumptions.
- Use `not_null`, `unique`, and `relationships` tests where appropriate.
- Record why schema drift is allowed when a source is intentionally flexible.
- Add smoke tests for pipeline outputs before claiming a run is healthy.

## 0.6 Repo Hygiene

- Use ASCII by default for code and docs.
- Keep dependency files minimal and purposeful.
- Do not commit raw data files.
- When adding new source inputs, update the DAG, dbt source docs, and run
  instructions together.

## 0.7 Review Checklist

- DAG runs are idempotent or clearly documented when they are not.
- Airflow task dependencies are explicit and monotonic.
- dbt model naming follows the existing `bronze`, `silver`, and `gold` structure.
- Fallback logic is logged.
- README and docs reflect the current way to run the project.
