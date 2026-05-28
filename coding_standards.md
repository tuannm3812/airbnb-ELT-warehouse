# Coding Standards

These standards keep the Airflow + dbt repo predictable and reviewable as it grows.

## 1) General
- Keep changes focused and minimal to reduce coupling.
- Use explicit names over abbreviations.
- Prefer configuration via environment variables or Airflow Variables over hard-coded values.
- Keep one concern per file where possible.

## 2) Python / Airflow
- Python version: 3.9+.
- Follow PEP 8 and keep line length around 100.
- Use type hints for helper functions and task callables where practical.
- Use `logging` for operational messages; avoid silent `print()`.
- Keep constants in `UPPER_CASE` near the top of DAG files.
- Import only what is needed; do not leave unused imports.
- Task arguments should be explicit and documented in comments or docstrings.
- Use `Variable.get` or env vars for environment-specific values.
- Catch and re-raise meaningful errors at orchestration boundaries.

## 3) SQL / dbt
- One logical model = one SQL file.
- Each model should start with a short header comment:
  - purpose
  - grain
  - dependencies
- Prefer readable CTE names and avoid `SELECT *` in final selects.
- Explicitly cast types before metric calculations.
- Keep SQL deterministic and alias business keys clearly.
- Keep snapshots and marts aligned to the medallion layers:
  - bronze → silver → gold.

## 4) Data quality & contracts
- Add/extend dbt tests (`not_null`, `unique`, `relationships`) for key identifiers and expected business keys.
- Normalize path and naming expectations in code comments.
- When schema drift is allowed, document rationale and impact.

## 5) Repo hygiene
- Use ASCII by default for code and docs.
- Keep dependency files (`requirements.txt`) minimal and purposeful.
- When adding new source inputs, update:
  - `README.md` for run instructions
  - the relevant DAG loading logic
  - the relevant dbt source/schema docs

## 6) Review checklist
- DAG runs should be idempotent where practical.
- Airflow tasks should have clear and monotonic dependencies.
- dbt model paths and naming should follow existing `bronze/silver/gold` conventions.
- Any fallback logic should be logged for operational visibility.
