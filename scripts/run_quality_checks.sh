#!/usr/bin/env bash
set -euo pipefail

echo "Checking shell scripts..."
bash -n scripts/*.sh

echo "Checking Python syntax..."
python3 -m py_compile dags/*.py

if command -v pytest >/dev/null 2>&1; then
  echo "Running Python unit tests..."
  pytest tests/
else
  echo "pytest not found; skipping Python unit tests."
fi

if command -v sqlfluff >/dev/null 2>&1; then
  echo "Running SQL lint..."
  sqlfluff lint dbt/models
else
  echo "sqlfluff not found; skipping SQL lint."
fi

if command -v dbt >/dev/null 2>&1; then
  echo "Running dbt parse with local project..."
  DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$(pwd)/docker/dbt}" \
    dbt parse --project-dir dbt --profiles-dir "${DBT_PROFILES_DIR:-$(pwd)/docker/dbt}"
else
  echo "dbt command not found; skipping dbt parse."
fi

if [ "${RUN_DOCKER_DBT_BUILD:-false}" = "true" ]; then
  echo "Running Docker dbt build..."
  docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt build"
fi

if [ "${RUN_PIPELINE_SMOKE_TEST:-false}" = "true" ]; then
  echo "Running pipeline smoke test..."
  ./scripts/check_pipeline_outputs.sh
fi

echo "Quality checks completed."
