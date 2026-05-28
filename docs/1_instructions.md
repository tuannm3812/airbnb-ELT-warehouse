# Instructions

This is the primary local run guide for the Airbnb + Census ELT warehouse.

## Prerequisites

- Docker Desktop or Docker Engine with Compose.
- Source ZIP files for Airbnb listings, Census LGA, and NSW LGA mappings.
- A shell from the repository root.

## Start The Stack

```bash
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose up airflow-init
docker compose up -d
```

Airflow UI:

```text
http://localhost:8080
```

Local credentials:

```text
username: admin
password: admin
```

## Stage Source Data

Pass the folder that contains the source ZIP files:

```bash
./scripts/stage_at3_data.sh "/path/to/source-zips" data
```

Alternatively set `SOURCE_ARCHIVE_DIR`:

```bash
export SOURCE_ARCHIVE_DIR="/path/to/source-zips"
./scripts/stage_at3_data.sh
```

Expected input layout:

- `data/airbnb/05_2020.csv`
- `data/airbnb/06_2020.csv` through `data/airbnb/04_2021.csv`
- `data/census/G01/2016Census_G01_NSW_LGA.csv`
- `data/census/G02/2016Census_G02_NSW_LGA.csv`
- `data/mappings/NSW_LGA_CODE.csv`
- `data/mappings/NSW_LGA_SUBURB.csv`

## Run The Pipeline

In Airflow, trigger:

```text
airbnb_census_monthly_pipeline
```

The local Docker setup runs dbt with:

```text
DBT_RUN_MODE=local
```

This means Airflow executes `dbt build` inside the Airflow container after the baseline load and after each monthly file is appended.

## Verify Outputs

```bash
./scripts/check_pipeline_outputs.sh
```

Expected result:

```text
Smoke test passed.
```

## Run dbt Directly

Use this while changing dbt models or tests:

```bash
docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt build"
```

Use Airflow for full orchestration testing. Use direct dbt builds for faster transformation-layer development.

## Stop The Stack

```bash
docker compose down
```

To remove local database and Airflow state:

```bash
docker compose down -v
```
