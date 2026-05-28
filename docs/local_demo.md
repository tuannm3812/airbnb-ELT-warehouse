# Local Demo Runbook

This guide shows how to run the full Airbnb + Census ELT warehouse locally with Docker, Airflow, PostgreSQL, and dbt Core.

## What This Demo Proves

- Raw CSV files can be staged locally without Google Cloud Storage.
- Airflow can bootstrap Bronze, process monthly files, and archive inputs.
- dbt Core can build Bronze, Silver, Gold, snapshots, tests, and marts.
- PostgreSQL contains usable analytics outputs after the run.

## 1. Start Docker Stack

```bash
docker compose up airflow-init
docker compose up -d
```

Open Airflow:

```text
http://localhost:8080
```

Local credentials:

```text
username: admin
password: admin
```

## 2. Stage Source Files

If the AT3 ZIP files are in the default Google Drive folder:

```bash
./scripts/stage_at3_data.sh
```

If they are somewhere else:

```bash
./scripts/stage_at3_data.sh "/path/to/bde_AT3" data
```

Expected files:

- `data/airbnb/05_2020.csv`
- `data/airbnb/06_2020.csv` through `data/airbnb/04_2021.csv`
- `data/census/G01/2016Census_G01_NSW_LGA.csv`
- `data/census/G02/2016Census_G02_NSW_LGA.csv`
- `data/mappings/NSW_LGA_CODE.csv`
- `data/mappings/NSW_LGA_SUBURB.csv`

## 3. Run Airflow Pipeline

In Airflow, trigger:

```text
airbnb_census_monthly_pipeline
```

The Docker setup uses:

```text
DBT_RUN_MODE=local
```

That means Airflow runs `dbt build` inside the Airflow container after the baseline load and after each monthly load.

## 4. Check Pipeline Outputs

Run:

```bash
./scripts/check_pipeline_outputs.sh
```

Expected result:

```text
Smoke test passed.
```

The smoke test checks:

- latest Airflow run state is `success`
- `bronze.airbnb_listings_raw` has rows
- `analytics_silver.s_listings_clean` has rows
- `analytics_gold.g_fact_listing_monthly` has rows
- key Gold dimensions are populated

## 5. Run dbt Directly

Use this when changing dbt models or tests:

```bash
docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt build"
```

Use Airflow for full orchestration testing. Use direct dbt builds for faster model-layer development.

## 6. Stop The Stack

```bash
docker compose down
```

To remove local Postgres and Airflow state too:

```bash
docker compose down -v
```
