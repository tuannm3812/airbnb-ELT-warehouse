# Instructions

This is the primary local run guide for the Airbnb + Census ELT warehouse.

## 1.1 Prerequisites

- Docker Desktop or Docker Engine with Compose.
- Source ZIP files for Airbnb listings, Census LGA, and NSW LGA mappings.
- A shell from the repository root.

## 1.2 Start The Stack

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

Metabase UI:

```text
http://localhost:3000
```

Local credentials:

```text
username: admin
password: admin
```

Metabase asks you to create a local admin account on first launch.

## 1.3 Stage Source Data

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

## 1.4 Run The Pipeline

In Airflow, trigger:

```text
airbnb_census_monthly_pipeline
```

The local Docker setup runs dbt with:

```text
DBT_RUN_MODE=local
```

This means Airflow executes `dbt build` inside the Airflow container after the baseline load and after each monthly file is appended.

## 1.5 Verify Outputs

```bash
./scripts/check_pipeline_outputs.sh
```

Expected result:

```text
Smoke test passed.
```

## 1.6 Run dbt Directly

Use this while changing dbt models or tests:

```bash
docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt build"
```

Use Airflow for full orchestration testing. Use direct dbt builds for faster transformation-layer development.

## 1.7 SCD2 Snapshot Notes

Use the Airflow DAG for end-to-end SCD2 testing. The DAG loads the May 2020
baseline, runs dbt, then appends each later monthly file and runs dbt again.
That order is intentional: dbt snapshots capture dimension changes as each
monthly extract becomes visible.

The Gold fact table resolves SCD2 dimension keys during the fact build by
joining each fact row to the dimension version that was valid for that row's
month. Gold data marts then join through those resolved surrogate keys.

If you manually rebuild from an already fully loaded Bronze table, dbt can
validate models and tests, but it cannot reconstruct snapshot history that was
never observed through sequential snapshot runs.

## 1.8 Explore Dashboards

After the pipeline and dbt build complete, connect Metabase to the local
warehouse and build dashboards from the Gold marts:

```text
Host: postgres
Port: 5432
Database: airbnb_census
Username: postgres
Password: postgres
Schema: analytics_gold
```

See [5_dashboard_guide.md](5_dashboard_guide.md) for suggested dashboard cards.

## 1.9 Stop The Stack

```bash
docker compose down
```

To remove local database and Airflow state:

```bash
docker compose down -v
```
