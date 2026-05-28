# Operations

This document captures everyday commands for running, checking, and resetting the local warehouse.

## Start Services

```bash
docker compose up -d
```

If this is the first run:

```bash
docker compose up airflow-init
docker compose up -d
```

## Check Services

```bash
docker compose ps
```

Expected healthy services:

- `airbnb-postgres`
- `airbnb-airflow-webserver`
- `airbnb-airflow-scheduler`

## Trigger Pipeline From CLI

```bash
docker compose exec -T airflow-scheduler airflow dags trigger airbnb_census_monthly_pipeline
```

## Check Latest Pipeline Run

```bash
docker compose exec -T airflow-scheduler airflow dags list-runs -d airbnb_census_monthly_pipeline --no-backfill
```

## Run Smoke Test

```bash
./scripts/check_pipeline_outputs.sh
```

## Run dbt Build

```bash
docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt build"
```

## Query Warehouse

```bash
docker compose exec -T postgres psql -U postgres -d airbnb_census
```

Useful row count query:

```sql
select 'bronze.airbnb_listings_raw' as relation, count(*) from bronze.airbnb_listings_raw
union all
select 'analytics_silver.s_listings_clean', count(*) from analytics_silver.s_listings_clean
union all
select 'analytics_gold.g_fact_listing_monthly', count(*) from analytics_gold.g_fact_listing_monthly
union all
select 'analytics_gold.g_dim_host', count(*) from analytics_gold.g_dim_host
union all
select 'analytics_gold.g_dim_property', count(*) from analytics_gold.g_dim_property;
```

## Restage Source Data

```bash
./scripts/stage_at3_data.sh
```

This unpacks the source ZIPs into `data/` and removes macOS resource-fork files.

## Stop Services

```bash
docker compose down
```

## Full Reset

Use this when you want a fresh local database:

```bash
docker compose down -v
docker compose up airflow-init
docker compose up -d
./scripts/stage_at3_data.sh
docker compose exec -T airflow-scheduler airflow dags trigger airbnb_census_monthly_pipeline
```

## Troubleshooting

If Airflow cannot see DAG changes:

```bash
docker compose restart airflow-scheduler airflow-webserver
```

If a clean rerun fails because source files were archived:

```bash
./scripts/stage_at3_data.sh
```

If dbt tests fail after manual experiments, perform a full reset or rerun the main pipeline with `RUN_INITIAL_LOAD=true`.
