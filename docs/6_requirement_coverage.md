# Requirement Coverage

This document maps the original project brief to the current local-first repo
state. It is useful when reviewing completeness, even though the repository is
now maintained as a personal portfolio project.

## 6.1 Environment Requirement

Original environment requirement:

- Cloud Composer
- Cloud SQL for PostgreSQL
- dbt Cloud

Current project implementation:

- Docker Compose Airflow
- Docker Compose PostgreSQL
- dbt Core running inside the Airflow image
- Optional dbt Cloud mode still supported by Airflow variables

Status:

- Functionally complete as a local-first personal project.
- Not a literal GCP deployment unless the project is deployed to Cloud Composer,
  Cloud SQL, and dbt Cloud.

## 6.2 Source Datasets

Status: complete locally.

The source ZIP files are staged with:

```bash
./scripts/stage_source_data.sh "/path/to/source-zips" data
```

Expected staged files:

- `data/airbnb/05_2020.csv` through `data/airbnb/04_2021.csv`
- `data/census/G01/2016Census_G01_NSW_LGA.csv`
- `data/census/G02/2016Census_G02_NSW_LGA.csv`
- `data/mappings/NSW_LGA_CODE.csv`
- `data/mappings/NSW_LGA_SUBURB.csv`

## 6.3 Airflow Bronze Load

Status: complete locally.

The main DAG is:

```text
airbnb_census_monthly_pipeline
```

It has no schedule and is manually triggered. It creates/resets the Bronze
schema, loads May 2020 Airbnb data, loads Census G01/G02, loads NSW LGA mapping
files, and archives processed files.

The local filesystem replaces the Cloud Composer storage bucket.

## 6.4 dbt Warehouse

Status: complete.

Implemented layers:

- Bronze dbt views over raw Airflow-loaded tables.
- Silver cleaned models and dimension-prep models.
- Silver dbt snapshots for dimension history.
- Gold star schema and datamarts.

Gold dimensions:

- `analytics_gold.g_dim_host`
- `analytics_gold.g_dim_property`
- `analytics_gold.g_dim_neighbourhood`
- `analytics_gold.g_dim_lga`

Gold Census reference tables:

- `analytics_gold.g_census_g01`
- `analytics_gold.g_census_g02`

Gold fact table:

- `analytics_gold.g_fact_listing_monthly`

Datamart views:

- `analytics_gold.dm_listing_neighbourhood`
- `analytics_gold.dm_property_type`
- `analytics_gold.dm_host_neighbourhood`

SCD2 handling:

- Snapshots use timestamp strategy.
- Listing-derived snapshots use `updated_at` from `scraped_date`.
- LGA uses a stable source-effective timestamp because it is static reference data.
- The fact table resolves SCD2 dimension surrogate keys by validity window.
- Datamarts join through those resolved dimension keys.

## 6.5 Remaining Monthly Loads

Status: complete locally.

The DAG processes remaining monthly Airbnb files in chronological order. After
each monthly append, it runs dbt before archiving the file.

This preserves snapshot order and SCD2 correctness.

## 6.6 Ad-Hoc Analysis

Status: complete.

The five SQL answers are in:

```text
sql/analysis_queries.sql
```

The queries run against the populated local warehouse and cover:

- top and bottom LGA demographics by estimated revenue per active listing
- median age correlation with revenue per active listing
- best listing configuration for the top 5 neighbourhoods
- multi-listing host LGA concentration
- single-listing host mortgage repayment coverage

## 6.7 Current Verification

Latest local checks:

- `dbt build`: passing
- pipeline smoke test: passing
- Gold datamarts return rows
- analysis SQL runs end to end

## 6.8 Optional Cloud Deployment Gap

These are not needed for the local personal-project version, but would be
required to reproduce the original managed-cloud environment:

- deploy the DAGs to Cloud Composer
- upload staged files to the Composer bucket
- provision Cloud SQL PostgreSQL
- configure Airflow's `postgres` connection to Cloud SQL
- deploy the dbt project to dbt Cloud
- set `DBT_RUN_MODE=cloud` and configure dbt Cloud Airflow variables
