# Airbnb & Census ELT Warehouse

![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-orange?logo=apache-airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Core%20%7C%20Cloud-FF694B?logo=dbt&logoColor=white)
![Postgres](https://img.shields.io/badge/PostgreSQL-13+-336791?logo=postgresql&logoColor=white)

An end-to-end ELT pipeline for analysing Sydney Airbnb listings alongside Australian Census demographic data. The project uses Airflow for orchestration, PostgreSQL as the warehouse, and dbt to model the data through Bronze, Silver, and Gold layers.

![Header Image](https://www.realestate.com.au/news-image/w_1280,h_720/v1743109398/news-lifestyle-content-assets/wp-content/production/capi_66e50ad6861c43dbf0bfbe364f663d5f_e58997b3f701d49d4cb6291f6204b1e1.jpeg?_i=AA)

![Architecture Diagram](docs/architecture_flow.png)

## Overview

The warehouse combines monthly Airbnb listing extracts with ABS Census G01/G02 data and NSW LGA mapping files. It produces a dimensional model and analytics marts for rental-market performance, host concentration, neighbourhood demand, and mortgage-affordability analysis.

Core design choices:

- **Medallion architecture:** raw Bronze ingestion, cleaned Silver models, and Gold star-schema/reporting marts.
- **Idempotent ingestion:** bootstrap loads recreate the Bronze schema, while monthly listing files are appended chronologically.
- **Slowly changing dimensions:** dbt snapshots track history for hosts, properties, neighbourhoods, and LGAs.
- **Sequential monthly processing:** Airflow triggers dbt after each monthly load so snapshot validity remains aligned with the source timeline.

## Repository Structure

```text
.
|-- dags/
|   |-- airbnb_census_pipeline.py     # Main Airflow pipeline for initial and monthly loads
|   `-- initial_bronze_load.py        # Standalone Bronze bootstrap DAG
|-- dbt/
|   |-- models/
|   |   |-- bronze/                   # Source-facing raw models
|   |   |-- silver/                   # Cleaning, typing, deduplication, and bridge models
|   |   `-- gold/                     # Star schema and analytics marts
|   |-- snapshots/                    # SCD Type 2 snapshot definitions
|   `-- dbt_project.yml
|-- docs/
|   |-- architecture_flow.png
|   `-- airbnb_census_warehouse_report.pdf
|-- sql/
|   |-- init_bronze_schema.sql        # Warehouse schema bootstrap DDL
|   `-- analysis_queries.sql          # Business analysis query pack
|-- requirements.txt
`-- README.md
```

## Setup

Prerequisites:

- Google Cloud Composer or an Airflow 2.x environment.
- PostgreSQL warehouse connection available to Airflow as `postgres`.
- dbt Cloud job connected to the same PostgreSQL warehouse.
- Raw data staged under `/home/airflow/gcs/data` with `airbnb`, `census/G01`, `census/G02`, and `mappings` subdirectories.

Airflow variables used by the main pipeline:

- `RUN_INITIAL_LOAD`: set to `true` to run the bootstrap load before monthly processing. The legacy `RUN_PART1` variable is still supported as a fallback.
- `DBT_CLOUD_URL`: defaults to `cloud.getdbt.com`.
- `DBT_CLOUD_ACCOUNT_ID`
- `DBT_CLOUD_JOB_ID`
- `DBT_CLOUD_API_TOKEN`
- `DBT_CLOUD_WAIT_TIMEOUT_SEC`: defaults to `3600`.

Install Python dependencies in the Airflow environment:

```bash
pip install -r requirements.txt
```

## Running The Pipeline

1. Deploy the contents of `dags/` to Airflow.
2. Deploy the dbt project in `dbt/` to dbt Cloud or your dbt runner.
3. Stage the source CSV files in the expected GCS-mounted data folders.
4. Trigger the `airbnb_census_monthly_pipeline` DAG.

The main DAG bootstraps the Bronze schema with `sql/init_bronze_schema.sql`, loads the May 2020 baseline data and reference files, runs dbt, then processes the remaining monthly Airbnb extracts in chronological order. Each monthly load appends to `bronze.airbnb_listings_raw`, triggers dbt, and archives the processed CSV.

## Data Model

- **Bronze:** raw CSV-aligned models for Airbnb listings, Census G01/G02, NSW LGA codes, and suburb mappings.
- **Silver:** cleaned listing records, normalized dimensions, and suburb-to-LGA bridge logic.
- **Snapshots:** SCD Type 2 history for host, property, neighbourhood, and LGA entities.
- **Gold:** monthly listing fact table, conformed dimensions, Census reference models, and data marts for host, listing-neighbourhood, and property-type performance.

## Analysis Queries

Run `sql/analysis_queries.sql` against the populated warehouse to answer:

- Which LGAs have the strongest and weakest revenue per active listing, and how do their demographics differ?
- How does median age correlate with revenue per active listing?
- Which property configurations perform best in the top revenue neighbourhoods?
- Are multi-listing hosts concentrated within a single LGA or distributed across several LGAs?
- How often can single-listing hosts cover annualized median mortgage repayments from Airbnb revenue?

## Notes

The project keeps raw Bronze fields as text for resilient ingestion and performs type casting in dbt. This makes source-file drift easier to isolate while keeping analytics models strongly typed.
