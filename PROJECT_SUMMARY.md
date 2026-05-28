# Project Summary

## 1. Project Goal

This project builds a local end-to-end ELT warehouse for analysing Sydney Airbnb
listings alongside Australian Census and NSW LGA reference data.

The goal is to demonstrate a practical data engineering workflow:

- orchestrate file ingestion with Airflow
- store raw and transformed data in PostgreSQL
- model Bronze, Silver, and Gold layers with dbt
- preserve dimension history with SCD2 snapshots
- expose business-ready marts for SQL analysis and BI dashboards

## 2. Business Questions

The warehouse supports questions such as:

- Which LGAs generate the strongest estimated revenue per active listing?
- How do top and bottom performing LGAs differ demographically?
- Does median age correlate with Airbnb revenue per active listing?
- Which property configurations perform best in top neighbourhoods?
- Are multi-listing hosts concentrated in one LGA or spread across LGAs?
- Can single-listing hosts cover annualised median mortgage repayments?

## 3. Data Sources

Input data includes:

- 12 monthly Sydney Airbnb listing extracts from May 2020 to April 2021
- ABS Census G01 selected person characteristics at LGA level
- ABS Census G02 selected medians and averages at LGA level
- NSW LGA code and suburb mapping files

Raw files are staged locally under `data/` and are not committed to Git.

## 4. Architecture

The project uses a local-first stack:

- Docker Compose for reproducible services
- Airflow for orchestration
- PostgreSQL for warehouse storage
- dbt Core for transformations, tests, and snapshots
- Metabase for optional dashboarding

The main flow is:

```text
Source ZIP files
  -> local staged CSV files
  -> Airflow Bronze load
  -> dbt Bronze views
  -> dbt Silver cleaned models
  -> dbt snapshots
  -> dbt Gold dimensions, facts, and marts
  -> SQL analysis / Metabase dashboards
```

## 5. dbt Design

The dbt project implements a medallion architecture:

- Bronze: source-facing views over Airflow-loaded raw tables
- Silver: cleaned, typed, deduplicated, and conformed models
- Snapshots: SCD2 history for dimension entities
- Gold: star schema, Census reference tables, and business marts

Gold dimensions:

- `analytics_gold.g_dim_host`
- `analytics_gold.g_dim_property`
- `analytics_gold.g_dim_neighbourhood`
- `analytics_gold.g_dim_lga`

Gold fact:

- `analytics_gold.g_fact_listing_monthly`

Gold marts:

- `analytics_gold.dm_listing_neighbourhood`
- `analytics_gold.dm_property_type`
- `analytics_gold.dm_host_neighbourhood`

## 6. SCD2 Strategy

SCD2 is used because the analysis needs to join each monthly fact row to the
dimension values that were valid at that point in time.

Snapshots are created only for dimension entities:

- `snap_host`
- `snap_property`
- `snap_neighbourhood`
- `snap_lga`

The Airflow DAG loads each monthly Airbnb file in chronological order and runs
dbt after each load. This matters because dbt snapshots can only capture states
that dbt observes at run time.

The Gold fact table resolves SCD2 surrogate keys using validity windows:

```text
fact timestamp >= dbt_valid_from
fact timestamp <  coalesce(dbt_valid_to, open-ended future timestamp)
```

Gold marts then join facts to dimensions through those resolved surrogate keys.

## 7. Verification

Current checks include:

- dbt model tests for primary keys and relationships
- smoke test for latest Airflow DAG state and warehouse row counts
- executable ad-hoc SQL analysis pack
- local dbt build verification

Useful commands:

```bash
docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt build"
./scripts/check_pipeline_outputs.sh
docker compose exec -T postgres psql -U postgres -d airbnb_census -f sql/analysis_queries.sql
```

## 8. Portfolio Extensions

Strong next extensions:

- finish the Metabase dashboard and save screenshots under `docs/assets/screenshots/`
- add dbt docs screenshots or lineage views
- add CI for dbt parsing and syntax checks
- improve Airflow monthly-load observability with task mapping
- publish a short project write-up with findings from the analysis queries
