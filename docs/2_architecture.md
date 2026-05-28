# Architecture

This project implements a local ELT warehouse for analysing Sydney Airbnb listings alongside ABS Census and NSW LGA reference data.

## System Components

- Airflow orchestrates loading, transformation, and archiving.
- PostgreSQL stores raw, cleaned, dimensional, and mart tables.
- dbt builds the transformation layer, tests, and snapshots.
- Docker Compose provides a reproducible local development environment.

## Data Flow

```text
CSV ZIP files
  -> local data folders
  -> Airflow Bronze load
  -> dbt Bronze views
  -> dbt Silver clean models
  -> dbt snapshots
  -> dbt Gold star schema
  -> dbt Gold data marts
  -> SQL analysis queries / BI layer
```

## Medallion Layers

Bronze:

- Raw source-aligned tables.
- All fields are loaded as text where practical.
- The goal is ingestion resilience.

Silver:

- Type casting, cleanup, normalization, and deduplication.
- Bridge logic maps suburbs to LGAs.
- This layer creates reusable conformed entities.

Gold:

- Star schema and reporting marts.
- Includes facts, dimensions, Census reference tables, and business-facing aggregate views.

## Orchestration Design

The main Airflow DAG is:

```text
airbnb_census_monthly_pipeline
```

It performs:

- schema bootstrap
- baseline reference load
- baseline dbt build
- sequential monthly listing loads
- dbt build after each monthly load
- processed-file archiving

Monthly files are discovered at task runtime so archiving files does not mutate the DAG graph while a run is active.

## dbt Design

The dbt project uses:

- source declarations for Bronze raw tables
- Bronze pass-through views
- Silver typed/cleaned models
- timestamp snapshots for SCD2-style dimension history
- Gold dimensions and monthly fact table
- Gold marts for property, listing-neighbourhood, and host-neighbourhood analysis

## SCD2 Design

Dimension snapshots are created only for entities that become Gold dimensions:
host, property, listing neighbourhood, and LGA.

The listing-derived dimensions use `updated_at`, derived from `scraped_date`, as
the snapshot timestamp. The LGA reference dimension uses a stable source
effective timestamp because the NSW LGA mapping is static reference data in this
project.

The Gold fact table stores only business keys, resolved dimension surrogate
keys, and metrics. It resolves each dimension key with a validity-window join:

```text
fact timestamp >= dbt_valid_from
fact timestamp <  coalesce(dbt_valid_to, open-ended future timestamp)
```

The Gold marts join facts to dimensions through those SCD2-resolved surrogate
keys, so metrics are reported using the dimension values valid for the fact
month rather than blindly using the latest dimension version.

## Local Runtime

Docker Compose starts:

- `postgres`: Airflow metadata database and warehouse database
- `airflow-webserver`: Airflow UI
- `airflow-scheduler`: DAG scheduler and local dbt runner
- `airflow-init`: one-time metadata setup and local admin user creation

## Current Tradeoffs

- Local runs prioritize reproducibility over production-grade security.
- Raw CSV files are staged locally and ignored by Git.
- The monthly processing loop is stable and simple, but less visually granular than Airflow dynamic task mapping.
- dbt snapshots are reset during clean local bootstrap runs so reruns are deterministic.
