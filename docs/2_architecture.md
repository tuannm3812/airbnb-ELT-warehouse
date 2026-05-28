# Architecture

This project implements a local ELT warehouse for analysing Sydney Airbnb listings alongside ABS Census and NSW LGA reference data.

## 2.1 System Components

- Airflow orchestrates loading, transformation, and archiving.
- PostgreSQL stores raw, cleaned, dimensional, and mart tables.
- dbt builds the transformation layer, tests, and snapshots.
- Docker Compose provides a reproducible local development environment.

## 2.2 Data Flow

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

## 2.3 Medallion Layers

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

## 2.4 Orchestration Design

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

## 2.5 dbt Design

The dbt project uses:

- source declarations for Bronze raw tables
- Bronze pass-through views
- Silver typed/cleaned models
- timestamp snapshots for SCD2-style dimension history
- Gold dimensions and monthly fact table
- Gold marts for property, listing-neighbourhood, and host-neighbourhood analysis

## 2.6 SCD2 Design

SCD2 means the warehouse keeps historical versions of a dimension row. Instead
of overwriting a host, property, neighbourhood, or LGA record, dbt snapshots
create validity ranges using:

- `dbt_valid_from`
- `dbt_valid_to`

Dimension snapshots are created only for entities that become Gold dimensions:

- host
- property
- listing neighbourhood
- LGA

The listing-derived dimensions use `updated_at`, derived from `scraped_date`, as
the snapshot timestamp. The LGA reference dimension uses a stable source
effective timestamp because the NSW LGA mapping is static reference data in this
project.

The project does not snapshot the fact table. The fact table is rebuilt from the
cleaned listing observations and resolves dimension keys from the snapshot
history.

The Gold fact table stores only business keys, resolved dimension surrogate
keys, and metrics. It resolves each dimension key with a validity-window join:

```text
fact timestamp >= dbt_valid_from
fact timestamp <  coalesce(dbt_valid_to, open-ended future timestamp)
```

The Gold marts join facts to dimensions through those SCD2-resolved surrogate
keys, so metrics are reported using the dimension values valid for the fact
month rather than blindly using the latest dimension version.

## 2.7 Why Sequential dbt Runs Matter

dbt snapshots record what dbt can observe at run time. For this project, the
Airflow DAG intentionally loads one monthly file, runs dbt, loads the next
monthly file, and runs dbt again.

That sequence lets snapshots capture how dimension values changed month by
month. If every monthly file is loaded first and dbt runs only once at the end,
dbt can build the final models but cannot recreate intermediate snapshot states
that were never observed.

## 2.8 Other SCD Patterns

This project currently uses SCD2 because analytics need to report historical
facts with the dimension values that were valid at the time.

Other SCD patterns could also be applied:

- SCD0: keep immutable attributes that should never change, such as a stable source identifier.
- SCD1: overwrite corrections where history is not useful, such as typo cleanup in display names.
- SCD2: keep full row history with validity windows, as used by the current Gold dimensions.
- SCD3: keep limited previous values in extra columns, useful for small before/after comparisons.
- SCD6: combine SCD1, SCD2, and SCD3 when both current and historical attributes are needed in one dimension.

Recommended extension:

- keep SCD2 for host, property, neighbourhood, and LGA dimensions
- use SCD1-style cleanup in Silver for standardised text fields
- consider SCD0 for source IDs and stable Census reference attributes

## 2.9 Local Runtime

Docker Compose starts:

- `postgres`: Airflow metadata database and warehouse database
- `airflow-webserver`: Airflow UI
- `airflow-scheduler`: DAG scheduler and local dbt runner
- `airflow-init`: one-time metadata setup and local admin user creation

## 2.10 Current Tradeoffs

- Local runs prioritize reproducibility over production-grade security.
- Raw CSV files are staged locally and ignored by Git.
- The monthly processing loop is stable and simple, but less visually granular than Airflow dynamic task mapping.
- dbt snapshots are reset during clean local bootstrap runs so reruns are deterministic.
