# dbt Guide

This guide explains what dbt does in this project and how to inspect it locally.

## 7.1 What dbt Owns

dbt owns the transformation layer after Airflow has loaded raw files into
PostgreSQL.

Airflow is responsible for:

- reading CSV files
- loading raw Bronze tables
- running dbt after the baseline load and after each monthly append
- archiving processed files

dbt is responsible for:

- Bronze source-facing views
- Silver cleaning, typing, deduplication, and entity preparation
- dimension snapshots
- Gold dimensions, facts, and marts
- model tests and relationship checks

## 7.2 Project Structure

Important dbt folders:

```text
dbt/
|-- dbt_project.yml
|-- models/
|   |-- bronze/
|   |-- silver/
|   `-- gold/
|-- snapshots/
`-- profiles.example.yml
```

The local Docker profile is mounted from:

```text
docker/dbt/profiles.yml
```

## 7.3 Model Flow

The dbt model flow is:

```text
bronze raw tables
  -> analytics_bronze views
  -> analytics_silver cleaned tables/views
  -> silver snapshot tables
  -> analytics_gold dimensions and fact
  -> analytics_gold datamart views
```

The schemas are intentionally split:

- `bronze`: raw Airflow-loaded tables
- `analytics_bronze`: dbt Bronze views
- `analytics_silver`: dbt Silver cleaned models
- `silver`: dbt snapshot tables
- `analytics_gold`: dbt Gold star schema and marts

## 7.4 SCD2 In dbt

Snapshots live in:

```text
dbt/snapshots/
```

Current snapshots:

- `snap_host`
- `snap_property`
- `snap_neighbourhood`
- `snap_lga`

The listing-derived snapshots use timestamp strategy with:

```text
updated_at = scraped_date::timestamp
```

The LGA snapshot also uses timestamp strategy. Because LGA mapping is static
reference data in this project, Silver assigns a stable source-effective
timestamp:

```text
1900-01-01 00:00:00
```

Gold facts then resolve dimension IDs by joining facts to the snapshot row whose
validity window contains the fact timestamp.

## 7.5 Useful Local Commands

Run all models, snapshots, and tests:

```bash
docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt build"
```

Run only Gold:

```bash
docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt build --select gold"
```

Run only snapshots:

```bash
docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt snapshot"
```

List dbt resources:

```bash
docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt ls"
```

Generate dbt docs:

```bash
docker compose exec -T airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt docs generate"
```

Serve dbt docs locally from inside the container:

```bash
docker compose exec airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt docs serve --host 0.0.0.0 --port 8088"
```

If you want to browse docs from your host browser, expose port `8088` in Docker
Compose or run `dbt docs generate` and inspect the generated files under
`dbt/target/`.

## 7.6 Should You Open dbt?

Yes, but for this local project you do not need dbt Cloud.

Best ways to inspect dbt now:

1. Read the model SQL files under `dbt/models/`.
2. Run `dbt build` after edits.
3. Generate dbt docs when you want lineage and model descriptions.
4. Use PostgreSQL or Metabase to inspect the built tables in `analytics_gold`.

Use dbt Cloud only if you want the project to match the original GCP assignment
environment or you want a hosted scheduler, lineage UI, and job history.

## 7.7 What To Review First

Start with these files:

- `dbt/models/silver/s_listings_clean.sql`
- `dbt/snapshots/snap_host.sql`
- `dbt/snapshots/snap_property.sql`
- `dbt/snapshots/snap_neighbourhood.sql`
- `dbt/snapshots/snap_lga.sql`
- `dbt/models/gold/star/g_fact_listing_monthly.sql`
- `dbt/models/gold/mart/dm_listing_neighbourhood.sql`
- `dbt/models/gold/mart/dm_property_type.sql`
- `dbt/models/gold/mart/dm_host_neighbourhood.sql`

These files show the main logic: cleaning, SCD2 capture, fact construction, and
business-facing marts.
