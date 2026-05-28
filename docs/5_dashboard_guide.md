# Dashboard Guide

This guide turns the local warehouse into a simple BI demo using Metabase.

## Start Metabase

Start the full local stack:

```bash
docker compose up -d
```

Or start only Metabase after PostgreSQL is already running:

```bash
docker compose up -d metabase
```

Open Metabase:

```text
http://localhost:3000
```

On the first launch, create a local Metabase admin account.

## Connect The Warehouse

Add a PostgreSQL database connection in Metabase with:

```text
Display name: Airbnb Census Warehouse
Host: postgres
Port: 5432
Database name: airbnb_census
Username: postgres
Password: postgres
Schema: analytics_gold
```

If Metabase is opened from your browser, the database host is still `postgres`
because Metabase runs inside the same Docker Compose network as PostgreSQL.

## Recommended Tables

Start with the Gold data marts:

- `analytics_gold.dm_listing_neighbourhood`
- `analytics_gold.dm_host_neighbourhood`
- `analytics_gold.dm_property_type`

Use the star schema when you need more control:

- `analytics_gold.g_fact_listing_monthly`
- `analytics_gold.g_dim_host`
- `analytics_gold.g_dim_lga`
- `analytics_gold.g_dim_neighbourhood`
- `analytics_gold.g_dim_property`
- `analytics_gold.g_census_g01`
- `analytics_gold.g_census_g02`

## First Dashboard

Create a dashboard named:

```text
Sydney Airbnb Market Overview
```

Suggested cards:

- Active listings by month: line chart from `dm_listing_neighbourhood`.
- Estimated revenue by neighbourhood: bar chart from `dm_listing_neighbourhood`.
- Median price by neighbourhood: bar chart from `dm_listing_neighbourhood`.
- Superhost rate by neighbourhood: bar chart from `dm_listing_neighbourhood`.
- Revenue per host by host LGA: bar chart from `dm_host_neighbourhood`.
- Property mix by room type: grouped bar chart from `dm_property_type`.

Useful filters:

- `month_year`
- `listing_neighbourhood`
- `property_type`
- `room_type`
- `accommodates`

## Portfolio Screenshots

After the first dashboard is built:

1. Capture the dashboard overview.
2. Capture one drill-down chart for neighbourhood performance.
3. Capture one chart that compares property types.
4. Save screenshots under `docs/assets/`.
5. Reference them from the README or `PROJECT_SUMMARY.md`.

## Reset Metabase

Metabase stores its application data in the local PostgreSQL `metabase`
database. To fully reset local BI state, remove the Compose volumes:

```bash
docker compose down -v
```

Then restart the stack.
