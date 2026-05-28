# Dashboard Guide

This guide turns the local warehouse into a simple BI demo using Metabase.

## 5.1 Start Metabase

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

## 5.2 Connect The Warehouse

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

## 5.3 Recommended Tables

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

## 5.4 First Dashboard

Create a dashboard named:

```text
Sydney Airbnb Market Overview
```

Suggested cards:

- Active listings by month: line chart from `g_fact_listing_monthly`.
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

For active listing trend charts, use:

```text
Table: analytics_gold.g_fact_listing_monthly
Filter: active_flag = true
Summarize: count of distinct listing_id
Group by: fact_month
Visualization: line chart
```

## 5.5 Metabase Card Plan

Use this table as the build list when you return to the dashboard.

| Card | Source table | Filter | Summarize / group by | Visualization |
| --- | --- | --- | --- | --- |
| KPI: active listings | `g_fact_listing_monthly` | `active_flag = true` and optional `fact_month` | Count distinct `listing_id` | Number |
| KPI: estimated revenue | `g_fact_listing_monthly` | optional `fact_month` | Sum `revenue_active` | Number |
| Active listings trend | `g_fact_listing_monthly` | `active_flag = true` | Count distinct `listing_id` by `fact_month` | Line |
| Top neighbourhood revenue | `dm_listing_neighbourhood` | latest or selected `month_year` | Average `avg_estimated_revenue_per_active_listing` by `listing_neighbourhood` | Bar |
| Neighbourhood median price | `dm_listing_neighbourhood` | latest or selected `month_year` | Average `median_price` by `listing_neighbourhood` | Bar |
| Superhost rate | `dm_listing_neighbourhood` | latest or selected `month_year` | Average `superhost_rate` by `listing_neighbourhood` | Bar |
| Revenue per host LGA | `dm_host_neighbourhood` | optional `month_year` | Average `estimated_revenue_per_host` by `host_neighbourhood_lga` | Bar |
| Property type performance | `dm_property_type` | optional `month_year` | Average `avg_estimated_revenue_per_active_listing` by `property_type`, `room_type` | Grouped bar |
| Occupancy proxy by property | `dm_property_type` | optional `month_year` | Sum `total_number_of_stays` by `property_type`, `room_type`, `accommodates` | Bar |
| Raw monthly fact table | `g_fact_listing_monthly` | optional `fact_month`, `active_flag` | No summarize | Table |

Recommended dashboard filters:

- `month_year` or `fact_month`
- `listing_neighbourhood`
- `host_neighbourhood_lga`
- `property_type`
- `room_type`
- `accommodates`

## 5.6 Portfolio Screenshots

After the first dashboard is built:

1. Capture the dashboard overview.
2. Capture one drill-down chart for neighbourhood performance.
3. Capture one chart that compares property types.
4. Save screenshots under `docs/assets/screenshots/`.
5. Reference them from the README or `PROJECT_SUMMARY.md`.

## 5.7 Reset Metabase

Metabase stores its application data in the local PostgreSQL `metabase`
database. To fully reset local BI state, remove the Compose volumes:

```bash
docker compose down -v
```

Then restart the stack.
