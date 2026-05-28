#!/usr/bin/env bash
set -euo pipefail

DAG_ID="${1:-airbnb_census_monthly_pipeline}"
MIN_BRONZE_ROWS="${MIN_BRONZE_ROWS:-1}"
MIN_GOLD_FACT_ROWS="${MIN_GOLD_FACT_ROWS:-1}"

latest_run_sql="
select run_id || '|' || state
from dag_run
where dag_id = '${DAG_ID}'
order by execution_date desc
limit 1;
"

latest_run="$(
  docker compose exec -T postgres \
    psql -U postgres -d airflow -Atc "$latest_run_sql"
)"

if [ -z "$latest_run" ]; then
  echo "No DAG runs found for ${DAG_ID}" >&2
  exit 1
fi

latest_run_id="${latest_run%%|*}"
latest_state="${latest_run##*|}"

echo "Latest ${DAG_ID} run: ${latest_run_id} (${latest_state})"

if [ "$latest_state" != "success" ]; then
  echo "Latest DAG run is not successful." >&2
  exit 1
fi

counts_sql="
select 'bronze.airbnb_listings_raw', count(*) from bronze.airbnb_listings_raw
union all
select 'analytics_silver.s_listings_clean', count(*) from analytics_silver.s_listings_clean
union all
select 'analytics_gold.g_fact_listing_monthly', count(*) from analytics_gold.g_fact_listing_monthly
union all
select 'analytics_gold.g_dim_host', count(*) from analytics_gold.g_dim_host
union all
select 'analytics_gold.g_dim_property', count(*) from analytics_gold.g_dim_property
order by 1;
"

echo
docker compose exec -T postgres \
  psql -U postgres -d airbnb_census -c "$counts_sql"

bronze_rows="$(
  docker compose exec -T postgres \
    psql -U postgres -d airbnb_census -Atc "select count(*) from bronze.airbnb_listings_raw;"
)"

gold_fact_rows="$(
  docker compose exec -T postgres \
    psql -U postgres -d airbnb_census -Atc "select count(*) from analytics_gold.g_fact_listing_monthly;"
)"

if [ "$bronze_rows" -lt "$MIN_BRONZE_ROWS" ]; then
  echo "Bronze row count is too low: ${bronze_rows}" >&2
  exit 1
fi

if [ "$gold_fact_rows" -lt "$MIN_GOLD_FACT_ROWS" ]; then
  echo "Gold fact row count is too low: ${gold_fact_rows}" >&2
  exit 1
fi

echo
echo "Smoke test passed."
