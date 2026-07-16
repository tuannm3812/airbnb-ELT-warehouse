/* ============================================================================
   MODEL:  g_dim_neighbourhood
   PURPOSE: SCD2 Dimension for Listing Neighbourhoods.
   INPUT:   silver.snap_neighbourhood
   GRAIN:   1 row per (neigh_nk, dbt_valid_from)
   ============================================================================ */
{{ config(materialized='table') }}

SELECT
    MD5(neigh_nk || '|' || dbt_valid_from::text) AS neighbourhood_dim_id,
    neigh_nk,
    listing_neighbourhood,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('snap_neighbourhood') }}
