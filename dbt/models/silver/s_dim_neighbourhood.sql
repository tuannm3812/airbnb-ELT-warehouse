/* ============================================================================
  MODEL:   s_dim_neighbourhood
  PURPOSE: Current-state listing neighbourhood.
  INPUT:   silver.s_listings_clean
  OUTPUT:  silver.s_dim_neighbourhood
  GRAIN:   1 row per neigh_nk (latest updated_at)
============================================================================ */
{{ config(materialized='view') }}

WITH src AS (
    SELECT
        MD5(UPPER(TRIM(listing_neighbourhood))) AS neigh_nk,
        listing_neighbourhood,
        updated_at
    FROM {{ ref('s_listings_clean') }}
    WHERE listing_neighbourhood IS NOT NULL
),

latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY neigh_nk
            ORDER BY updated_at DESC
        ) AS rn
    FROM src
)

SELECT
    neigh_nk,
    listing_neighbourhood,
    updated_at
FROM latest
WHERE rn = 1
