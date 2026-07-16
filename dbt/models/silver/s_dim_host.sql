/* ============================================================================
  MODEL:   s_dim_host
  PURPOSE: Current-state host rows (one row per host_id).
  INPUT:   silver.s_listings_clean
  OUTPUT:  silver.s_dim_host
  GRAIN:   1 row per host_id (latest updated_at)
============================================================================ */
{{ config(materialized='view') }}

WITH src AS (
    SELECT
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        host_neighbourhood,
        updated_at
    FROM {{ ref('s_listings_clean') }}
    WHERE host_id IS NOT NULL
),

latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY host_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM src
)

SELECT
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    updated_at
FROM latest
WHERE rn = 1
