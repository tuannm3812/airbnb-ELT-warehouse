/* ============================================================================
  MODEL:   s_dim_property
  PURPOSE: Current-state property attributes (type/room/accommodates).
  INPUT:   silver.s_listings_clean
  OUTPUT:  silver.s_dim_property
  GRAIN:   1 row per property_nk (latest updated_at)
============================================================================ */
{{ config(materialized='view') }}

WITH src AS (
  SELECT
    -- Create Surrogate Key (NK)
    MD5(
      COALESCE(property_type, 'UNK') || '|' ||
      COALESCE(room_type,     'UNK') || '|' ||
      COALESCE(accommodates::text,  '0')
    ) AS property_nk,
    property_type,
    room_type,
    accommodates,
    updated_at
  FROM {{ ref('s_listings_clean') }}
),

latest AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY property_nk
      ORDER BY updated_at DESC
    ) AS rn
  FROM src
)

SELECT
  property_nk,
  property_type,
  room_type,
  accommodates,
  updated_at
FROM latest
WHERE rn = 1