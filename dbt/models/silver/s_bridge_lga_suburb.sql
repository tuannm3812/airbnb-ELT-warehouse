/* ============================================================================
   MODEL:    s_bridge_lga_suburb
   PURPOSE:  Clean bridge (LGA_CODE ↔ SUBURB_NAME) using code master + suburb map.
   INPUT:    bronze.b_nsw_lga_code, bronze.b_nsw_lga_suburb
   OUTPUT:   silver.s_bridge_lga_suburb
   GRAIN:    1 row per (lga_code, suburb_name)
   ============================================================================ */
{{ config(materialized='table') }}

WITH lga_codes AS (
  SELECT
    lga_code,
    lga_name,
    -- Normalize for joining
    UPPER(REGEXP_REPLACE(TRIM(lga_name), '\s+', ' ', 'g')) AS lga_name_norm
  FROM {{ ref('b_nsw_lga_code') }}
),

lga_suburbs AS (
  SELECT
    suburb_name,
    UPPER(REGEXP_REPLACE(TRIM(lga_name), '\s+', ' ', 'g')) AS lga_name_norm
  FROM {{ ref('b_nsw_lga_suburb') }}
)

SELECT
  c.lga_code,
  c.lga_name,
  s.suburb_name
FROM lga_suburbs s
INNER JOIN lga_codes c ON s.lga_name_norm = c.lga_name_norm