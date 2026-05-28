/* ============================================================================
   MODEL:   s_dim_lga
   PURPOSE: Distinct LGA list for timestamp-based snapshotting.
   INPUT:   silver.s_bridge_lga_suburb
   GRAIN:   1 row per lga_code
   ============================================================================ */
{{ config(materialized='view') }}

SELECT DISTINCT
    lga_code,
    lga_name,
    -- NSW LGA reference data is static in this project. Use a stable source
    -- effective timestamp so facts from 2020-2021 can resolve the SCD2 row.
    TIMESTAMP '1900-01-01 00:00:00' AS updated_at
FROM {{ ref('s_bridge_lga_suburb') }}
