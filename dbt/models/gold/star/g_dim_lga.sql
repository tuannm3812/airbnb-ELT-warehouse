/* ============================================================================
   MODEL:  g_dim_lga
   PURPOSE: SCD2 Dimension for Local Government Areas.
   INPUT:   silver.snap_lga
   GRAIN:   1 row per (lga_code, dbt_valid_from)
   ============================================================================ */
{{ config(materialized='table') }}

SELECT
    -- Surrogate Key
    MD5(lga_code::text || '|' || dbt_valid_from::text) AS lga_dim_id,

    -- Natural Keys & Attributes
    lga_code,
    lga_name,

    -- SCD Metadata
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('snap_lga') }}
