/* ============================================================================
   MODEL:   s_dim_lga
   PURPOSE: Distinct LGA list for snapshotting.
   INPUT:   silver.s_bridge_lga_suburb
   GRAIN:   1 row per lga_code
   ============================================================================ */
{{ config(materialized='view') }}

SELECT DISTINCT
    lga_code,
    lga_name
FROM {{ ref('s_bridge_lga_suburb') }}