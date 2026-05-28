/* ============================================================================
   MODEL:   g_dim_host
   PURPOSE: SCD2 Dimension for Hosts.
   INPUT:   silver.snap_host
   GRAIN:   1 row per (host_id, dbt_valid_from)
   ============================================================================ */
{{ config(materialized='table') }}

SELECT
    md5(host_id::text || '|' || dbt_valid_from::text)  AS host_dim_id,
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('snap_host') }}
