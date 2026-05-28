/* ============================================================================
   MODEL:  g_dim_property
   PURPOSE: SCD2 Dimension for Property Configurations.
   INPUT:   silver.snap_property
   GRAIN:   1 row per (property_nk, dbt_valid_from)
   ============================================================================ */
{{ config(materialized='table') }}

SELECT
    md5(property_nk || '|' || dbt_valid_from::text)     AS property_dim_id,
    property_nk,
    property_type,
    room_type,
    accommodates,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('snap_property') }}
