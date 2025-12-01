/* ============================================================================
  MODEL:   g_fact_listing_monthly
  PURPOSE: Monthly fact table with specific keys for Host Loc and Listing Loc.
  INPUTS:  silver.s_listings_clean
           silver.s_bridge_lga_suburb
           gold.g_dim_*
  OUTPUT:  gold.g_fact_listing_monthly
  GRAIN:   1 row per (listing_id, month)
============================================================================ */
{{ config(materialized='table') }}

WITH base AS (
    SELECT
        listing_id,
        host_id,
        updated_at,
        host_neighbourhood,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        has_availability,
        availability_30,
        price,
        review_scores_rating
    FROM {{ ref('s_listings_clean') }}
),

agg AS (
    SELECT
        listing_id,
        DATE_TRUNC('month', updated_at)::date   AS fact_month,
        MIN(updated_at)                         AS min_fact_ts, -- Anchor for SCD2
        
        -- Measures
        BOOL_OR(has_availability)               AS active_flag,
        AVG(price)::numeric(10,2)               AS avg_price,
        AVG(review_scores_rating)::numeric(5,2) AS avg_review_rating,
        
        SUM(CASE WHEN has_availability THEN (30 - COALESCE(availability_30,0)) ELSE 0 END) AS total_stays_active,
        
        SUM(CASE WHEN has_availability THEN (30 - COALESCE(availability_30,0)) * COALESCE(price,0) ELSE 0 END)::numeric(12,2) AS revenue_active,

        -- Degenerate Dimensions (take arbitrary valid value for the month)
        MAX(host_id)               AS host_id,
        MAX(property_type)         AS property_type,
        MAX(room_type)             AS room_type,
        MAX(accommodates)          AS accommodates,
        MAX(listing_neighbourhood) AS listing_neighbourhood,
        MAX(host_neighbourhood)    AS host_neighbourhood
    FROM base
    GROUP BY listing_id, DATE_TRUNC('month', updated_at)
),

-- Prepare Neighborhoods for Matching
prep AS (
    SELECT *,
        UPPER(REGEXP_REPLACE(TRIM(host_neighbourhood), '\s+', ' ', 'g')) AS host_norm,
        UPPER(REGEXP_REPLACE(TRIM(listing_neighbourhood), '\s+', ' ', 'g')) AS list_norm
    FROM agg
),

bridge AS (
    SELECT 
        UPPER(REGEXP_REPLACE(TRIM(suburb_name), '\s+', ' ', 'g')) AS suburb_norm,
        lga_code
    FROM {{ ref('s_bridge_lga_suburb') }}
)

SELECT
    p.listing_id,
    p.fact_month,
    
    -- 1. Listing Neighbourhood Dimension
    COALESCE(dn.neighbourhood_dim_id, '-1') AS neighbourhood_dim_id,
    
    -- 2. Property Dimension
    COALESCE(dp.property_dim_id, '-1')      AS property_dim_id,
    
    -- 3. Host Dimension
    COALESCE(dh.host_dim_id, '-1')          AS host_dim_id,
    
    -- 4. Listing LGA Dimension (Where the house is)
    COALESCE(dlg_list.lga_dim_id, '-1')     AS listing_lga_dim_id,
    
    -- 5. Host LGA Dimension (Where the host lives)
    COALESCE(dlg_host.lga_dim_id, '-1')     AS host_lga_dim_id,

    -- Metrics
    p.active_flag,
    p.avg_price,
    p.avg_review_rating,
    p.total_stays_active,
    p.revenue_active

FROM prep p

-- Join Neighbourhood
LEFT JOIN {{ ref('g_dim_neighbourhood') }} dn
    ON dn.listing_neighbourhood = p.listing_neighbourhood
   AND p.min_fact_ts >= dn.dbt_valid_from
   AND p.min_fact_ts <  COALESCE(dn.dbt_valid_to, '9999-12-31'::timestamp)

-- Join Property
LEFT JOIN {{ ref('g_dim_property') }} dp
    ON dp.property_type = p.property_type
   AND dp.room_type     = p.room_type
   AND dp.accommodates  = p.accommodates
   AND p.min_fact_ts >= dp.dbt_valid_from
   AND p.min_fact_ts <  COALESCE(dp.dbt_valid_to, '9999-12-31'::timestamp)

-- Join Host
LEFT JOIN {{ ref('g_dim_host') }} dh
    ON dh.host_id = p.host_id
   AND p.min_fact_ts >= dh.dbt_valid_from
   AND p.min_fact_ts <  COALESCE(dh.dbt_valid_to, '9999-12-31'::timestamp)

-- Join Bridge -> LGA (Listing)
LEFT JOIN bridge b_list ON b_list.suburb_norm = p.list_norm
LEFT JOIN {{ ref('g_dim_lga') }} dlg_list
    ON dlg_list.lga_code = b_list.lga_code
   AND p.min_fact_ts >= dlg_list.dbt_valid_from
   AND p.min_fact_ts <  COALESCE(dlg_list.dbt_valid_to, '9999-12-31'::timestamp)

-- Join Bridge -> LGA (Host)
LEFT JOIN bridge b_host ON b_host.suburb_norm = p.host_norm
LEFT JOIN {{ ref('g_dim_lga') }} dlg_host
    ON dlg_host.lga_code = b_host.lga_code
   AND p.min_fact_ts >= dlg_host.dbt_valid_from
   AND p.min_fact_ts <  COALESCE(dlg_host.dbt_valid_to, '9999-12-31'::timestamp)