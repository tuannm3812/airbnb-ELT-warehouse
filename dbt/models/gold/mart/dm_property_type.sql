/* ============================================================================
   VIEW:   dm_property_type
   PURPOSE: KPIs per property_type, room_type, accommodates & month.
   ============================================================================ */
{{ config(materialized='view') }}

WITH f AS (
    SELECT * FROM {{ ref('g_fact_listing_monthly') }}
),

dim_prop AS (
    SELECT property_dim_id, property_type, room_type, accommodates 
    FROM {{ ref('g_dim_property') }}
),

dim_host AS (
    SELECT host_dim_id, host_is_superhost 
    FROM {{ ref('g_dim_host') }}
),

metrics AS (
    SELECT
        dp.property_type,
        dp.room_type,
        dp.accommodates,
        f.fact_month AS month_year,
        
        COUNT(DISTINCT f.listing_id) AS total_listings,
        COUNT(DISTINCT CASE WHEN f.active_flag THEN f.listing_id END) AS active_listings,
        COUNT(DISTINCT f.host_dim_id) AS distinct_hosts,
        COUNT(DISTINCT CASE WHEN dh.host_is_superhost THEN f.host_dim_id END) AS distinct_superhosts,
        
        MIN(CASE WHEN f.active_flag THEN f.avg_price END) AS min_price,
        MAX(CASE WHEN f.active_flag THEN f.avg_price END) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN f.active_flag THEN f.avg_price END)::numeric(10,2) AS median_price,
        AVG(CASE WHEN f.active_flag THEN f.avg_price END)::numeric(10,2) AS avg_price,
        
        AVG(CASE WHEN f.active_flag THEN f.avg_review_rating END)::numeric(5,2) AS avg_review_scores_rating,
        SUM(CASE WHEN f.active_flag THEN f.total_stays_active END) AS total_stays,
        SUM(CASE WHEN f.active_flag THEN f.revenue_active END)::numeric(14,2) AS total_estimated_revenue

    FROM f
    JOIN dim_prop dp ON dp.property_dim_id = f.property_dim_id
    JOIN dim_host dh ON dh.host_dim_id = f.host_dim_id
    GROUP BY 1, 2, 3, 4
)

SELECT
    property_type,
    room_type,
    accommodates,
    month_year,
    
    ROUND(active_listings * 100.0 / NULLIF(total_listings, 0), 2) AS active_listings_rate,
    
    min_price,
    max_price,
    median_price,
    avg_price,
    
    distinct_hosts,
    ROUND(distinct_superhosts * 100.0 / NULLIF(distinct_hosts, 0), 2) AS superhost_rate,
    
    avg_review_scores_rating,
    
    -- Lag Metrics
    ROUND(100.0 * (active_listings - LAG(active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year)) / NULLIF(LAG(active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year), 0), 2) AS pct_change_active,
    
    ROUND(100.0 * ((total_listings - active_listings) - LAG(total_listings - active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year)) / NULLIF(LAG(total_listings - active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year), 0), 2) AS pct_change_inactive,
    
    total_stays,
    CASE 
        WHEN active_listings > 0 THEN ROUND(total_estimated_revenue / active_listings, 2) 
        ELSE 0 
    END AS avg_estimated_revenue_per_active_listing

FROM metrics
ORDER BY property_type, room_type, accommodates, month_year