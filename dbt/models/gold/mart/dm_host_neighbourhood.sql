/* ============================================================================
   VIEW:   dm_host_neighbourhood
   PURPOSE: KPIs per host LGA and month.
   ============================================================================ */
{{ config(materialized='view') }}

WITH f AS (
    SELECT 
        fact_month, 
        host_lga_dim_id, -- Crucial: Use Host Location
        host_dim_id, 
        revenue_active
    FROM {{ ref('g_fact_listing_monthly') }}
    WHERE host_lga_dim_id != '-1' 
),

lga AS (
    SELECT lga_dim_id, lga_name FROM {{ ref('g_dim_lga') }}
),

joined AS (
    SELECT
        f.fact_month,
        lga.lga_name AS host_neighbourhood_lga,
        f.host_dim_id,
        f.revenue_active
    FROM f
    JOIN lga ON lga.lga_dim_id = f.host_lga_dim_id
),

metrics AS (
    SELECT
        host_neighbourhood_lga,
        fact_month AS month_year,
        COUNT(DISTINCT host_dim_id)         AS distinct_hosts,
        SUM(revenue_active)::numeric(14,2)  AS estimated_revenue
    FROM joined
    GROUP BY 1, 2
)

SELECT
    host_neighbourhood_lga,
    month_year,
    distinct_hosts,
    estimated_revenue,
    CASE 
        WHEN distinct_hosts > 0 THEN ROUND(estimated_revenue / distinct_hosts, 2)
        ELSE 0 
    END AS estimated_revenue_per_host
FROM metrics
ORDER BY host_neighbourhood_lga, month_year