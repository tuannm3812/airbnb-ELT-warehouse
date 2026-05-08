/* ============================================================================
   File: analysis_queries.sql
   Purpose: Business analysis queries for the Airbnb and Census warehouse.
   NOTES: 
     - Uses 'listing_lga_dim_id' for property location analysis.
     - Window defined dynamically (Last 12 months from max date in data).
   ============================================================================ */

/* ============================================================================
   Analysis 1: demographic differences between top-3 and bottom-3 LGAs.
   ----------------------------------------------------------------------------
   Task: Rank LGAs by revenue/active listing. Compare Top 3 vs Bottom 3 on
         Demographics (Age, Household Size).
   Strategy: 
     1. Agg rev/active by listing_lga_dim_id.
     2. Rank and tag 'top3'/'bottom3'.
     3. Join Census data.
   ============================================================================ */

WITH params AS (
    SELECT 12::int AS window_months
),
-- Define the 12-month window based on the latest data
win AS (
    SELECT 
        (MAX(fact_month) - (INTERVAL '1 month' * ((SELECT window_months FROM params) - 1)))::date AS start_m,
        MAX(fact_month) AS end_m
    FROM gold.g_fact_listing_monthly
),
-- Aggregate Revenue per LGA
lga_perf AS (
    SELECT 
        f.listing_lga_dim_id, -- Use LISTING location
        SUM(f.revenue_active) AS rev_sum,
        COUNT(DISTINCT CASE WHEN f.active_flag THEN f.listing_id END) AS active_listings,
        SUM(f.revenue_active) / NULLIF(COUNT(DISTINCT CASE WHEN f.active_flag THEN f.listing_id END), 0) AS rev_per_active
    FROM gold.g_fact_listing_monthly f
    JOIN win w ON f.fact_month BETWEEN w.start_m AND w.end_m
    WHERE f.listing_lga_dim_id != '-1' -- Exclude unknown LGAs
    GROUP BY 1
),
-- Add LGA Names
lga_details AS (
    SELECT 
        p.*,
        l.lga_code, 
        l.lga_name
    FROM lga_perf p
    JOIN gold.g_dim_lga l ON l.lga_dim_id = p.listing_lga_dim_id
),
-- Rank them
ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY rev_per_active DESC) AS rnk_desc,
        ROW_NUMBER() OVER (ORDER BY rev_per_active ASC)  AS rnk_asc
    FROM lga_details
    WHERE active_listings > 50 -- Opt: Filter out LGAs with very little data for stability
),
-- Pick Top 3 and Bottom 3
pick AS (
    SELECT * FROM ranked WHERE rnk_desc <= 3
    UNION ALL
    SELECT * FROM ranked WHERE rnk_asc <= 3
),
-- Census G01: Age buckets
g01 AS (
    SELECT 
        NULLIF(REGEXP_REPLACE(lga_code_2016, '[^0-9]', '', 'g'), '')::int AS lga_code,
        (age_0_4_yr_p::numeric + age_5_14_yr_p::numeric + age_15_19_yr_p::numeric) / NULLIF(tot_p_p::numeric, 0) AS pct_0_19,
        (age_20_24_yr_p::numeric + age_25_34_yr_p::numeric + age_35_44_yr_p::numeric) / NULLIF(tot_p_p::numeric, 0) AS pct_20_44,
        (age_45_54_yr_p::numeric + age_55_64_yr_p::numeric) / NULLIF(tot_p_p::numeric, 0) AS pct_45_64,
        (age_65_74_yr_p::numeric + age_75_84_yr_p::numeric + age_85ov_p::numeric) / NULLIF(tot_p_p::numeric, 0) AS pct_65_plus
    FROM bronze.census_g01_raw
),
-- Census G02: Medians
g02 AS (
    SELECT 
        NULLIF(REGEXP_REPLACE(lga_code_2016, '[^0-9]', '', 'g'), '')::int AS lga_code,
        median_age_persons::numeric AS median_age,
        average_household_size::numeric AS avg_household_size
    FROM bronze.census_g02_raw
)

SELECT 
    CASE WHEN rnk_desc <= 3 THEN 'Top 3' ELSE 'Bottom 3' END AS category,
    p.lga_name,
    p.lga_code,
    ROUND(p.rev_per_active::numeric, 2) AS rev_per_active_listing,
    ROUND(g01.pct_0_19 * 100, 1) AS pct_age_0_19,
    ROUND(g01.pct_20_44 * 100, 1) AS pct_age_20_44,
    ROUND(g01.pct_45_64 * 100, 1) AS pct_age_45_64,
    ROUND(g01.pct_65_plus * 100, 1) AS pct_age_65_plus,
    g02.median_age,
    g02.avg_household_size
FROM pick p
LEFT JOIN g01 ON g01.lga_code = p.lga_code
LEFT JOIN g02 ON g02.lga_code = p.lga_code
ORDER BY rnk_desc;


/* ============================================================================
   Analysis 2: correlation between median age and revenue.
   ----------------------------------------------------------------------------
   Task: Correlation (Pearson) between Census Median Age (LGA level) and 
         Revenue per Active Listing (Neighbourhood level).
   Strategy:
     1. Calc Revenue per Active Listing by Neighbourhood.
     2. Map Neighbourhood -> LGA using Bridge.
     3. Join LGA -> Census Median Age.
     4. Compute CORR().
   ============================================================================ */

WITH params AS ( SELECT 12::int AS window_months ),
win AS (
    SELECT 
        (MAX(fact_month) - (INTERVAL '1 month' * ((SELECT window_months FROM params) - 1)))::date AS start_m,
        MAX(fact_month) AS end_m
    FROM gold.g_fact_listing_monthly
),
neigh_stats AS (
    SELECT 
        n.listing_neighbourhood,
        SUM(f.revenue_active) / NULLIF(COUNT(DISTINCT CASE WHEN f.active_flag THEN f.listing_id END), 0) AS rev_per_active
    FROM gold.g_fact_listing_monthly f
    JOIN gold.g_dim_neighbourhood n ON f.neighbourhood_dim_id = n.neighbourhood_dim_id
    JOIN win w ON f.fact_month BETWEEN w.start_m AND w.end_m
    GROUP BY 1
    HAVING SUM(f.revenue_active) IS NOT NULL
),
-- Map Neighbourhoods to LGAs using the Silver Bridge
neigh_lga AS (
    SELECT 
        ns.listing_neighbourhood,
        ns.rev_per_active,
        -- Normalize match key
        UPPER(REGEXP_REPLACE(ns.listing_neighbourhood, '[^A-Z0-9]', '', 'g')) AS join_key
    FROM neigh_stats ns
),
bridge AS (
    SELECT 
        UPPER(REGEXP_REPLACE(suburb_name, '[^A-Z0-9]', '', 'g')) AS join_key,
        lga_code::int AS lga_code
    FROM silver.s_bridge_lga_suburb
),
census AS (
    SELECT 
        NULLIF(REGEXP_REPLACE(lga_code_2016, '[^0-9]', '', 'g'), '')::int AS lga_code,
        median_age_persons::numeric AS median_age
    FROM bronze.census_g02_raw
),
dataset AS (
    SELECT 
        nl.listing_neighbourhood,
        nl.rev_per_active,
        c.median_age
    FROM neigh_lga nl
    JOIN bridge b ON nl.join_key = b.join_key
    JOIN census c ON b.lga_code = c.lga_code
)
-- Output Correlation
SELECT 
    CORR(median_age, rev_per_active) AS correlation_coefficient,
    COUNT(*) AS data_points
FROM dataset;


/* ============================================================================
   Analysis 3: best listing configuration for top 5 neighbourhoods.
   ----------------------------------------------------------------------------
   Task: Find the best (Property Type, Room Type, Accommodates) for highest Stays
         in the Top 5 Revenue-generating Neighbourhoods.
   ============================================================================ */

WITH params AS ( SELECT 12::int AS window_months ),
win AS (
    SELECT 
        (MAX(fact_month) - (INTERVAL '1 month' * ((SELECT window_months FROM params) - 1)))::date AS start_m,
        MAX(fact_month) AS end_m
    FROM gold.g_fact_listing_monthly
),
-- 1. Identify Top 5 Neighbourhoods by Rev/Active
top5_neigh AS (
    SELECT 
        n.listing_neighbourhood,
        SUM(f.revenue_active) / NULLIF(COUNT(DISTINCT CASE WHEN f.active_flag THEN f.listing_id END), 0) AS rev_per_active
    FROM gold.g_fact_listing_monthly f
    JOIN gold.g_dim_neighbourhood n ON f.neighbourhood_dim_id = n.neighbourhood_dim_id
    JOIN win w ON f.fact_month BETWEEN w.start_m AND w.end_m
    GROUP BY 1
    ORDER BY rev_per_active DESC NULLS LAST
    LIMIT 5
),
-- 2. Analyze configurations within those 5
configs AS (
    SELECT 
        n.listing_neighbourhood,
        p.property_type,
        p.room_type,
        p.accommodates,
        SUM(f.total_stays_active) AS total_stays
    FROM gold.g_fact_listing_monthly f
    JOIN gold.g_dim_neighbourhood n ON f.neighbourhood_dim_id = n.neighbourhood_dim_id
    JOIN gold.g_dim_property p ON f.property_dim_id = p.property_dim_id
    JOIN win w ON f.fact_month BETWEEN w.start_m AND w.end_m
    WHERE n.listing_neighbourhood IN (SELECT listing_neighbourhood FROM top5_neigh)
    GROUP BY 1, 2, 3, 4
),
-- 3. Rank configurations
ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY listing_neighbourhood ORDER BY total_stays DESC) as rn
    FROM configs
)
SELECT 
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    total_stays AS predicted_highest_stays
FROM ranked
WHERE rn = 1
ORDER BY listing_neighbourhood;


/* ============================================================================
   Analysis 4: host concentration across LGAs.
   ----------------------------------------------------------------------------
   Task: Do multi-listing hosts keep properties in one LGA or spread them out?
   Refinement: Must use 'listing_lga_dim_id' to see where properties ARE.
   ============================================================================ */

WITH params AS ( SELECT 12::int AS window_months ),
win AS (
    SELECT 
        (MAX(fact_month) - (INTERVAL '1 month' * ((SELECT window_months FROM params) - 1)))::date AS start_m,
        MAX(fact_month) AS end_m
    FROM gold.g_fact_listing_monthly
),
-- Get active listings and their locations per host
host_activity AS (
    SELECT DISTINCT
        f.host_dim_id,
        f.listing_id,
        f.listing_lga_dim_id -- CRITICAL: Use Listing Location
    FROM gold.g_fact_listing_monthly f
    JOIN win w ON f.fact_month BETWEEN w.start_m AND w.end_m
    WHERE f.active_flag = TRUE
      AND f.listing_lga_dim_id != '-1' -- Ignore unknown locations
),
-- Aggregate per host
host_stats AS (
    SELECT 
        host_dim_id,
        COUNT(DISTINCT listing_id) AS total_listings,
        COUNT(DISTINCT listing_lga_dim_id) AS distinct_lgas
    FROM host_activity
    GROUP BY 1
    HAVING COUNT(DISTINCT listing_id) > 1 -- Only multi-listing hosts
)
-- Classify
SELECT 
    CASE 
        WHEN distinct_lgas = 1 THEN 'Concentrated (Single LGA)'
        ELSE 'Distributed (Multiple LGAs)'
    END AS distribution_type,
    COUNT(host_dim_id) AS host_count,
    ROUND(COUNT(host_dim_id) * 100.0 / SUM(COUNT(host_dim_id)) OVER (), 2) AS percentage
FROM host_stats
GROUP BY 1;


/* ============================================================================
   Analysis 5: mortgage repayment affordability.
   ----------------------------------------------------------------------------
   Task: Can single-listing hosts cover the LGA's median mortgage with revenue?
   Refinement: Compare Listing Revenue vs Listing LGA Mortgage.
   ============================================================================ */

WITH params AS ( SELECT 12::int AS window_months ),
win AS (
    SELECT 
        (MAX(fact_month) - (INTERVAL '1 month' * ((SELECT window_months FROM params) - 1)))::date AS start_m,
        MAX(fact_month) AS end_m
    FROM gold.g_fact_listing_monthly
),
-- 1. Get Hosts with exactly 1 active listing
host_counts AS (
    SELECT 
        f.host_dim_id,
        COUNT(DISTINCT f.listing_id) as listing_count
    FROM gold.g_fact_listing_monthly f
    JOIN win w ON f.fact_month BETWEEN w.start_m AND w.end_m
    WHERE f.active_flag = TRUE
    GROUP BY 1
    HAVING COUNT(DISTINCT f.listing_id) = 1
),
-- 2. Calculate Revenue for that single listing
single_listing_revenue AS (
    SELECT 
        f.host_dim_id,
        MAX(l.lga_code) as lga_code, -- Use Listing LGA
        MAX(l.lga_name) as lga_name,
        SUM(f.revenue_active) as annual_revenue
    FROM gold.g_fact_listing_monthly f
    JOIN host_counts hc ON f.host_dim_id = hc.host_dim_id
    JOIN gold.g_dim_lga l ON f.listing_lga_dim_id = l.lga_dim_id -- CRITICAL: Listing Location
    JOIN win w ON f.fact_month BETWEEN w.start_m AND w.end_m
    GROUP BY 1
),
-- 3. Get Annualized Mortgage
mortgage AS (
    SELECT 
        NULLIF(REGEXP_REPLACE(lga_code_2016, '[^0-9]', '', 'g'), '')::int AS lga_code,
        median_mortgage_repay_monthly::numeric * 12 AS annual_mortgage
    FROM bronze.census_g02_raw
)
-- 4. Compare and Aggregate by LGA
SELECT 
    r.lga_name,
    COUNT(r.host_dim_id) AS total_single_hosts,
    SUM(CASE WHEN r.annual_revenue >= m.annual_mortgage THEN 1 ELSE 0 END) AS hosts_covering_mortgage,
    ROUND(SUM(CASE WHEN r.annual_revenue >= m.annual_mortgage THEN 1 ELSE 0 END) * 100.0 / COUNT(r.host_dim_id), 2) AS pct_covering
FROM single_listing_revenue r
JOIN mortgage m ON r.lga_code = m.lga_code
GROUP BY 1
ORDER BY pct_covering DESC;
