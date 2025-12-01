/* ============================================================================
   MODEL:    s_listings_clean
   PURPOSE:  Clean & standardize raw Airbnb listings from Bronze.
   INPUT:    bronze.b_airbnb_listings
   OUTPUT:   silver.s_listings_clean (table)
   GRAIN:    1 row per (listing_id, scraped_date)
   ============================================================================ */
{{ config(materialized='table') }}

WITH raw AS (
    SELECT * FROM {{ ref('b_airbnb_listings') }}
),

cleaned AS (
    SELECT
        -- IDs
        NULLIF(TRIM(listing_id), '')::bigint AS listing_id,
        NULLIF(TRIM(scrape_id), '') AS scrape_id,
        
        -- Dates: Handle potential format drift (YYYY-MM-DD vs DD/MM/YYYY)
        CASE 
            WHEN scraped_date ~ '^\d{4}-\d{2}-\d{2}' THEN scraped_date::date
            WHEN scraped_date ~ '^\d{1,2}/\d{1,2}/\d{2,4}' THEN TO_DATE(scraped_date, 'DD/MM/YYYY')
            ELSE NULL 
        END AS scraped_date,

        -- Host Details
        NULLIF(TRIM(host_id), '')::bigint AS host_id,
        NULLIF(TRIM(host_name), 'NaN') AS host_name,
        CASE 
            WHEN host_since ~ '^\d{4}-\d{2}-\d{2}' THEN host_since::date
            WHEN host_since ~ '^\d{1,2}/\d{1,2}/\d{2,4}' THEN TO_DATE(host_since, 'DD/MM/YYYY')
            ELSE NULL 
        END AS host_since,
        
        -- Boolean normalization
        CASE WHEN LOWER(host_is_superhost) = 't' THEN TRUE ELSE FALSE END AS host_is_superhost,
        
        -- Location & Property
        NULLIF(TRIM(host_neighbourhood), 'NaN') AS host_neighbourhood,
        NULLIF(TRIM(listing_neighbourhood), 'NaN') AS listing_neighbourhood,
        NULLIF(TRIM(property_type), 'NaN') AS property_type,
        NULLIF(TRIM(room_type), 'NaN') AS room_type,
        NULLIF(accommodates, 'NaN')::int AS accommodates,
        
        -- Metrics (Clean $ and , from price strings like "$1,200.00")
        NULLIF(REGEXP_REPLACE(price, '[^0-9\.]+', '', 'g'), '')::numeric(10,2) AS price,
        
        CASE WHEN LOWER(has_availability) = 't' THEN TRUE ELSE FALSE END AS has_availability,
        NULLIF(availability_30, 'NaN')::int AS availability_30,
        NULLIF(number_of_reviews, 'NaN')::int AS number_of_reviews,
        NULLIF(review_scores_rating, 'NaN')::int AS review_scores_rating
    
    FROM raw
),

deduped AS (
    SELECT 
        *,
        -- Create a reliable timestamp for snapshots
        scraped_date::timestamp AS updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY listing_id, scraped_date 
            ORDER BY scrape_id DESC
        ) AS rn
    FROM cleaned
    WHERE listing_id IS NOT NULL
)

SELECT * FROM deduped
WHERE rn = 1