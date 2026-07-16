/*
 * Model: b_airbnb_listings
 * Purpose: Raw passthrough of Airbnb listings data.
 * Source: raw.airbnb_listings_raw (Loaded by Airflow)
 */

SELECT *
FROM {{ source('raw', 'airbnb_listings_raw') }}
