/* ============================================================================
   File: init_bronze_schema.sql
   Purpose: Initialize Bronze schema and base tables for Airflow ingestion.
   Notes:
     - Uses TEXT for all columns to ensure robust loading.
     - Type casting happens in dbt (Silver layer).
     - Census tables (G01/G02) are not created here; the Python DAG creates
       them dynamically to adapt to changing column counts.
   ============================================================================ */

-- Setup schemas.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Airbnb listings raw table.
DROP TABLE IF EXISTS bronze.airbnb_listings_raw;
CREATE TABLE bronze.airbnb_listings_raw (
    listing_id                  TEXT,
    scrape_id                   TEXT,
    scraped_date                TEXT,
    host_id                     TEXT,
    host_name                   TEXT,
    host_since                  TEXT,
    host_is_superhost           TEXT,
    host_neighbourhood          TEXT,
    listing_neighbourhood       TEXT,
    property_type               TEXT,
    room_type                   TEXT,
    accommodates                TEXT,
    price                       TEXT,
    has_availability            TEXT,
    availability_30             TEXT,
    number_of_reviews           TEXT,
    review_scores_rating        TEXT,
    review_scores_accuracy      TEXT,
    review_scores_cleanliness   TEXT,
    review_scores_checkin       TEXT,
    review_scores_communication TEXT,
    review_scores_value         TEXT
);

-- LGA code mapping.
DROP TABLE IF EXISTS bronze.nsw_lga_code_raw;
CREATE TABLE bronze.nsw_lga_code_raw (
    lga_code TEXT,
    lga_name TEXT
);

-- LGA suburb mapping.
DROP TABLE IF EXISTS bronze.nsw_lga_suburb_raw;
CREATE TABLE bronze.nsw_lga_suburb_raw (
    lga_name    TEXT,
    suburb_name TEXT
);
