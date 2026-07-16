/*
 * Model: b_nsw_lga_suburb
 * Purpose: Raw passthrough of Suburb to LGA mapping.
 * Source: raw.nsw_lga_suburb_raw
 */

SELECT *
FROM {{ source('raw', 'nsw_lga_suburb_raw') }}
