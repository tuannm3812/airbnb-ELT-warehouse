/*
 * Model: b_nsw_lga_code
 * Purpose: Raw passthrough of NSW LGA Code definitions.
 * Source: raw.nsw_lga_code_raw
 */

SELECT *
FROM {{ source('raw', 'nsw_lga_code_raw') }}
