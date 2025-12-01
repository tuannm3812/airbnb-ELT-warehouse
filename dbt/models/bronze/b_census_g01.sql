/*
 * Model: b_census_g01
 * Purpose: Raw passthrough of Census G01 (Demographics) data.
 * Source: raw.census_g01_raw
 */

SELECT *
FROM {{ source('raw', 'census_g01_raw') }}