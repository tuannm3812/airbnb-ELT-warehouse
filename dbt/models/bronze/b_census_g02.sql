/*
 * Model: b_census_g02
 * Purpose: Raw passthrough of Census G02 (Medians) data.
 * Note: Keeps all columns (wide) for filtering in Silver.
 * Source: raw.census_g02_raw
 */

SELECT *
FROM {{ source('raw', 'census_g02_raw') }}