{{ config(materialized='table') }}

SELECT
    lga_code_2016 AS lga_code,
    tot_p_m AS total_population_male,
    tot_p_f AS total_population_female,
    tot_p_p AS total_population_person,
    age_0_4_yr_p AS age_0_4_yr_p,
    age_5_14_yr_p AS age_5_14_yr_p,
    age_15_19_yr_p AS age_15_19_yr_p,
    age_20_24_yr_p AS age_20_24_yr_p,
    age_25_34_yr_p AS age_25_34_yr_p,
    age_35_44_yr_p AS age_35_44_yr_p,
    age_45_54_yr_p AS age_45_54_yr_p,
    age_55_64_yr_p AS age_55_64_yr_p,
    age_65_74_yr_p AS age_65_74_yr_p,
    age_75_84_yr_p AS age_75_84_yr_p,
    age_85ov_p AS age_85ov_p
    -- Add other G01 columns if needed for demographic analysis
FROM {{ ref('b_census_g01') }}
