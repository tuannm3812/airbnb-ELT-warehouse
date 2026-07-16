{{ config(materialized='table') }}

SELECT
    lga_code_2016 AS lga_code,
    median_age_persons AS median_age,
    median_mortgage_repay_monthly AS median_mortgage_repay_monthly,
    median_tot_prsnl_inc_weekly AS median_weekly_personal_income,
    median_rent_weekly AS median_weekly_rent,
    median_tot_fam_inc_weekly AS median_weekly_family_income,
    average_num_psns_per_bedroom AS avg_persons_per_bedroom,
    median_tot_hhd_inc_weekly AS median_weekly_household_income,
    average_household_size AS avg_household_size
FROM {{ ref('b_census_g02') }}
