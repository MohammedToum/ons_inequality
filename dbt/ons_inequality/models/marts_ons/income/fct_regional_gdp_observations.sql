{{
    config(
        materialized='table',
        schema='fct_ons',
        description="""
            SCD type 2 fact table for regional GDP observations from ONS annual
            and quarterly datasets.
        """
    )
}}

select
    regional_gdp_observation_sk,
    source_dataset,
    source_observation_sk,
    observation_value,
    data_marking,
    is_suppressed,
    time_period_type,
    time_period_id,
    time_label,
    time_grain,
    calendar_year,
    quarter,
    region_code,
    region_name,
    industry_code,
    industry_name,
    price_basis_code,
    price_basis_name,
    measure_code,
    measure_name,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to is null as is_latest
from {{ ref('snap_fct_regional_gdp_observations') }}
