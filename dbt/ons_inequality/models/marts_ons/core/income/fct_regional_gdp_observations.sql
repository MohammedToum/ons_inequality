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
    fact.regional_gdp_observation_sk,
    fact.source_dataset,
    fact.source_observation_sk,
    fact.observation_value,
    fact.data_marking,
    fact.is_suppressed,
    time_period.time_period_sk,
    time_quarter.time_quarter_sk,
    fact.time_period_type,
    fact.time_period_id,
    fact.time_label,
    fact.time_grain,
    fact.calendar_year,
    fact.quarter,
    geography.geography_sk,
    fact.region_code,
    fact.region_name,
    fact.industry_code,
    fact.industry_name,
    fact.price_basis_code,
    fact.price_basis_name,
    fact.measure_code,
    fact.measure_name,
    fact.dbt_scd_id,
    fact.dbt_updated_at,
    fact.dbt_valid_from,
    fact.dbt_valid_to,
    fact.dbt_valid_to is null as is_latest
from {{ ref('snap_fct_regional_gdp_observations') }} as fact
left join {{ ref('dim_time_periods') }} as time_period
    on fact.time_period_type = time_period.time_period_type
    and fact.calendar_year = time_period.calendar_year
left join {{ ref('dim_time_quarters') }} as time_quarter
    on fact.calendar_year = time_quarter.calendar_year
    and fact.quarter = time_quarter.quarter
left join {{ ref('dim_geographies') }} as geography
    on fact.region_code = geography.geography_code
