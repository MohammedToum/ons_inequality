{{
    config(
        materialized='table',
        description="""
            SCD type 2 fact table for wellbeing observations from the ONS local
            authority and quarterly wellbeing datasets.
        """
    )
}}

select
    fact.wellbeing_observation_sk,
    fact.source_dataset,
    fact.source_observation_sk,
    fact.observation_value,
    fact.data_marking,
    fact.is_suppressed,
    fact.lower_confidence_limit,
    fact.upper_confidence_limit,
    time_period.time_period_sk,
    time_quarter.time_quarter_sk,
    fact.time_period_type,
    fact.time_period_id,
    fact.time_label,
    fact.start_year,
    fact.end_year,
    fact.calendar_year,
    fact.quarter,
    geography.geography_sk,
    fact.geography_code,
    fact.geography_name,
    fact.wellbeing_measure_code,
    fact.wellbeing_measure_name,
    fact.wellbeing_estimate_code,
    fact.wellbeing_estimate_name,
    fact.seasonal_adjustment_code,
    fact.seasonal_adjustment_name,
    fact.dbt_scd_id,
    fact.dbt_updated_at,
    fact.dbt_valid_from,
    fact.dbt_valid_to,
    fact.dbt_valid_to is null as is_latest
from {{ ref('snap_fct_wellbeing_observations') }} as fact
left join {{ ref('dim_time_periods') }} as time_period
    on fact.time_period_type = time_period.time_period_type
    and fact.start_year = time_period.start_year
    and fact.end_year = time_period.end_year
left join {{ ref('dim_time_quarters') }} as time_quarter
    on fact.calendar_year = time_quarter.calendar_year
    and fact.quarter = time_quarter.quarter
left join {{ ref('dim_geographies') }} as geography
    on fact.geography_code = geography.geography_code
