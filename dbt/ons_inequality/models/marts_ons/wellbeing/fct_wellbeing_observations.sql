{{
    config(
        materialized='table',
        schema='fct_ons',
        description="""
            SCD type 2 fact table for wellbeing observations from the ONS local
            authority and quarterly wellbeing datasets.
        """
    )
}}

select
    wellbeing_observation_sk,
    source_dataset,
    source_observation_sk,
    observation_value,
    data_marking,
    is_suppressed,
    lower_confidence_limit,
    upper_confidence_limit,
    time_period_type,
    time_period_id,
    time_label,
    start_year,
    end_year,
    calendar_year,
    quarter,
    geography_code,
    geography_name,
    wellbeing_measure_code,
    wellbeing_measure_name,
    wellbeing_estimate_code,
    wellbeing_estimate_name,
    seasonal_adjustment_code,
    seasonal_adjustment_name,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to is null as is_latest
from {{ ref('snap_fct_wellbeing_observations') }}
