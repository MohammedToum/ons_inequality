{{
    config(
        materialized='view',
        schema='int_ons',
        description="""
            Current-state wellbeing observations used as the source for the SCD
            type 2 fact snapshot.
        """
    )
}}

with wellbeing_local_authority as (

    select
        'wellbeing_local_authority' as source_dataset,
        observation_sk as source_observation_sk,
        observation_value,
        data_marking,
        is_suppressed,
        lower_confidence_limit,
        upper_confidence_limit,
        'financial_year' as time_period_type,
        financial_year_id as time_period_id,
        time_label,
        start_year,
        end_year,
        cast(null as int64) as calendar_year,
        cast(null as int64) as quarter,
        geography_code,
        geography_name,
        wellbeing_measure_code,
        wellbeing_measure_name,
        wellbeing_estimate_code,
        wellbeing_estimate_name,
        seasonal_adjustment_code,
        seasonal_adjustment_name
    from {{ ref('stg_wellbeing_local_authority') }}

),

wellbeing_quarterly as (

    select
        'wellbeing_quarterly' as source_dataset,
        observation_sk as source_observation_sk,
        observation_value,
        cast(null as string) as data_marking,
        false as is_suppressed,
        lower_confidence_limit,
        upper_confidence_limit,
        'quarter' as time_period_type,
        quarter_id as time_period_id,
        time_label,
        year as start_year,
        year as end_year,
        year as calendar_year,
        quarter,
        geography_code,
        geography_name,
        wellbeing_measure_code,
        wellbeing_measure_name,
        wellbeing_estimate_code,
        wellbeing_estimate_name,
        seasonal_adjustment_code,
        seasonal_adjustment_name
    from {{ ref('stg_wellbeing_quarterly') }}

),

unioned as (

    select *
    from wellbeing_local_authority

    union all

    select *
    from wellbeing_quarterly

)

select
    {{ dbt_utils.generate_surrogate_key([
        'source_dataset',
        'source_observation_sk'
    ]) }} as wellbeing_observation_sk,
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
    seasonal_adjustment_name
from unioned
