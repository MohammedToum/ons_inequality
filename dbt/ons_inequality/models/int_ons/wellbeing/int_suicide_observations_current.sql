{{
    config(
        materialized='view',
        schema='int_ons',
        description="""
            Current-state suicide observations used as the source for the SCD
            type 2 fact snapshot.
        """
    )
}}

with suicide_observations as (

    select
        observation_sk as source_observation_sk,
        observation_value,
        'calendar_year' as time_period_type,
        cast(calendar_year as string) as time_period_id,
        time_label,
        calendar_year,
        geography_code,
        geography_name
    from {{ ref('stg_suicides_in_the_uk') }}

)

select
    {{ dbt_utils.generate_surrogate_key([
        'source_observation_sk'
    ]) }} as suicide_observation_sk,
    source_observation_sk,
    observation_value,
    time_period_type,
    time_period_id,
    time_label,
    calendar_year,
    geography_code,
    geography_name
from suicide_observations
