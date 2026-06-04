{{
    config(
        materialized='table',
        description="""
            SCD type 2 fact table for suicide observations from the ONS
            suicides in the UK dataset.
        """
    )
}}

select
    fact.suicide_observation_sk,
    fact.source_observation_sk,
    fact.observation_value,
    time_period.time_period_sk,
    fact.time_period_type,
    fact.time_period_id,
    fact.time_label,
    fact.calendar_year,
    geography.geography_sk,
    fact.geography_code,
    fact.geography_name,
    fact.dbt_scd_id,
    fact.dbt_updated_at,
    fact.dbt_valid_from,
    fact.dbt_valid_to,
    fact.dbt_valid_to is null as is_latest
from {{ ref('snap_fct_suicide_observations') }} as fact
left join {{ ref('dim_time_periods') }} as time_period
    on fact.time_period_type = time_period.time_period_type
    and fact.calendar_year = time_period.calendar_year
left join {{ ref('dim_geographies') }} as geography
    on fact.geography_code = geography.geography_code
