{{
    config(
        materialized='table',
        schema='fct_ons',
        description="""
            SCD type 2 fact table for suicide observations from the ONS
            suicides in the UK dataset.
        """
    )
}}

select
    suicide_observation_sk,
    source_observation_sk,
    observation_value,
    time_period_type,
    time_period_id,
    time_label,
    calendar_year,
    geography_code,
    geography_name,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to is null as is_latest
from {{ ref('snap_fct_suicide_observations') }}
