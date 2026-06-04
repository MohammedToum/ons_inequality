{{
    config(
        materialized='table',
        description="""
            SCD type 2 fact table for tax and benefits observations from the
            ONS effects of taxes and benefits on household income dataset.
        """
    )
}}

select
    fact.tax_benefit_observation_sk,
    fact.source_observation_sk,
    fact.observation_value,
    time_period.time_period_sk,
    fact.time_period_type,
    fact.time_period_id,
    fact.period_label,
    fact.time_label,
    fact.period_start_year,
    fact.period_end_year,
    fact.period_start_date,
    fact.period_end_date,
    geography.geography_sk,
    fact.geography_code,
    fact.geography_name,
    fact.quintile_code,
    fact.quintile_name,
    fact.summary_statistic_code,
    fact.summary_statistic_name,
    fact.income_type_code,
    fact.income_type_name,
    fact.deflation_basis_code,
    fact.deflation_basis_name,
    fact.dbt_scd_id,
    fact.dbt_updated_at,
    fact.dbt_valid_from,
    fact.dbt_valid_to,
    fact.dbt_valid_to is null as is_latest
from {{ ref('snap_fct_tax_benefit_observations') }} as fact
left join {{ ref('dim_time_periods') }} as time_period
    on fact.time_period_type = time_period.time_period_type
    and fact.period_start_year = time_period.start_year
    and fact.period_end_year = time_period.end_year
left join {{ ref('dim_geographies') }} as geography
    on fact.geography_code = geography.geography_code
