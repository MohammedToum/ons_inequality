{{
    config(
        materialized='table',
        schema='fct_ons',
        description="""
            SCD type 2 fact table for tax and benefits observations from the
            ONS effects of taxes and benefits on household income dataset.
        """
    )
}}

select
    tax_benefit_observation_sk,
    source_observation_sk,
    observation_value,
    time_period_type,
    time_period_id,
    period_label,
    time_label,
    period_start_year,
    period_end_year,
    period_start_date,
    period_end_date,
    geography_code,
    geography_name,
    quintile_code,
    quintile_name,
    summary_statistic_code,
    summary_statistic_name,
    income_type_code,
    income_type_name,
    deflation_basis_code,
    deflation_basis_name,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to is null as is_latest
from {{ ref('snap_fct_tax_benefit_observations') }}
