{{
    config(
        materialized='view',
        schema='int_ons',
        description="""
            Current-state tax and benefits observations used as the source for
            the SCD type 2 fact snapshot.
        """
    )
}}

with tax_benefit_observations as (

    select
        observation_sk as source_observation_sk,
        observation_value,
        period_type as time_period_type,
        period_label as time_period_id,
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
        deflation_basis_name
    from {{ ref('stg_tax_benefits_statistics') }}
    where period_start_year >= 2012
      and period_end_year <= 2021

)

select
    {{ dbt_utils.generate_surrogate_key([
        'source_observation_sk'
    ]) }} as tax_benefit_observation_sk,
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
    deflation_basis_name
from tax_benefit_observations
