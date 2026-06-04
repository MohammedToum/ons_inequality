{{
    config(
        materialized='table',
        schema='marts_ons',
        description="""
                    Dimensional model for time periods dimension, sourced from the int_time_periods intermediate model.
                """
    )
}}
select
    time_period_sk,
    time_period_type,
    time_period_label,
    start_year,
    end_year,
    start_date,
    end_date,
    calendar_year,
    -- add financial year SK to time periods for easier joining
     {{ dbt_utils.generate_surrogate_key(
        ["financial_year_label", "financial_year_start", "financial_year_end"]
     ) }} as financial_year_sk,
    financial_year_label,
    financial_year_start,
    financial_year_end
from {{ ref("int_time_periods") }}
