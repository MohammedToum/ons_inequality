{{
    config(
        materialized='table',
        schema='int_ons',
        description="""
            Canonical time period lookup for ONS observations.

            Grain:
                One row per time period and period type.
            """
    )
}}

with
    source as (
        select *
        from {{ ref("seed_time_periods_lookup") }}
    ),

    renamed as (
        select
            cast(time_period_type as string) as time_period_type,
            cast(year_label as string) as time_period_label,
            safe_cast(start_year as int64) as start_year,
            safe_cast(end_year as int64) as end_year,
            safe_cast(start_date as date) as start_date,
            safe_cast(end_date as date) as end_date,
            safe_cast(calendar_year as int64) as calendar_year,
            cast(financial_year_label as string) as financial_year_label,
            safe_cast(financial_year_start as int64) as financial_year_start,
            safe_cast(financial_year_end as int64) as financial_year_end
        from source
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(
                ["time_period_type", "start_year", "end_year"]
            ) }} as time_period_sk,
            time_period_type,
            time_period_label,
            start_year,
            end_year,
            start_date,
            end_date,
            calendar_year,
            financial_year_label,
            financial_year_start,
            financial_year_end
        from renamed
        where time_period_type is not null
            and start_year is not null
            and end_year is not null
    )

select *
from final
