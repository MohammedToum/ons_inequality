{{
    config(
        materialized='table',
        description="""
            Dimension table of canonical calendar quarters used by quarterly
            ONS observation fact tables.
        """
    )
}}

with calendar_years as (

    select
        calendar_year
    from {{ ref('dim_time_periods') }}
    where time_period_type = 'calendar_year'
      and calendar_year is not null

),

quarters as (

    select
        calendar_year,
        quarter
    from calendar_years
    cross join unnest([1, 2, 3, 4]) as quarter

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'calendar_year',
            'quarter'
        ]) }} as time_quarter_sk,
        concat(cast(calendar_year as string), 'q', cast(quarter as string)) as quarter_id,
        concat(cast(calendar_year as string), ' Q', cast(quarter as string)) as quarter_label,
        calendar_year,
        quarter,
        date(calendar_year, ((quarter - 1) * 3) + 1, 1) as quarter_start_date,
        date_sub(
            date_add(date(calendar_year, ((quarter - 1) * 3) + 1, 1), interval 3 month),
            interval 1 day
        ) as quarter_end_date
    from quarters

)

select *
from final
