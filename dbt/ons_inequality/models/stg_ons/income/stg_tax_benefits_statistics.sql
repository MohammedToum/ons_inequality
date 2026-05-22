with source as (

    select *
    from {{ source('raw_ons', 'raw_tax_benefits_statistics') }}

),

renamed as (

    select
        safe_cast(`v4_0` as numeric) as observation_value,

        cast(`financial_and_calendar_years` as string) as period_label,
        cast(`Time` as string) as time_label,

        case
            when regexp_contains(cast(`financial_and_calendar_years` as string), r'^\d{4}$')
                then 'calendar_year'
            when regexp_contains(cast(`financial_and_calendar_years` as string), r'^\d{4}-\d{2}$')
                then 'financial_year'
        end as period_type,

        case
            when regexp_contains(cast(`financial_and_calendar_years` as string), r'^\d{4}$')
                then safe_cast(cast(`financial_and_calendar_years` as string) as int64)
            when regexp_contains(cast(`financial_and_calendar_years` as string), r'^\d{4}-\d{2}$')
                then safe_cast(substr(cast(`financial_and_calendar_years` as string), 1, 4) as int64)
        end as period_start_year,

        case
            when regexp_contains(cast(`financial_and_calendar_years` as string), r'^\d{4}$')
                then safe_cast(cast(`financial_and_calendar_years` as string) as int64)
            when regexp_contains(cast(`financial_and_calendar_years` as string), r'^\d{4}-\d{2}$')
                then safe_cast(concat('20', substr(cast(`financial_and_calendar_years` as string), 6, 2)) as int64)
        end as period_end_year,

        cast(`uk_only` as string) as geography_code,
        cast(`Geography` as string) as geography_name,

        cast(`quintile` as string) as quintile_code,
        cast(`Quintile` as string) as quintile_name,

        cast(`averages_and_percentiles` as string) as summary_statistic_code,
        cast(`AveragesAndPercentiles` as string) as summary_statistic_name,

        cast(`income_type` as string) as income_type_code,
        cast(`Income` as string) as income_type_name,

        cast(`value_deflation` as string) as deflation_basis_code,
        cast(`Deflation` as string) as deflation_basis_name

    from source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'period_type',
            'time_label',
            'geography_code',
            'quintile_code',
            'summary_statistic_code',
            'income_type_code'
        ]) }} as observation_sk,

        observation_value,

        period_label,
        time_label,
        period_type,
        period_start_year,
        period_end_year,

        case
            when period_type = 'calendar_year'
                then date(period_start_year, 1, 1)
            when period_type = 'financial_year'
                then date(period_start_year, 4, 1)
        end as period_start_date,

        case
            when period_type = 'calendar_year'
                then date(period_end_year, 12, 31)
            when period_type = 'financial_year'
                then date(period_end_year, 3, 31)
        end as period_end_date,

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

    from renamed

)

select *
from final
where period_start_year >= {{ var('start_year') }}
  and period_end_year <= {{ var('end_year') }}