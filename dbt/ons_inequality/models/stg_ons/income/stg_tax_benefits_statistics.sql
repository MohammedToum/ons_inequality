with source as (

    select *
    from {{ source('raw_ons', 'tax_benefits_statistics_time_series') }}

),

renamed as (

    select
        safe_cast(`v4_0` as numeric) as observation_value,

        safe_cast(`financial-and-calendar-years` as int64) as financial_or_calendar_year,
        cast(`Time` as string) as time_label,

        cast(`uk-only` as string) as geography_code,
        cast(`Geography` as string) as geography_name,

        cast(`quintile` as string) as quintile_code,
        cast(`Quintile` as string) as quintile_name,

        cast(`averages-and-percentiles` as string) as summary_statistic_code,
        cast(`AveragesAndPercentiles` as string) as summary_statistic_name,

        cast(`income-type` as string) as income_type_code,
        cast(`Income` as string) as income_type_name,

        cast(`value-deflation` as string) as deflation_basis_code,
        cast(`Deflation` as string) as deflation_basis_name

    from source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'financial_or_calendar_year',
            'geography_code',
            'quintile_code',
            'summary_statistic_code',
            'income_type_code',
            'deflation_basis_code'
        ]) }} as observation_sk,

        observation_value,
        financial_or_calendar_year,
        time_label,
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
