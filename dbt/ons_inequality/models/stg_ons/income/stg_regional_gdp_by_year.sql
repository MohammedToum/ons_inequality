with source as (

    select *
    from {{ source('raw_ons', 'regional_gdp_by_year_time_series') }}

),

renamed as (

    select
        safe_cast(`v4_1` as numeric) as raw_observation_value,

        nullif(trim(cast(`Data Marking` as string)), '') as data_marking,

        safe_cast(`calendar-years` as int64) as calendar_year,
        cast(`Time` as string) as time_label,

        cast(`nuts` as string) as region_code,
        cast(`Geography` as string) as region_name,

        cast(`sic-unofficial` as string) as industry_code,
        cast(`UnofficialStandardIndustrialClassification` as string) as industry_name,

        cast(`type-of-prices` as string) as price_basis_code,
        cast(`Prices` as string) as price_basis_name,

        cast(`quarterly-index-and-growth-rate` as string) as measure_code,
        cast(`GrowthRate` as string) as measure_name

    from source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'calendar_year',
            'region_code',
            'industry_code',
            'price_basis_code',
            'measure_code'
        ]) }} as observation_sk,

        case
            when data_marking in ('.', 'x', 'c', '[x]', '[c]', '[w]', '[u]') then null
            else raw_observation_value
        end as observation_value,

        data_marking,

        case
            when data_marking in ('.', 'x', 'c', '[x]', '[c]', '[w]', '[u]') then true
            else false
        end as is_suppressed,

        calendar_year,
        time_label,
        region_code,
        region_name,
        industry_code,
        industry_name,
        price_basis_code,
        price_basis_name,
        measure_code,
        measure_name

    from renamed

)

select *
from final
