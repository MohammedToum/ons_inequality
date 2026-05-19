{{
    config(
            materialized='view',
            schema='stg_ons',
            description="""
                        Staging model for yearly regional GDP observations from ONS.

                        Grain:
                            One GDP observation per geography, year, industry, measure, and pricing basis.

                        Natural keys:
                            region_code/region_name, year, industry_code/industry_name,
                            price_basis_code/price_basis_name, measure_code/measure_name.

                        Time columns:
                            year, time_label.
                            
                        Geography columns:
                            region_code, region_name.

                        Industry columns:
                            industry_code, industry_name.

                        Measure/value columns:
                            observation_value, data_marking, is_suppressed, price_basis_code/price_basis_name,
                            measure_code/measure_name.
                    """
    )
}}

with source as (

    select *
    from {{ source('raw_ons', 'raw_regional_gdp_by_year') }}

),

renamed as (

    select
        safe_cast(`v4_1` as numeric) as raw_observation_value,

        nullif(trim(cast(`data_marking` as string)), '') as data_marking,

        safe_cast(`calendar_years` as int64) as calendar_year,
        cast(`Time` as string) as time_label,

        cast(`nuts` as string) as region_code,
        cast(`Geography` as string) as region_name,

        cast(`sic_unofficial` as string) as industry_code,
        cast(`unofficial_standard_industrial_classification` as string) as industry_name,

        cast(`type_of_prices` as string) as price_basis_code,
        cast(`Prices` as string) as price_basis_name,

        cast(`quarterly_index_and_growth_rate` as string) as measure_code,
        cast(`growth_rate` as string) as measure_name

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
