with source as (

    select *
    from {{ source('raw_ons', 'raw_wellbeing_local_authority') }}

),

renamed as (

    select
        safe_cast(`v4_3` as numeric) as raw_observation_value,

        nullif(trim(cast(`data_marking` as string)), '') as data_marking,

        safe_cast(`lower_limit` as numeric) as raw_lower_confidence_limit,
        safe_cast(`upper_limit` as numeric) as raw_upper_confidence_limit,

        cast(`yyyy_yy` as string) as financial_year_id,
        cast(`Time` as string) as time_label,

        safe_cast(substr(`yyyy_yy`, 1, 4) as int64) as start_year,
        safe_cast(concat('20', substr(`yyyy_yy`, 6, 2)) as int64) as end_year,

        cast(`administrative_geography` as string) as geography_code,
        cast(`Geography` as string) as geography_name,

        cast(`measure_of_wellbeing` as string) as wellbeing_measure_code,
        cast(`measure_of_wellbeing` as string) as wellbeing_measure_name,

        cast(`wellbeing_estimate` as string) as wellbeing_estimate_code,
        cast(`Estimate` as string) as wellbeing_estimate_name,

        cast(null as string) as seasonal_adjustment_code,
        cast(null as string) as seasonal_adjustment_name
    from source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'financial_year_id',
            'geography_code',
            'wellbeing_measure_code',
            'wellbeing_estimate_code',
            'seasonal_adjustment_code'
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

        case
            when data_marking in ('.', 'x', 'c', '[x]', '[c]', '[w]', '[u]') then null
            else raw_lower_confidence_limit
        end as lower_confidence_limit,

        case
            when data_marking in ('.', 'x', 'c', '[x]', '[c]', '[w]', '[u]') then null
            else raw_upper_confidence_limit
        end as upper_confidence_limit,

        financial_year_id,
        time_label,
        start_year,
        end_year,
        geography_code,
        geography_name,
        wellbeing_measure_code,
        wellbeing_measure_name,
        wellbeing_estimate_code,
        wellbeing_estimate_name,
        seasonal_adjustment_code,
        seasonal_adjustment_name

    from renamed

)

select *
from final
where calendar_year between {{ var('start_year') }} and {{ var('end_year') }}

