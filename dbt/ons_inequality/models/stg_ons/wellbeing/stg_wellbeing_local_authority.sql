with source as (

    select *
    from {{ source('raw_ons', 'wellbeing_local_authority_time_series') }}

),

renamed as (

    select
        safe_cast(`v4_3` as numeric) as raw_observation_value,

        nullif(trim(cast(`Data marking` as string)), '') as data_marking,

        safe_cast(`Lower limit` as numeric) as raw_lower_confidence_limit,
        safe_cast(`Upper limit` as numeric) as raw_upper_confidence_limit,

        cast(`yyyy-yy` as string) as financial_year_id,
        cast(`Time` as string) as time_label,

        safe_cast(substr(`yyyy-yy`, 1, 4) as int64) as start_year,
        safe_cast(concat('20', substr(`yyyy-yy`, 6, 2)) as int64) as end_year,

        cast(`administrative-geography` as string) as geography_code,
        cast(`Geography` as string) as geography_name,

        cast(`measure-of-wellbeing` as string) as wellbeing_measure_code,
        cast(`MeasureOfWellbeing` as string) as wellbeing_measure_name,

        cast(`wellbeing-estimate` as string) as wellbeing_estimate_code,
        cast(`Estimate` as string) as wellbeing_estimate_name

    from source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'financial_year_id',
            'geography_code',
            'wellbeing_measure_code',
            'wellbeing_estimate_code'
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
        wellbeing_estimate_name

    from renamed

)

select *
from final
