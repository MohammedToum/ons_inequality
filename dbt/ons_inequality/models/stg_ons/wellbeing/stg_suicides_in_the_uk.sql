with source as (

    select *
    from {{ source('raw_ons', 'suicides_in_the_uk_2023') }}

),

renamed as (

    select
        safe_cast(`v4_0` as numeric) as observation_value,

        safe_cast(`calendar-years` as int64) as calendar_year,
        cast(`Time` as string) as time_label,

        cast(`administrative-geography` as string) as geography_code,
        cast(`Geography` as string) as geography_name

    from source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'calendar_year',
            'geography_code'
        ]) }} as observation_sk,

        observation_value,
        calendar_year,
        time_label,
        geography_code,
        geography_name

    from renamed

)

select *
from final
