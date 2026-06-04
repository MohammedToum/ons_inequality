{{
    config(
            materialized='table',
            schema='stg_ons',
            description="""
                        Staging model for wellbeing observations quarterly dataset.
                    """
    )
}}

with source as (

    select *
    from {{ source('raw_ons', 'raw_wellbeing_quarterly') }}

),

renamed as (

    select
        safe_cast(`v4_2` as numeric) as observation_value,

        safe_cast(`LCL` as numeric) as lower_confidence_limit,
        safe_cast(`UCL` as numeric) as upper_confidence_limit,

        cast(`yyyy_qq` as string) as quarter_id,
        cast(`Time` as string) as time_label,

        safe_cast(substr(`yyyy_qq`, 1, 4) as int64) as year,
        safe_cast(regexp_extract(`yyyy_qq`, r'q([1-4])') as int64) as quarter,

        cast(`uk_only` as string) as geography_code,
        cast(`Geography` as string) as geography_name,

        cast(`measure_of_wellbeing` as string) as wellbeing_measure_code,
        cast(`measure_of_wellbeing` as string) as wellbeing_measure_name,

        cast(`wellbeing_estimate` as string) as wellbeing_estimate_code,
        cast(`Estimate` as string) as wellbeing_estimate_name,

        cast(`seasonal_adjustment` as string) as seasonal_adjustment_code,
        cast(`seasonal_adjustment` as string) as seasonal_adjustment_name

    from source

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'quarter_id',
            'geography_code',
            'wellbeing_measure_code',
            'wellbeing_estimate_code',
            'seasonal_adjustment_code'
        ]) }} as observation_sk,

        observation_value,
        lower_confidence_limit,
        upper_confidence_limit,
        quarter_id,
        time_label,
        year,
        quarter,
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
where 1 = 1
{{ optional_quarter_window('year', 'quarter') }}
