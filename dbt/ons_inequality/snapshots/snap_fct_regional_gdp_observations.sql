{% snapshot snap_fct_regional_gdp_observations %}

{{
    config(
        target_schema='fct_ons',
        strategy='check',
        unique_key='regional_gdp_observation_sk',
        check_cols=[
            'source_dataset',
            'source_observation_sk',
            'observation_value',
            'data_marking',
            'is_suppressed',
            'time_period_type',
            'time_period_id',
            'time_label',
            'time_grain',
            'calendar_year',
            'quarter',
            'region_code',
            'region_name',
            'industry_code',
            'industry_name',
            'price_basis_code',
            'price_basis_name',
            'measure_code',
            'measure_name'
        ]
    )
}}

select *
from {{ ref('int_regional_gdp_observations_current') }}

{% endsnapshot %}
