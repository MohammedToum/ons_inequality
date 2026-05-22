{% snapshot snap_fct_suicide_observations %}

{{
    config(
        target_schema='fct_ons',
        strategy='check',
        unique_key='source_observation_sk',
        check_cols=[
            'observation_value',
            'time_period_type',
            'time_period_id',
            'time_label',
            'calendar_year',
            'geography_code',
            'geography_name'
        ]
    )
}}

select *
from {{ ref('int_suicide_observations_current') }}

{% endsnapshot %}
