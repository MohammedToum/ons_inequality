{% snapshot snap_fct_wellbeing_observations %}

{{
    config(
        target_schema='fct_ons',
        strategy='check',
        unique_key='wellbeing_observation_sk',
        check_cols=[
            'source_dataset',
            'source_observation_sk',
            'observation_value',
            'data_marking',
            'is_suppressed',
            'lower_confidence_limit',
            'upper_confidence_limit',
            'time_period_type',
            'time_period_id',
            'time_label',
            'start_year',
            'end_year',
            'calendar_year',
            'quarter',
            'geography_code',
            'geography_name',
            'wellbeing_measure_code',
            'wellbeing_measure_name',
            'wellbeing_estimate_code',
            'wellbeing_estimate_name',
            'seasonal_adjustment_code',
            'seasonal_adjustment_name'
        ]
    )
}}

select *
from {{ ref('int_wellbeing_observations_current') }}

{% endsnapshot %}
