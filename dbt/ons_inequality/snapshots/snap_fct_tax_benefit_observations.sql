{% snapshot snap_fct_tax_benefit_observations %}

{{
    config(
        target_schema='fct_ons',
        strategy='check',
        unique_key='source_observation_sk',
        check_cols=[
            'observation_value',
            'time_period_type',
            'time_period_id',
            'period_label',
            'time_label',
            'period_start_year',
            'period_end_year',
            'period_start_date',
            'period_end_date',
            'geography_code',
            'geography_name',
            'quintile_code',
            'quintile_name',
            'summary_statistic_code',
            'summary_statistic_name',
            'income_type_code',
            'income_type_name',
            'deflation_basis_code',
            'deflation_basis_name'
        ]
    )
}}

select *
from {{ ref('int_tax_benefit_observations_current') }}

{% endsnapshot %}
