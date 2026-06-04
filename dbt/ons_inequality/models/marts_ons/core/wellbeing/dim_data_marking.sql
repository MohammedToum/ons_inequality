{{
    config(
        materialized='table',
        schema='marts_ons',
        description="""
                    Dimensional model for data marking dimension, sourced from the int_data_markings intermediate model.
                """
    )
}}
select
    data_marking_sk,
    data_marking_code,
    data_marking_reason,
    is_suppressed,
    is_low_quality,
    data_marking_description
from {{ ref("int_data_markings") }}
