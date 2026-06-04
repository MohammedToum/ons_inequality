{{
    config(
        materialized='view',
        schema='int_ons',
        description="""
            Integrated dimension table for ONS data markings, which are used to indicate
            the quality and reliability of ONS data. This dimension is used to
            link to fact tables in order to provide context on the quality of the
            data being observed.
        """
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(["data_marking_code"]) }} as data_marking_sk,
    data_marking_code,
    data_marking_reason,
    is_suppressed,
    is_low_quality,
    description AS `data_marking_description`
from {{ ref("seed_data_marking_values_lookup") }}