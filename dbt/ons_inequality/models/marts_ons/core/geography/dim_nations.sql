{{
    config(
        materialized='table',
        schema='marts_ons',
        description="""
                    Dimensional model for nations dimension, sourced from the int_nations intermediate model.
                """
    )
}}

select 
    nation_sk,
    nation_code,
    nation_name
from {{ ref("int_nations") }}