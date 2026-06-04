{{
    config(
        materialized='table',
        schema='marts_ons',
        description="""
                    Dimensional model for local authority districts dimension, sourced from the seed_local_authority_district_lookup table.
                """
    )
}}

select 
    local_authority_sk,
    local_authority_code,
    local_authority_name,
    region_code,
    region_name
from {{ ref("int_local_authority_districts") }}