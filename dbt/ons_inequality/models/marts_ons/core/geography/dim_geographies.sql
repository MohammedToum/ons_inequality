{{
    config(
        materialized='table',
        schema='dim_ons',
        description="""
            Unified geography dimension for ONS fact tables across local
            authority, region, nation, United Kingdom, and GDP NUTS geographies.
        """
    )
}}

with local_authorities as (

    select
        local_authority_code as geography_code,
        local_authority_name as geography_name,
        'local_authority' as geography_level,
        region_code as parent_region_code,
        region_name as parent_region_name,
        case
            when region_code like 'E12%' then 'E92000001'
            when region_code = 'S92000003' then 'S92000003'
            when region_code = 'W92000004' then 'W92000004'
            when region_code = 'N92000002' then 'N92000002'
        end as parent_nation_code,
        case
            when region_code like 'E12%' then 'England'
            when region_code = 'S92000003' then 'Scotland'
            when region_code = 'W92000004' then 'Wales'
            when region_code = 'N92000002' then 'Northern Ireland'
        end as parent_nation_name
    from {{ ref('int_local_authority_districts') }}
    where local_authority_code != region_code

),

regions as (

    select distinct
        region_code as geography_code,
        region_name as geography_name,
        'region' as geography_level,
        cast(null as string) as parent_region_code,
        cast(null as string) as parent_region_name,
        'E92000001' as parent_nation_code,
        'England' as parent_nation_name
    from {{ ref('int_local_authority_districts') }}
    where region_code like 'E12%'

),

nations as (

    select
        nation_code as geography_code,
        nation_name as geography_name,
        case
            when nation_code = 'K02000001' then 'united_kingdom'
            else 'nation'
        end as geography_level,
        cast(null as string) as parent_region_code,
        cast(null as string) as parent_region_name,
        case
            when nation_code = 'K02000001' then cast(null as string)
            else nation_code
        end as parent_nation_code,
        case
            when nation_code = 'K02000001' then cast(null as string)
            else nation_name
        end as parent_nation_name
    from {{ ref('int_nations') }}

),

gdp_nuts_regions as (

    select distinct
        region_code as geography_code,
        region_name as geography_name,
        'nuts_region' as geography_level,
        cast(null as string) as parent_region_code,
        cast(null as string) as parent_region_name,
        case
            when region_code in ('UK0', 'UKC', 'UKD', 'UKE', 'UKF', 'UKG', 'UKH', 'UKI', 'UKJ', 'UKK') then 'E92000001'
            when region_code = 'UKL' then 'W92000004'
            when region_code = 'UKM' then 'S92000003'
            when region_code = 'UKN' then 'N92000002'
        end as parent_nation_code,
        case
            when region_code in ('UK0', 'UKC', 'UKD', 'UKE', 'UKF', 'UKG', 'UKH', 'UKI', 'UKJ', 'UKK') then 'England'
            when region_code = 'UKL' then 'Wales'
            when region_code = 'UKM' then 'Scotland'
            when region_code = 'UKN' then 'Northern Ireland'
        end as parent_nation_name
    from (
        select region_code, region_name
        from {{ ref('stg_regional_gdp_by_year') }}

        union distinct

        select region_code, region_name
        from {{ ref('stg_regional_gdp_by_quarter') }}
    )
    where region_code is not null

),

unioned as (

    select * from local_authorities
    union all
    select * from regions
    union all
    select * from nations
    union all
    select * from gdp_nuts_regions

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['geography_code']) }} as geography_sk,
        geography_code,
        geography_name,
        geography_level,
        parent_region_code,
        parent_region_name,
        parent_nation_code,
        parent_nation_name
    from unioned
    where geography_code is not null

)

select *
from final
