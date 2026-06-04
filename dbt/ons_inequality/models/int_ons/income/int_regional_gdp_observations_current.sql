{{
    config(
        materialized="view",
        schema="int_ons",
        description="""
            Current-state regional GDP observations used as the source for the
            SCD type 2 fact snapshot.
        """,
    )
}}

with
    regional_gdp_by_year as (

        select
            'reg_gdp_by_year' as source_dataset,
            observation_sk as source_observation_sk,
            observation_value,
            data_marking,
            is_suppressed,
            'calendar_year' as time_period_type,
            cast(calendar_year as string) as time_period_id,
            time_label,
            time_grain,
            calendar_year,
            cast(null as int64) as quarter,
            region_code,
            region_name,
            industry_code,
            industry_name,
            price_basis_code,
            price_basis_name,
            measure_code,
            measure_name
        from {{ ref("stg_regional_gdp_by_year") }}

    ),

    regional_gdp_by_quarter as (

        select
            'reg_gdp_by_quarter' as source_dataset,
            observation_sk as source_observation_sk,
            observation_value,
            data_marking,
            is_suppressed,
            'quarter' as time_period_type,
            quarter_id as time_period_id,
            time_label,
            time_grain,
            year as calendar_year,
            quarter,
            region_code,
            region_name,
            industry_code,
            industry_name,
            price_basis_code,
            price_basis_name,
            measure_code,
            measure_name
        from {{ ref("stg_regional_gdp_by_quarter") }}

    ),

    unioned as (

        select *
        from regional_gdp_by_year
        where calendar_year between 2012 and 2021

        union all

        select *
        from regional_gdp_by_quarter
        where
            (calendar_year > 2012 or (calendar_year = 2012 and quarter >= 1))
            and (calendar_year < 2021 or (calendar_year = 2021 and quarter <= 4))

    )

select
    {{ dbt_utils.generate_surrogate_key(["source_dataset", "source_observation_sk"]) }}
    as regional_gdp_observation_sk,
    source_dataset,
    source_observation_sk,
    observation_value,
    data_marking,
    is_suppressed,
    time_period_type,
    time_period_id,
    time_label,
    time_grain,
    calendar_year,
    quarter,
    u.region_code,
    u.region_name,
    ons_geo.region_code as ons_region_code,
    ons_geo.region_name as ons_region_name,
    industry_code,
    industry_name,
    price_basis_code,
    price_basis_name,
    measure_code,
    measure_name

from unioned u
left join
    {{ ref("int_local_authority_districts") }} ons_geo
    on u.region_name = ons_geo.region_name
