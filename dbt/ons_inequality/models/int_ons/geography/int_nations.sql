with
    source as (
        select *
        from {{ ref("seed_nation_lookup") }}
    ),
renamed AS (
    select
        `Nation` as nation_name,
        `Nation Code` as nation_code
    from source
),
final as (
    select
        {{ dbt_utils.generate_surrogate_key(["nation_code"]) }} as nation_sk,
        nation_code,
        nation_name
    from renamed
    where nation_code is not null
)