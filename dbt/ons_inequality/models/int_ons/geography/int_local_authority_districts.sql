with
    source as (
        select *
        from {{ ref("seed_local_authority_to_region_lookup") }}
    ),

    renamed as (
        select
            cast(`Local Authority Code` as string) as local_authority_code,
            cast(`Local Authority Name` as string) as local_authority_name,
            cast(`Region Code` as string) as region_code,
            cast(`Region Name` as string) as region_name,
            safe_cast(`ObjectId` as int64) as object_id
        from source
    ),

    final as (
        select
            {{ dbt_utils.generate_surrogate_key(["local_authority_code"]) }}
            as local_authority_sk,
            local_authority_code,
            local_authority_name,
            region_code,
            region_name,
            object_id
        from renamed
        where local_authority_code is not null
        and local_authority_name not in (
            'Wales',
            'Scotland',
            'Northern Ireland',
            'England',
            'United Kingdom'
            )   
        )

select *
from final
