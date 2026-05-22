select 
    local_authority_sk,
    local_authority_code,
    local_authority_name,
    region_code,
    region_name
from {{ ref("int_local_authority_districts") }}