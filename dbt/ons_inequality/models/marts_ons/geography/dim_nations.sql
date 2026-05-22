select 
    nation_sk,
    nation_code,
    nation_name
from {{ ref("int_nations") }}