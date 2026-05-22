select
    {{ dbt_utils.generate_surrogate_key(["data_marking_code"]) }} as data_marking_sk,
    data_marking_code,
    data_marking_reason,
    is_suppressed,
    is_low_quality,
    desctiption AS `data_marking_description`
from {{ ref("data_marking_values") }}