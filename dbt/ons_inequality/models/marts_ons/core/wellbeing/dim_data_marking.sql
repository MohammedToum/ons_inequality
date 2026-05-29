select
    data_marking_sk,
    data_marking_code,
    data_marking_reason,
    is_suppressed,
    is_low_quality,
    data_marking_description
from {{ ref("int_data_markings") }}
