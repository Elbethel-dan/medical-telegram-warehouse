-- Test: Ensure view_count is non-negative
select *
from {{ ref('fct_messages') }}
where cast(view_count as integer) < 0

