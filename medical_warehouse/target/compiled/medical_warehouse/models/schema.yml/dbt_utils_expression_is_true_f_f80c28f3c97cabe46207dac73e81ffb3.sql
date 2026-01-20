

with meet_condition as (
    select * from "telegram_db"."raw"."fct_messages" where 1=1
)

select
    *
from meet_condition

where not(view_count cast(view_count as integer) >= 0)

