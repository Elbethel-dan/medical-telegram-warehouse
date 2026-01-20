

with meet_condition as (
    select * from "telegram_db"."raw"."fct_messages" where 1=1
)

select
    *
from meet_condition

where not(view_count view_count >= 0)

