
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: Ensure view_count is non-negative
select *
from "telegram_db"."raw"."fct_messages"
where cast(view_count as integer) < 0
  
  
      
    ) dbt_internal_test