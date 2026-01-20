
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test: Ensure no Telegram messages have a message_date in the future
select *
from "telegram_db"."raw"."stg_telegram_messages"
where message_date > current_date
  
  
      
    ) dbt_internal_test