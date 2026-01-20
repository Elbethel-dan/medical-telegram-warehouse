
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sender
from "telegram_db"."raw"."stg_telegram_messages"
where sender is null



  
  
      
    ) dbt_internal_test