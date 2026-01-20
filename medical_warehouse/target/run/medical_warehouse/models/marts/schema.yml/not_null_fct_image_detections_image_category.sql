
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select image_category
from "telegram_db"."raw"."fct_image_detections"
where image_category is null



  
  
      
    ) dbt_internal_test