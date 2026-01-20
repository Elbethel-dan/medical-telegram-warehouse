
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- tests/assert_positive_views.sql
SELECT *
FROM "telegram_db"."raw"."fct_messages"
WHERE view_count < 0
   OR forward_count < 0
  
  
      
    ) dbt_internal_test