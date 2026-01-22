
  
    

  create  table "telegram_db"."raw"."fct_messages__dbt_tmp"
  
  
    as
  
  (
    -- models/marts/fct_messages.sql
select
    s.message_id,
    c.channel_key,
    d.date_key,
    s.message_text,
    s.message_length,
    s.view_count,
    s.forward_count,
    s.has_image
from "telegram_db"."raw"."stg_telegram_messages" s
left join "telegram_db"."raw"."dim_channels" c
    on s.channel_name = c.channel_name
left join "telegram_db"."raw"."dim_dates" d
    on s.message_date::date = d.full_date
  );
  