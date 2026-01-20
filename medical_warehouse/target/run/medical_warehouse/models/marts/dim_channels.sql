
  create view "telegram_db"."raw"."dim_channels__dbt_tmp"
    
    
  as (
    -- models/marts/dim_channels.sql
with channel_data as (
    select
        channel_name,
        case
            when channel_name ilike '%pharma%' then 'Pharmaceutical'
            when channel_name ilike '%cosmetic%' then 'Cosmetics'
            else 'Medical'
        end as channel_type,
        min(message_date) as first_post_date,
        max(message_date) as last_post_date,
        count(*) as total_posts,
        avg(view_count) as avg_views
    from "telegram_db"."raw"."stg_telegram_messages"
    group by channel_name
)

select
    row_number() over () as channel_key,
    channel_name,
    channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    avg_views
from channel_data
  );