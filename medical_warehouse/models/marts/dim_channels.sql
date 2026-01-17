-- models/marts/dim_channels.sql
with channel_data as (
    select
        sender as channel_name,
        case
            when sender ilike '%pharma%' then 'Pharmaceutical'
            when sender ilike '%cosmetic%' then 'Cosmetics'
            else 'Medical'
        end as channel_type,
        min(message_date) as first_post_date,
        max(message_date) as last_post_date,
        count(*) as total_posts,
        avg(message_length) as avg_views
    from {{ ref('stg_telegram_messages') }}
    group by sender
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
