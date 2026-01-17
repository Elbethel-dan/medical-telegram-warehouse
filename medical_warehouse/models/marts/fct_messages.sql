-- models/marts/fct_messages.sql
select
    id as message_id,
    c.channel_key,
    d.date_key,
    stg.message_text,
    stg.message_length,
    null as view_count,
    null as forward_count,
    stg.has_media as has_image
from {{ ref('stg_telegram_messages') }} stg
join {{ ref('dim_channels') }} c
    on stg.sender = c.channel_name
join {{ ref('dim_dates') }} d
    on stg.message_date = d.full_date
