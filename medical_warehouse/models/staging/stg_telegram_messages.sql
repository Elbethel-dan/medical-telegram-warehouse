with source as (
    select *
    from {{ source('raw', 'telegram_messages') }}
),

cleaned as (
    select
        message_id,
        channel_name,
        message_text,
        char_length(message_text) as message_length,
        message_date::timestamp as message_date,
        views as view_count,
        forwards as forward_count,
        coalesce(has_media, false) as has_image
    from source
    where message_text is not null
      and message_text <> ''
      and channel_name is not null
)

select *
from cleaned
