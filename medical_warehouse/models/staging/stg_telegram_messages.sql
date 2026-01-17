-- models/staging/stg_telegram_messages.sql
with raw_messages as (
    select
        id,
        source_file,
        ingested_at,
        message
    from {{ source('raw', 'telegram_messages') }}
),

cleaned_messages as (
    select
        id,
        source_file,
        ingested_at::timestamp as ingested_at_ts,
        (message->>'date')::date as message_date,
        message->>'from' as sender,
        case
            when jsonb_typeof(message->'text') = 'array'
                then array_to_string(array(select jsonb_array_elements_text(message->'text')), ' ')
            else message->>'text'
        end as message_text,
        message->'media' as media_json,
        length(
            case
                when jsonb_typeof(message->'text') = 'array'
                    then array_to_string(array(select jsonb_array_elements_text(message->'text')), ' ')
                else message->>'text'
            end
        ) as message_length,
        case
            when message->'media' is not null then true
            else false
        end as has_media
    from raw_messages
)

select *
from cleaned_messages
where message_text is not null
  and message_text <> ''

