with yolo as (
    select
        message_id,
        detected_class,
        confidence_score,
        image_category
    from "telegram_db"."raw"."stg_yolo_detections"  -- use ref instead of source here
),

messages as (
    select
        message_id,
        channel_key,
        date_key
    from "telegram_db"."raw"."fct_messages"
)

select
    m.message_id,
    m.channel_key,
    m.date_key,
    y.detected_class,
    y.confidence_score,
    y.image_category
from messages m
left join yolo y
    on m.message_id = y.message_id