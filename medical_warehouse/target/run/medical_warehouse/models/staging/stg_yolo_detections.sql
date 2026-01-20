
  create view "telegram_db"."raw"."stg_yolo_detections__dbt_tmp"
    
    
  as (
    with raw as (
    select
        message_id,
        channel_name,
        class_id,
        detected_class,
        confidence_score,
        image_category
    from "telegram_db"."raw"."yolo_detections"
)

select
    message_id,
    channel_name,
    class_id,
    detected_class,
    confidence_score,
    image_category
from raw
  );