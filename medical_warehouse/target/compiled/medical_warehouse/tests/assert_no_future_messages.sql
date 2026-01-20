-- Test: Ensure no Telegram messages have a message_date in the future
select *
from "telegram_db"."raw"."stg_telegram_messages"
where message_date > current_date