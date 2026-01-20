-- tests/assert_positive_views.sql
SELECT *
FROM "telegram_db"."raw"."fct_messages"
WHERE view_count < 0
   OR forward_count < 0