from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

async def get_image_performance(db: AsyncSession):
    query = text("""
        SELECT
            y.image_category,
            COUNT(DISTINCT y.message_id) AS post_count,
            AVG(m.view_count) AS avg_views,
            AVG(m.forward_count) AS avg_forwards
        FROM raw.stg_yolo_detections y
        JOIN raw.stg_telegram_messages m ON y.message_id = m.message_id
        GROUP BY y.image_category
        ORDER BY avg_views DESC
    """)
    result = await db.execute(query)
    return result.fetchall()
