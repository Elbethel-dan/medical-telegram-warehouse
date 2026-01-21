from fastapi import APIRouter, Depends
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from api.app.database import get_db
from api.app.schemas import VisualContentStat

router = APIRouter(tags=["Reports"])

@router.get(
    "/visual-content",
    response_model=List[VisualContentStat],
    tags=["Reports"],
    summary="Visual content statistics",
    description="""
Aggregates image categories detected per channel.
Useful for understanding visual marketing trends.
""",
)
async def visual_content_stats(
    db: AsyncSession = Depends(get_db),
):
    sql = text("""
        SELECT
            m.channel_name,
            y.image_category,
            COUNT(DISTINCT y.message_id) AS post_count
        FROM raw.stg_yolo_detections y
        JOIN raw.stg_telegram_messages m
            ON y.message_id = m.message_id
        GROUP BY m.channel_name, y.image_category
        ORDER BY post_count DESC
    """)

    result = await db.execute(sql)
    rows = result.fetchall()

    return [
        {
            "channel_name": r.channel_name,
            "image_category": r.image_category,
            "post_count": r.post_count,
        }
        for r in rows
    ]
