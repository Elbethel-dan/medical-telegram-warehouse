from fastapi import APIRouter, Query, Depends
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from api.app.database import get_db
from api.app.schemas import ChannelActivity

router = APIRouter(tags=["Channel"])

@router.get(
    "/channel_activity",
    response_model=List[ChannelActivity],
    tags=["Channel Analytics"],
    summary="Channel posting activity",
    description="""
Returns daily message counts per channel over a specified time window.
""",
)
async def channel_activity(
    days: int = Query(
        30,
        gt=0,
        le=365,
        description="Number of past days to analyze",
        example=30,
    ),
    db: AsyncSession = Depends(get_db),
):

    query = text("""
        SELECT
            channel_name,
            DATE(message_date) AS date,
            COUNT(*) AS post_count
        FROM raw.stg_telegram_messages
        WHERE message_date >= CURRENT_DATE - make_interval(days => :days)
        GROUP BY channel_name, DATE(message_date)
        ORDER BY channel_name, date;
    """)

    result = await db.execute(query, {"days": days})
    rows = result.fetchall()

    return [
        {
            "channel_name": r.channel_name,
            "date": r.date,
            "post_count": r.post_count,
        }
        for r in rows
    ]
