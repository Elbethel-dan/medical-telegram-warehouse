from fastapi import APIRouter, Query, Depends, HTTPException
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from api.app.database import get_db
from api.app.schemas import MessageSearchResult

router = APIRouter(tags=["Search"])

@router.get(
    "/messages",
    response_model=List[MessageSearchResult],
    tags=["Search"],
    summary="Search messages by keyword",
    description="""
Search Telegram messages containing a given keyword.

- Case-insensitive search
- Partial matches supported
- Results limited by the `limit` parameter
""",
)
async def search_messages(
    query: str = Query(
    ...,
        min_length=2,
        description="Keyword to search for in message text",
        examples={"example1": {"summary": "Example keyword", "value": "ibuprofen"}},
    ),

    limit: int = Query(
        20,
        gt=0,
        le=100,
        description="Maximum number of messages to return",
        examples=5,
    ),
    db: AsyncSession = Depends(get_db),
):
    sql = text("""
        SELECT message_id, channel_name, message_text, message_date
        FROM raw.stg_telegram_messages
        WHERE message_text ILIKE :pattern
        ORDER BY message_date DESC
        LIMIT :limit
    """)

    try:
        result = await db.execute(
            sql,
            {"pattern": f"%{query}%", "limit": limit}
        )
        rows = result.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        {
            "message_id": r.message_id,
            "channel_name": r.channel_name,
            "message_text": r.message_text,
            "message_date": r.message_date.date(),
        }
        for r in rows
    ]
