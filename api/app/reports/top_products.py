from fastapi import APIRouter, Query, Depends
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from api.app.database import get_db
from api.app.schemas import TopProduct

router = APIRouter(tags=["Reports"])

@router.get(
    "/top-products",
    response_model=List[TopProduct],
    tags=["Reports"],
    summary="Top detected products",
    description="""
Returns the most frequently detected medical products
from image-based YOLO detections.
""",
)

async def top_products(
    limit: int = Query(10, gt=0, le=100),
    db: AsyncSession = Depends(get_db),
):
    query = text("""
        SELECT detected_class, COUNT(*) AS count
        FROM raw.stg_yolo_detections
        GROUP BY detected_class
        ORDER BY count DESC
        LIMIT :limit
    """)

    result = await db.execute(query, {"limit": limit})
    rows = result.fetchall()

    return [{"detected_class": r.detected_class, "count": r.count} for r in rows]
