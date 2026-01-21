from fastapi import FastAPI

from api.app.search.message_search import router as message_search_router
from api.app.channel.channel_activity import router as channel_activity_router
from api.app.reports.top_products import router as top_products_router
from api.app.reports.visual_content import router as visual_content_router

app = FastAPI(
    title="Medical Telegram Analytics API",
    description="""
API for analyzing Ethiopian medical Telegram channels.

Features include:
- Message search
- Channel activity analytics
- Visual content analysis
- Top detected medical products
""",
    version="1.0.0",
    contact={
        "name": "Elbethel Zewdie",
        "email": "elbetheldaniel8@gmail.com",
    },
)


@app.get("/", summary="Health check")
async def root():
    return {"status": "API is running"}

app.include_router(message_search_router, prefix="/api/search")
app.include_router(channel_activity_router, prefix="/api/channel")
app.include_router(top_products_router, prefix="/api/reports")
app.include_router(visual_content_router, prefix="/api/reports")
