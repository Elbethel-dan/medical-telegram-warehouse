from pydantic import BaseModel
from datetime import date

class MessageSearchResult(BaseModel):
    message_id: int
    channel_name: str
    message_text: str
    message_date: date

class ChannelActivity(BaseModel):
    channel_name: str
    date: date
    post_count: int

class TopProduct(BaseModel):
    detected_class: str
    count: int

class VisualContentStat(BaseModel):
    channel_name: str
    image_category: str
    post_count: int
