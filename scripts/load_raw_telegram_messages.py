import os
import json
from pathlib import Path
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

DATA_LAKE_PATH = "/Users/elbethelzewdie/Downloads/medical-telegram-warehouse/medical-telegram-warehouse/data/raw/telegram_messages/2026-01-17"

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)

cursor = conn.cursor()

INSERT_QUERY = """
INSERT INTO raw.telegram_messages (
    id, channel_name, channel_title, message_text, message_date, views, forwards, has_media, image_path
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING
"""

def load_json_file(file_path: Path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        for msg in data:
            cursor.execute(INSERT_QUERY, (
                msg.get("message_id"),
                msg.get("channel_name"),
                msg.get("channel_title"),
                msg.get("message_text"),
                msg.get("message_date"),
                msg.get("views"),
                msg.get("forwards"),
                msg.get("has_media"),
                msg.get("image_path")
            ))
    else:
        msg = data
        cursor.execute(INSERT_QUERY, (
            msg.get("message_id"),
            msg.get("channel_name"),
            msg.get("channel_title"),
            msg.get("message_text"),
            msg.get("message_date"),
            msg.get("views"),
            msg.get("forwards"),
            msg.get("has_media"),
            msg.get("image_path")
        ))

def main():
    json_files = Path(DATA_LAKE_PATH).rglob("*.json")
    count = 0
    for file_path in json_files:
        if file_path.name.lower() == "_manifest.json":
            continue
        try:
            load_json_file(file_path)
            count += 1
        except Exception as e:
            print(f"❌ Failed to load {file_path}: {e}")
    conn.commit()
    print(f"✅ Loaded data from {count} JSON files into raw.telegram_messages")

if __name__ == "__main__":
    main()
    cursor.close()
    conn.close()
