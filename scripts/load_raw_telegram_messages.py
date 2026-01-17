import os
import json
from pathlib import Path
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Path to your raw JSON files
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

# SQL query to insert a JSON message
INSERT_QUERY = """
INSERT INTO raw.telegram_messages (source_file, message)
VALUES (%s, %s)
"""

def load_json_file(file_path: Path):
    """
    Load a single JSON file and insert its content into the database.
    Supports both single JSON object or list of objects.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        for msg in data:
            cursor.execute(INSERT_QUERY, (file_path.name, Json(msg)))
    else:
        cursor.execute(INSERT_QUERY, (file_path.name, Json(data)))

def main():
    # Find all JSON files recursively
    json_files = Path(DATA_LAKE_PATH).rglob("*.json")

    count = 0
    for file_path in json_files:
        if file_path.name.lower() == "_manifest.json":
            continue  # skip manifest
        try:
            load_json_file(file_path)
            count += 1
        except Exception as e:
            print(f"❌ Failed to load {file_path}: {e}")
    # Commit all inserts
    conn.commit()
    print(f"✅ Loaded data from {count} JSON files into raw.telegram_messages")

if __name__ == "__main__":
    main()
    cursor.close()
    conn.close()
