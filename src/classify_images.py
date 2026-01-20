import pandas as pd
from pathlib import Path

# -------------------------
# Settings
# -------------------------
DETECTIONS_FILE = Path("/Users/elbethelzewdie/Downloads/medical-telegram-warehouse/medical-telegram-warehouse/data/processed/yolo_detections.csv")

# Load detection results
if not DETECTIONS_FILE.exists():
    print(f"{DETECTIONS_FILE} not found. Run YOLO detection first.")
    exit()

df = pd.read_csv(DETECTIONS_FILE)

PRODUCT_CLASSES = {"bottle", "box", "cup", "container"}
PERSON_CLASSES = {"person"}

# Classification function
def classify_image(group):
    detected_classes = set(group["class_name"].str.lower())
    has_person = any(cls in PERSON_CLASSES for cls in detected_classes)
    has_product = any(cls in PRODUCT_CLASSES for cls in detected_classes)

    if has_person and has_product:
        return "promotional"
    elif has_product and not has_person:
        return "product_display"
    elif has_person and not has_product:
        return "lifestyle"
    else:
        return "other"

# Get image categories per message_id
categories = df.groupby("message_id").apply(classify_image, include_groups=False).reset_index()
categories.columns = ["message_id", "image_category"]

# Merge classification back into the detections dataframe
df = df.drop(columns=["image_category"], errors='ignore')  # remove if exists
df = df.merge(categories, on="message_id", how="left")

# Overwrite the same detections CSV file with added image_category column
df.to_csv(DETECTIONS_FILE, index=False)

print(f"Updated {DETECTIONS_FILE} with image_category column.")
