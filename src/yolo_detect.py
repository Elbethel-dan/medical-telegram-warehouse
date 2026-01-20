# src/yolo_detect.py

import pandas as pd
from pathlib import Path
from ultralytics import YOLO

# -------------------------
# Settings
# -------------------------
IMAGE_DIR = Path("/Users/elbethelzewdie/Downloads/medical-telegram-warehouse/medical-telegram-warehouse/data/raw/images")          # Folder containing subfolders with images
RESULTS_DIR = Path("/Users/elbethelzewdie/Downloads/medical-telegram-warehouse/medical-telegram-warehouse/data/processed")
RESULTS_FILE = RESULTS_DIR / "yolo_detections.csv"
YOLO_MODEL = "yolov8n.pt"                    # YOLOv8 nano model

# Ensure results folder exists
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------
# Find all images recursively
# -------------------------
image_files = list(IMAGE_DIR.rglob("*.jpg"))

if not image_files:
    print(f"No images found in {IMAGE_DIR} or its subfolders.")
    exit()

print(f"Found {len(image_files)} images in {IMAGE_DIR} and subfolders.")

# -------------------------
# Load YOLOv8 model
# -------------------------
model = YOLO(YOLO_MODEL)

# -------------------------
# Run detection and record results
# -------------------------
all_results = []

for img_path in image_files:
    message_id = img_path.stem              # filename = message_id
    channel_name = img_path.parent.name     # parent folder = channel

    print(f"Processing {img_path} (message_id={message_id})")

    results = model(img_path, verbose=False)

    for result in results:
        boxes = result.boxes
        if boxes is not None:
            for box in boxes:
                class_id = int(box.cls[0])
                all_results.append({
                    "message_id": message_id,
                    "channel_name": channel_name,
                    "image_path": str(img_path),
                    "class_id": class_id,
                    "class_name": model.names[class_id],
                    "confidence": float(box.conf[0])
                })

# -------------------------
# Save results to CSV
# -------------------------
if all_results:
    df = pd.DataFrame(all_results)
    df.to_csv(RESULTS_FILE, index=False)
    print(f"Detection results saved to {RESULTS_FILE}")
else:
    print("No objects detected.")
