# Ethiopian Medical Telegram Data Platform

## 📌 Project Overview

This project builds an end-to-end ELT data platform to analyze Ethiopian medical businesses using data scraped from public Telegram channels. The system transforms raw Telegram data into a clean, tested, analytics-ready data warehouse, enriched with image-based insights using YOLO object detection.

---

## 🏢 Business Goal

Enable data-driven insights such as:

- Most frequently mentioned medical products

- Channel-level activity and posting trends

- Visual content analysis (images with products vs people)

- Daily and weekly message volume trends

---
## Tech Stack

- Python (Telethon, Pandas)

- PostgreSQL (Data Warehouse)

- dbt (Transformations, testing, documentation)

- YOLOv8 (Object detection)

- Docker (PostgreSQL environment)
  
---

## ✅ Pipeline Summary:
#### 1. Extract & Load

- Scrape Telegram messages and images

- Store raw JSON and images in a data lake structure

#### 2. Transform (dbt)

- Clean and standardize data (staging models)

- Build a star schema:

  - dim_channels

  - dim_dates

  - fct_messages

- Enforce data quality with schema and custom tests

#### 3. Image Enrichment

- Detect objects in images using YOLOv8

- Classify images as:

  - promotional

  - product_display

  - lifestyle

  - other

- Store results in fct_image_detections

---

## Data Quality

- Primary key and foreign key tests

- Custom business rules:

  - No future-dated messages
  
  - Non-negative view counts

- All tests validated via dbt test

---
## How to Run
bash 
```
dbt run
dbt test
dbt docs generate
dbt docs serve
```

