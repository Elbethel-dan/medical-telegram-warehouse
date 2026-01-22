import asyncio
import subprocess
from dagster import op, job, get_dagster_logger, schedule, failure_hook
import os
import requests

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

@failure_hook
def slack_failure_alert(context):
    job_name = context.job_name
    step_key = context.step_key
    error_message = context.failure_event.error.message if context.failure_event else "Unknown error"

    message = (
        f":red_circle: Pipeline *{job_name}* failed at step *{step_key}*.\n"
        f"Error: {error_message}"
    )

    # Send message to Slack
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": message})
    except Exception as e:
        context.log.error(f"Failed to send Slack alert: {e}")


logger = get_dagster_logger()

@op
def scrape_telegram_data():
    logger.info("Starting Telegram scraping...")

    # Run your telegram scraper script as subprocess
    # Adjust the path if needed
    result = subprocess.run(
        ["python", "scripts/telegram.py", "--path", "data", "--limit", "500"],
        capture_output=True,
        text=True
    )

    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception("Telegram scraper failed")

    return "Scraping complete"


@op
def load_raw_to_postgres(scrape_result):
    logger.info("Loading raw JSON data into PostgreSQL...")

    result = subprocess.run(
        ["python", "scripts/load_raw_telegram_messages.py", "--path", "data"],
        capture_output=True,
        text=True
    )

    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception("Loading raw data failed")

    return "Load complete"


@op
def run_dbt_models(load_result):
    logger.info("Running dbt models...")

    cmd = [
        "dbt",
        "run",
        "--project-dir", "medical_warehouse",
        "--profiles-dir", "medical_warehouse",
        "--select",
        "stg_telegram_messages",
        "dim_channels",
        "dim_dates",
        "fct_messages",
    ]

    logger.info(f"Running command: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)

    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception("dbt run failed")

    return True

@op
def run_dbt_tests(models_result):
    logger.info("Running dbt tests...")

    cmd = [
        "dbt",
        "test",
        "--project-dir", "medical_warehouse",
        "--profiles-dir", "medical_warehouse",
        "--select",
        "assert_no_future_messages",
        "assert_positive_views",
    ]

    logger.info(f"Running command: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)

    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception("dbt tests failed")

    return True



@op
def run_yolo_enrichment(models_result):
    logger.info("Running YOLO enrichment...")

    result = subprocess.run(
        ["python", "src/yolo_detect.py"],
        capture_output=True,
        text=True
    )

    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception("YOLO enrichment failed")

    return "YOLO enrichment complete"


@job(hooks={slack_failure_alert})
def telegram_pipeline():
    scrape = scrape_telegram_data()
    load = load_raw_to_postgres(scrape)
    models = run_dbt_models(load)
    tests = run_dbt_tests(models)
    yolo = run_yolo_enrichment(models)


@schedule(
    cron_schedule="0 0 1 * *",  # at midnight on day 1 of every month
    job=telegram_pipeline,
    execution_timezone="Africa/Addis_Ababa",
)
def monthly_telegram_schedule(_context):
    return {}