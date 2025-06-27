from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import asyncio
import json
import os

from main import load_villa_data, run_scraping_cycle

# Default args
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

STORAGE_PATH = "/opt/airflow/dags/storage.json"

def load_storage():
    if os.path.exists(STORAGE_PATH):
        with open(STORAGE_PATH, "r") as f:
            return json.load(f)
    else:
        return {}

def save_storage(data):
    with open(STORAGE_PATH, "w") as f:
        json.dump(data, f)

def run_scheduled_scraper():
    storage = load_storage()
    
    start_date = storage.get("start_date", "2024-06-25")
    run_count = storage.get("run_count", 0)
    
    # Update run count
    run_count += 1
    storage["run_count"] = run_count

    # Convert to datetime
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = start_dt + timedelta(days=30)

    # หากรันครบ 4 ครั้ง ให้ขยับ start_date ไปอีก 1 วัน
    if run_count >= 4:
        start_dt += timedelta(days=1)
        run_count = 0  # reset run count
        storage["start_date"] = start_dt.strftime("%Y-%m-%d")
        storage["run_count"] = run_count

    config = {
        "start_date": start_dt.strftime("%Y-%m-%d"),
        "end_date": end_dt.strftime("%Y-%m-%d"),
        "output": "villa_data.csv",
        "type": "both",
        "mode": "continuous",
    }

    save_storage(storage)

    villa_df = load_villa_data()
    if villa_df is None:
        raise ValueError("Cannot load villa data from Google Sheet")

    asyncio.run(run_scraping_cycle(config, villa_df))


# Define DAG
with DAG(
    dag_id='villa_scheduler_v2_dag',
    description='Scrape villa every 6 hours and slide window every 4 runs',
    default_args=default_args,
    schedule='0 */6 * * *',  # ทุก 6 ชั่วโมง
    start_date=datetime(2024, 6, 25),
    catchup=False,
    tags=['villa'],
) as dag:

    scheduled_scrape_task = PythonOperator(
        task_id='scheduled_scrape_task',
        python_callable=run_scheduled_scraper,
    )
