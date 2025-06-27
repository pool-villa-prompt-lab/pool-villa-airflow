from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import os
import logging

# Import the function to upload to Google Sheets
from main import save_to_google_sheets

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def upload_to_google_sheet():
    file_path = "/opt/airflow/data/villa_data.csv"
    sheet_name = "output_villa.csv"  # Same as filename or customize as needed
    spreadsheet_id="1d2ceX5UrhktK5trydMheAm6wBn0ub-fh-g23t1vHkvg"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    save_to_google_sheets(
        file_path="/opt/airflow/data/villa_data.csv",
        file_name="villa_data.csv",          # ไม่ได้ใช้เมื่อส่ง id ตรง ๆ
        spreadsheet_id="1d2ceX5UrhktK5trydMheAm6wBn0ub-fh-g23t1vHkvg"
    )

    if spreadsheet_id:
        logging.info(f"Successfully uploaded to Google Sheet: {spreadsheet_id}")
    else:
        raise Exception("Failed to upload to Google Sheets")

with DAG(
    dag_id='villa_uploader_dag',
    description='Upload villa CSV to Google Sheets every hour',
    default_args=default_args,
    schedule='*/5 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['google_sheets', 'villa'],
) as dag:

    upload_task = PythonOperator(
        task_id='upload_villa_csv',
        python_callable=upload_to_google_sheet,
    )
