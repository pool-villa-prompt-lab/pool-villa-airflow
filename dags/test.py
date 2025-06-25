from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from playwright.sync_api import sync_playwright

def scrape_python_org():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto("https://www.python.org/")
        title = page.title()
        print(f"Page title: {title}")
        browser.close()

with DAG(
    dag_id="playwright_scrape_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "playwright"],
) as dag:
    scrape_task = PythonOperator(
        task_id="scrape_python_homepage",
        python_callable=scrape_python_org,
    )
