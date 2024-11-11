from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
import os

# AWS S3 settings
S3_BUCKET_NAME = 'bq1-airflow-scraping-bucket'
S3_PREFIX = 'aircraft_fuel_data'

# Wikipedia URL
URL = "https://en.wikipedia.org/wiki/Fuel_economy_in_aircraft"

# Set up default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'scrape_and_load_to_s3',
    default_args=default_args,
    description='Scrape data from Wikipedia and upload to S3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def scrape_and_save_csv():
    # Scrape tables from Wikipedia page
    tables = pd.read_html(URL)

    # Define tables to save with appropriate filenames
    tables_to_save = {
        "Regional_Flights.csv": tables[2],
        "Short_Haul_Flights.csv": tables[3],
        "Medium_Haul_Flights.csv": tables[4]
    }

    # Save each table as a CSV file locally
    for filename, table in tables_to_save.items():
        table.to_csv(filename, index=False)
        print(f"Saved {filename}")

def upload_to_s3():
    # Set up S3 client
    s3 = boto3.client('s3')

    # Upload each CSV file to S3
    for filename in ["Commuter_flights.csv", "Regional_Flights.csv", "Short_Haul_Flights.csv", "Medium_Haul_Flights.csv"]:
        s3.upload_file(filename, S3_BUCKET_NAME, f"{S3_PREFIX}/{filename}")
        print(f"Uploaded {filename} to S3://{S3_BUCKET_NAME}/{S3_PREFIX}/{filename}")
        
        # Optionally, delete the file after upload to save space
        os.remove(filename)

# Define tasks
scrape_task = PythonOperator(
    task_id='scrape_and_save_csv',
    python_callable=scrape_and_save_csv,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    dag=dag,
)

# Task dependencies
scrape_task >> upload_task
