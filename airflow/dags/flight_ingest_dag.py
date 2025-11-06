from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.ingest import fetch_and_upload
from src.transform import transform
from src.load_to_snowflake import load

default_args = {'owner':'you','start_date':datetime(2024,2,1),'retries':1,'retry_delay':timedelta(minutes=5)}
with DAG('flight_pipeline', default_args=default_args, schedule_interval='@daily') as dag:
    t1 = PythonOperator(task_id='ingest', python_callable=fetch_and_upload)
    t2 = PythonOperator(task_id='transform', python_callable=lambda: transform("data/raw/latest.csv", "data/processed/processed.csv"))
    t3 = PythonOperator(task_id='load', python_callable=lambda: load("data/processed/processed.csv"))
    t1 >> t2 >> t3
