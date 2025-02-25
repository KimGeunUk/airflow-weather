import os
import sys
import pendulum
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

FILE_PATH = os.path.abspath(__file__); print(f'FILE_PATH : {FILE_PATH}')
PROJECT_PATH = os.path.dirname(FILE_PATH); print(f'PROJECT_PATH : {PROJECT_PATH}')
sys.path.append(PROJECT_PATH)

from update_weather import main

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'User',
    'start_date': datetime(2024, 12, 30, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

dag = DAG(
    dag_id='update_weather_dag',
    default_args=default_args,
    schedule_interval='10 0 * * *',  # 매일 00:10시에 실행
    catchup=False
)

process_task = PythonOperator(
    task_id='update_weather',
    python_callable=main,
    dag=dag
)
