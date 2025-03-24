import os
import sys
import pendulum
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

FILE_PATH = os.path.abspath(__file__)
PROJECT_PATH = os.path.dirname(FILE_PATH)
sys.path.append(PROJECT_PATH)

from update_weather_forecast import main

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'User',
    'start_date': datetime(2024, 12, 31, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

dag = DAG(
    dag_id='update_weather_forecast_dag',
    default_args=default_args,
    schedule_interval='10 2/3 * * *',  # 매일 02:10부터 3시간 마다 실행
    catchup=False
)

task = PythonOperator(
    task_id='update_weather_forecast',
    python_callable=main,
    dag=dag
)

task