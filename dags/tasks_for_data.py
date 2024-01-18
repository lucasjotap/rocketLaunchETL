import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exceptions
from ETLprocess.job.data_files.new_extract import api_data_to_parquet
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="tasks_for_data",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

def _run_job():
    api_data_to_parquet()

run_job = PythonOperator(
    task_id="run_job",
    python_callable=_run_job,
    dag=dag
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)

run_job >> notify
