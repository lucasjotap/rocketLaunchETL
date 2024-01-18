import json
import pathlib

import airflow
import requests
import requests.exception as requests_exceptions
import new_extract.api_data_to_parquet as parquet_job
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
	dag_id="new_extract",
	start_date=aiflow.utils.dates.days_ago(14),
	schedule_interval=None,
	)

def _run_job():
	parquet_job()

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