import json
import requests
from new_extract import ExtractJob
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
import requests.exceptions as requests_exceptions

# start_date=airflow.utils.dates.days_ago(14)
dag = DAG(
    dag_id="tasks_for_data",
    start_date=datetime.now(),
    schedule_interval=None,
)


def _run_job():
    ExtractJob.run()

run_job = PythonOperator(
    task_id="run_job",
    python_callable=_run_job,
    dag=dag
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "Files: $(ls /lake/space_flight | wc -l)."',
    dag=dag,
)

run_job >> notify
