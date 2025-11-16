"""
Lakehouse Blockchain DAG - Loads blockchain data from JSON files into Delta tables
using the Lakehouse Engine.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'lakehouse_blockchain_load',
    default_args=default_args,
    description='Load blockchain data from JSON to Delta using Lakehouse Engine',
    schedule_interval='0 */5 * * *',  # Every 5 hours
    catchup=False,
    tags=['lakehouse', 'blockchain', 'delta', 'etl'],
)

def check_data_files(**context):
    """Check if source data files exist"""
    data_dir = '/opt/airflow/data_files/blockchain'
    if not os.path.exists(data_dir):
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    json_files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {data_dir}")
    
    print(f"Found {len(json_files)} JSON files in {data_dir}")
    return json_files

def load_blockchain_to_delta(**context):
    """Load blockchain data to Delta using Lakehouse Engine"""
    try:
        from lakehouse.jobs import run_blockchain_etl
        
        print("Starting Blockchain ETL pipeline...")
        result = run_blockchain_etl()
        print(f"ETL pipeline completed successfully. Result: {result}")
        return result
    except Exception as e:
        print(f"Error in Blockchain ETL: {str(e)}")
        raise

def verify_delta_table(**context):
    """Verify Delta table was created successfully"""
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("verify_delta_table") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        delta_path = "/opt/airflow/lakehouse/silver/blockchain/stats"
        
        if os.path.exists(delta_path):
            df = spark.read.format("delta").load(delta_path)
            count = df.count()
            print(f"✓ Delta table verified. Row count: {count}")
            return count
        else:
            raise FileNotFoundError(f"Delta table not found at {delta_path}")
    except Exception as e:
        print(f"Error verifying Delta table: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

check_files = PythonOperator(
    task_id='check_data_files',
    python_callable=check_data_files,
    dag=dag,
)

load_to_delta = PythonOperator(
    task_id='load_blockchain_to_delta',
    python_callable=load_blockchain_to_delta,
    dag=dag,
)

verify_table = PythonOperator(
    task_id='verify_delta_table',
    python_callable=verify_delta_table,
    dag=dag,
)

notify = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Blockchain data loaded to Delta Lake at $(date)"',
    dag=dag,
)

check_files >> load_to_delta >> verify_table >> notify

