"""
Blockchain DAG - Fetches blockchain data from Blockchain.info API
Fetches Bitcoin blockchain statistics and transaction data
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import json
import os

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
    'blockchain_data',
    default_args=default_args,
    description='Fetch blockchain statistics and data from Blockchain.info',
    schedule_interval='0 */4 * * *',  # Every 4 hours
    catchup=False,
    tags=['blockchain', 'bitcoin', 'crypto', 'btc'],
)

def fetch_blockchain_stats():
    """Fetch blockchain statistics from Blockchain.info"""
    data_dir = '/opt/airflow/data_files/blockchain'
    os.makedirs(data_dir, exist_ok=True)
    
    url = 'https://blockchain.info/stats?format=json'
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            result = {
                'stats': data,
                'fetched_at': datetime.now().isoformat(),
                'source': 'blockchain.info'
            }
            
            output_file = f"{data_dir}/blockchain_stats_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            print(f"Blockchain stats saved to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error fetching blockchain stats: {str(e)}")
        return None

def fetch_blockchain_ticker():
    """Fetch Bitcoin ticker data from Blockchain.info"""
    data_dir = '/opt/airflow/data_files/blockchain'
    os.makedirs(data_dir, exist_ok=True)
    
    url = 'https://blockchain.info/ticker'
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            result = {
                'ticker': data,
                'fetched_at': datetime.now().isoformat(),
                'source': 'blockchain.info'
            }
            
            output_file = f"{data_dir}/blockchain_ticker_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            print(f"Blockchain ticker saved to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error fetching blockchain ticker: {str(e)}")
        return None

def fetch_blockchain_charts():
    """Fetch blockchain charts data"""
    data_dir = '/opt/airflow/data_files/blockchain'
    os.makedirs(data_dir, exist_ok=True)
    
    # Various chart endpoints
    charts = {
        'market_price': 'https://blockchain.info/charts/market-price?format=json',
        'transactions_per_second': 'https://blockchain.info/charts/transactions-per-second?format=json',
        'hash_rate': 'https://blockchain.info/charts/hash-rate?format=json',
        'difficulty': 'https://blockchain.info/charts/difficulty?format=json',
    }
    
    all_charts = {}
    
    for chart_name, url in charts.items():
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                all_charts[chart_name] = {
                    'data': data,
                    'fetched_at': datetime.now().isoformat()
                }
                print(f"Successfully fetched {chart_name}")
        except Exception as e:
            print(f"Error fetching {chart_name}: {str(e)}")
    
    if all_charts:
        result = {
            'charts': all_charts,
            'fetched_at': datetime.now().isoformat(),
            'source': 'blockchain.info'
        }
        
        output_file = f"{data_dir}/blockchain_charts_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        
        print(f"Blockchain charts saved to {output_file}")
        return output_file
    
    return None

def fetch_blockchain_latest_block():
    """Fetch latest block information"""
    data_dir = '/opt/airflow/data_files/blockchain'
    os.makedirs(data_dir, exist_ok=True)
    
    url = 'https://blockchain.info/latestblock'
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            result = {
                'latest_block': data,
                'fetched_at': datetime.now().isoformat(),
                'source': 'blockchain.info'
            }
            
            output_file = f"{data_dir}/latest_block_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            print(f"Latest block saved to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error fetching latest block: {str(e)}")
        return None

def process_blockchain_data(**context):
    """Process and summarize blockchain data"""
    data_dir = '/opt/airflow/data_files/blockchain'
    
    if not os.path.exists(data_dir):
        print("No blockchain data directory found")
        return
    
    files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    
    if not files:
        print("No blockchain data files found")
        return
    
    summary = {
        'total_files': len(files),
        'fetch_timestamp': datetime.now().isoformat(),
        'files': files
    }
    
    # Try to extract key metrics from latest stats file
    stats_files = [f for f in files if 'stats' in f]
    if stats_files:
        try:
            latest_stats = sorted(stats_files, reverse=True)[0]
            with open(os.path.join(data_dir, latest_stats), 'r') as f:
                stats_data = json.load(f)
                if 'stats' in stats_data:
                    summary['key_metrics'] = {
                        'n_blocks_total': stats_data['stats'].get('n_blocks_total'),
                        'n_blocks_mined': stats_data['stats'].get('n_blocks_mined'),
                        'btc_mined': stats_data['stats'].get('btc_mined'),
                    }
        except:
            pass
    
    summary_file = f"{data_dir}/summary_{datetime.now().strftime('%Y%m%d')}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"Blockchain summary saved to {summary_file}")

fetch_stats = PythonOperator(
    task_id='fetch_blockchain_stats',
    python_callable=fetch_blockchain_stats,
    dag=dag,
)

fetch_ticker = PythonOperator(
    task_id='fetch_blockchain_ticker',
    python_callable=fetch_blockchain_ticker,
    dag=dag,
)

fetch_charts = PythonOperator(
    task_id='fetch_blockchain_charts',
    python_callable=fetch_blockchain_charts,
    dag=dag,
)

fetch_latest_block = PythonOperator(
    task_id='fetch_latest_block',
    python_callable=fetch_blockchain_latest_block,
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_blockchain_data',
    python_callable=process_blockchain_data,
    dag=dag,
)

notify = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Blockchain data extraction completed at $(date)"',
    dag=dag,
)

[fetch_stats, fetch_ticker, fetch_charts, fetch_latest_block] >> process_data >> notify

