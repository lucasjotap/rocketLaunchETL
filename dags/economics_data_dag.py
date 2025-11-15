"""
Economics DAG - Fetches economic data from FRED (Federal Reserve Economic Data) API
Alternative: World Bank API for global economic indicators
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import json
import os
import pandas as pd

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
    'economics_data',
    default_args=default_args,
    description='Fetch economic indicators from FRED and World Bank APIs',
    schedule_interval='0 10 * * 1',  # Every Monday at 10 AM
    catchup=False,
    tags=['economics', 'fred', 'world-bank', 'indicators'],
)

def fetch_fred_data():
    """Fetch economic data from FRED API"""
    api_key = os.getenv('FRED_API_KEY', 'demo')
    
    # Popular economic indicators
    series_ids = {
        'GDP': 'GDP',
        'UNRATE': 'UNRATE',  # Unemployment Rate
        'CPIAUCSL': 'CPIAUCSL',  # Consumer Price Index
        'FEDFUNDS': 'FEDFUNDS',  # Federal Funds Rate
        'DGS10': 'DGS10',  # 10-Year Treasury Rate
    }
    
    data_dir = '/opt/airflow/data_files/economics'
    os.makedirs(data_dir, exist_ok=True)
    
    all_data = []
    
    for name, series_id in series_ids.items():
        url = 'https://api.stlouisfed.org/fred/series/observations'
        params = {
            'series_id': series_id,
            'api_key': api_key,
            'file_type': 'json',
            'limit': 100,
            'sort_order': 'desc'
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                all_data.append({
                    'indicator': name,
                    'series_id': series_id,
                    'data': data,
                    'fetched_at': datetime.now().isoformat()
                })
                print(f"Successfully fetched {name} data")
        except Exception as e:
            print(f"Error fetching {name}: {str(e)}")
    
    # Save to JSON
    output_file = f"{data_dir}/fred_data_{datetime.now().strftime('%Y%m%d')}.json"
    with open(output_file, 'w') as f:
        json.dump(all_data, f, indent=2)
    
    print(f"FRED data saved to {output_file}")
    return output_file

def fetch_world_bank_data():
    """Fetch economic data from World Bank API"""
    data_dir = '/opt/airflow/data_files/economics'
    os.makedirs(data_dir, exist_ok=True)
    
    # World Bank indicators
    indicators = {
        'NY.GDP.MKTP.CD': 'GDP (current US$)',
        'SP.POP.TOTL': 'Population, total',
        'NY.GDP.PCAP.CD': 'GDP per capita (current US$)',
        'SL.UEM.TOTL.ZS': 'Unemployment, total (% of total labor force)',
    }
    
    countries = ['USA', 'CHN', 'JPN', 'DEU', 'GBR']  # Country codes
    
    all_data = []
    
    for indicator_code, indicator_name in indicators.items():
        for country in countries:
            url = f'https://api.worldbank.org/v2/country/{country}/indicator/{indicator_code}'
            params = {
                'format': 'json',
                'date': '2010:2024',
                'per_page': 100
            }
            
            try:
                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if len(data) > 1 and data[1]:
                        all_data.append({
                            'country': country,
                            'indicator': indicator_name,
                            'indicator_code': indicator_code,
                            'data': data[1],
                            'fetched_at': datetime.now().isoformat()
                        })
                        print(f"Successfully fetched {indicator_name} for {country}")
            except Exception as e:
                print(f"Error fetching {indicator_code} for {country}: {str(e)}")
    
    # Save to JSON
    output_file = f"{data_dir}/worldbank_data_{datetime.now().strftime('%Y%m%d')}.json"
    with open(output_file, 'w') as f:
        json.dump(all_data, f, indent=2)
    
    print(f"World Bank data saved to {output_file}")
    return output_file

def process_economics_data(**context):
    """Process and summarize economics data"""
    data_dir = '/opt/airflow/data_files/economics'
    
    if not os.path.exists(data_dir):
        print("No economics data directory found")
        return
    
    files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    
    if not files:
        print("No data files found")
        return
    
    summary = {
        'total_files': len(files),
        'fetch_timestamp': datetime.now().isoformat(),
        'files': files
    }
    
    summary_file = f"{data_dir}/summary_{datetime.now().strftime('%Y%m%d')}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"Economics summary saved to {summary_file}")

fetch_fred = PythonOperator(
    task_id='fetch_fred_data',
    python_callable=fetch_fred_data,
    dag=dag,
)

fetch_worldbank = PythonOperator(
    task_id='fetch_worldbank_data',
    python_callable=fetch_world_bank_data,
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_economics_data',
    python_callable=process_economics_data,
    dag=dag,
)

notify = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Economics data extraction completed at $(date)"',
    dag=dag,
)

[fetch_fred, fetch_worldbank] >> process_data >> notify

