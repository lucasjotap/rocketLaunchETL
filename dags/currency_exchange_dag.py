"""
Currency Exchange DAG - Fetches exchange rates from ExchangeRate-API
Alternative: Fixer.io API for currency conversion data
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
    'currency_exchange',
    default_args=default_args,
    description='Fetch currency exchange rates from multiple APIs',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    tags=['currency', 'exchange-rate', 'money', 'forex'],
)

def fetch_exchangerate_api():
    """Fetch exchange rates from ExchangeRate-API (free tier)"""
    api_key = os.getenv('EXCHANGE_RATE_API_KEY', '')
    
    data_dir = '/opt/airflow/data_files/currency'
    os.makedirs(data_dir, exist_ok=True)
    
    # Base currency
    base_currency = 'USD'
    target_currencies = ['EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'INR', 'BRL']
    
    url = f'https://v6.exchangerate-api.com/v6/{api_key}/latest/{base_currency}'
    
    try:
        if not api_key or api_key == '':
            # Use free endpoint without API key
            url = f'https://api.exchangerate-api.com/v4/latest/{base_currency}'
        
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            result = {
                'base_currency': base_currency,
                'date': data.get('date', datetime.now().strftime('%Y-%m-%d')),
                'rates': data.get('rates', {}),
                'fetched_at': datetime.now().isoformat(),
                'source': 'exchangerate-api'
            }
            
            # Filter to target currencies
            filtered_rates = {k: v for k, v in result['rates'].items() if k in target_currencies}
            result['rates'] = filtered_rates
            
            output_file = f"{data_dir}/exchange_rates_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            print(f"Exchange rates saved to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error fetching exchange rates: {str(e)}")
        return None

def fetch_fixer_io():
    """Fetch exchange rates from Fixer.io API"""
    api_key = os.getenv('FIXER_API_KEY', '')
    
    data_dir = '/opt/airflow/data_files/currency'
    os.makedirs(data_dir, exist_ok=True)
    
    symbols = 'EUR,GBP,JPY,CAD,AUD,CHF,CNY,INR,BRL'
    url = f'http://data.fixer.io/api/latest'
    
    params = {
        'access_key': api_key,
        'symbols': symbols,
        'format': 1
    }
    
    try:
        if not api_key or api_key == '':
            print("Fixer.io API key not provided, skipping")
            return None
        
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            if data.get('success'):
                result = {
                    'base_currency': data.get('base', 'EUR'),
                    'date': data.get('date'),
                    'rates': data.get('rates', {}),
                    'fetched_at': datetime.now().isoformat(),
                    'source': 'fixer.io'
                }
                
                output_file = f"{data_dir}/fixer_rates_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
                with open(output_file, 'w') as f:
                    json.dump(result, f, indent=2)
                
                print(f"Fixer.io rates saved to {output_file}")
                return output_file
    except Exception as e:
        print(f"Error fetching Fixer.io rates: {str(e)}")
        return None

def fetch_currencylayer():
    """Fetch exchange rates from CurrencyLayer API (alternative)"""
    api_key = os.getenv('CURRENCYLAYER_API_KEY', '')
    
    data_dir = '/opt/airflow/data_files/currency'
    os.makedirs(data_dir, exist_ok=True)
    
    currencies = 'EUR,GBP,JPY,CAD,AUD,CHF,CNY,INR,BRL'
    url = f'http://api.currencylayer.com/live'
    
    params = {
        'access_key': api_key,
        'currencies': currencies,
        'format': 1
    }
    
    try:
        if not api_key or api_key == '':
            print("CurrencyLayer API key not provided, skipping")
            return None
        
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            if data.get('success'):
                result = {
                    'base_currency': 'USD',
                    'timestamp': data.get('timestamp'),
                    'quotes': data.get('quotes', {}),
                    'fetched_at': datetime.now().isoformat(),
                    'source': 'currencylayer'
                }
                
                output_file = f"{data_dir}/currencylayer_rates_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
                with open(output_file, 'w') as f:
                    json.dump(result, f, indent=2)
                
                print(f"CurrencyLayer rates saved to {output_file}")
                return output_file
    except Exception as e:
        print(f"Error fetching CurrencyLayer rates: {str(e)}")
        return None

def process_currency_data(**context):
    """Process and compare currency data from different sources"""
    data_dir = '/opt/airflow/data_files/currency'
    
    if not os.path.exists(data_dir):
        print("No currency data directory found")
        return
    
    files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    
    if not files:
        print("No currency data files found")
        return
    
    summary = {
        'total_files': len(files),
        'fetch_timestamp': datetime.now().isoformat(),
        'files': files,
        'latest_rates': {}
    }
    
    # Try to get latest rates
    latest_files = sorted([f for f in files if 'exchange_rates' in f], reverse=True)
    if latest_files:
        try:
            with open(os.path.join(data_dir, latest_files[0]), 'r') as f:
                data = json.load(f)
                summary['latest_rates'] = data.get('rates', {})
        except:
            pass
    
    summary_file = f"{data_dir}/summary_{datetime.now().strftime('%Y%m%d')}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"Currency summary saved to {summary_file}")

fetch_exchangerate = PythonOperator(
    task_id='fetch_exchangerate_api',
    python_callable=fetch_exchangerate_api,
    dag=dag,
)

fetch_fixer = PythonOperator(
    task_id='fetch_fixer_io',
    python_callable=fetch_fixer_io,
    dag=dag,
)

fetch_currencylayer = PythonOperator(
    task_id='fetch_currencylayer',
    python_callable=fetch_currencylayer,
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_currency_data',
    python_callable=process_currency_data,
    dag=dag,
)

notify = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Currency exchange data extraction completed at $(date)"',
    dag=dag,
)

[fetch_exchangerate, fetch_fixer, fetch_currencylayer] >> process_data >> notify

