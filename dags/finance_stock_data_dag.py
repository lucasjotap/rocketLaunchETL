"""
Finance DAG - Fetches stock market data from Alpha Vantage API
Alternative: Uses yfinance as fallback (no API key required)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import pandas as pd
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
    'finance_stock_data',
    default_args=default_args,
    description='Fetch stock market data from finance APIs',
    schedule_interval='0 9 * * 1-5',  # Every weekday at 9 AM
    catchup=False,
    tags=['finance', 'stocks', 'market-data'],
)

def fetch_stock_data_alpha_vantage():
    """Fetch stock data from Alpha Vantage API"""
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY', 'demo')
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
    
    data_dir = '/opt/airflow/data_files/finance'
    os.makedirs(data_dir, exist_ok=True)
    
    all_data = []
    
    for symbol in symbols:
        url = f'https://www.alphavantage.co/query'
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'apikey': api_key,
            'outputsize': 'compact'
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'Time Series (Daily)' in data:
                    all_data.append({
                        'symbol': symbol,
                        'data': data,
                        'fetched_at': datetime.now().isoformat()
                    })
                    print(f"Successfully fetched data for {symbol}")
        except Exception as e:
            print(f"Error fetching {symbol}: {str(e)}")
    
    # Save to JSON
    output_file = f"{data_dir}/stock_data_{datetime.now().strftime('%Y%m%d')}.json"
    with open(output_file, 'w') as f:
        json.dump(all_data, f, indent=2)
    
    print(f"Data saved to {output_file}")
    return output_file

def fetch_stock_data_yfinance():
    """Fallback: Fetch stock data using yfinance (no API key needed)"""
    try:
        import yfinance as yf
        
        data_dir = '/opt/airflow/data_files/finance'
        os.makedirs(data_dir, exist_ok=True)
        
        symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        all_data = []
        
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(start=start_date, end=end_date)
                
                if not hist.empty:
                    data_dict = {
                        'symbol': symbol,
                        'data': hist.to_dict('records'),
                        'info': ticker.info,
                        'fetched_at': datetime.now().isoformat()
                    }
                    all_data.append(data_dict)
                    print(f"Successfully fetched {symbol} data via yfinance")
            except Exception as e:
                print(f"Error fetching {symbol} via yfinance: {str(e)}")
        
        # Save to JSON
        output_file = f"{data_dir}/stock_data_yfinance_{datetime.now().strftime('%Y%m%d')}.json"
        with open(output_file, 'w') as f:
            json.dump(all_data, f, indent=2, default=str)
        
        print(f"Data saved to {output_file}")
        return output_file
    except ImportError:
        print("yfinance not available, skipping")
        return None

def process_finance_data(**context):
    """Process and summarize finance data"""
    data_dir = '/opt/airflow/data_files/finance'
    files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    
    if not files:
        print("No data files found")
        return
    
    latest_file = max([os.path.join(data_dir, f) for f in files], key=os.path.getmtime)
    
    with open(latest_file, 'r') as f:
        data = json.load(f)
    
    summary = {
        'total_symbols': len(data),
        'fetch_timestamp': datetime.now().isoformat(),
        'symbols': [item.get('symbol') for item in data]
    }
    
    summary_file = f"{data_dir}/summary_{datetime.now().strftime('%Y%m%d')}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"Summary saved to {summary_file}")

fetch_alpha_vantage = PythonOperator(
    task_id='fetch_alpha_vantage',
    python_callable=fetch_stock_data_alpha_vantage,
    dag=dag,
)

fetch_yfinance = PythonOperator(
    task_id='fetch_yfinance',
    python_callable=fetch_stock_data_yfinance,
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_finance_data',
    python_callable=process_finance_data,
    dag=dag,
)

notify = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Finance data extraction completed at $(date)"',
    dag=dag,
)

[fetch_alpha_vantage, fetch_yfinance] >> process_data >> notify

