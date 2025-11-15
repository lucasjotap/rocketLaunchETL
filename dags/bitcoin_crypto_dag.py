"""
Bitcoin & Cryptocurrency DAG - Fetches Bitcoin and crypto data from CoinGecko API
Alternative: CoinAPI for comprehensive cryptocurrency data
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
    'bitcoin_crypto',
    default_args=default_args,
    description='Fetch Bitcoin and cryptocurrency data from CoinGecko and CoinAPI',
    schedule_interval='0 */2 * * *',  # Every 2 hours
    catchup=False,
    tags=['bitcoin', 'cryptocurrency', 'crypto', 'coingecko', 'coinapi'],
)

def fetch_coingecko_market_data():
    """Fetch cryptocurrency market data from CoinGecko API (free, no API key needed)"""
    data_dir = '/opt/airflow/data_files/bitcoin'
    os.makedirs(data_dir, exist_ok=True)
    
    # Top cryptocurrencies by market cap
    coin_ids = 'bitcoin,ethereum,binancecoin,ripple,cardano,solana,polkadot,dogecoin,matic-network,shiba-inu'
    
    url = 'https://api.coingecko.com/api/v3/simple/price'
    params = {
        'ids': coin_ids,
        'vs_currencies': 'usd,eur,btc',
        'include_market_cap': 'true',
        'include_24hr_vol': 'true',
        'include_24hr_change': 'true',
        'include_last_updated_at': 'true'
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            
            result = {
                'prices': data,
                'fetched_at': datetime.now().isoformat(),
                'source': 'coingecko'
            }
            
            output_file = f"{data_dir}/coingecko_prices_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            print(f"CoinGecko prices saved to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error fetching CoinGecko data: {str(e)}")
        return None

def fetch_coingecko_trending():
    """Fetch trending cryptocurrencies from CoinGecko"""
    data_dir = '/opt/airflow/data_files/bitcoin'
    os.makedirs(data_dir, exist_ok=True)
    
    url = 'https://api.coingecko.com/api/v3/search/trending'
    
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json()
            
            result = {
                'trending': data,
                'fetched_at': datetime.now().isoformat(),
                'source': 'coingecko'
            }
            
            output_file = f"{data_dir}/coingecko_trending_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            print(f"CoinGecko trending saved to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error fetching CoinGecko trending: {str(e)}")
        return None

def fetch_coingecko_global():
    """Fetch global cryptocurrency market data"""
    data_dir = '/opt/airflow/data_files/bitcoin'
    os.makedirs(data_dir, exist_ok=True)
    
    url = 'https://api.coingecko.com/api/v3/global'
    
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json()
            
            result = {
                'global_data': data,
                'fetched_at': datetime.now().isoformat(),
                'source': 'coingecko'
            }
            
            output_file = f"{data_dir}/coingecko_global_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            print(f"CoinGecko global data saved to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error fetching CoinGecko global data: {str(e)}")
        return None

def fetch_coinapi_data():
    """Fetch cryptocurrency data from CoinAPI (requires API key)"""
    api_key = os.getenv('COINAPI_API_KEY', '')
    
    data_dir = '/opt/airflow/data_files/bitcoin'
    os.makedirs(data_dir, exist_ok=True)
    
    if not api_key or api_key == '':
        print("CoinAPI key not provided, skipping")
        return None
    
    url = 'https://rest.coinapi.io/v1/exchangerate/BTC/USD'
    headers = {
        'X-CoinAPI-Key': api_key
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            result = {
                'exchange_rate': data,
                'fetched_at': datetime.now().isoformat(),
                'source': 'coinapi'
            }
            
            output_file = f"{data_dir}/coinapi_rates_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            print(f"CoinAPI data saved to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error fetching CoinAPI data: {str(e)}")
        return None

def fetch_bitcoin_ohlc():
    """Fetch Bitcoin OHLC (Open, High, Low, Close) data from CoinGecko"""
    data_dir = '/opt/airflow/data_files/bitcoin'
    os.makedirs(data_dir, exist_ok=True)
    
    url = 'https://api.coingecko.com/api/v3/coins/bitcoin/ohlc'
    params = {
        'vs_currency': 'usd',
        'days': '7'  # Last 7 days
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            
            result = {
                'ohlc_data': data,
                'vs_currency': 'usd',
                'period': '7_days',
                'fetched_at': datetime.now().isoformat(),
                'source': 'coingecko'
            }
            
            output_file = f"{data_dir}/bitcoin_ohlc_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
            with open(output_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            print(f"Bitcoin OHLC data saved to {output_file}")
            return output_file
    except Exception as e:
        print(f"Error fetching Bitcoin OHLC: {str(e)}")
        return None

def process_bitcoin_data(**context):
    """Process and summarize Bitcoin/crypto data"""
    data_dir = '/opt/airflow/data_files/bitcoin'
    
    if not os.path.exists(data_dir):
        print("No bitcoin data directory found")
        return
    
    files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    
    if not files:
        print("No bitcoin data files found")
        return
    
    summary = {
        'total_files': len(files),
        'fetch_timestamp': datetime.now().isoformat(),
        'files': files
    }
    
    # Try to extract latest Bitcoin price
    price_files = [f for f in files if 'prices' in f]
    if price_files:
        try:
            latest_price_file = sorted(price_files, reverse=True)[0]
            with open(os.path.join(data_dir, latest_price_file), 'r') as f:
                price_data = json.load(f)
                if 'prices' in price_data and 'bitcoin' in price_data['prices']:
                    btc_data = price_data['prices']['bitcoin']
                    summary['latest_btc_price'] = {
                        'usd': btc_data.get('usd'),
                        'eur': btc_data.get('eur'),
                        'market_cap_usd': btc_data.get('usd_market_cap'),
                        '24h_change': btc_data.get('usd_24h_change')
                    }
        except Exception as e:
            print(f"Error extracting price data: {str(e)}")
    
    summary_file = f"{data_dir}/summary_{datetime.now().strftime('%Y%m%d')}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"Bitcoin summary saved to {summary_file}")

fetch_market_data = PythonOperator(
    task_id='fetch_coingecko_market_data',
    python_callable=fetch_coingecko_market_data,
    dag=dag,
)

fetch_trending = PythonOperator(
    task_id='fetch_coingecko_trending',
    python_callable=fetch_coingecko_trending,
    dag=dag,
)

fetch_global = PythonOperator(
    task_id='fetch_coingecko_global',
    python_callable=fetch_coingecko_global,
    dag=dag,
)

fetch_ohlc = PythonOperator(
    task_id='fetch_bitcoin_ohlc',
    python_callable=fetch_bitcoin_ohlc,
    dag=dag,
)

fetch_coinapi = PythonOperator(
    task_id='fetch_coinapi_data',
    python_callable=fetch_coinapi_data,
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_bitcoin_data',
    python_callable=process_bitcoin_data,
    dag=dag,
)

notify = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Bitcoin/crypto data extraction completed at $(date)"',
    dag=dag,
)

[fetch_market_data, fetch_trending, fetch_global, fetch_ohlc, fetch_coinapi] >> process_data >> notify

