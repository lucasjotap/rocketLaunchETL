"""
Mock data generator for testing Lakehouse Engine DAGs
"""
import json
import os
from datetime import datetime
from pathlib import Path


def generate_mock_finance_data(output_dir: str = "/tmp/test_data_files/finance"):
    """Generate mock finance stock data"""
    os.makedirs(output_dir, exist_ok=True)
    
    data = [
        {
            "symbol": "AAPL",
            "data": {
                "Time Series (Daily)": {
                    "2024-01-01": {
                        "1. open": "150.0",
                        "2. high": "155.0",
                        "3. low": "149.0",
                        "4. close": "153.0",
                        "5. volume": "1000000"
                    }
                }
            },
            "fetched_at": datetime.now().isoformat()
        },
        {
            "symbol": "GOOGL",
            "data": {
                "Time Series (Daily)": {
                    "2024-01-01": {
                        "1. open": "140.0",
                        "2. high": "145.0",
                        "3. low": "139.0",
                        "4. close": "143.0",
                        "5. volume": "2000000"
                    }
                }
            },
            "fetched_at": datetime.now().isoformat()
        }
    ]
    
    output_file = os.path.join(output_dir, f"stock_data_{datetime.now().strftime('%Y%m%d')}.json")
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    return output_file


def generate_mock_economics_data(output_dir: str = "/tmp/test_data_files/economics"):
    """Generate mock economics data"""
    os.makedirs(output_dir, exist_ok=True)
    
    data = {
        "fred_data": {
            "observations": [
                {"date": "2024-01-01", "value": 100.0},
                {"date": "2024-01-02", "value": 101.0}
            ]
        },
        "fetched_at": datetime.now().isoformat()
    }
    
    output_file = os.path.join(output_dir, f"fred_data_{datetime.now().strftime('%Y%m%d')}.json")
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    return output_file


def generate_mock_currency_data(output_dir: str = "/tmp/test_data_files/currency"):
    """Generate mock currency exchange data"""
    os.makedirs(output_dir, exist_ok=True)
    
    data = {
        "base": "USD",
        "rates": {
            "EUR": 0.85,
            "GBP": 0.73,
            "JPY": 110.0
        },
        "date": datetime.now().strftime('%Y-%m-%d'),
        "fetched_at": datetime.now().isoformat()
    }
    
    output_file = os.path.join(output_dir, f"exchange_rates_{datetime.now().strftime('%Y%m%d')}.json")
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    return output_file


def generate_mock_blockchain_data(output_dir: str = "/tmp/test_data_files/blockchain"):
    """Generate mock blockchain data"""
    os.makedirs(output_dir, exist_ok=True)
    
    data = {
        "stats": {
            "n_blocks_total": 800000,
            "n_blocks_mined": 800000,
            "btc_mined": 21000000
        },
        "fetched_at": datetime.now().isoformat()
    }
    
    output_file = os.path.join(output_dir, f"blockchain_stats_{datetime.now().strftime('%Y%m%d')}.json")
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    return output_file


def generate_mock_bitcoin_data(output_dir: str = "/tmp/test_data_files/bitcoin"):
    """Generate mock bitcoin/crypto data"""
    os.makedirs(output_dir, exist_ok=True)
    
    data = {
        "prices": {
            "bitcoin": {
                "usd": 45000.0,
                "eur": 38250.0,
                "usd_market_cap": 850000000000,
                "usd_24h_change": 2.5
            }
        },
        "fetched_at": datetime.now().isoformat()
    }
    
    output_file = os.path.join(output_dir, f"coingecko_prices_{datetime.now().strftime('%Y%m%d')}.json")
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    return output_file


def generate_all_mock_data(base_dir: str = "/tmp/test_data_files"):
    """Generate all mock data files"""
    files = {
        "finance": generate_mock_finance_data(f"{base_dir}/finance"),
        "economics": generate_mock_economics_data(f"{base_dir}/economics"),
        "currency": generate_mock_currency_data(f"{base_dir}/currency"),
        "blockchain": generate_mock_blockchain_data(f"{base_dir}/blockchain"),
        "bitcoin": generate_mock_bitcoin_data(f"{base_dir}/bitcoin"),
    }
    
    print("Generated mock data files:")
    for source, filepath in files.items():
        print(f"  {source}: {filepath}")
    
    return files


if __name__ == "__main__":
    generate_all_mock_data()

