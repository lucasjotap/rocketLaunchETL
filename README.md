# Rocket Launch ETL - Airflow DAGs

This project contains Apache Airflow DAGs for extracting data from various finance, economics, currency, blockchain, and cryptocurrency APIs.

## 🚀 Features

- **5 New DAGs** for different data sources:
  1. **Finance Stock Data** - Stock market data from Alpha Vantage and yfinance
  2. **Economics Data** - Economic indicators from FRED and World Bank
  3. **Currency Exchange** - Exchange rates from multiple APIs
  4. **Blockchain Data** - Bitcoin blockchain statistics and charts
  5. **Bitcoin/Crypto** - Cryptocurrency market data from CoinGecko

- **Docker Setup** - Complete Docker Compose configuration for Airflow
- **Automated Scheduling** - Each DAG runs on its own schedule
- **Data Persistence** - All extracted data saved to `data_files/` directory

## 📋 Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available
- Ports 8080 (Airflow UI) available

## 🛠️ Setup

### 1. Clone and Navigate

```bash
cd ~/Projects/rocketLaunchETL
```

### 2. Set Environment Variables (Optional)

Create a `.env` file for API keys (optional - many APIs work without keys):

```bash
cp .env.example .env
# Edit .env and add your API keys
```

**Available API Keys:**
- `ALPHA_VANTAGE_API_KEY` - For stock data (get free key at https://www.alphavantage.co/support/#api-key)
- `FRED_API_KEY` - For economic data (get free key at https://fred.stlouisfed.org/docs/api/api_key.html)
- `FIXER_API_KEY` - For currency data (https://fixer.io/)
- `COINAPI_API_KEY` - For cryptocurrency data (https://www.coinapi.io/)
- `EXCHANGE_RATE_API_KEY` - For exchange rates (optional)
- `CURRENCYLAYER_API_KEY` - Alternative currency API

**Note:** Most DAGs work without API keys using free endpoints!

### 3. Build and Start Airflow

```bash
# Set Airflow user (Linux)
export AIRFLOW_UID=$(id -u)

# Initialize Airflow (first time only)
docker-compose up airflow-init

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f
```

### 4. Access Airflow UI

Open your browser and navigate to:
- **URL:** http://localhost:8080
- **Username:** `airflow`
- **Password:** `airflow`

## 📊 DAGs Overview

### 1. Finance Stock Data (`finance_stock_data`)
- **Schedule:** Every weekday at 9 AM
- **Sources:** Alpha Vantage, yfinance
- **Data:** Stock prices for AAPL, GOOGL, MSFT, AMZN, TSLA
- **Output:** `data_files/finance/`

### 2. Economics Data (`economics_data`)
- **Schedule:** Every Monday at 10 AM
- **Sources:** FRED API, World Bank API
- **Data:** GDP, unemployment, CPI, interest rates, economic indicators
- **Output:** `data_files/economics/`

### 3. Currency Exchange (`currency_exchange`)
- **Schedule:** Every 6 hours
- **Sources:** ExchangeRate-API, Fixer.io, CurrencyLayer
- **Data:** Exchange rates for major currencies
- **Output:** `data_files/currency/`

### 4. Blockchain Data (`blockchain_data`)
- **Schedule:** Every 4 hours
- **Sources:** Blockchain.info
- **Data:** Blockchain stats, ticker, charts, latest block
- **Output:** `data_files/blockchain/`

### 5. Bitcoin/Crypto (`bitcoin_crypto`)
- **Schedule:** Every 2 hours
- **Sources:** CoinGecko, CoinAPI
- **Data:** Cryptocurrency prices, trending coins, market data, OHLC
- **Output:** `data_files/bitcoin/`

## 📁 Project Structure

```
rocketLaunchETL/
├── dags/                          # Airflow DAGs
│   ├── finance_stock_data_dag.py
│   ├── economics_data_dag.py
│   ├── currency_exchange_dag.py
│   ├── blockchain_data_dag.py
│   ├── bitcoin_crypto_dag.py
│   ├── tasks_for_data.py          # Original DAG
│   └── new_extract.py             # Original extract job
├── data_files/                    # Extracted data (created automatically)
│   ├── finance/
│   ├── economics/
│   ├── currency/
│   ├── blockchain/
│   └── bitcoin/
├── logs/                          # Airflow logs (created automatically)
├── plugins/                       # Airflow plugins (optional)
├── Dockerfile                     # Airflow Docker image
├── docker-compose.yml             # Docker Compose configuration
├── requirements.txt               # Python dependencies
├── .env.example                   # Environment variables template
└── README.md                      # This file
```

## 🔧 Management Commands

### Start Services
```bash
docker-compose up -d
```

### Stop Services
```bash
docker-compose down
```

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
```

### Restart a Service
```bash
docker-compose restart airflow-scheduler
```

### Rebuild After Changes
```bash
docker-compose down
docker-compose build
docker-compose up -d
```

### Access Airflow CLI
```bash
docker-compose exec airflow-webserver airflow version
docker-compose exec airflow-webserver airflow dags list
docker-compose exec airflow-webserver airflow tasks list <dag_id>
```

### Clean Up
```bash
# Stop and remove containers, networks
docker-compose down

# Remove volumes (WARNING: deletes database)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

## 📝 DAG Development

### Adding a New DAG

1. Create a new Python file in `dags/` directory
2. Follow the existing DAG structure
3. The DAG will automatically appear in Airflow UI after a few seconds

### Testing DAGs Locally

```bash
# Test DAG syntax
docker-compose exec airflow-webserver python -m py_compile /opt/airflow/dags/your_dag.py

# Run a specific task
docker-compose exec airflow-webserver airflow tasks test <dag_id> <task_id> <execution_date>
```

## 🔐 Security Notes

- Default Airflow credentials are `airflow/airflow` - **change in production!**
- API keys are optional for most DAGs
- Data is stored locally in `data_files/` directory
- Use `.env` file for sensitive credentials (not committed to git)

## 🐛 Troubleshooting

### DAGs Not Appearing
- Check DAG files for syntax errors
- Verify files are in `dags/` directory
- Check scheduler logs: `docker-compose logs airflow-scheduler`

### Port Already in Use
```bash
# Find process using port 8080
lsof -ti:8080

# Kill process
kill -9 $(lsof -ti:8080)
```

### Permission Issues
```bash
# Fix permissions
sudo chown -R $USER:$USER dags/ data_files/ logs/
```

### Out of Memory
- Increase Docker memory limit
- Reduce number of concurrent tasks
- Adjust `parallelism` in `docker-compose.yml`

## 📚 API Documentation

- **Alpha Vantage:** https://www.alphavantage.co/documentation/
- **FRED:** https://fred.stlouisfed.org/docs/api/
- **CoinGecko:** https://www.coingecko.com/en/api/documentation
- **Blockchain.info:** https://www.blockchain.com/api
- **ExchangeRate-API:** https://www.exchangerate-api.com/docs

## 🤝 Contributing

1. Create a new branch
2. Add your DAG to `dags/` directory
3. Test locally
4. Submit a pull request

## 📄 License

This project is open source and available for educational purposes.

---

**Happy Data Engineering! 🚀**

