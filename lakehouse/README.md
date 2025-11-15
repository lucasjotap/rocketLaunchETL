# Financial Data Lakehouse Architecture

This directory contains the Lakehouse Engine implementation for processing financial data. The architecture follows the medallion (bronze/silver/gold) pattern, using the [Lakehouse Engine](https://github.com/adidas/lakehouse-engine) framework.

## Architecture Overview

```
data_files/          (Raw JSON data from APIs - Bronze Layer)
    ├── finance/
    ├── economics/
    ├── currency/
    ├── blockchain/
    └── bitcoin/

lakehouse/
    ├── bronze/      (Raw data in Delta format)
    ├── silver/      (Cleaned, validated, transformed data)
    └── gold/        (Aggregated, business-ready data)
```

## Directory Structure

```
lakehouse/
├── acon_configs/           # ACON (Algorithm Configuration) files
│   ├── finance_stocks_acon.py
│   ├── economics_acon.py
│   ├── currency_acon.py
│   ├── blockchain_acon.py
│   └── bitcoin_acon.py
├── jobs/                   # ETL job scripts
│   ├── finance_stocks_etl.py
│   ├── economics_etl.py
│   ├── currency_etl.py
│   ├── blockchain_etl.py
│   ├── bitcoin_etl.py
│   └── orchestrator.py     # Run all pipelines
└── README.md
```

## Features

- **Configuration-Driven**: All ETL logic is defined in ACON (Algorithm Configuration) files
- **Data Quality**: Built-in data quality validations using Great Expectations
- **Delta Lake**: All data stored in Delta format for ACID transactions and time travel
- **Transformations**: Easy-to-configure transformations without writing Spark code
- **Optimization**: Automatic dataset optimization after writes

## Installation

Make sure you have installed all dependencies:

```bash
pip install -r requirements.txt
```

The Lakehouse Engine is included as a local package in this project.

## Usage

### Run Individual ETL Pipeline

```python
from lakehouse.jobs import run_finance_stocks_etl

# Run finance stocks ETL
run_finance_stocks_etl()
```

### Run All Pipelines

```python
from lakehouse.jobs.orchestrator import run_all_etl_pipelines

# Run all pipelines
run_all_etl_pipelines()

# Or run specific pipelines
run_all_etl_pipelines(['finance', 'economics'])
```

### Command Line Usage

```bash
# Run all pipelines
python -m lakehouse.jobs.orchestrator

# Run specific pipelines
python -m lakehouse.jobs.orchestrator --pipelines finance economics
```

### Direct ACON Usage

You can also use the ACON configurations directly with the Lakehouse Engine:

```python
from lakehouse_engine.engine import load_data
from lakehouse.acon_configs.finance_stocks_acon import FINANCE_STOCKS_ACON

load_data(acon=FINANCE_STOCKS_ACON)
```

## ACON Configuration Structure

Each ACON configuration includes:

1. **input_specs**: Define how to read data from source (JSON files)
2. **transform_specs**: Apply transformations (add row IDs, timestamps, etc.)
3. **dq_specs**: Data quality validations using Great Expectations
4. **output_specs**: Define how to write data to Delta format
5. **terminate_specs**: Post-processing actions (optimization, etc.)
6. **exec_env**: Spark session configurations

## Data Flow

1. **Bronze Layer**: Raw JSON data from `data_files/` directory
2. **Silver Layer**: 
   - Data is read from bronze
   - Transformations are applied
   - Data quality checks are performed
   - Data is written to Delta format in `lakehouse/silver/`
3. **Gold Layer**: (Future) Aggregated and business-ready datasets

## Available Pipelines

- **Finance Stocks**: Processes stock market data from Alpha Vantage and yfinance
- **Economics**: Processes economic indicators from FRED and World Bank
- **Currency**: Processes exchange rates from various APIs
- **Blockchain**: Processes blockchain statistics and ticker data
- **Bitcoin/Crypto**: Processes cryptocurrency market data from CoinGecko

## Customization

To customize the ETL pipelines:

1. Edit the ACON configuration files in `lakehouse/acon_configs/`
2. Add new transformations in `transform_specs`
3. Add new data quality checks in `dq_specs`
4. Modify output locations in `output_specs`

## Integration with Airflow

The Lakehouse Engine jobs can be integrated with your existing Airflow DAGs:

```python
from airflow.operators.python import PythonOperator
from lakehouse.jobs import run_finance_stocks_etl

finance_etl_task = PythonOperator(
    task_id='finance_stocks_etl',
    python_callable=run_finance_stocks_etl,
    dag=dag,
)
```

## References

- [Lakehouse Engine Documentation](https://adidas.github.io/lakehouse-engine-docs/)
- [Lakehouse Engine GitHub](https://github.com/adidas/lakehouse-engine)
- [Great Expectations Documentation](https://docs.greatexpectations.io/)

