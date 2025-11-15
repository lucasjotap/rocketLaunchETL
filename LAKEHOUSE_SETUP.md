# Lakehouse Engine Integration - Setup Guide

This guide explains how the Lakehouse Engine has been integrated into your financial data ETL project.

## What Was Done

### 1. Lakehouse Engine Integration
- Cloned and integrated the [Lakehouse Engine](https://github.com/adidas/lakehouse-engine) framework
- The framework is now available as a local package in your project

### 2. Directory Structure Created
```
lakehouse/
├── acon_configs/        # ACON configuration files for each data source
├── jobs/                # ETL job scripts
├── bronze/              # Raw data in Delta format
├── silver/              # Cleaned and validated data
└── gold/                # Aggregated business-ready data
```

### 3. ACON Configurations
Created ACON (Algorithm Configuration) files for each data source:
- `finance_stocks_acon.py` - Stock market data
- `economics_acon.py` - Economic indicators
- `currency_acon.py` - Exchange rates
- `blockchain_acon.py` - Blockchain statistics
- `bitcoin_acon.py` - Cryptocurrency data

### 4. ETL Jobs
Created individual ETL jobs for each data source that:
- Read JSON data from `data_files/`
- Apply transformations
- Perform data quality checks
- Write to Delta format in silver layer
- Optimize datasets

### 5. Orchestrator
Created an orchestrator script to run all pipelines together.

## Quick Start

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Run a Single Pipeline

```python
from lakehouse.jobs import run_finance_stocks_etl

run_finance_stocks_etl()
```

### Run All Pipelines

```bash
python -m lakehouse.jobs.orchestrator
```

Or with specific pipelines:

```bash
python -m lakehouse.jobs.orchestrator --pipelines finance economics
```

## Architecture

The Lakehouse Engine follows a configuration-driven approach:

1. **ACON Files**: Define the entire ETL pipeline in Python dictionaries
2. **Input Specs**: Define how to read data
3. **Transform Specs**: Define transformations (no Spark code needed)
4. **DQ Specs**: Define data quality validations
5. **Output Specs**: Define how to write data
6. **Terminate Specs**: Define post-processing actions

## Key Features

- **No Spark Code Required**: Everything is configuration-driven
- **Data Quality Built-in**: Uses Great Expectations for validations
- **Delta Lake**: All data stored in Delta format
- **Automatic Optimization**: Datasets are optimized after writes
- **Extensible**: Easy to add new transformations and validations

## Next Steps

1. **Customize ACONs**: Edit the ACON files in `lakehouse/acon_configs/` to match your needs
2. **Add Transformations**: Add more transformations in `transform_specs`
3. **Add DQ Checks**: Add more data quality checks in `dq_specs`
4. **Create Gold Layer**: Add aggregations and business logic for the gold layer
5. **Integrate with Airflow**: Use the jobs in your existing Airflow DAGs

## Integration with Existing Code

Your existing Airflow DAGs in `dags/` continue to work as before. The Lakehouse Engine jobs can be:
- Called from Airflow tasks
- Run independently
- Scheduled separately

## Documentation

- See `lakehouse/README.md` for detailed usage
- See `lakehouse/examples/example_usage.py` for code examples
- Visit [Lakehouse Engine Docs](https://adidas.github.io/lakehouse-engine-docs/) for framework documentation

## Troubleshooting

### Import Errors
Make sure you're running from the project root and the Python path includes the project directory.

### Data Not Found
Ensure your JSON files are in the `data_files/` directory with the expected naming pattern.

### Spark Session Issues
The Lakehouse Engine creates its own Spark session. Make sure PySpark and Delta Spark are installed correctly.

## Support

For issues with:
- **Lakehouse Engine**: Check the [official documentation](https://adidas.github.io/lakehouse-engine-docs/)
- **Your Integration**: Check the ACON configurations and job scripts
- **Data Issues**: Verify your source data format matches the ACON expectations

