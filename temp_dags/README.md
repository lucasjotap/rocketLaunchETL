# Lakehouse Engine DAGs

These DAGs load JSON files from `data_files/` into Delta tables using the Lakehouse Engine.

## Files

- `lakehouse_finance_dag.py` - Loads finance stock data
- `lakehouse_economics_dag.py` - Loads economics data
- `lakehouse_currency_dag.py` - Loads currency exchange data
- `lakehouse_blockchain_dag.py` - Loads blockchain data
- `lakehouse_bitcoin_dag.py` - Loads bitcoin/crypto data

## Installation

To move these DAGs to the `dags/` directory:

```bash
# Option 1: Fix permissions and copy
sudo chown -R $USER:$USER dags/
cp temp_dags/lakehouse_*.py dags/

# Option 2: Use the helper script
bash scripts/move_dags.sh
```

## Usage

These DAGs will automatically appear in Airflow UI after copying to `dags/` directory.

Each DAG:
1. Checks for source JSON files in `data_files/`
2. Loads data to Delta using Lakehouse Engine
3. Verifies the Delta table was created
4. Sends completion notification

## Testing

See `tests/test_lakehouse_dags.py` for mock tests and `tests/mock_data_generator.py` for generating test data.

