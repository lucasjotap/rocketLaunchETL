"""
ETL job for processing Finance Stock Data using Lakehouse Engine.
This job reads stock data from JSON files, applies transformations and DQ checks,
and writes to Delta format in the silver layer.
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from lakehouse_engine.engine import load_data
from lakehouse.acon_configs.finance_stocks_acon import FINANCE_STOCKS_ACON


def run_finance_stocks_etl():
    """
    Execute the Finance Stocks ETL pipeline using Lakehouse Engine.
    
    This function:
    1. Reads stock data from JSON files (bronze layer)
    2. Applies transformations (adds row IDs and timestamps)
    3. Performs data quality validations
    4. Writes to Delta format (silver layer)
    5. Optimizes the dataset
    """
    print("Starting Finance Stocks ETL pipeline...")
    print("=" * 60)
    
    try:
        result = load_data(acon=FINANCE_STOCKS_ACON)
        print("=" * 60)
        print("Finance Stocks ETL pipeline completed successfully!")
        return result
    except Exception as e:
        print(f"Error in Finance Stocks ETL pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    run_finance_stocks_etl()

