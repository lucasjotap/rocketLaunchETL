"""
ETL job for processing Currency Exchange Data using Lakehouse Engine.
This job reads exchange rates from JSON files, applies transformations and DQ checks,
and writes to Delta format in the silver layer.
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from lakehouse_engine.engine import load_data
from lakehouse.acon_configs.currency_acon import CURRENCY_ACON


def run_currency_etl():
    """
    Execute the Currency Exchange ETL pipeline using Lakehouse Engine.
    
    This function:
    1. Reads currency data from JSON files (bronze layer)
    2. Applies transformations (adds row IDs and timestamps)
    3. Performs data quality validations
    4. Writes to Delta format (silver layer)
    5. Optimizes the dataset
    """
    print("Starting Currency Exchange ETL pipeline...")
    print("=" * 60)
    
    try:
        result = load_data(acon=CURRENCY_ACON)
        print("=" * 60)
        print("Currency Exchange ETL pipeline completed successfully!")
        return result
    except Exception as e:
        print(f"Error in Currency Exchange ETL pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    run_currency_etl()

