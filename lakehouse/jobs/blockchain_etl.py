"""
ETL job for processing Blockchain Data using Lakehouse Engine.
This job reads blockchain statistics from JSON files, applies transformations and DQ checks,
and writes to Delta format in the silver layer.
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from lakehouse_engine.engine import load_data
from lakehouse.acon_configs.blockchain_acon import BLOCKCHAIN_ACON


def run_blockchain_etl():
    """
    Execute the Blockchain ETL pipeline using Lakehouse Engine.
    
    This function:
    1. Reads blockchain data from JSON files (bronze layer)
    2. Applies transformations (adds row IDs and timestamps)
    3. Performs data quality validations
    4. Writes to Delta format (silver layer)
    5. Optimizes the dataset
    """
    print("Starting Blockchain ETL pipeline...")
    print("=" * 60)
    
    try:
        result = load_data(acon=BLOCKCHAIN_ACON)
        print("=" * 60)
        print("Blockchain ETL pipeline completed successfully!")
        return result
    except Exception as e:
        print(f"Error in Blockchain ETL pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    run_blockchain_etl()

