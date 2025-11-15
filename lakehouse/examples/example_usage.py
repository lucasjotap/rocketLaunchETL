"""
Example usage of the Financial Data Lakehouse Engine.

This script demonstrates how to use the Lakehouse Engine to process financial data.
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from lakehouse_engine.engine import load_data
from lakehouse.acon_configs.finance_stocks_acon import FINANCE_STOCKS_ACON


def example_single_pipeline():
    """Example: Run a single ETL pipeline."""
    print("Example 1: Running Finance Stocks ETL Pipeline")
    print("=" * 60)
    
    # Load data using the Lakehouse Engine
    result = load_data(acon=FINANCE_STOCKS_ACON)
    
    print(f"Pipeline completed. Result: {result}")
    return result


def example_custom_acon():
    """Example: Create and use a custom ACON configuration."""
    print("\nExample 2: Custom ACON Configuration")
    print("=" * 60)
    
    # Custom ACON for a simple data load
    custom_acon = {
        "input_specs": [
            {
                "spec_id": "custom_bronze",
                "read_type": "batch",
                "data_format": "json",
                "options": {
                    "multiLine": True,
                    "inferSchema": True,
                },
                "location": "data_files/finance/stock_data_*.json",
            }
        ],
        "output_specs": [
            {
                "spec_id": "custom_silver",
                "input_id": "custom_bronze",
                "data_format": "delta",
                "write_type": "overwrite",
                "location": "lakehouse/bronze/finance/stocks/",
            }
        ],
    }
    
    result = load_data(acon=custom_acon)
    print(f"Custom pipeline completed. Result: {result}")
    return result


def example_with_transformations():
    """Example: ACON with custom transformations."""
    print("\nExample 3: ACON with Transformations")
    print("=" * 60)
    
    acon_with_transforms = {
        "input_specs": [
            {
                "spec_id": "finance_bronze",
                "read_type": "batch",
                "data_format": "json",
                "options": {
                    "multiLine": True,
                    "inferSchema": True,
                },
                "location": "data_files/finance/stock_data_*.json",
            }
        ],
        "transform_specs": [
            {
                "spec_id": "finance_transformed",
                "input_id": "finance_bronze",
                "transformers": [
                    {
                        "function": "with_row_id",
                    },
                    {
                        "function": "add_current_date",
                        "args": {
                            "output_col": "processed_at",
                        },
                    },
                    {
                        "function": "repartition",
                        "args": {
                            "num_partitions": 1,
                        },
                    },
                ],
            }
        ],
        "output_specs": [
            {
                "spec_id": "finance_silver",
                "input_id": "finance_transformed",
                "data_format": "delta",
                "write_type": "append",
                "location": "lakehouse/silver/finance/stocks/",
            }
        ],
    }
    
    result = load_data(acon=acon_with_transforms)
    print(f"Pipeline with transformations completed. Result: {result}")
    return result


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Financial Data Lakehouse Engine - Usage Examples")
    print("=" * 60 + "\n")
    
    try:
        # Run examples
        example_single_pipeline()
        # Uncomment to run other examples:
        # example_custom_acon()
        # example_with_transformations()
        
        print("\n" + "=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)
    except Exception as e:
        print(f"\nError running examples: {str(e)}")
        import traceback
        traceback.print_exc()

