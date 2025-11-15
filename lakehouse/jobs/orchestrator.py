"""
Orchestrator script to run all financial data ETL pipelines using Lakehouse Engine.
This script can be used to process all data sources in sequence or parallel.
"""

import sys
import os
from typing import List, Optional

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from lakehouse.jobs import (
    run_finance_stocks_etl,
    run_economics_etl,
    run_currency_etl,
    run_blockchain_etl,
    run_bitcoin_etl,
)


def run_all_etl_pipelines(pipelines: Optional[List[str]] = None):
    """
    Execute all or selected ETL pipelines.
    
    Args:
        pipelines: List of pipeline names to run. If None, runs all pipelines.
                   Available: 'finance', 'economics', 'currency', 'blockchain', 'bitcoin'
    """
    available_pipelines = {
        'finance': run_finance_stocks_etl,
        'economics': run_economics_etl,
        'currency': run_currency_etl,
        'blockchain': run_blockchain_etl,
        'bitcoin': run_bitcoin_etl,
    }
    
    if pipelines is None:
        pipelines = list(available_pipelines.keys())
    
    print("=" * 80)
    print("Financial Data Lakehouse ETL Orchestrator")
    print("=" * 80)
    print(f"Running pipelines: {', '.join(pipelines)}")
    print("=" * 80)
    
    results = {}
    
    for pipeline_name in pipelines:
        if pipeline_name not in available_pipelines:
            print(f"Warning: Unknown pipeline '{pipeline_name}'. Skipping...")
            continue
        
        print(f"\n{'='*80}")
        print(f"Pipeline: {pipeline_name.upper()}")
        print(f"{'='*80}\n")
        
        try:
            pipeline_func = available_pipelines[pipeline_name]
            result = pipeline_func()
            results[pipeline_name] = {
                'status': 'success',
                'result': result
            }
            print(f"\n✓ Pipeline '{pipeline_name}' completed successfully")
        except Exception as e:
            results[pipeline_name] = {
                'status': 'failed',
                'error': str(e)
            }
            print(f"\n✗ Pipeline '{pipeline_name}' failed: {str(e)}")
    
    # Summary
    print("\n" + "=" * 80)
    print("ETL Orchestration Summary")
    print("=" * 80)
    for pipeline_name, result in results.items():
        status_icon = "✓" if result['status'] == 'success' else "✗"
        print(f"{status_icon} {pipeline_name}: {result['status']}")
    print("=" * 80)
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run financial data ETL pipelines')
    parser.add_argument(
        '--pipelines',
        nargs='+',
        choices=['finance', 'economics', 'currency', 'blockchain', 'bitcoin'],
        help='Specific pipelines to run (default: all)'
    )
    
    args = parser.parse_args()
    run_all_etl_pipelines(args.pipelines)

