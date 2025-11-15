"""
ACON configuration for Finance Stock Data processing using Lakehouse Engine.
This processes stock market data from Alpha Vantage and yfinance into bronze/silver/gold layers.
"""

FINANCE_STOCKS_ACON = {
    "input_specs": [
        {
            "spec_id": "finance_stocks_bronze",
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
            "spec_id": "finance_stocks_with_metadata",
            "input_id": "finance_stocks_bronze",
            "transformers": [
                {
                    "function": "with_row_id",
                },
                {
                    "function": "add_current_date",
                    "args": {
                        "output_col": "lhe_processing_timestamp",
                    },
                },
            ],
        }
    ],
    "dq_specs": [
        {
            "spec_id": "finance_stocks_dq",
            "input_id": "finance_stocks_with_metadata",
            "dq_type": "validator",
            "result_sink_db_table": "finance.stocks_dq_checks",
            "fail_on_error": False,
            "dq_functions": [
                {
                    "dq_function": "expect_column_values_to_not_be_null",
                    "args": {
                        "column": "symbol",
                    },
                },
                {
                    "dq_function": "expect_column_values_to_be_in_set",
                    "args": {
                        "column": "symbol",
                        "value_set": ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
                    },
                },
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "finance_stocks_silver",
            "input_id": "finance_stocks_dq",
            "data_format": "delta",
            "write_type": "append",
            "location": "lakehouse/silver/finance/stocks/",
            "options": {
                "mergeSchema": True,
            },
        }
    ],
    "terminate_specs": [
        {
            "function": "optimize_dataset",
            "args": {
                "location": "lakehouse/silver/finance/stocks/",
            }
        }
    ],
    "exec_env": {
        "spark.sql.adaptive.enabled": True,
        "spark.sql.adaptive.coalescePartitions.enabled": True,
    },
}

