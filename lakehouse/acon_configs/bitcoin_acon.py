"""
ACON configuration for Bitcoin/Crypto Data processing using Lakehouse Engine.
This processes cryptocurrency market data from CoinGecko and CoinAPI.
"""

BITCOIN_ACON = {
    "input_specs": [
        {
            "spec_id": "bitcoin_bronze",
            "read_type": "batch",
            "data_format": "json",
            "options": {
                "multiLine": True,
                "inferSchema": True,
            },
            "location": "data_files/bitcoin/*.json",
        }
    ],
    "transform_specs": [
        {
            "spec_id": "bitcoin_with_metadata",
            "input_id": "bitcoin_bronze",
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
            "spec_id": "bitcoin_dq",
            "input_id": "bitcoin_with_metadata",
            "dq_type": "validator",
            "result_sink_db_table": "bitcoin.crypto_dq_checks",
            "fail_on_error": False,
            "dq_functions": [
                {
                    "dq_function": "expect_column_values_to_not_be_null",
                    "args": {
                        "column": "id",
                    },
                },
                {
                    "dq_function": "expect_column_values_to_be_between",
                    "args": {
                        "column": "price",
                        "min_value": 0,
                        "max_value": 1000000000,
                    },
                },
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "bitcoin_silver",
            "input_id": "bitcoin_dq",
            "data_format": "delta",
            "write_type": "append",
            "location": "lakehouse/silver/bitcoin/market_data/",
            "options": {
                "mergeSchema": True,
            },
        }
    ],
    "terminate_specs": [
        {
            "function": "optimize_dataset",
            "args": {
                "location": "lakehouse/silver/bitcoin/market_data/",
            }
        }
    ],
    "exec_env": {
        "spark.sql.adaptive.enabled": True,
        "spark.sql.adaptive.coalescePartitions.enabled": True,
    },
}

