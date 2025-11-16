"""
ACON configuration for Currency Exchange Data processing using Lakehouse Engine.
This processes exchange rates from various currency APIs.
"""

CURRENCY_ACON = {
    "input_specs": [
        {
            "spec_id": "currency_bronze",
            "read_type": "batch",
            "data_format": "json",
            "options": {
                "multiLine": True,
                "inferSchema": True,
            },
            "location": "data_files/currency/*.json",
        }
    ],
    "transform_specs": [
        {
            "spec_id": "currency_with_metadata",
            "input_id": "currency_bronze",
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
            "spec_id": "currency_dq",
            "input_id": "currency_with_metadata",
            "dq_type": "validator",
            "result_sink_db_table": "currency.exchange_rates_dq_checks",
            "fail_on_error": False,
            "dq_functions": [
                {
                    "function": "expect_column_values_to_not_be_null",
                    "args": {
                        "column": "base",
                    },
                },
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "currency_silver",
            "input_id": "currency_dq",
            "data_format": "delta",
            "write_type": "append",
            "location": "lakehouse/silver/currency/exchange_rates/",
            "options": {
                "mergeSchema": True,
            },
        }
    ],
    "terminate_specs": [
        {
            "function": "optimize_dataset",
            "args": {
                "location": "lakehouse/silver/currency/exchange_rates/",
            }
        }
    ],
    "exec_env": {
        "spark.sql.adaptive.enabled": True,
        "spark.sql.adaptive.coalescePartitions.enabled": True,
    },
}

