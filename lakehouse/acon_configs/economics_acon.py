"""
ACON configuration for Economics Data processing using Lakehouse Engine.
This processes economic indicators from FRED and World Bank APIs.
"""

ECONOMICS_ACON = {
    "input_specs": [
        {
            "spec_id": "economics_bronze",
            "read_type": "batch",
            "data_format": "json",
            "options": {
                "multiLine": True,
                "inferSchema": True,
            },
            "location": "data_files/economics/*.json",
        }
    ],
    "transform_specs": [
        {
            "spec_id": "economics_with_metadata",
            "input_id": "economics_bronze",
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
            "spec_id": "economics_dq",
            "input_id": "economics_with_metadata",
            "dq_type": "validator",
            "result_sink_db_table": "economics.economics_dq_checks",
            "fail_on_error": False,
            "dq_functions": [
                {
                    "function": "expect_column_values_to_not_be_null",
                    "args": {
                        "column": "date",
                    },
                },
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "economics_silver",
            "input_id": "economics_dq",
            "data_format": "delta",
            "write_type": "append",
            "location": "lakehouse/silver/economics/indicators/",
            "options": {
                "mergeSchema": True,
            },
        }
    ],
    "terminate_specs": [
        {
            "function": "optimize_dataset",
            "args": {
                "location": "lakehouse/silver/economics/indicators/",
            }
        }
    ],
    "exec_env": {
        "spark.sql.adaptive.enabled": True,
        "spark.sql.adaptive.coalescePartitions.enabled": True,
    },
}

