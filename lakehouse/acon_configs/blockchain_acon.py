"""
ACON configuration for Blockchain Data processing using Lakehouse Engine.
This processes blockchain statistics and ticker data.
"""

BLOCKCHAIN_ACON = {
    "input_specs": [
        {
            "spec_id": "blockchain_bronze",
            "read_type": "batch",
            "data_format": "json",
            "options": {
                "multiLine": True,
                "inferSchema": True,
            },
            "location": "data_files/blockchain/*.json",
        }
    ],
    "transform_specs": [
        {
            "spec_id": "blockchain_with_metadata",
            "input_id": "blockchain_bronze",
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
            "spec_id": "blockchain_dq",
            "input_id": "blockchain_with_metadata",
            "dq_type": "validator",
            "store_backend": "file_system",
            "local_fs_root_dir": "/opt/airflow/lakehouse/dq_artifacts",
            "result_sink_db_table": "blockchain.blockchain_dq_checks",
            "fail_on_error": False,
            "dq_functions": [
                {
                    "function": "expect_column_values_to_not_be_null",
                    "args": {
                        "column": "hash",
                    },
                },
            ],
        }
    ],
    "output_specs": [
        {
            "spec_id": "blockchain_silver",
            "input_id": "blockchain_dq",
            "data_format": "delta",
            "write_type": "append",
            "location": "lakehouse/silver/blockchain/stats/",
            "options": {
                "mergeSchema": True,
            },
        }
    ],
    "terminate_specs": [
        {
            "function": "optimize_dataset",
            "args": {
                "location": "lakehouse/silver/blockchain/stats/",
            }
        }
    ],
    "exec_env": {
        "spark.sql.adaptive.enabled": True,
        "spark.sql.adaptive.coalescePartitions.enabled": True,
    },
}

