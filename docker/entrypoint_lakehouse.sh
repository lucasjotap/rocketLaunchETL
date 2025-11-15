#!/bin/bash
# Entrypoint script for Lakehouse Engine container

set -e

# Set Python path
export PYTHONPATH=/opt/airflow:${PYTHONPATH}

# Create necessary directories if they don't exist
mkdir -p /opt/airflow/lakehouse/{bronze,silver,gold}/{finance,economics,currency,blockchain,bitcoin}
mkdir -p /opt/airflow/lakehouse/schemas/{bronze,silver,gold}
mkdir -p /opt/airflow/logs

# Set Spark configuration for local mode
export SPARK_MASTER=${SPARK_MASTER:-"local[*]"}
export SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-"2g"}
export SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-"2g"}

# Execute the command
exec "$@"

