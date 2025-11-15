#!/bin/bash
# Helper script to run Lakehouse Engine jobs in Docker

set -e

JOB_NAME=${1:-"orchestrator"}
PIPELINES=${2:-""}

echo "=========================================="
echo "Running Lakehouse Engine Job: $JOB_NAME"
echo "=========================================="

if [ "$JOB_NAME" = "orchestrator" ]; then
    if [ -n "$PIPELINES" ]; then
        docker-compose exec lakehouse-engine python -m lakehouse.jobs.orchestrator --pipelines $PIPELINES
    else
        docker-compose exec lakehouse-engine python -m lakehouse.jobs.orchestrator
    fi
else
    docker-compose exec lakehouse-engine python -m lakehouse.jobs.${JOB_NAME}
fi

