# Docker Setup for Lakehouse Engine

This directory contains Docker configuration and helper scripts for running the Lakehouse Engine locally.

## Quick Start

### Using Make (Recommended)

```bash
# Build and start services
make build
make init  # First time only
make up

# Run all pipelines
make run-orchestrator

# Run individual pipeline
make run-finance

# View logs
make logs-lakehouse

# Open shell in container
make shell
```

### Using Docker Compose Directly

```bash
# Set Airflow user
export AIRFLOW_UID=$(id -u)

# Initialize (first time only)
docker-compose up airflow-init

# Build and start
docker-compose up -d --build

# Run Lakehouse Engine job
docker-compose exec lakehouse-engine python -m lakehouse.jobs.orchestrator
```

## Files

- `entrypoint_lakehouse.sh` - Entrypoint script for lakehouse-engine service
- `run_lakehouse_job.sh` - Helper script to run jobs from host
- `Makefile` - Convenient make targets for common operations

## Services

- **airflow-webserver**: Airflow UI at http://localhost:8080
- **airflow-scheduler**: Airflow scheduler
- **lakehouse-engine**: Service for running Lakehouse Engine jobs
- **postgres**: PostgreSQL database

## Common Commands

See `DOCKER_USAGE.md` in the project root for detailed usage instructions.

