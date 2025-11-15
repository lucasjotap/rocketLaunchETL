# Docker Usage Guide for Lakehouse Engine

This guide explains how to use Docker to run and test the Lakehouse Engine integration.

## Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available
- Ports 8080 (Airflow UI) and 4040 (Spark UI) available

## Quick Start

### 1. Build and Start Services

```bash
# Set Airflow user (Linux)
export AIRFLOW_UID=$(id -u)

# Initialize Airflow (first time only)
docker-compose up airflow-init

# Build and start all services
docker-compose up -d --build

# View logs
docker-compose logs -f
```

### 2. Access Services

- **Airflow UI**: http://localhost:8080
  - Username: `airflow`
  - Password: `airflow`
- **Spark UI**: http://localhost:4040 (when Spark jobs are running)

## Running Lakehouse Engine Jobs

### Option 1: Using Docker Exec (Recommended)

```bash
# Run all pipelines
docker-compose exec lakehouse-engine python -m lakehouse.jobs.orchestrator

# Run specific pipelines
docker-compose exec lakehouse-engine python -m lakehouse.jobs.orchestrator --pipelines finance economics

# Run individual pipeline
docker-compose exec lakehouse-engine python -m lakehouse.jobs.finance_stocks_etl
```

### Option 2: Using Helper Script

```bash
# Make script executable
chmod +x docker/run_lakehouse_job.sh

# Run orchestrator
./docker/run_lakehouse_job.sh orchestrator

# Run orchestrator with specific pipelines
./docker/run_lakehouse_job.sh orchestrator "finance economics"

# Run individual job
./docker/run_lakehouse_job.sh finance_stocks_etl
```

### Option 3: Interactive Shell

```bash
# Enter the container
docker-compose exec lakehouse-engine bash

# Inside the container, run jobs
python -m lakehouse.jobs.finance_stocks_etl
python -m lakehouse.jobs.orchestrator --pipelines finance
```

## Services

### Available Services

1. **postgres**: PostgreSQL database for Airflow
2. **airflow-init**: Initializes Airflow database
3. **airflow-scheduler**: Airflow scheduler service
4. **airflow-webserver**: Airflow web UI
5. **lakehouse-engine**: Service for running Lakehouse Engine jobs

### Lakehouse Engine Service

The `lakehouse-engine` service is a dedicated container for running Lakehouse Engine jobs. It includes:
- All Python dependencies
- Lakehouse Engine framework
- Spark and Delta Lake
- Access to all data files and lakehouse directories

## Directory Structure in Docker

```
/opt/airflow/
├── dags/                    # Airflow DAGs
├── data_files/              # Source JSON data (mounted from host)
├── lakehouse/               # Lakehouse architecture
│   ├── acon_configs/        # ACON configurations
│   ├── jobs/                # ETL job scripts
│   ├── bronze/              # Bronze layer (raw data)
│   ├── silver/              # Silver layer (cleaned data)
│   └── gold/                # Gold layer (aggregated data)
├── lakehouse_engine/        # Lakehouse Engine framework
└── logs/                    # Application logs
```

## Volumes

The following directories are mounted from your host machine:

- `./dags` → `/opt/airflow/dags`
- `./data_files` → `/opt/airflow/data_files`
- `./lakehouse` → `/opt/airflow/lakehouse`
- `./job` → `/opt/airflow/job`
- `./logs` → `/opt/airflow/logs`

This means changes to your code on the host are immediately available in the container.

## Environment Variables

You can customize Spark configuration by setting environment variables:

```bash
# In docker-compose.yml or .env file
SPARK_MASTER=local[*]
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
```

## Common Commands

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f lakehouse-engine
docker-compose logs -f airflow-scheduler
```

### Restart Services

```bash
# Restart all services
docker-compose restart

# Restart specific service
docker-compose restart lakehouse-engine
```

### Rebuild After Changes

```bash
# Rebuild and restart
docker-compose down
docker-compose build
docker-compose up -d
```

### Clean Up

```bash
# Stop services
docker-compose down

# Remove volumes (WARNING: deletes data)
docker-compose down -v
```

## Testing Lakehouse Engine

### 1. Test Individual Pipeline

```bash
docker-compose exec lakehouse-engine python -m lakehouse.jobs.finance_stocks_etl
```

### 2. Test All Pipelines

```bash
docker-compose exec lakehouse-engine python -m lakehouse.jobs.orchestrator
```

### 3. Test with Python Shell

```bash
docker-compose exec lakehouse-engine python

# In Python shell:
>>> from lakehouse.jobs import run_finance_stocks_etl
>>> run_finance_stocks_etl()
```

### 4. Check Generated Data

```bash
# List files in silver layer
docker-compose exec lakehouse-engine ls -la /opt/airflow/lakehouse/silver/finance/

# Or from host
ls -la lakehouse/silver/finance/
```

## Troubleshooting

### Issue: Import Errors

**Solution**: Make sure PYTHONPATH is set correctly:
```bash
docker-compose exec lakehouse-engine python -c "import sys; print(sys.path)"
```

### Issue: Spark Not Starting

**Solution**: Check Java installation:
```bash
docker-compose exec lakehouse-engine java -version
```

### Issue: Permission Errors

**Solution**: Set correct AIRFLOW_UID:
```bash
export AIRFLOW_UID=$(id -u)
docker-compose up -d
```

### Issue: Out of Memory

**Solution**: Reduce Spark memory settings in docker-compose.yml:
```yaml
SPARK_DRIVER_MEMORY: 1g
SPARK_EXECUTOR_MEMORY: 1g
```

### Issue: Data Not Found

**Solution**: Check volume mounts:
```bash
docker-compose exec lakehouse-engine ls -la /opt/airflow/data_files/
```

## Integration with Airflow

You can create Airflow DAGs that use the Lakehouse Engine jobs:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from lakehouse.jobs import run_finance_stocks_etl

dag = DAG('lakehouse_finance_etl', ...)

finance_etl_task = PythonOperator(
    task_id='finance_stocks_etl',
    python_callable=run_finance_stocks_etl,
    dag=dag,
)
```

## Next Steps

1. Run your first pipeline: `docker-compose exec lakehouse-engine python -m lakehouse.jobs.finance_stocks_etl`
2. Check the results in `lakehouse/silver/`
3. Customize ACON configurations in `lakehouse/acon_configs/`
4. Add more transformations and DQ checks
5. Create Airflow DAGs to schedule the jobs

## Additional Resources

- [Lakehouse Engine Documentation](https://adidas.github.io/lakehouse-engine-docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)

