# Minimal Airflow Docker Setup

This is a minimal Docker configuration for Apache Airflow that runs DAGs **sequentially** (one at a time).

## Key Features

- **SequentialExecutor**: Ensures only one task runs at a time
- **Minimal Dependencies**: Only essential packages installed
- **Alpine PostgreSQL**: Lightweight database image
- **No Web UI by default**: Commented out to keep it minimal (can be enabled if needed)
- **Sequential DAG Execution**: Configured to run one DAG at a time

## Configuration for Sequential Execution

The following Airflow settings ensure sequential execution:

```yaml
AIRFLOW__CORE__EXECUTOR: SequentialExecutor
AIRFLOW__SCHEDULER__MAX_THREADS: '1'
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: '1'
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: '1'
```

## What's Included

### Services
1. **postgres**: PostgreSQL 15 Alpine (lightweight database)
2. **airflow-init**: One-time initialization service
3. **airflow-scheduler**: Runs DAGs sequentially
4. **airflow-webserver**: Commented out (uncomment if needed)

### Minimal Dockerfile
- Base: `apache/airflow:2.7.3-python3.11`
- Only essential system packages (curl for health checks)
- Python dependencies from `requirements.txt`
- DAGs and data files copied

## Usage

### Start Airflow
```bash
docker-compose up -d
```

### View Logs
```bash
# Scheduler logs
docker-compose logs -f airflow-scheduler

# All logs
docker-compose logs -f
```

### Stop Airflow
```bash
docker-compose down
```

### Stop and Remove Volumes
```bash
docker-compose down -v
```

### Enable Web UI (Optional)

Uncomment the `airflow-webserver` service in `docker-compose.yml`, then:
```bash
docker-compose up -d
```

Access at: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

## Verify Sequential Execution

1. Check scheduler logs:
   ```bash
   docker-compose logs airflow-scheduler | grep -i "executing"
   ```

2. You should see tasks running one at a time, not in parallel.

## Resource Usage

This minimal setup uses:
- **PostgreSQL**: ~50MB (Alpine image)
- **Airflow**: ~500MB (base image + dependencies)
- **Total**: ~600MB when running

## Troubleshooting

### Permission Issues (Linux)
```bash
# Set AIRFLOW_UID to your user ID
echo "AIRFLOW_UID=$(id -u)" > .env
```

### DAGs Not Running
1. Check if DAGs are unpaused:
   ```bash
   docker-compose exec airflow-scheduler airflow dags list
   ```

2. Trigger a DAG manually:
   ```bash
   docker-compose exec airflow-scheduler airflow dags trigger <dag_id>
   ```

### Database Connection Issues
```bash
# Check postgres health
docker-compose ps postgres

# View postgres logs
docker-compose logs postgres
```

## File Structure

```
rocketLaunchETL/
├── Dockerfile              # Minimal Airflow image
├── docker-compose.yml      # Minimal services
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables
├── .dockerignore          # Files to exclude from build
├── dags/                   # Your DAG files
├── data_files/            # Data files
└── logs/                   # Airflow logs (created automatically)
```

## Customization

### Add More Python Packages
Edit `requirements.txt` and rebuild:
```bash
docker-compose build
docker-compose up -d
```

### Change Sequential Settings
Edit `docker-compose.yml` under `airflow-common` environment variables.

### Enable Parallel Execution (if needed later)
Change executor to `LocalExecutor` and increase thread/task limits.

