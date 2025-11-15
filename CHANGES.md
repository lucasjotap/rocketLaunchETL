# Docker Minimal Setup - Changes Summary

## What Was Changed

### 1. Dockerfile - Minimized
**Before:**
- Installed `gcc`, `g++`, `openjdk-11-jdk` (full JDK)
- Multiple unnecessary build tools

**After:**
- Only `curl` (for health checks) and `openjdk-11-jdk-headless` (minimal Java, required for PySpark)
- Removed `gcc` and `g++` (not needed unless compiling C extensions)
- Used `--no-install-recommends` to minimize package size

**Size Reduction:** ~200MB smaller image

### 2. docker-compose.yml - Sequential Execution
**Before:**
- `LocalExecutor` (can run tasks in parallel)
- Web server enabled
- Multiple health checks
- Complex initialization script

**After:**
- `SequentialExecutor` (runs one task at a time)
- Web server commented out (can be enabled if needed)
- Simplified health checks
- Minimal initialization
- **Sequential configuration:**
  - `AIRFLOW__SCHEDULER__MAX_THREADS: '1'`
  - `AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: '1'`
  - `AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: '1'`

### 3. PostgreSQL - Lightweight
**Before:**
- `postgres:15` (full image)

**After:**
- `postgres:15-alpine` (Alpine Linux, ~50MB vs ~200MB)

### 4. New Files
- `.env` - Environment variables
- `.dockerignore` - Exclude unnecessary files from build
- `MINIMAL_SETUP.md` - Documentation
- `CHANGES.md` - This file

## Resource Usage Comparison

| Component | Before | After | Savings |
|-----------|--------|-------|---------|
| Dockerfile Base | ~800MB | ~600MB | ~200MB |
| PostgreSQL | ~200MB | ~50MB | ~150MB |
| Total | ~1GB | ~650MB | ~350MB |

## Sequential Execution Guarantees

The setup ensures DAGs run sequentially through:

1. **SequentialExecutor**: Only one task can run at a time
2. **MAX_THREADS=1**: Scheduler uses single thread
3. **MAX_ACTIVE_RUNS_PER_DAG=1**: Only one DAG run at a time
4. **MAX_ACTIVE_TASKS_PER_DAG=1**: Only one task per DAG at a time

## Quick Start

```bash
# Build and start
docker-compose up -d

# View logs
docker-compose logs -f airflow-scheduler

# Stop
docker-compose down
```

## Enabling Web UI (Optional)

If you need the web interface, uncomment the `airflow-webserver` service in `docker-compose.yml`:

```yaml
airflow-webserver:
  <<: *airflow-common
  command: webserver
  ports:
    - "8080:8080"
  restart: unless-stopped
  depends_on:
    <<: *airflow-common-depends-on
    airflow-init:
      condition: service_completed_successfully
```

Then restart:
```bash
docker-compose up -d
```

Access at: http://localhost:8080 (airflow/airflow)

