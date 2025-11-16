# Spark Installation Fix

## Problem
PySpark was trying to find `/opt/spark/./bin/spark-submit` but Spark wasn't installed in the Docker container.

## Solution
Installed Spark 3.5.0 in the Docker container, which is compatible with PySpark 3.5.5.

## Changes Made

1. **Dockerfile**: Added Spark 3.5.0 download and installation
2. **Entrypoint script**: Removed `unset SPARK_HOME` since Spark is now properly installed
3. **Environment variables**: Set `SPARK_HOME=/opt/spark` correctly

## Rebuild Required

**IMPORTANT**: You must rebuild the Docker image for this fix to take effect:

```bash
# Stop containers
docker compose down

# Rebuild images (this will download Spark ~400MB)
docker compose build

# Start services
docker compose up -d
```

## Verification

After rebuilding, verify Spark is installed:

```bash
# Check Spark installation
docker compose exec lakehouse-engine ls -la /opt/spark/bin/spark-submit

# Test PySpark
docker compose exec lakehouse-engine python -c "from pyspark.sql import SparkSession; print('PySpark works!')"
```

## What Was Fixed

- ✅ Spark 3.5.0 installed at `/opt/spark`
- ✅ `SPARK_HOME` environment variable set correctly
- ✅ Spark binaries added to PATH
- ✅ Proper permissions set for airflow user
- ✅ Delta Lake extensions configured in PYSPARK_SUBMIT_ARGS

The Lakehouse Engine DAGs should now work correctly!

