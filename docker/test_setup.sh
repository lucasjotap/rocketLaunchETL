#!/bin/bash
# Test script to verify Docker setup for Lakehouse Engine

set -e

echo "=========================================="
echo "Testing Docker Setup for Lakehouse Engine"
echo "=========================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker."
    exit 1
fi
echo "✓ Docker is running"

# Check if containers are running
if ! docker-compose ps | grep -q "lakehouse-engine"; then
    echo "⚠️  lakehouse-engine container is not running"
    echo "   Run: docker-compose up -d"
    exit 1
fi
echo "✓ lakehouse-engine container is running"

# Test Python import
echo ""
echo "Testing Python imports..."
if docker-compose exec -T lakehouse-engine python -c "import lakehouse_engine; print('✓ lakehouse_engine imported successfully')" 2>/dev/null; then
    echo "✓ Lakehouse Engine package is available"
else
    echo "❌ Failed to import lakehouse_engine"
    exit 1
fi

# Test Spark
echo ""
echo "Testing Spark..."
if docker-compose exec -T lakehouse-engine python -c "from pyspark.sql import SparkSession; print('✓ PySpark imported successfully')" 2>/dev/null; then
    echo "✓ PySpark is available"
else
    echo "❌ Failed to import PySpark"
    exit 1
fi

# Test Delta Lake
echo ""
echo "Testing Delta Lake..."
if docker-compose exec -T lakehouse-engine python -c "import delta; print('✓ Delta Lake imported successfully')" 2>/dev/null; then
    echo "✓ Delta Lake is available"
else
    echo "❌ Failed to import Delta Lake"
    exit 1
fi

# Test project imports
echo ""
echo "Testing project imports..."
if docker-compose exec -T lakehouse-engine python -c "from lakehouse.jobs import run_finance_stocks_etl; print('✓ Project modules imported successfully')" 2>/dev/null; then
    echo "✓ Project modules are available"
else
    echo "❌ Failed to import project modules"
    exit 1
fi

# Check directories
echo ""
echo "Checking directory structure..."
docker-compose exec -T lakehouse-engine bash -c "
    test -d /opt/airflow/lakehouse && echo '✓ lakehouse directory exists' || echo '❌ lakehouse directory missing'
    test -d /opt/airflow/data_files && echo '✓ data_files directory exists' || echo '❌ data_files directory missing'
    test -d /opt/airflow/lakehouse/acon_configs && echo '✓ acon_configs directory exists' || echo '❌ acon_configs directory missing'
    test -d /opt/airflow/lakehouse/jobs && echo '✓ jobs directory exists' || echo '❌ jobs directory missing'
"

echo ""
echo "=========================================="
echo "✅ All tests passed! Docker setup is ready."
echo "=========================================="
echo ""
echo "You can now run Lakehouse Engine jobs:"
echo "  docker-compose exec lakehouse-engine python -m lakehouse.jobs.orchestrator"

