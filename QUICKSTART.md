# Quick Start Guide

Get up and running with the Financial Data Lakehouse Engine in minutes!

## 🚀 Docker Setup (Recommended)

### Prerequisites
- Docker and Docker Compose installed
- 4GB+ RAM available
- Ports 8080 and 4040 available

### Step 1: Initialize Airflow (First Time Only)

```bash
export AIRFLOW_UID=$(id -u)
docker-compose up airflow-init
```

### Step 2: Start Services

```bash
docker-compose up -d --build
```

### Step 3: Verify Setup

```bash
./docker/test_setup.sh
```

### Step 4: Run Your First Pipeline

```bash
# Run all pipelines
docker-compose exec lakehouse-engine python -m lakehouse.jobs.orchestrator

# Or run individual pipeline
docker-compose exec lakehouse-engine python -m lakehouse.jobs.finance_stocks_etl
```

## 📊 Access Services

- **Airflow UI**: http://localhost:8080
  - Username: `airflow`
  - Password: `airflow`
- **Spark UI**: http://localhost:4040 (when jobs are running)

## 🎯 Common Commands

### Using Docker Compose

```bash
# View logs
docker-compose logs -f lakehouse-engine

# Open shell in container
docker-compose exec lakehouse-engine bash

# Stop services
docker-compose down

# Restart services
docker-compose restart
```

### Using Make (from docker/ directory)

```bash
cd docker

make build              # Build Docker images
make up                 # Start services
make run-orchestrator   # Run all pipelines
make run-finance        # Run finance pipeline
make shell              # Open shell
make logs-lakehouse     # View logs
make down               # Stop services
```

## 📁 Project Structure

```
rocketLaunchETL/
├── dags/                    # Airflow DAGs
├── data_files/              # Source JSON data (bronze layer)
├── lakehouse/               # Lakehouse architecture
│   ├── acon_configs/        # ACON configurations
│   ├── jobs/                # ETL job scripts
│   ├── bronze/              # Raw data (Delta)
│   ├── silver/              # Cleaned data (Delta)
│   └── gold/                # Aggregated data (Delta)
└── docker/                  # Docker scripts and configs
```

## 🔄 Data Flow

1. **Bronze**: Raw JSON from `data_files/` → `lakehouse/bronze/`
2. **Silver**: Cleaned & validated → `lakehouse/silver/`
3. **Gold**: Aggregated & business-ready → `lakehouse/gold/`

## 🧪 Test Your Setup

```bash
# Run test script
./docker/test_setup.sh

# Test individual pipeline
docker-compose exec lakehouse-engine python -m lakehouse.jobs.finance_stocks_etl

# Check results
ls -la lakehouse/silver/finance/
```

## 📚 Available Pipelines

- `finance_stocks_etl` - Stock market data
- `economics_etl` - Economic indicators
- `currency_etl` - Exchange rates
- `blockchain_etl` - Blockchain statistics
- `bitcoin_etl` - Cryptocurrency data

## 🐛 Troubleshooting

### Issue: Container won't start
```bash
docker-compose down
docker-compose up -d --build
```

### Issue: Import errors
```bash
# Check Python path
docker-compose exec lakehouse-engine python -c "import sys; print(sys.path)"
```

### Issue: Out of memory
Edit `docker-compose.yml` and reduce Spark memory:
```yaml
SPARK_DRIVER_MEMORY: 1g
SPARK_EXECUTOR_MEMORY: 1g
```

## 📖 Next Steps

1. ✅ Run your first pipeline
2. 📝 Check results in `lakehouse/silver/`
3. 🔧 Customize ACON configs in `lakehouse/acon_configs/`
4. 📊 View data in Spark UI
5. 🔄 Schedule jobs in Airflow

## 📚 Documentation

- [DOCKER_USAGE.md](DOCKER_USAGE.md) - Detailed Docker guide
- [LAKEHOUSE_SETUP.md](LAKEHOUSE_SETUP.md) - Lakehouse Engine setup
- [lakehouse/README.md](lakehouse/README.md) - Lakehouse architecture details

---

**Ready to go!** 🎉 Start with: `docker-compose up -d --build`

