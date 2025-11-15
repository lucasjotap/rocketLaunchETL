.PHONY: help build up down logs shell run-orchestrator run-pipeline clean

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build Docker images
	docker-compose build

up: ## Start all services
	docker-compose up -d

down: ## Stop all services
	docker-compose down

logs: ## View logs from all services
	docker-compose logs -f

logs-lakehouse: ## View logs from lakehouse-engine service
	docker-compose logs -f lakehouse-engine

shell: ## Open shell in lakehouse-engine container
	docker-compose exec lakehouse-engine bash

run-orchestrator: ## Run all Lakehouse Engine pipelines
	docker-compose exec lakehouse-engine python -m lakehouse.jobs.orchestrator

run-finance: ## Run finance stocks ETL pipeline
	docker-compose exec lakehouse-engine python -m lakehouse.jobs.finance_stocks_etl

run-economics: ## Run economics ETL pipeline
	docker-compose exec lakehouse-engine python -m lakehouse.jobs.economics_etl

run-currency: ## Run currency ETL pipeline
	docker-compose exec lakehouse-engine python -m lakehouse.jobs.currency_etl

run-blockchain: ## Run blockchain ETL pipeline
	docker-compose exec lakehouse-engine python -m lakehouse.jobs.blockchain_etl

run-bitcoin: ## Run bitcoin ETL pipeline
	docker-compose exec lakehouse-engine python -m lakehouse.jobs.bitcoin_etl

init: ## Initialize Airflow (first time setup)
	export AIRFLOW_UID=$$(id -u) && docker-compose up airflow-init

rebuild: ## Rebuild and restart services
	docker-compose down
	docker-compose build
	docker-compose up -d

clean: ## Remove containers, volumes, and images
	docker-compose down -v --rmi all

test: ## Run a test pipeline
	docker-compose exec lakehouse-engine python -m lakehouse.jobs.finance_stocks_etl

