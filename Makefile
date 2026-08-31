# Convenience targets.  Everything here is a plain docker compose / python
# command -- run `make -n <target>` to see exactly what it would execute.

COMPOSE := docker compose

.DEFAULT_GOAL := help
.PHONY: help init up down logs ps build demo backfill dags shell-airflow warehouse clean reset test

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

init:  ## Create ./data and write .env with your uid (run this first)
	@mkdir -p data/lake/landing/stream data/lake/landing/batch data/lake/bronze \
	          data/lake/quarantine data/lake/gold_exports data/lake/archive data/warehouse
	@chmod -R 777 data
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		sed -i.bak "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$$(id -u)/" .env && rm -f .env.bak; \
		echo "wrote .env with AIRFLOW_UID=$$(id -u)"; \
	else echo ".env already exists, leaving it alone"; fi

build:  ## Build all images
	$(COMPOSE) build

up: init  ## Start the whole platform in the background
	$(COMPOSE) up -d --build
	@echo ""
	@echo "  Airflow    http://localhost:8080   (airflow / airflow)"
	@echo "  API docs   http://localhost:8000/docs"
	@echo "  Dashboard  http://localhost:8501"
	@echo ""
	@echo "  Airflow needs ~60s to start. Then watch: make logs"

down:  ## Stop everything (keeps ./data and the volumes)
	$(COMPOSE) down

ps:  ## Show container status
	$(COMPOSE) ps

logs:  ## Follow logs from every service
	$(COMPOSE) logs -f

demo:  ## Run bronze -> silver -> gold once, in one container, no Airflow
	$(COMPOSE) run --rm pipeline python -m pipeline.demo --generate

backfill:  ## Generate N previous days of batch log files (make backfill DAYS=7)
	$(COMPOSE) run --rm pipeline python -m pipeline.simulator --mode backfill --days $(or $(DAYS),3)

dags:  ## Trigger the daily batch DAG right now
	$(COMPOSE) exec airflow-scheduler airflow dags trigger medallion_batch_daily

shell-airflow:  ## Open a shell inside the Airflow scheduler container
	$(COMPOSE) exec airflow-scheduler bash

warehouse:  ## Open a DuckDB SQL shell on the warehouse (read-only, safe while running)
	$(COMPOSE) run --rm pipeline python -c "import duckdb;duckdb.connect('/data/warehouse/medallion.duckdb', read_only=True).sql('SHOW ALL TABLES').show()"

test:  ## Run the unit tests
	$(COMPOSE) run --rm pipeline python -m pytest tests -q

clean:  ## Remove containers and named volumes (keeps ./data)
	$(COMPOSE) down -v

reset: clean  ## Remove containers, volumes AND all generated data
	rm -rf data/lake data/warehouse
	@echo "data wiped -- run 'make up' for a clean start"
