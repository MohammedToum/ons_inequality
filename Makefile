# ============================================================
# PROJECT SETTINGS & GLOBALS
# ============================================================

SHELL := /bin/bash

GREEN  := \033[0;32m
YELLOW := \033[1;33m
RED    := \033[0;31m
BLUE   := \033[0;34m
NC     := \033[0m

PROJECT_NAME := ons_inequality

AIRFLOW_DIR := airflow
DBT_DIR     := dbt
CONFIG_DIR  := configs
SRC_DIR     := src

COMPOSE := docker compose

AIRFLOW_SCHEDULER := $(shell docker ps --format '{{.Names}}' | grep airflow-scheduler | head -n 1)
AIRFLOW_WEBSERVER := $(shell docker ps --format '{{.Names}}' | grep airflow-webserver | head -n 1)

# Airflow 3 local executor setups may not have a worker.
# Prefer worker if it exists, otherwise use scheduler for exec commands.
AIRFLOW_EXEC_CONTAINER := $(shell docker ps --format '{{.Names}}' | grep -E 'airflow-worker|airflow-scheduler' | head -n 1)

DBT_PROJECT_DIR  := /opt/airflow/dbt/ons_inequality
DBT_PROFILES_DIR := /opt/airflow/dbt/profiles

# ============================================================
# HELPER MACROS
# ============================================================

define banner
	@echo -e "$(BLUE)============================================================$(NC)"
	@echo -e "$(GREEN)$1$(NC)"
	@echo -e "$(BLUE)============================================================$(NC)"
endef

define require_airflow_container
	@if [ -z "$(AIRFLOW_EXEC_CONTAINER)" ]; then \
	  echo -e "$(RED)No Airflow execution container is running.$(NC)"; \
	  echo -e "$(YELLOW)Start Airflow with: make afu$(NC)"; \
	  exit 1; \
	fi
endef

# ============================================================
# LOCAL PYTHON / INGESTION COMMANDS
# ============================================================

.PHONY: install ingest ingest-one test lint format

install:
	$(call banner,Installing local project dependencies...)
	uv sync --dev

ingest:
	$(call banner,Running local ONS batch ingestion...)
	PYTHONPATH=$(SRC_DIR) python scripts/run_ingestion.py \
		--config $(CONFIG_DIR)/ons_datasets.yml

ingest-one:
	$(call banner,Running local single dataset ingestion...)
	@if [ -z "$(DATASET_ID)" ] || [ -z "$(EDITION)" ] || [ -z "$(VERSION)" ]; then \
	  echo -e "$(RED)Usage: make ingest-one DATASET_ID=... EDITION=... VERSION=...$(NC)"; \
	  exit 1; \
	fi
	PYTHONPATH=$(SRC_DIR) python scripts/run_ingestion.py \
		--dataset_id $(DATASET_ID) \
		--edition $(EDITION) \
		--version $(VERSION)

test:
	$(call banner,Running tests...)
	uv run pytest

lint:
	$(call banner,Running Ruff lint...)
	uv run ruff check .

format:
	$(call banner,Formatting with Ruff...)
	uv run ruff format .

# ============================================================
# AIRFLOW LIFECYCLE SHORT COMMANDS
# ============================================================

.PHONY: afu afd afdu afr afrb afrs afl afs afinit ps logs kill

afinit:
	$(call banner,Initialising Airflow...)
	$(COMPOSE) up airflow-init

afu:
	$(call banner,Starting Airflow...)
	$(COMPOSE) up -d

afd:
	$(call banner,Stopping Airflow containers...)
	$(COMPOSE) down

afdu:
	$(call banner,Stopping Airflow containers...)
	$(COMPOSE) down
	$(call banner,Starting Airflow...)
	$(COMPOSE) up -d

afr:
	$(call banner,Full Airflow reset including volumes...)
	$(COMPOSE) down --volumes --remove-orphans

afrb:
	$(call banner,Rebuilding Airflow Docker image...)
	$(COMPOSE) build --no-cache

afrs:
	$(call banner,Stopping Airflow + rebuilding + restarting...)
	$(COMPOSE) down --volumes --remove-orphans
	$(COMPOSE) build --no-cache
	$(COMPOSE) up -d --build

afl:
	$(call banner,Tailing Airflow logs...)
	$(COMPOSE) logs -f

afs:
	$(call require_airflow_container)
	$(call banner,Opening Airflow container shell...)
	docker exec -it $(AIRFLOW_EXEC_CONTAINER) bash

# ============================================================
# INGESTION INSIDE AIRFLOW CONTAINER
# ============================================================

.PHONY: airflow-ingest airflow-ingest-one

airflow-ingest:
	$(call require_airflow_container)
	$(call banner,Running ONS batch ingestion inside Airflow container...)
	docker exec -it $(AIRFLOW_EXEC_CONTAINER) bash -lc "\
		cd /opt/airflow && \
		python /opt/airflow/project/scripts/run_ingestion.py \
			--config /opt/airflow/configs/ons_datasets.yml \
	"

airflow-ingest-one:
	$(call require_airflow_container)
	$(call banner,Running single ONS ingestion inside Airflow container...)
	@if [ -z "$(DATASET_ID)" ] || [ -z "$(EDITION)" ] || [ -z "$(VERSION)" ]; then \
	  echo -e "$(RED)Usage: make airflow-ingest-one DATASET_ID=... EDITION=... VERSION=...$(NC)"; \
	  exit 1; \
	fi
	docker exec -it $(AIRFLOW_EXEC_CONTAINER) bash -lc "\
		cd /opt/airflow && \
		python /opt/airflow/project/scripts/run_ingestion.py \
			--dataset_id $(DATASET_ID) \
			--edition $(EDITION) \
			--version $(VERSION) \
	"

# ============================================================
# DBT INSIDE AIRFLOW CONTAINER
# ============================================================

.PHONY: dbt-debug dbt-run dbt-build dbt-test dbt-clean dbt-docs

dbt-debug:
	$(call require_airflow_container)
	$(call banner,Running dbt debug inside Airflow container...)
	docker exec -it $(AIRFLOW_EXEC_CONTAINER) dbt debug \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)

dbt-deps:
	$(call require_airflow_container)
	$(call banner,Running dbt deps inside Airflow container...)
	docker exec -it $(AIRFLOW_EXEC_CONTAINER) dbt deps \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)	

dbt-compile:
	$(call require_airflow_container)
	$(call banner,Running dbt compile inside Airflow container...)
	docker exec -it $(AIRFLOW_EXEC_CONTAINER) dbt compile \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)

dbt-seed:
	$(call require_airflow_container)
	$(call banner,Running dbt seed inside Airflow container...)
	docker exec -it $(AIRFLOW_EXEC_CONTAINER) dbt seed \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)

dbt-run:
	$(call require_airflow_container)
	$(call banner,Running dbt run inside Airflow container...)
	docker exec -it $(AIRFLOW_EXEC_CONTAINER) dbt run \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)

dbt-build:
	$(call require_airflow_container)
	$(call banner,Running dbt build inside Airflow container...)
	docker exec -it $(AIRFLOW_EXEC_CONTAINER) dbt build \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)

dbt-test:
	$(call require_airflow_container)
	$(call banner,Running dbt test inside Airflow container...)
	docker exec -it $(AIRFLOW_EXEC_CONTAINER) dbt test \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)

dbt-clean:
	$(call require_airflow_container)
	$(call banner,Cleaning dbt artifacts inside Airflow container...)
	docker exec -it $(AIRFLOW_EXEC_CONTAINER) dbt clean \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)

dbt-docs:
	$(call require_airflow_container)
	$(call banner,Generating dbt docs inside Airflow container...)
	docker exec -it $(AIRFLOW_EXEC_CONTAINER) dbt docs generate \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)

dbt-ls:
	$(call require_airflow_container)
	$(call banner,Listing dbt resources inside Airflow container...)
	docker exec -it $(AIRFLOW_EXEC_CONTAINER) dbt ls --resource-type source \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)

dbt-sf:
	$(call require_airflow_container)
	$(call banner,Checking source freshness with DBT inside Airflow container...)
	docker exec -it $(AIRFLOW_EXEC_CONTAINER) dbt source freshness \
		--project-dir $(DBT_PROJECT_DIR) \
		--profiles-dir $(DBT_PROFILES_DIR)


# ============================================================
# QUALITY-OF-LIFE COMMANDS
# ============================================================

.PHONY: docker-ps docker-logs ip

docker-ps:
	$(call banner,Listing project containers...)
	$(COMPOSE) ps

logs:
	$(call banner,Streaming Docker logs...)
	$(COMPOSE) logs -f

ip:
	$(call banner,Showing container IP addresses...)
	docker inspect -f '{{.Name}} - {{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $$(docker ps -q)

kill:
	$(call banner,Force removing Airflow containers...)
	docker rm -f $$(docker ps -aq --filter "name=airflow") || true