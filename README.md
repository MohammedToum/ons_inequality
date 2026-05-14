# 🚧 WORK IN PROGRESS 🚧

> This repository is actively being developed and is not yet production complete.
> The architecture, models, tests, orchestration, and documentation are still evolving.
> The project has been made public early to demonstrate engineering approach, technical decision-making, and development standards.

# ONS Inequality Data Platform

An end-to-end modern data engineering project built using public UK Office for National Statistics (ONS) datasets.

The goal of this project is to simulate a production-style analytics engineering and data platform environment using modern tooling, cloud infrastructure, orchestration, testing, transformation, and documentation practices.

The project focuses on ingesting inequality, wellbeing, and socio-economic datasets from the ONS API, transforming them into analytics-ready models, and ultimately serving them for downstream BI and analytical use cases.

---

# Project Goals

- Build a production-style data platform from scratch
- Demonstrate strong Analytics Engineering + Data Engineering practices
- Showcase orchestration, infrastructure-as-code, testing, and modular transformations
- Create reusable and scalable ingestion pipelines
- Model public inequality and wellbeing datasets into clean dimensional structures
- Produce a portfolio-quality project representative of mid-to-senior level engineering work

---

# Current Tech Stack

## Languages

- Python 3.13
- SQL
- YAML
- Bash

## Data Engineering

- Apache Airflow
- dbt Core
- BigQuery
- Google Cloud Storage
- Terraform

## Development Tooling

- Docker & Docker Compose
- uv
- pytest
- Ruff
- mypy
- pre-commit

## Cloud

- Google Cloud Platform (GCP)

---

# High-Level Architecture

```text
                ┌────────────────────┐
                │    ONS API         │
                └─────────┬──────────┘
                          │
                          ▼
                ┌────────────────────┐
                │ Python Ingestion   │
                │ (httpx + retries)  │
                └─────────┬──────────┘
                          │
                          ▼
                ┌────────────────────┐
                │ Raw Storage Layer  │
                │ CSV / CSVW / GCS   │
                └─────────┬──────────┘
                          │
                          ▼
                ┌────────────────────┐
                │ Airflow DAGs       │
                │ Orchestration      │
                └─────────┬──────────┘
                          │
                          ▼
                ┌────────────────────┐
                │ BigQuery Raw Layer │
                └─────────┬──────────┘
                          │
                          ▼
                ┌────────────────────┐
                │ dbt Transformations│
                │ staging/int/marts  │
                └─────────┬──────────┘
                          │
                          ▼
                ┌────────────────────┐
                │ Analytics / BI     │
                │ (planned)          │
                └────────────────────┘
```

---

# Repository Structure

```text
.
├── airflow/
├── configs/
├── dashboards/
├── data/
├── dbt/
├── docs/
├── infra/
├── ingestion/
├── scripts/
├── tests/
├── Makefile
├── pyproject.toml
└── README.md
```

---

# Running the Project

## Start Airflow

```bash
docker compose up -d
```

## Run Ingestion

```bash
make ingest
```

## Run dbt

```bash
make dbt-build
```

---

# Engineering Principles

- Modular architecture
- Separation of concerns
- Infrastructure as Code
- Automated testing
- Reproducibility
- Clear documentation
- Scalable design patterns
- Production-oriented standards

---

# Disclaimer

This is an educational and portfolio project and is not affiliated with the Office for National Statistics.

ONS datasets remain the property of the UK Office for National Statistics.
