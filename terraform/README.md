# ONS Inequality Terraform

This Terraform folder provisions the GCP infrastructure for the ONS Inequality portfolio project.

## What Terraform owns

- BigQuery datasets:
  - `raw_ons`
  - `stg_ons`
  - `int_ons`
  - `mart_ons`
- Raw BigQuery tables in `raw_ons`
- Raw table schemas inferred from the uploaded ONS CSV files
- Service accounts for Airflow loading and dbt transformation
- IAM permissions for those service accounts

## What dbt owns

- `stg_*` models in `stg_ons`
- `int_*` models in `int_ons`
- final marts/dim/fact models in `mart_ons`
- tests, docs, contracts, and transformed model definitions

## Commands

```bash
terraform init
terraform fmt -recursive
terraform validate
terraform plan
terraform apply
```

## Loading raw CSVs

Terraform creates empty raw tables with schemas. It does not load the CSV row data.

Example local load command:

```bash
bq load \
  --project_id=ons-inequality-dev \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --replace \
  raw_ons.raw_wellbeing_quarterly \
  ../data/raw/ons/wellbeing-quarterly/data.csv
```

## dbt profile direction

Your dbt profile should target BigQuery project `ons-inequality-dev`. Use dbt custom schemas or a `generate_schema_name` macro so models land in:

- `stg_ons`
- `int_ons`
- `mart_ons`

Keep raw ingestion tables out of dbt model ownership.
