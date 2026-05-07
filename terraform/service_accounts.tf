resource "google_service_account" "airflow" {
  project      = var.project_id
  account_id   = "airflow-ons-loader"
  display_name = "Airflow ONS Loader"
  description  = "Service account used by local Airflow/Docker jobs to load raw ONS CSVs into BigQuery."

  depends_on = [google_project_service.iam]
}

resource "google_service_account" "dbt" {
  project      = var.project_id
  account_id   = "dbt-ons-transformer"
  display_name = "dbt ONS Transformer"
  description  = "Service account used by dbt to read raw ONS tables and build staging, intermediate, and mart models."

  depends_on = [google_project_service.iam]
}
