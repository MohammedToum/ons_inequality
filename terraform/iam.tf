# BigQuery jobs are project-level. Data access is dataset-level below.
resource "google_project_iam_member" "airflow_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "dbt_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dbt.email}"
}

# Airflow loads/replaces raw tables only.
resource "google_bigquery_dataset_iam_member" "airflow_raw_data_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.airflow.email}"
}

# dbt reads raw tables.
resource "google_bigquery_dataset_iam_member" "dbt_raw_data_viewer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.dbt.email}"
}

# dbt creates and replaces models in transformed datasets.
resource "google_bigquery_dataset_iam_member" "dbt_staging_data_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.staging.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dbt.email}"
}

resource "google_bigquery_dataset_iam_member" "dbt_intermediate_data_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.intermediate.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dbt.email}"
}

resource "google_bigquery_dataset_iam_member" "dbt_marts_data_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.marts.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dbt.email}"
}
