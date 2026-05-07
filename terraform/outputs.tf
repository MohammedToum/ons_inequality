output "project_id" {
  value = var.project_id
}

output "bigquery_datasets" {
  value = {
    raw          = google_bigquery_dataset.raw.dataset_id
    staging      = google_bigquery_dataset.staging.dataset_id
    intermediate = google_bigquery_dataset.intermediate.dataset_id
    marts        = google_bigquery_dataset.marts.dataset_id
  }
}

output "raw_table_ids" {
  value = [for table in google_bigquery_table.raw_tables : table.table_id]
}

output "airflow_service_account_email" {
  value = google_service_account.airflow.email
}

output "dbt_service_account_email" {
  value = google_service_account.dbt.email
}
