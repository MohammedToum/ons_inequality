locals {
  common_labels = {
    project     = "ons_inequality"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "google_bigquery_dataset" "raw" {
  dataset_id                 = var.raw_dataset_id
  project                    = var.project_id
  location                   = var.bigquery_location
  description                = "Raw ONS landing dataset. Tables and schemas are managed by Terraform."
  delete_contents_on_destroy = var.delete_contents_on_destroy
  labels                     = local.common_labels

  depends_on = [google_project_service.bigquery]
}

resource "google_bigquery_dataset" "staging" {
  dataset_id                 = var.staging_dataset_id
  project                    = var.project_id
  location                   = var.bigquery_location
  description                = "dbt staging dataset for cleaned source-aligned ONS models."
  delete_contents_on_destroy = var.delete_contents_on_destroy
  labels                     = local.common_labels

  depends_on = [google_project_service.bigquery]
}

resource "google_bigquery_dataset" "intermediate" {
  dataset_id                 = var.intermediate_dataset_id
  project                    = var.project_id
  location                   = var.bigquery_location
  description                = "dbt intermediate dataset for reusable transformation models."
  delete_contents_on_destroy = var.delete_contents_on_destroy
  labels                     = local.common_labels

  depends_on = [google_project_service.bigquery]
}

resource "google_bigquery_dataset" "marts" {
  dataset_id                 = var.marts_dataset_id
  project                    = var.project_id
  location                   = var.bigquery_location
  description                = "dbt marts dataset for final analytics-ready ONS tables."
  delete_contents_on_destroy = var.delete_contents_on_destroy
  labels                     = local.common_labels

  depends_on = [google_project_service.bigquery]
}

resource "google_bigquery_table" "raw_tables" {
  for_each = local.raw_tables

  project             = var.project_id
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = each.value.table_id
  description         = "Raw ONS table for ${each.key}. Schema managed by Terraform; row data loaded separately."
  schema              = jsonencode(each.value.schema)
  deletion_protection = var.raw_table_deletion_protection
  labels              = local.common_labels
}
