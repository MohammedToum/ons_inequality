variable "project_id" {
  description = "GCP project ID for the ONS Inequality project."
  type        = string
  default     = "ons-inequality-dev"
}

variable "region" {
  description = "Default GCP region for regional resources."
  type        = string
  default     = "europe-west2"
}

variable "bigquery_location" {
  description = "BigQuery dataset location. Use EU for multi-region EU, or europe-west2 for London region."
  type        = string
  default     = "europe-west2"
}

variable "environment" {
  description = "Environment label applied to resources."
  type        = string
  default     = "dev"
}

variable "raw_dataset_id" {
  description = "Dataset containing raw Terraform-managed ONS landing tables."
  type        = string
  default     = "ons_raw"
}

variable "staging_dataset_id" {
  description = "Dataset where dbt creates staging models."
  type        = string
  default     = "ons_stg"
}

variable "intermediate_dataset_id" {
  description = "Dataset where dbt creates intermediate models."
  type        = string
  default     = "ons_int"
}

variable "marts_dataset_id" {
  description = "Dataset where dbt creates final marts/dim/fact models."
  type        = string
  default     = "ons_mart"
}

variable "delete_contents_on_destroy" {
  description = "Allows Terraform to delete non-empty BigQuery datasets. Keep false unless deliberately tearing down dev resources."
  type        = bool
  default     = false
}

variable "raw_table_deletion_protection" {
  description = "Deletion protection for Terraform-managed raw BigQuery tables."
  type        = bool
  default     = false
}

variable "enable_required_apis" {
  description = "Whether Terraform should enable the required GCP APIs."
  type        = bool
  default     = true
}
