resource "google_project_service" "bigquery" {
  count = var.enable_required_apis ? 1 : 0

  project            = var.project_id
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "iam" {
  count = var.enable_required_apis ? 1 : 0

  project            = var.project_id
  service            = "iam.googleapis.com"
  disable_on_destroy = false
}
