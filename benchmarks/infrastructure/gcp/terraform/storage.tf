resource "google_storage_bucket" "benchmarks-data" {
  provider      = google
  name          = var.bucket_name
  location      = var.region
  storage_class = "STANDARD"
  labels        = var.labels
}
