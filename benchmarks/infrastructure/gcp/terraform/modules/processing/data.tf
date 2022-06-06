data "google_client_openid_userinfo" "me" {
}

data "google_compute_instance" "benchmarks_master" {
  provider   = google-beta
  depends_on = [google_dataproc_cluster.benchmarks]
  name       = google_dataproc_cluster.benchmarks.cluster_config.0.master_config.0.instance_names.0
  zone       = var.zone
}
