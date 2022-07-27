resource "google_os_login_ssh_public_key" "key_to_login_to_master_node" {
  user = data.google_client_openid_userinfo.me.email
  key  = file(var.public_key_path)
}

resource "google_dataproc_metastore_service" "this" {
  provider   = google-beta
  service_id = "dataproc-metastore-for-benchmarks"
  location   = var.region
  tier       = "ENTERPRISE"
  hive_metastore_config {
    version = "3.1.2"
  }
  labels = var.labels
}

resource "google_dataproc_cluster" "benchmarks" {
  provider = google-beta
  name     = "delta-performance-benchmarks-cluster"
  region   = var.region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n2-highmem-8"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 100
        num_local_ssds    = 2
      }
    }
    worker_config {
      num_instances = var.dataproc_workers
      machine_type  = "n2-highmem-8"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 100
        num_local_ssds    = 4
      }
    }
    software_config {
      image_version = "2.0-debian10"
    }
    metastore_config {
      dataproc_metastore_service = google_dataproc_metastore_service.this.id
    }
    gce_cluster_config {
      zone = var.zone
    }
    endpoint_config {
      enable_http_port_access = "true"
    }
  }
  labels     = var.labels
  depends_on = [
    google_dataproc_metastore_service.this
  ]
}
