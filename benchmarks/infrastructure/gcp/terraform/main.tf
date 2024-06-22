module "processing" {
  source = "./modules/processing"

  project          = var.project
  credentials_file = var.credentials_file
  public_key_path  = var.public_key_path
  region           = var.region
  zone             = var.zone
  dataproc_workers = var.dataproc_workers
  labels           = var.labels
}

module "storage" {
  source = "./modules/storage"

  bucket_name = var.bucket_name
  region      = var.region
  labels      = var.labels
}
