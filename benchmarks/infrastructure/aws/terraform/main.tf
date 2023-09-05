module "networking" {
  source = "./modules/networking"

  availability_zone1 = var.availability_zone1
  availability_zone2 = var.availability_zone2
}

module "storage" {
  source = "./modules/storage"

  benchmarks_bucket_name = var.benchmarks_bucket_name
}

module "processing" {
  source = "./modules/processing"

  vpc_id     = module.networking.vpc_id
  subnet1_id = module.networking.subnet1_id
  subnet2_id = module.networking.subnet2_id

  availability_zone1     = var.availability_zone1
  benchmarks_bucket_name = var.benchmarks_bucket_name
  source_bucket_name     = var.source_bucket_name
  mysql_user             = var.mysql_user
  mysql_password         = var.mysql_password
  emr_public_key_path    = var.emr_public_key_path
  emr_workers            = var.emr_workers
  user_ip_address        = var.user_ip_address

  depends_on = [module.networking, module.storage]
}
