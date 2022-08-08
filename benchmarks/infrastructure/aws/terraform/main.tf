module "networking" {
  source = "./modules/networking"

  availability_zone1 = var.availability_zone1
  availability_zone2 = var.availability_zone2
}

module "storage" {
  source = "./modules/storage"

  benchmarks_bucket_name = var.benchmarks_bucket_name
}

module "metastore-rds" {
  source = "./modules/metastore-rds"

  vpc_id     = module.networking.vpc_id
  subnet1_id = module.networking.subnet1_id
  subnet2_id = module.networking.subnet2_id

  availability_zone1 = var.availability_zone1
  mysql_user         = var.mysql_user
  mysql_password     = var.mysql_password

  depends_on = [module.networking, module.storage]
}

module "processing-emr" {
  source = "./modules/processing-emr"

  vpc_id     = module.networking.vpc_id
  subnet1_id = module.networking.subnet1_id
  subnet2_id = module.networking.subnet2_id

  metastore_endpoint = module.metastore-rds.metastore_endpoint

  availability_zone1     = var.availability_zone1
  benchmarks_bucket_name = var.benchmarks_bucket_name
  source_bucket_name     = var.source_bucket_name
  mysql_user             = var.mysql_user
  mysql_password         = var.mysql_password
  emr_public_key_path    = var.emr_public_key_path
  emr_workers            = var.emr_workers
  emr_version            = var.emr_version
  user_ip_address        = var.user_ip_address

  depends_on = [module.networking, module.storage, module.metastore-rds]
}

module "processing-eks" {
  source = "./modules/processing-eks"

  vpc_id     = module.networking.vpc_id
  subnet1_id = module.networking.subnet1_id
  subnet2_id = module.networking.subnet2_id

  region                 = var.region
  benchmarks_bucket_name = var.benchmarks_bucket_name
  source_bucket_name     = var.source_bucket_name
  tags                   = var.tags
  mysql_user             = var.mysql_user
  mysql_password         = var.mysql_password
  eks_workers            = var.eks_workers

  depends_on = [module.networking, module.storage]
}

module "ecr" {
  source = "./modules/ecr"
}
