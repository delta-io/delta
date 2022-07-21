output "account_id" {
  value = module.processing-eks.account_id
}

output "region" {
  value = module.processing-eks.region
}

output "ecr_uri" {
  value = module.ecr.uri
}

output "ecr_repository_name" {
  value = module.ecr.name
}

output "master_node_address" {
  value = module.processing-emr.master_node_address
}

output "eks_cluster_endpoint" {
  value = module.processing-eks.cluster_endpoint
}

output "metastore_endpoint" {
  value = module.metastore-rds.metastore_endpoint
}

output "mysql_user" {
  value = module.metastore-rds.mysql_user
}

output "mysql_password" {
  value = module.metastore-rds.mysql_password
  sensitive = true
}
