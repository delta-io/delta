output "metastore_endpoint" {
  value = aws_db_instance.metastore_service.endpoint
}

output "mysql_user" {
  value = var.mysql_user
}

output "mysql_password" {
  value = var.mysql_password
}
