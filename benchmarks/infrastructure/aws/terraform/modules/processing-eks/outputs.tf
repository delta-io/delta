output "region" {
  value = var.region
}

output "account_id" {
  value = data.aws_caller_identity.current.account_id
}

output "cluster_name" {
  value = aws_eks_cluster.benchmarks.name
}

output "cluster_endpoint" {
  value = aws_eks_cluster.benchmarks.endpoint
}

output "certificate_authority_data" {
  value     = aws_eks_cluster.benchmarks.certificate_authority[0].data
  sensitive = true
}

output "container_role" {
  value = aws_iam_role.container_role.name
}
