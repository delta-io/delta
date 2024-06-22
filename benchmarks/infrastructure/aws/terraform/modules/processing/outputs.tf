output "master_node_address" {
  value = aws_emr_cluster.benchmarks.master_public_dns
}
