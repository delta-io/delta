output "uri" {
  value = aws_ecr_repository.delta_benchmarks_spark.repository_url
}

output "name" {
  value = aws_ecr_repository.delta_benchmarks_spark.name
}
