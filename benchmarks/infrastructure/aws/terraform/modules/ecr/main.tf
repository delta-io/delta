resource "aws_ecr_repository" "delta_benchmarks_spark" {
  name                 = "delta-benchmarks-spark-${var.benchmark_run_id}"
  image_tag_mutability = "MUTABLE"
}
