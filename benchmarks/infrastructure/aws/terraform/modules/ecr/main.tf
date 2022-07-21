resource "aws_ecr_repository" "delta_benchmarks_spark" {
  name                 = "delta-benchmarks-spark"
  image_tag_mutability = "MUTABLE"
}
