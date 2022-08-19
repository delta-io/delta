resource "aws_s3_bucket" "benchmarks_data" {
  bucket = var.benchmarks_bucket_name
}
