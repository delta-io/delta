resource "aws_key_pair" "benchmarks" {
  key_name   = "benchmarks_key_pair"
  public_key = file(var.emr_public_key_path)
}
