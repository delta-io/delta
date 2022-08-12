variable "region" {
  description = "The default region to manage resources in."
  type        = string
}

variable "availability_zone1" {
  description = "The default availability zone to manage resources in."
  type        = string
}

variable "availability_zone2" {
  description = "The secondary availability zone."
  type        = string
}

variable "benchmarks_bucket_name" {
  description = "The name of the AWS S3 bucket that will be used to store benchmark data."
  type        = string
}

variable "source_bucket_name" {
  description = "The S3 bucket name where the raw input data is stored."
  type        = string
  default     = "devrel-delta-datasets"
}

variable "mysql_user" {
  description = "MySQL database user."
  type        = string
  default     = "benchmark"
}

variable "mysql_password" {
  description = "MySQL database password."
  type        = string
  default     = "benchmark"
}

variable "emr_public_key_path" {
  description = "The path to the public key in the typical format, specified in RFC4716. The key is necessary to SSH to EMR cluster nodes."
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "emr_workers" {
  description = "The number of worker nodes in EMR cluster."
  type        = number
  default     = 16
}

variable "user_ip_address" {
  description = "The IP of the machine which is used to access master node."
  type        = string
}

variable "eks_workers" {
  description = "The number of worker nodes in EKS cluster."
  type        = number
}

variable "emr_version" {
  description = <<EOT
  Release label for the Amazon EMR release. Each EMR version supports different application versions (e.g. Spark version).
  Please check in documentation which EMR version contains desired versions:
  https://docs.amazonaws.cn/en_us/emr/latest/ReleaseGuide/emr-release-app-versions-6.x.html
  By default EMR-6.6.0 is used which contains Spark 3.2.0.
  EOT
  type = string
  default = "emr-6.6.0"
}

variable "tags" {
  description = "Common tags assigned to each resource."
  type        = map(string)
  default     = {}
}
