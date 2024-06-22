variable "availability_zone1" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "subnet1_id" {
  type = string
}

variable "subnet2_id" {
  type = string
}

variable "benchmarks_bucket_name" {
  type = string
}

variable "source_bucket_name" {
  type = string
}

variable "mysql_user" {
  type = string
}

variable "mysql_password" {
  type = string
}

variable "emr_public_key_path" {
  type = string
}

variable "emr_workers" {
  type = number
}

variable "user_ip_address" {
  type = string
}
