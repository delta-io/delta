variable "vpc_id" {
  type = string
}

variable "region" {
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

variable "eks_workers" {
  type = number
}

variable "tags" {
  type = map(string)
}
