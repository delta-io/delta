variable "project" {
  type = string
}

variable "credentials_file" {
  type = string
}

variable "public_key_path" {
  type = string
}

variable "region" {
  type = string
}

variable "zone" {
  type = string
}

variable "dataproc_workers" {
  type = number
}

variable "labels" {
  type = map(string)
}
