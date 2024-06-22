variable "project" {
  description = "The ID of the GCP project."
  type        = string
}

variable "credentials_file" {
  description = "The path to a service account key file in JSON format."
  type        = string
}

variable "public_key_path" {
  description = "The path to the public key in the typical format, specified in RFC4716. The key is necessary to SSH to Dataproc cluster nodes."
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "region" {
  description = "The default region to manage resources in."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "The default zone to manage resources in."
  type        = string
  default     = "us-central1-a"
}

variable "bucket_name" {
  description = "The name of the Google Storage bucket that will be used to store benchmark data. Please note that the bucket name has to be globally unique."
  type        = string
}

variable "dataproc_workers" {
  description = "The number of worker nodes in Dataproc cluster."
  type        = number
  default     = 16
}

variable "labels" {
  description = "Labels that will be assigned to each resource."
  type        = map(string)
  default     = {}
}
