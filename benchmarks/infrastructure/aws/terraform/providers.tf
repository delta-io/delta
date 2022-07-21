provider "aws" {
  region = var.region
  default_tags {
    tags = var.tags
  }
}

provider "kubernetes" {
  host                   = module.processing-eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.processing-eks.certificate_authority_data)
  exec {
    api_version = "client.authentication.k8s.io/v1alpha1"
    args        = ["eks", "get-token", "--cluster-name", module.processing-eks.cluster_name]
    command     = "aws"
  }
}

