terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.15.1"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0.1"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 3.4.0"
    }
    random = {
      source = "hashicorp/random"
      version = "3.3.2"
    }
  }
}
