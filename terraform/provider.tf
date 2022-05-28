terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "4.15.1"
    }
  }

  required_version = "1.2.0"
}

provider "aws" {
  profile = "default"
  region  = "eu-central-1"
}
