terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Configuração parcial: bucket/key/region ficam em backend.hcl (ignorado pelo
  # git; modelo em backend.hcl.example). Uso:
  #   terraform init -backend-config=backend.hcl
  backend "s3" {}
}
