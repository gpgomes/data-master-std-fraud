terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Sem backend remoto: este é o root module que CRIA o bucket de state, então o
  # state dele é local (terraform.tfstate, ignorado pelo git). Depois do primeiro
  # apply dá para migrá-lo para o próprio bucket — ver docs/runbook.md.
}
