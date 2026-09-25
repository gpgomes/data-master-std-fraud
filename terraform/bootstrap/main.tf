# Baseline da conta AWS — aplicado uma vez, antes de qualquer ambiente:
#   1. bucket de state remoto (usado pelo ambiente dev)
#   2. budget mensal (guarda de custo — vive fora dos ambientes para nunca ser
#      destruído junto com a stack de teste)
#   3. acesso OIDC do GitHub Actions para o `terraform plan` (github_oidc.tf)

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project    = var.project_name
      Layer      = "bootstrap"
      ManagedBy  = "terraform"
      Repository = var.github_repository
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
}

module "state_bucket" {
  source = "../modules/s3"

  name_prefix = "${var.project_name}-${local.account_id}"

  buckets = {
    tfstate = { noncurrent_version_expiration_days = 90 }
  }
}

module "budget" {
  source = "../modules/budget"

  name              = "${var.project_name}-monthly"
  limit_usd         = var.budget_limit_usd
  subscriber_emails = var.budget_alert_emails
}
