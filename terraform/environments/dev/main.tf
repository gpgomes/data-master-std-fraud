provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = local.environment
      ManagedBy   = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  # Único ambiente do projeto (orçamento de US$ 50/mês): constante, não variável.
  environment = "dev"

  # Nomes de bucket S3 são globais: o account id garante unicidade.
  name_prefix = "${var.project_name}-${local.environment}"
  bucket_base = "${local.name_prefix}-${data.aws_caller_identity.current.account_id}"
}

# Data lake — mesmas camadas do MinIO local (bronze/silver/gold/checkpoints).
module "s3" {
  source = "../../modules/s3"

  name_prefix   = local.bucket_base
  force_destroy = var.force_destroy_buckets

  buckets = {
    bronze      = {}
    silver      = {}
    gold        = {}
    checkpoints = { noncurrent_version_expiration_days = 3 }
  }
}

# Policies de acesso por camada, para as roles de EMR/MWAA (módulos futuros).
module "iam" {
  source = "../../modules/iam"

  name_prefix          = local.name_prefix
  datalake_bucket_arns = module.s3.bucket_arns
}

# Recursos caros (NAT, MSK, EMR, MWAA) entram nos próximos PRs, cada um atrás de
# uma flag enable_* desligada por padrão — orçamento de US$ 50/mês (ver runbook).
