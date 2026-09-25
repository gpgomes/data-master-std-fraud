# Acesso do GitHub Actions à AWS sem chave estática: o workflow troca o token OIDC
# do GitHub por credenciais temporárias desta role. Ela só faz LEITURA — o CI roda
# `terraform plan -lock=false`, então nem escreve o lock no bucket de state.
#
# Convenção: cada módulo de infra novo (msk, emr, mwaa, glue...) estende a
# statement "ReadInfrastructureConfig" com as ações Get*/List*/Describe* do seu
# serviço, senão o plan do CI falha com AccessDenied.

resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_github_oidc ? 1 : 0

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

data "aws_iam_policy_document" "github_plan_trust" {
  count = var.create_github_oidc ? 1 : 0

  statement {
    sid     = "GitHubActionsOidc"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github[0].arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Só PRs e a branch main deste repositório.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${var.github_repository}:pull_request",
        "repo:${var.github_repository}:ref:refs/heads/main",
      ]
    }
  }
}

data "aws_iam_policy_document" "github_plan" {
  #checkov:skip=CKV_AWS_356:Get/List/Describe/View não aceitam restrição por resource; "*" só em ações somente-leitura de configuração (garantido por tests/unit/test_terraform_guardrails.py).
  count = var.create_github_oidc ? 1 : 0

  statement {
    sid       = "ListStateBucket"
    actions   = ["s3:ListBucket"]
    resources = [module.state_bucket.bucket_arns["tfstate"]]
  }

  statement {
    sid       = "ReadTerraformState"
    actions   = ["s3:GetObject"]
    resources = ["${module.state_bucket.bucket_arns["tfstate"]}/*"]
  }

  # APIs de leitura (Get/List/Describe/View) não aceitam restrição por resource,
  # por isso "*" — é a única exceção a "nenhum resource *" e é só leitura de
  # configuração: nenhuma ação lê o CONTEÚDO de objetos do data lake.
  statement {
    sid    = "ReadInfrastructureConfig"
    effect = "Allow"
    actions = [
      "s3:GetBucket*",
      "s3:GetEncryptionConfiguration",
      "s3:GetLifecycleConfiguration",
      "s3:GetAccelerateConfiguration",
      "s3:GetReplicationConfiguration",
      "s3:ListAllMyBuckets",
      "iam:Get*",
      "iam:List*",
      "budgets:ViewBudget",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role" "github_plan" {
  count = var.create_github_oidc ? 1 : 0

  name                 = "${var.project_name}-github-plan"
  description          = "Assumida pelo GitHub Actions (OIDC) para rodar terraform plan somente-leitura."
  assume_role_policy   = data.aws_iam_policy_document.github_plan_trust[0].json
  max_session_duration = 3600
}

resource "aws_iam_role_policy" "github_plan" {
  count = var.create_github_oidc ? 1 : 0

  name   = "terraform-plan-read"
  role   = aws_iam_role.github_plan[0].id
  policy = data.aws_iam_policy_document.github_plan[0].json
}
