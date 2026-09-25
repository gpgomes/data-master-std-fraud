variable "project_name" {
  description = "Nome do projeto: prefixo de recursos e valor da tag Project."
  type        = string
  default     = "data-master-std-fraud"
}

variable "region" {
  description = "Região AWS. Decisão do projeto: us-east-1."
  type        = string
  default     = "us-east-1"
}

variable "budget_limit_usd" {
  description = "Teto de gasto mensal da conta, em dólares. Decisão do projeto: US$ 50."
  type        = number
  default     = 50
}

variable "budget_alert_emails" {
  description = "E-mails que recebem os alertas do budget. Sem default de propósito: passe via terraform.tfvars (ignorado pelo git) ou TF_VAR_budget_alert_emails."
  type        = list(string)
}

variable "github_repository" {
  description = "Repositório (dono/nome) autorizado a assumir a role de plan via OIDC."
  type        = string
  default     = "gpgomes/data-master-std-fraud"
}

variable "create_github_oidc" {
  description = "Cria o OIDC provider do GitHub e a role de plan do CI. Só pode existir um provider por conta: use false se já houver um."
  type        = bool
  default     = true
}
