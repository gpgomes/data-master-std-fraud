variable "name" {
  description = "Nome do budget."
  type        = string
}

variable "limit_usd" {
  description = "Teto de gasto mensal em dólares."
  type        = number

  validation {
    condition     = var.limit_usd > 0
    error_message = "limit_usd deve ser positivo."
  }
}

variable "subscriber_emails" {
  description = "E-mails que recebem os alertas. Passe via terraform.tfvars (ignorado pelo git) ou TF_VAR_budget_alert_emails — não versione."
  type        = list(string)

  validation {
    condition     = length(var.subscriber_emails) > 0
    error_message = "Informe ao menos um e-mail: sem destinatário o budget não avisa ninguém."
  }
}

variable "actual_thresholds" {
  description = "Percentuais do teto que disparam alerta sobre o gasto real."
  type        = list(number)
  default     = [50, 80, 100]
}

variable "forecast_threshold" {
  description = "Percentual do teto que dispara alerta sobre o gasto previsto."
  type        = number
  default     = 100
}
