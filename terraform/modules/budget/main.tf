# Guarda de custo da conta. O AWS Budgets só AVISA — os dados de cobrança atrasam
# algumas horas e nada é bloqueado. A proteção real é derrubar MWAA/MSK/EMR/NAT
# ao fim de cada sessão de teste (ver docs/runbook.md, "Infraestrutura AWS").
resource "aws_budgets_budget" "monthly" {
  name         = var.name
  budget_type  = "COST"
  limit_amount = tostring(var.limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  # Mede o consumo bruto: créditos e reembolsos não "escondem" o gasto real,
  # que voltaria a ser cobrado quando os créditos acabarem.
  cost_types {
    include_credit = false
    include_refund = false
  }

  dynamic "notification" {
    for_each = var.actual_thresholds

    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = var.subscriber_emails
    }
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = var.forecast_threshold
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = var.subscriber_emails
  }
}
