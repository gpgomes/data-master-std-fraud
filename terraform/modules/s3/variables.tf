variable "name_prefix" {
  description = "Prefixo dos buckets. Inclua o account id: nomes de bucket S3 são globais (ex: data-master-std-fraud-dev-123456789012)."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{2,50}[a-z0-9]$", var.name_prefix))
    error_message = "name_prefix deve ter 4-52 caracteres, só minúsculas, números e hífens (o nome final do bucket tem no máximo 63)."
  }
}

variable "buckets" {
  description = "Buckets a criar, indexados pelo sufixo (ex: bronze, silver, gold, checkpoints). noncurrent_version_expiration_days controla por quanto tempo versões antigas de objetos são mantidas."
  type = map(object({
    noncurrent_version_expiration_days = optional(number, 30)
  }))

  validation {
    condition     = length(var.buckets) > 0
    error_message = "Informe ao menos um bucket."
  }
}

variable "kms_key_arn" {
  description = "ARN da chave KMS para criptografia. Com null, usa SSE-S3 (AES256), sem custo de chave — decisão de orçamento (US$ 50/mês)."
  type        = string
  default     = null
}

variable "force_destroy" {
  description = "Permite destruir buckets não vazios. Mantenha false: protege os dados contra um terraform destroy acidental."
  type        = bool
  default     = false
}
