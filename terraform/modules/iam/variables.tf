variable "name_prefix" {
  description = "Prefixo dos nomes das policies (ex: data-master-std-fraud-dev)."
  type        = string
}

variable "datalake_bucket_arns" {
  description = "ARN dos buckets do data lake, indexado pela camada (bronze, silver, gold, checkpoints). Vem do output bucket_arns do módulo s3."
  type        = map(string)

  validation {
    condition     = length(var.datalake_bucket_arns) > 0
    error_message = "Informe ao menos um bucket."
  }
}
