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

variable "force_destroy_buckets" {
  description = "Permite destruir buckets do data lake não vazios. Mantenha false: protege os dados contra um destroy acidental."
  type        = bool
  default     = false
}
