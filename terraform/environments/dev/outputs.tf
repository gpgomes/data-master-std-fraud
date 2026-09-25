# Os quatro bucket_* alimentam as variáveis do app (config.py):
# MINIO_BUCKET_BRONZE / SILVER / GOLD / CHECKPOINTS.

output "bucket_bronze" {
  description = "Bucket da camada Bronze (MINIO_BUCKET_BRONZE)."
  value       = module.s3.bucket_names["bronze"]
}

output "bucket_silver" {
  description = "Bucket da camada Silver (MINIO_BUCKET_SILVER)."
  value       = module.s3.bucket_names["silver"]
}

output "bucket_gold" {
  description = "Bucket da camada Gold (MINIO_BUCKET_GOLD)."
  value       = module.s3.bucket_names["gold"]
}

output "bucket_checkpoints" {
  description = "Bucket de checkpoints do streaming (MINIO_BUCKET_CHECKPOINTS)."
  value       = module.s3.bucket_names["checkpoints"]
}

output "datalake_read_policy_arns" {
  description = "Policies de leitura por camada, para anexar às roles dos jobs."
  value       = module.iam.read_policy_arns
}

output "datalake_write_policy_arns" {
  description = "Policies de leitura e escrita por camada, para anexar às roles dos jobs."
  value       = module.iam.write_policy_arns
}
