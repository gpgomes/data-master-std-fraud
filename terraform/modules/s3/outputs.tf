output "bucket_names" {
  description = "Nome de cada bucket, indexado pelo sufixo (ex: bronze)."
  value       = { for key, bucket in aws_s3_bucket.this : key => bucket.bucket }
}

output "bucket_arns" {
  description = "ARN de cada bucket, indexado pelo sufixo (ex: bronze)."
  value       = { for key, bucket in aws_s3_bucket.this : key => bucket.arn }
}
