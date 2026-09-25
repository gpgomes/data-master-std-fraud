output "read_policy_arns" {
  description = "ARN da policy de leitura de cada camada, indexado pela camada."
  value       = { for layer, policy in aws_iam_policy.read : layer => policy.arn }
}

output "write_policy_arns" {
  description = "ARN da policy de leitura e escrita de cada camada, indexado pela camada."
  value       = { for layer, policy in aws_iam_policy.write : layer => policy.arn }
}
