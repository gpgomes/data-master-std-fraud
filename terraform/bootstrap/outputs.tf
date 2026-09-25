output "state_bucket_name" {
  description = "Bucket do state remoto — vai no backend.hcl do ambiente dev."
  value       = module.state_bucket.bucket_names["tfstate"]
}

output "budget_name" {
  description = "Nome do budget mensal criado."
  value       = module.budget.name
}

output "github_plan_role_arn" {
  description = "ARN da role de plan do CI. Guarde na variável de repositório AWS_PLAN_ROLE_ARN do GitHub (não é segredo). Null se create_github_oidc = false."
  value       = var.create_github_oidc ? aws_iam_role.github_plan[0].arn : null
}

output "backend_config_example" {
  description = "Conteúdo pronto para o backend.hcl do ambiente dev."
  value       = <<-EOT
    bucket       = "${module.state_bucket.bucket_names["tfstate"]}"
    key          = "dev/terraform.tfstate"
    region       = "${var.region}"
    encrypt      = true
    use_lockfile = true
  EOT
}
