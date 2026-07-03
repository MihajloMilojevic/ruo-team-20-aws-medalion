output "vpc_id" {
  value = module.networking.vpc_id
}

output "data_lake_bucket_name" {
  value = module.storage.data_lake_bucket_name
}

output "ingestion_lambda_name" {
  value = module.compute.ingestion_lambda_name
}

output "normalization_hn_lambda_name" {
  value = module.compute.normalization_hn_lambda_name
}

output "normalization_x_lambda_name" {
  value = module.compute.normalization_x_lambda_name
}

output "analytics_lambda_name" {
  value = module.compute.analytics_lambda_name
}

output "delivery_lambda_name" {
  value = module.compute.delivery_lambda_name
}

output "ec2_public_ip" {
  description = "Public IP of the visualization instance (SSH + Superset). Empty when enable_ec2 = false."
  value       = try(module.visualization[0].public_ip, "")
}

output "ec2_private_ip" {
  description = "Private IP the delivery Lambda connects to on 5432"
  value       = try(module.visualization[0].private_ip, "")
}

output "superset_url" {
  value = try(module.visualization[0].superset_url, "")
}

output "pipeline_state_machine_arn" {
  value = module.orchestration.pipeline_state_machine_arn
}
