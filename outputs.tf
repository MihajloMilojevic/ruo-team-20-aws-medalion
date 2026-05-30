output "vpc_id" {
  value = module.networking.vpc_id
}

output "data_lake_bucket_name" {
  value = module.storage.data_lake_bucket_name
}

output "ingestion_lambda_name" {
  value = module.compute.ingestion_lambda_name
}

output "normalization_lambda_name" {
  value = module.compute.normalization_lambda_name
}

output "analytics_lambda_name" {
  value = module.compute.analytics_lambda_name
}

output "delivery_lambda_name" {
  value = module.compute.delivery_lambda_name
}
