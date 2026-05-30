terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # These options are here for LocalStack compatibility and have no effect on real AWS.
  # default_tags is omitted due to a PutBucketTagging bug in LocalStack Community.
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true
}

module "networking" {
  source = "./modules/networking"

  project_name = var.project_name
  environment  = var.environment
}

module "storage" {
  source = "./modules/storage"

  project_name = var.project_name
  environment  = var.environment
}

# Orchestration must be declared before security and compute because both
# need the SNS topic ARN that orchestration creates.
module "orchestration" {
  source = "./modules/orchestration"

  project_name              = var.project_name
  environment               = var.environment
  ingestion_lambda_arn      = module.compute.ingestion_lambda_arn
  ingestion_lambda_name     = module.compute.ingestion_lambda_name
  notification_lambda_arn   = module.compute.notification_lambda_arn
  notification_lambda_name  = module.compute.notification_lambda_name
  normalization_lambda_name = module.compute.normalization_lambda_name
  analytics_lambda_name     = module.compute.analytics_lambda_name
  delivery_lambda_name      = module.compute.delivery_lambda_name
}

module "security" {
  source = "./modules/security"

  project_name         = var.project_name
  environment          = var.environment
  data_lake_bucket_arn = module.storage.data_lake_bucket_arn
}

module "compute" {
  source = "./modules/compute"

  project_name           = var.project_name
  environment            = var.environment
  data_lake_bucket_name  = module.storage.data_lake_bucket_name
  ingestion_role_arn     = module.security.ingestion_role_arn
  normalization_role_arn = module.security.normalization_role_arn
  analytics_role_arn     = module.security.analytics_role_arn
  delivery_role_arn      = module.security.delivery_role_arn
  notification_role_arn  = module.security.notification_role_arn
  vpc_subnet_ids         = module.networking.public_subnet_ids
  vpc_security_group_ids = [module.networking.lambda_vpc_sg_id]
  discord_webhook_url    = var.discord_webhook_url
}
