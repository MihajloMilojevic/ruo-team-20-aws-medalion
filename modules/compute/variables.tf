variable "project_name"          { type = string }
variable "environment"           { type = string }
variable "data_lake_bucket_name" { type = string }
variable "ingestion_role_arn"    { type = string }
variable "normalization_hn_role_arn"{ type = string }
variable "normalization_x_role_arn"{ type = string }
variable "analytics_role_arn"    { type = string }
variable "delivery_role_arn"     { type = string }
variable "notification_role_arn" { type = string }
variable "vpc_subnet_ids"        { type = list(string) }
variable "vpc_security_group_ids"{ type = list(string) }
variable "discord_webhook_url"   { 
    type = string
    sensitive = true 
}

# Region used to build the ARN of the AWS-hosted awswrangler layer
# (arn:aws:lambda:<region>:336392948345:layer:AWSSDKPandas-Python312:29).
# The publishing account id is fixed by AWS across all regions; only the
# region segment changes depending on where this project is deployed.
variable "aws_region" {
  type    = string
  default = "us-east-1"
}
