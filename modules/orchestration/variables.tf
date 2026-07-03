variable "project_name" { type = string }
variable "environment"  { type = string }

# ARNs are what both the state machine definition and the SFN role's invoke
# policy need; names are what the CloudWatch alarms and the SNS resource
# permission need.
variable "ingestion_lambda_arn"         { type = string }
variable "ingestion_lambda_name"        { type = string }
variable "normalization_hn_lambda_arn"  { type = string }
variable "normalization_hn_lambda_name" { type = string }
variable "normalization_x_lambda_arn"   { type = string }
variable "normalization_x_lambda_name"  { type = string }
variable "analytics_lambda_arn"         { type = string }
variable "analytics_lambda_name"        { type = string }
variable "delivery_lambda_arn"          { type = string }
variable "delivery_lambda_name"         { type = string }
variable "notification_lambda_arn"      { type = string }
variable "notification_lambda_name"     { type = string }
