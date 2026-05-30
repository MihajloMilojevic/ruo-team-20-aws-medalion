variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "eu-central-1"
}

variable "project_name" {
  description = "Prefix applied to all resources for easy identification"
  type        = string
  default     = "social-media-pipeline"
}

variable "environment" {
  description = "Deployment environment (dev, prod)"
  type        = string
  default     = "dev"
}

variable "discord_webhook_url" {
  description = "Discord webhook URL for error notifications"
  type        = string
  sensitive   = true
}
