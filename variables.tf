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

# ── Visualization (EC2 + PostgreSQL + Superset) ──────────────────────────────

variable "enable_ec2" {
  description = "Provision the visualization EC2 instance. Set to false for LocalStack runs — LocalStack Community cannot emulate EC2."
  type        = bool
  default     = true
}

variable "ssh_public_key" {
  description = "Optional SSH public key for the EC2 instance (contents of ~/.ssh/id_ed25519.pub). Leave empty to skip."
  type        = string
  default     = ""
}

variable "db_username" {
  description = "PostgreSQL user for the analytics database"
  type        = string
  default     = "pipeline"
}

variable "db_password" {
  description = "PostgreSQL password"
  type        = string
  sensitive   = true
  default     = ""
}

variable "db_name" {
  description = "Database the delivery Lambda loads gold tables into"
  type        = string
  default     = "analytics"
}

variable "superset_admin_username" {
  description = "Initial Superset admin account username"
  type        = string
  default     = "admin"
}

variable "superset_admin_password" {
  description = "Initial Superset admin account password"
  type        = string
  sensitive   = true
  default     = ""
}

variable "superset_secret_key" {
  description = "Superset SECRET_KEY. Generate with: openssl rand -base64 42"
  type        = string
  sensitive   = true
  default     = ""
}
