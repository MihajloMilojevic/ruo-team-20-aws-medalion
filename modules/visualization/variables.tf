variable "project_name" { type = string }
variable "environment"  { type = string }

variable "subnet_id" {
  description = "Public subnet the instance is placed in"
  type        = string
}

variable "security_group_id" {
  description = "The ec2 security group from the networking module (8088, 22, 5432-from-Lambda)"
  type        = string
}

variable "instance_type" {
  description = "Free-tier eligible instance type"
  type        = string
  default     = "t3.micro"
}

variable "ssh_public_key" {
  description = "Optional SSH public key for debugging access. Leave empty to skip key pair creation."
  type        = string
  default     = ""
}

# ── PostgreSQL ────────────────────────────────────────────────────────────────

variable "db_username" {
  description = "PostgreSQL user for the analytics database (also owns Superset's metadata DB)"
  type        = string
}

variable "db_password" {
  description = "PostgreSQL password"
  type        = string
  sensitive   = true
}

variable "db_name" {
  description = "Name of the database the delivery Lambda loads gold tables into"
  type        = string
}

# ── Superset ──────────────────────────────────────────────────────────────────

variable "superset_admin_username" {
  description = "Initial Superset admin account username"
  type        = string
}

variable "superset_admin_password" {
  description = "Initial Superset admin account password"
  type        = string
  sensitive   = true
}

variable "superset_secret_key" {
  description = "Superset SECRET_KEY (session signing / credential encryption). Generate with: openssl rand -base64 42"
  type        = string
  sensitive   = true
}
