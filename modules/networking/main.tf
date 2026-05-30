data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_region" "current" {}

# ── VPC ───────────────────────────────────────────────────────────────────────

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.project_name}-${var.environment}-vpc"
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-${var.environment}-igw"
  }
}

# Two subnets in different AZs — Lambda requires at least two
# when configured for high availability.
resource "aws_subnet" "public" {
  count = 2

  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.${count.index}.0/24"
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-${var.environment}-subnet-${count.index + 1}"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-rt"
  }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# ── S3 Gateway Endpoint ───────────────────────────────────────────────────────
# VPC Lambdas use this endpoint to read/write S3 without traffic leaving
# the AWS network. Free and faster than going through the public internet.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]

  tags = {
    Name = "${var.project_name}-${var.environment}-s3-endpoint"
  }
}

# ── Security Groups ───────────────────────────────────────────────────────────

# SG for Lambdas inside the VPC (normalization, analytics, delivery).
# No inbound rules — Lambdas are triggered by events, not direct network calls.
# The only allowed outbound is to PostgreSQL on the EC2 SG (port 5432),
# defined as a separate resource below to avoid a cyclic dependency.
resource "aws_security_group" "lambda_vpc" {
  name        = "${var.project_name}-${var.environment}-lambda-vpc-sg"
  description = "VPC Lambda functions — egress to Postgres and S3 endpoint only"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-${var.environment}-lambda-vpc-sg"
  }
}

resource "aws_security_group" "ec2" {
  name        = "${var.project_name}-${var.environment}-ec2-sg"
  description = "EC2 instance running Superset and PostgreSQL"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Superset web UI"
    from_port   = 8088
    to_port     = 8088
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "SSH access"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-ec2-sg"
  }
}

# Postgres rules are defined here, separate from the SG definitions, because
# both SGs reference each other — inline rules would cause a cyclic dependency.
resource "aws_security_group_rule" "lambda_egress_postgres" {
  type                     = "egress"
  description              = "Lambda to PostgreSQL on EC2"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.lambda_vpc.id
  source_security_group_id = aws_security_group.ec2.id
}

resource "aws_security_group_rule" "ec2_ingress_postgres" {
  type                     = "ingress"
  description              = "PostgreSQL accepts connections from Lambda SG only"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.ec2.id
  source_security_group_id = aws_security_group.lambda_vpc.id
}
