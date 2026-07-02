# Each Lambda gets exactly the permissions it needs — no more.
# Sharing roles across Lambda functions is an anti-pattern that violates
# least privilege and makes access audits harder.

locals {
  # The trust policy is the same for all — only the Lambda service may assume the role.
  lambda_trust_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# ── Ingestion Lambda ──────────────────────────────────────────────────────────
# Outside VPC. Reads from HN/Algolia API, writes to bronze/. Nothing else.

resource "aws_iam_role" "ingestion" {
  name               = "${var.project_name}-${var.environment}-ingestion-role"
  assume_role_policy = local.lambda_trust_policy
}

resource "aws_iam_role_policy" "ingestion_s3" {
  name = "s3-bronze-write"
  role = aws_iam_role.ingestion.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "WriteBronze"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${var.data_lake_bucket_arn}/bronze/*"
      },
      {
        Sid      = "ListBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = var.data_lake_bucket_arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ingestion_logs" {
  role       = aws_iam_role.ingestion.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# ── Normalization Lambdas (Silver) ────────────────────────────────────────────
# Inside VPC. Two separate Lambdas/roles (normalization_hn, normalization_x) —
# see SILVER_LAYER_HANDOFF.md section 1 for why they're split. Each reads only
# its own Bronze prefix and writes silver/*. Both require the VPC execution
# role because AWS creates an ENI for the Lambda when it runs inside a VPC.

resource "aws_iam_role" "normalization_hn" {
  name               = "${var.project_name}-${var.environment}-normalization-hn-role"
  assume_role_policy = local.lambda_trust_policy
}

resource "aws_iam_role_policy" "normalization_hn_s3" {
  name = "s3-bronze-hn-read-silver-write"
  role = aws_iam_role.normalization_hn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadBronzeHN"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${var.data_lake_bucket_arn}/bronze/hacker_news/*"
      },
      {
        Sid      = "ReadWriteSilver"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${var.data_lake_bucket_arn}/silver/*"
      },
      {
        Sid      = "ListBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = var.data_lake_bucket_arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "normalization_hn_logs" {
  role       = aws_iam_role.normalization_hn.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role" "normalization_x" {
  name               = "${var.project_name}-${var.environment}-normalization-x-role"
  assume_role_policy = local.lambda_trust_policy
}

resource "aws_iam_role_policy" "normalization_x_s3" {
  name = "s3-bronze-x-read-silver-write"
  role = aws_iam_role.normalization_x.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadBronzeX"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${var.data_lake_bucket_arn}/bronze/x/*"
      },
      {
        Sid      = "ReadWriteSilver"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${var.data_lake_bucket_arn}/silver/*"
      },
      {
        Sid      = "ListBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = var.data_lake_bucket_arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "normalization_x_logs" {
  role       = aws_iam_role.normalization_x.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# ── Analytics Lambda ──────────────────────────────────────────────────────────
# Inside VPC. Reads silver/, writes gold/.

resource "aws_iam_role" "analytics" {
  name               = "${var.project_name}-${var.environment}-analytics-role"
  assume_role_policy = local.lambda_trust_policy
}

resource "aws_iam_role_policy" "analytics_s3" {
  name = "s3-silver-read-gold-write"
  role = aws_iam_role.analytics.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadSilver"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${var.data_lake_bucket_arn}/silver/*"
      },
      {
        Sid      = "WriteGold"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:DeleteObject"]
        Resource = "${var.data_lake_bucket_arn}/gold/*"
      },
      {
        Sid      = "ListBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = var.data_lake_bucket_arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "analytics_logs" {
  role       = aws_iam_role.analytics.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# ── Delivery Lambda ───────────────────────────────────────────────────────────
# Inside VPC. Reads gold/ and writes to PostgreSQL on EC2.
# The PostgreSQL connection is controlled via Security Group rules, not IAM.

resource "aws_iam_role" "delivery" {
  name               = "${var.project_name}-${var.environment}-delivery-role"
  assume_role_policy = local.lambda_trust_policy
}

resource "aws_iam_role_policy" "delivery_s3" {
  name = "s3-gold-read"
  role = aws_iam_role.delivery.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadGold"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${var.data_lake_bucket_arn}/gold/*"
      },
      {
        Sid      = "ListBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = var.data_lake_bucket_arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "delivery_logs" {
  role       = aws_iam_role.delivery.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# ── Notification Lambda ───────────────────────────────────────────────────────
# Outside VPC. Sends HTTP POST to the Discord webhook.
# No S3 access needed — CloudWatch logs only.

resource "aws_iam_role" "notification" {
  name               = "${var.project_name}-${var.environment}-notification-role"
  assume_role_policy = local.lambda_trust_policy
}

resource "aws_iam_role_policy_attachment" "notification_logs" {
  role       = aws_iam_role.notification.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}
