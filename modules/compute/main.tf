# The compute module is solely responsible for Lambda function definitions.
# IAM roles come from the security module, VPC config from the networking module.

locals {
  public_lambdas = ["ingestion", "notification"]
  vpc_lambdas    = ["normalization", "analytics", "delivery"]
}

# ── Log groups ────────────────────────────────────────────────────────────────
# Created explicitly to control retention. If Lambda creates them on its own,
# they are never deleted (retention = never).
resource "aws_cloudwatch_log_group" "lambdas" {
  for_each = toset(concat(local.public_lambdas, local.vpc_lambdas))

  name              = "/aws/lambda/${var.project_name}-${var.environment}-${each.key}"
  retention_in_days = 7
}

# ── Code packaging ────────────────────────────────────────────────────────────
data "archive_file" "ingestion" {
  type        = "zip"
  source_dir  = "${path.root}/src/ingestion"
  output_path = "${path.root}/src/ingestion/package.zip"
}

data "archive_file" "normalization" {
  type        = "zip"
  source_dir  = "${path.root}/src/normalization"
  output_path = "${path.root}/src/normalization/package.zip"
}

data "archive_file" "analytics" {
  type        = "zip"
  source_dir  = "${path.root}/src/analytics"
  output_path = "${path.root}/src/analytics/package.zip"
}

data "archive_file" "delivery" {
  type        = "zip"
  source_dir  = "${path.root}/src/delivery"
  output_path = "${path.root}/src/delivery/package.zip"
}

data "archive_file" "notification" {
  type        = "zip"
  source_dir  = "${path.root}/src/notification"
  output_path = "${path.root}/src/notification/package.zip"
}

# ── Lambda functions ──────────────────────────────────────────────────────────

resource "aws_lambda_function" "ingestion" {
  function_name    = "${var.project_name}-${var.environment}-ingestion"
  description      = "Bronze: fetches HN data and writes raw JSON to S3"
  filename         = data.archive_file.ingestion.output_path
  source_code_hash = data.archive_file.ingestion.output_base64sha256
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  timeout          = 300
  memory_size      = 256
  role             = var.ingestion_role_arn

  # No vpc_config — this Lambda needs to reach the Algolia/HN API on the internet.

  environment {
    variables = {
      DATA_LAKE_BUCKET = var.data_lake_bucket_name
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambdas["ingestion"]]
  tags       = { Layer = "Bronze" }
}

resource "aws_lambda_function" "normalization" {
  function_name    = "${var.project_name}-${var.environment}-normalization"
  description      = "Silver: normalizes bronze data and writes Parquet to S3"
  filename         = data.archive_file.normalization.output_path
  source_code_hash = data.archive_file.normalization.output_base64sha256
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  timeout          = 300
  memory_size      = 512
  role             = var.normalization_role_arn

  vpc_config {
    subnet_ids         = var.vpc_subnet_ids
    security_group_ids = var.vpc_security_group_ids
  }

  environment {
    variables = {
      DATA_LAKE_BUCKET = var.data_lake_bucket_name
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambdas["normalization"]]
  tags       = { Layer = "Silver" }
}

resource "aws_lambda_function" "analytics" {
  function_name    = "${var.project_name}-${var.environment}-analytics"
  description      = "Gold: computes metrics and KPIs from the silver layer"
  filename         = data.archive_file.analytics.output_path
  source_code_hash = data.archive_file.analytics.output_base64sha256
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  timeout          = 300
  memory_size      = 512
  role             = var.analytics_role_arn

  vpc_config {
    subnet_ids         = var.vpc_subnet_ids
    security_group_ids = var.vpc_security_group_ids
  }

  environment {
    variables = {
      DATA_LAKE_BUCKET = var.data_lake_bucket_name
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambdas["analytics"]]
  tags       = { Layer = "Gold" }
}

resource "aws_lambda_function" "delivery" {
  function_name    = "${var.project_name}-${var.environment}-delivery"
  description      = "Delivery: moves gold metrics from S3 into PostgreSQL on EC2"
  filename         = data.archive_file.delivery.output_path
  source_code_hash = data.archive_file.delivery.output_base64sha256
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  timeout          = 120
  memory_size      = 256
  role             = var.delivery_role_arn

  # Must be inside the VPC to reach EC2/PostgreSQL.
  vpc_config {
    subnet_ids         = var.vpc_subnet_ids
    security_group_ids = var.vpc_security_group_ids
  }

  environment {
    variables = {
      DATA_LAKE_BUCKET = var.data_lake_bucket_name
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambdas["delivery"]]
  tags       = { Layer = "Delivery" }
}

resource "aws_lambda_function" "notification" {
  function_name    = "${var.project_name}-${var.environment}-notification"
  description      = "Sends a Discord notification when a Lambda reports an error via SNS"
  filename         = data.archive_file.notification.output_path
  source_code_hash = data.archive_file.notification.output_base64sha256
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  timeout          = 30
  memory_size      = 128
  role             = var.notification_role_arn

  # No vpc_config — needs internet access to reach the Discord webhook.

  environment {
    variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambdas["notification"]]
  tags       = { Layer = "Notification" }
}
