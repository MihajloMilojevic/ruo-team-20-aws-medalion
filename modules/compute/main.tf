# The compute module is solely responsible for Lambda function definitions.
# IAM roles come from the security module, VPC config from the networking module.

locals {
  public_lambdas = ["ingestion", "notification"]
  vpc_lambdas    = ["normalization_hn", "normalization_x", "analytics", "delivery"]

  # AWS-hosted awswrangler layer (AWS SDK for pandas). Not built by this
  # project — we just reference the publicly published layer version, region
  # is the only part that varies (the account id 336392948345 is AWS's own
  # publishing account and is fixed across all regions).
  awswrangler_layer_arn = "arn:aws:lambda:${var.aws_region}:336392948345:layer:AWSSDKPandas-Python312:29"
}

# ── Log groups ────────────────────────────────────────────────────────────────
# Created explicitly to control retention. If Lambda creates them on its own,
# they are never deleted (retention = never).
resource "aws_cloudwatch_log_group" "lambdas" {
  for_each = toset(concat(local.public_lambdas, local.vpc_lambdas))

  name              = "/aws/lambda/${var.project_name}-${var.environment}-${each.key}"
  retention_in_days = 7
}

# ── Lambda Layers ─────────────────────────────────────────────────────────────
# awswrangler: no layer resource needed here, it's already hosted by AWS —
# see local.awswrangler_layer_arn above, referenced directly in each function.

# silver_common: our own layer. Contains silver_common.py (shared Silver
# helpers used by normalization_hn and normalization_x) plus beautifulsoup4,
# installed manually into layers/silver_common/python/ (see
# layers/silver_common/README.md) since this project has no build step.
data "archive_file" "silver_common_layer" {
  type        = "zip"
  source_dir  = "${path.root}/src/layers/silver_common"
  output_path = "${path.root}/src/layers/silver_common_layer.zip"
}

resource "aws_lambda_layer_version" "silver_common" {
  layer_name          = "${var.project_name}-${var.environment}-silver-common"
  description         = "Shared Silver layer helpers (silver_common.py) + beautifulsoup4"
  filename            = data.archive_file.silver_common_layer.output_path
  source_code_hash    = data.archive_file.silver_common_layer.output_base64sha256
  compatible_runtimes = ["python3.12"]
}

# delivery_common: pg8000 (pure-Python PostgreSQL driver) for the delivery
# Lambda — same vendored-into-the-repo pattern as silver_common, see
# layers/delivery_common/README.md for why pg8000 over psycopg2.
data "archive_file" "delivery_common_layer" {
  type        = "zip"
  source_dir  = "${path.root}/src/layers/delivery_common"
  output_path = "${path.root}/src/layers/delivery_common_layer.zip"
}

resource "aws_lambda_layer_version" "delivery_common" {
  layer_name          = "${var.project_name}-${var.environment}-delivery-common"
  description         = "PostgreSQL driver (pg8000) for the delivery Lambda"
  filename            = data.archive_file.delivery_common_layer.output_path
  source_code_hash    = data.archive_file.delivery_common_layer.output_base64sha256
  compatible_runtimes = ["python3.12"]
}

# ── Code packaging ────────────────────────────────────────────────────────────
data "archive_file" "ingestion" {
  type        = "zip"
  source_dir  = "${path.root}/src/ingestion"
  output_path = "${path.root}/src/ingestion/package.zip"
}

data "archive_file" "normalization_hn" {
  type        = "zip"
  source_dir  = "${path.root}/src/normalization_hn"
  output_path = "${path.root}/src/normalization_hn/package.zip"
}

data "archive_file" "normalization_x" {
  type        = "zip"
  source_dir  = "${path.root}/src/normalization_x"
  output_path = "${path.root}/src/normalization_x/package.zip"
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

resource "aws_lambda_function" "normalization_hn" {
  function_name    = "${var.project_name}-${var.environment}-normalization-hn"
  description      = "Silver: normalizes Hacker News bronze data and writes Parquet to S3"
  filename         = data.archive_file.normalization_hn.output_path
  source_code_hash = data.archive_file.normalization_hn.output_base64sha256
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  timeout          = 300
  memory_size      = 1024
  role             = var.normalization_hn_role_arn

  # awswrangler from the AWS-hosted layer, silver_common.py + bs4 from our
  # own layer. Neither is bundled into package.zip anymore.
  layers = [
    aws_lambda_layer_version.silver_common.arn,
  ]

  vpc_config {
    subnet_ids         = var.vpc_subnet_ids
    security_group_ids = var.vpc_security_group_ids
  }

  environment {
    variables = {
      DATA_LAKE_BUCKET = var.data_lake_bucket_name
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambdas["normalization_hn"]]
  tags       = { Layer = "Silver" }
}

resource "aws_lambda_function" "normalization_x" {
  function_name    = "${var.project_name}-${var.environment}-normalization-x"
  description      = "Silver: normalizes X/Twitter bronze data and writes Parquet to S3"
  filename         = data.archive_file.normalization_x.output_path
  source_code_hash = data.archive_file.normalization_x.output_base64sha256
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  timeout          = 300
  memory_size      = 1024
  role             = var.normalization_x_role_arn

  # Same two layers as normalization_hn — same shared helpers, same
  # dependency set.
  layers = [
    aws_lambda_layer_version.silver_common.arn,
  ]

  vpc_config {
    subnet_ids         = var.vpc_subnet_ids
    security_group_ids = var.vpc_security_group_ids
  }

  environment {
    variables = {
      DATA_LAKE_BUCKET = var.data_lake_bucket_name
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambdas["normalization_x"]]
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
  memory_size      = 2048
  role             = var.analytics_role_arn

  # awswrangler only — analytics keeps gold_common.py bundled in its own
  # package.zip (not extracted to a layer).
  layers = [
    local.awswrangler_layer_arn,
  ]

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
  # 300s/512MB: a {"full_refresh": true} run reads every gold table through
  # pandas — the old 120s/256MB sizing left no headroom for the awswrangler
  # cold import alone.
  timeout          = 300
  memory_size      = 512
  role             = var.delivery_role_arn

  # pandas + awswrangler for reading gold Parquet (same AWS-hosted layer
  # analytics uses), pg8000 for PostgreSQL from our own layer.
  layers = [
    local.awswrangler_layer_arn,
    aws_lambda_layer_version.delivery_common.arn,
  ]

  # Must be inside the VPC to reach EC2/PostgreSQL.
  vpc_config {
    subnet_ids         = var.vpc_subnet_ids
    security_group_ids = var.vpc_security_group_ids
  }

  environment {
    variables = {
      DATA_LAKE_BUCKET = var.data_lake_bucket_name
      # Private IP of the visualization EC2 instance — empty string until
      # the instance exists (enable_ec2 = false), in which case invoking
      # this Lambda fails fast on connect rather than doing anything useful.
      DB_HOST     = var.db_host
      DB_PORT     = var.db_port
      DB_NAME     = var.db_name
      DB_USER     = var.db_username
      DB_PASSWORD = var.db_password
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
