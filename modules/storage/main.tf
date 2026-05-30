resource "random_id" "suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "data_lake" {
  bucket        = "${var.project_name}-${var.environment}-datalake-${random_id.suffix.hex}"
  force_destroy = true

  # Tags intentionally omitted from the bucket resource due to a LocalStack
  # Community bug with PutBucketTagging. Safe to add on real AWS.
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Lifecycle rules keep costs under control — raw bronze data is the most
# expensive to store and the least useful long-term.
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "bronze-lifecycle"
    status = "Enabled"

    filter { prefix = "bronze/" }

    transition {
      days          = 30
      storage_class = "GLACIER"
    }

    expiration { days = 90 }
  }

  rule {
    id     = "silver-lifecycle"
    status = "Enabled"

    filter { prefix = "silver/" }

    expiration { days = 180 }
  }

  rule {
    id     = "gold-lifecycle"
    status = "Enabled"

    filter { prefix = "gold/" }

    expiration { days = 365 }
  }
}

# Bucket policy that denies all non-HTTPS requests.
resource "aws_s3_bucket_policy" "https_only" {
  bucket = aws_s3_bucket.data_lake.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [
        aws_s3_bucket.data_lake.arn,
        "${aws_s3_bucket.data_lake.arn}/*"
      ]
      Condition = {
        Bool = { "aws:SecureTransport" = "false" }
      }
    }]
  })

  depends_on = [aws_s3_bucket_public_access_block.data_lake]
}

# S3 has no concept of folders — these .keep objects simply make the structure
# visible in the console and tools that render "folders".
resource "aws_s3_object" "prefixes" {
  for_each = toset([
    "bronze/hacker_news/.keep",
    "bronze/x/.keep",
    "silver/posts/.keep",
    "silver/users/.keep",
    "gold/.keep",
  ])

  bucket  = aws_s3_bucket.data_lake.id
  key     = each.value
  content = ""
}
