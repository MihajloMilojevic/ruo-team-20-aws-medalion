# The orchestration module coordinates when and how Lambda functions are triggered.
# Currently: EventBridge triggers the ingestion Lambda once a day.
# Next step: Step Functions state machine for the full pipeline
# (ingestion → normalization → analytics → delivery).

# ── Daily ingestion trigger ───────────────────────────────────────────────────

resource "aws_cloudwatch_event_rule" "daily_ingestion" {
  name                = "${var.project_name}-${var.environment}-daily-ingestion"
  description         = "Triggers the ingestion Lambda every day at 02:00 UTC"
  schedule_expression = "cron(0 2 * * ? *)"
}

resource "aws_cloudwatch_event_target" "ingestion" {
  rule      = aws_cloudwatch_event_rule.daily_ingestion.name
  target_id = "IngestionLambda"
  arn       = var.ingestion_lambda_arn
}

resource "aws_lambda_permission" "eventbridge_ingestion" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = var.ingestion_lambda_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_ingestion.arn
}

# ── CloudWatch error alarms ───────────────────────────────────────────────────
# Each Lambda has an alarm that fires on at least one error.
# The alarm triggers an SNS topic which in turn invokes the notification Lambda.

resource "aws_sns_topic" "lambda_errors" {
  name = "${var.project_name}-${var.environment}-lambda-errors"
}

resource "aws_sns_topic_subscription" "notify_on_error" {
  topic_arn = aws_sns_topic.lambda_errors.arn
  protocol  = "lambda"
  endpoint  = var.notification_lambda_arn
}

resource "aws_lambda_permission" "sns_notification" {
  statement_id  = "AllowSNSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = var.notification_lambda_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.lambda_errors.arn
}

# One alarm per Lambda — fires as soon as >= 1 error appears within 5 minutes.
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each = toset([
    var.ingestion_lambda_name,
    var.normalization_hn_lambda_name,
    var.normalization_x_lambda_name,
    var.analytics_lambda_name,
    var.delivery_lambda_name,
  ])

  alarm_name          = "${each.key}-errors"
  alarm_description   = "Lambda ${each.key} reported an error"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = each.key }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.lambda_errors.arn]
}
