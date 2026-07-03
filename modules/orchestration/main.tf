# The orchestration module coordinates when and how the pipeline runs.
#
# A Step Functions state machine chains the whole pipeline:
#   ingestion -> (normalization_hn || normalization_x in parallel) -> analytics -> delivery
#
# It is started daily by EventBridge with an empty input (every Lambda then
# defaults to "yesterday"), or manually with {"date": "YYYY-MM-DD"} to run
# the full chain for one specific day:
#
#   aws stepfunctions start-execution \
#     --state-machine-arn <arn> --input '{"date": "2026-07-01"}'
#
# Ingestion returns the date it resolved, and the state machine threads that
# value into every downstream step — so all steps are guaranteed to operate
# on the same day even if the execution crosses midnight, and normalization_x
# (which deliberately has no default date) always receives an explicit one.
#
# Failure handling: every task has a Catch routing to NotifyFailure, which
# invokes the notification Lambda DIRECTLY with the failing Lambda's full
# errorType/errorMessage/stackTrace (Step Functions receives these natively
# in $.Cause). This finally restores rich error notifications for VPC
# Lambdas — the gap left when the notify_on_error decorator was removed
# (VPC Lambdas have no network path to SNS, but Step Functions is an AWS
# service outside the VPC and needs no such path).

# ── Step Functions state machine ─────────────────────────────────────────────

# Execution role: the state machine only ever invokes Lambdas, so that is
# the only permission it gets. The ":*" variants cover qualified ARNs
# (versions/aliases) that the Lambda service may report on invoke.
resource "aws_iam_role" "sfn" {
  name = "${var.project_name}-${var.environment}-pipeline-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_invoke_lambdas" {
  name = "invoke-pipeline-lambdas"
  role = aws_iam_role.sfn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "InvokePipelineLambdas"
      Effect = "Allow"
      Action = ["lambda:InvokeFunction"]
      Resource = flatten([
        for arn in [
          var.ingestion_lambda_arn,
          var.normalization_hn_lambda_arn,
          var.normalization_x_lambda_arn,
          var.analytics_lambda_arn,
          var.delivery_lambda_arn,
          var.notification_lambda_arn,
        ] : [arn, "${arn}:*"]
      ])
    }]
  })
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.project_name}-${var.environment}-pipeline"
  role_arn = aws_iam_role.sfn.arn

  definition = templatefile("${path.module}/templates/pipeline.asl.json", {
    ingestion_lambda_arn        = var.ingestion_lambda_arn
    normalization_hn_lambda_arn = var.normalization_hn_lambda_arn
    normalization_x_lambda_arn  = var.normalization_x_lambda_arn
    analytics_lambda_arn        = var.analytics_lambda_arn
    delivery_lambda_arn         = var.delivery_lambda_arn
    notification_lambda_arn     = var.notification_lambda_arn
  })
}

# ── Daily pipeline trigger ────────────────────────────────────────────────────
# EventBridge now starts the state machine instead of invoking the ingestion
# Lambda directly. Input is a clean {} (rather than the raw scheduled-event
# JSON) so ingestion's parse_target_date sees no "date" key and defaults to
# yesterday — the same behavior the direct trigger had.

resource "aws_cloudwatch_event_rule" "daily_pipeline" {
  name                = "${var.project_name}-${var.environment}-daily-ingestion"
  description         = "Starts the full pipeline state machine every day at 02:00 UTC"
  schedule_expression = "cron(0 2 * * ? *)"
}

# EventBridge needs its own role to start executions — Lambda targets use
# resource-based permissions instead, which state machines don't support.
resource "aws_iam_role" "eventbridge_sfn" {
  name = "${var.project_name}-${var.environment}-eventbridge-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_start_execution" {
  name = "start-pipeline-execution"
  role = aws_iam_role.eventbridge_sfn.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = aws_sfn_state_machine.pipeline.arn
    }]
  })
}

resource "aws_cloudwatch_event_target" "pipeline" {
  rule      = aws_cloudwatch_event_rule.daily_pipeline.name
  target_id = "PipelineStateMachine"
  arn       = aws_sfn_state_machine.pipeline.arn
  role_arn  = aws_iam_role.eventbridge_sfn.arn
  input     = jsonencode({})
}

# ── CloudWatch error alarms ───────────────────────────────────────────────────
# Each Lambda has an alarm that fires on at least one error.
# The alarm triggers an SNS topic which in turn invokes the notification Lambda.
#
# Kept alongside the Step Functions Catch notifications as a backstop: the
# alarms also cover manual out-of-band invocations, and error modes the code
# can't report itself (timeout, OOM). The tradeoff is that a Lambda failing
# INSIDE a pipeline execution now produces two Discord messages — one
# generic from the alarm, one rich (with stack trace) from the state
# machine. If that's too noisy, remove the pipeline Lambdas from the
# for_each set below and rely on the state machine's Catch alone.

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
