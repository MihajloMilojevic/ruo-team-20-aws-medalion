output "error_sns_topic_arn" {
  description = "SNS topic ARN that alarms publish errors to"
  value       = aws_sns_topic.lambda_errors.arn
}
