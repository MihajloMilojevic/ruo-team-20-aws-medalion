output "error_sns_topic_arn" {
  description = "SNS topic ARN that alarms publish errors to"
  value       = aws_sns_topic.lambda_errors.arn
}

output "pipeline_state_machine_arn" {
  description = "Start manually with: aws stepfunctions start-execution --state-machine-arn <arn> --input '{\"date\": \"YYYY-MM-DD\"}'"
  value       = aws_sfn_state_machine.pipeline.arn
}
