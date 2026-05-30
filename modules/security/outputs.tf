output "ingestion_role_arn"     { value = aws_iam_role.ingestion.arn }
output "normalization_role_arn" { value = aws_iam_role.normalization.arn }
output "analytics_role_arn"     { value = aws_iam_role.analytics.arn }
output "delivery_role_arn"      { value = aws_iam_role.delivery.arn }
output "notification_role_arn"  { value = aws_iam_role.notification.arn }
