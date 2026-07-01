output "ingestion_role_arn"        { value = aws_iam_role.ingestion.arn }
output "normalization_hn_role_arn" { value = aws_iam_role.normalization_hn.arn }
output "normalization_x_role_arn"  { value = aws_iam_role.normalization_x.arn }
output "analytics_role_arn"        { value = aws_iam_role.analytics.arn }
output "delivery_role_arn"         { value = aws_iam_role.delivery.arn }
output "notification_role_arn"     { value = aws_iam_role.notification.arn }
