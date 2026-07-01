output "ingestion_lambda_arn"         { value = aws_lambda_function.ingestion.arn }
output "ingestion_lambda_name"        { value = aws_lambda_function.ingestion.function_name }
output "normalization_hn_lambda_arn"  { value = aws_lambda_function.normalization_hn.arn }
output "normalization_hn_lambda_name" { value = aws_lambda_function.normalization_hn.function_name }
output "normalization_x_lambda_arn"   { value = aws_lambda_function.normalization_x.arn }
output "normalization_x_lambda_name"  { value = aws_lambda_function.normalization_x.function_name }
output "analytics_lambda_arn"         { value = aws_lambda_function.analytics.arn }
output "analytics_lambda_name"        { value = aws_lambda_function.analytics.function_name }
output "delivery_lambda_arn"          { value = aws_lambda_function.delivery.arn }
output "delivery_lambda_name"         { value = aws_lambda_function.delivery.function_name }
output "notification_lambda_arn"      { value = aws_lambda_function.notification.arn }
output "notification_lambda_name"     { value = aws_lambda_function.notification.function_name }

output "awswrangler_layer_arn"        { value = local.awswrangler_layer_arn }
output "silver_common_layer_arn"      { value = aws_lambda_layer_version.silver_common.arn }
