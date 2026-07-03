output "instance_id" {
  value = aws_instance.visualization.id
}

output "public_ip" {
  description = "Public IP — Superset UI and SSH. Changes on stop/start."
  value       = aws_instance.visualization.public_ip
}

output "private_ip" {
  description = "Private IP — what the delivery Lambda connects to on 5432"
  value       = aws_instance.visualization.private_ip
}

output "superset_url" {
  value = "http://${aws_instance.visualization.public_ip}:8088"
}
