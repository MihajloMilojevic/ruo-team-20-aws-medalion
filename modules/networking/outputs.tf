output "vpc_id" {
  value = aws_vpc.main.id
}

output "public_subnet_ids" {
  value = aws_subnet.public[*].id
}

output "lambda_vpc_sg_id" {
  description = "Security group ID for VPC Lambda functions"
  value       = aws_security_group.lambda_vpc.id
}

output "ec2_sg_id" {
  value = aws_security_group.ec2.id
}
