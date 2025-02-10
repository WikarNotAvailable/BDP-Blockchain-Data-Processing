output "glue_role_arn" {
  value = aws_iam_role.glue_service_role.arn
}

output "sagemaker_execution_role_arn" {
  value = aws_iam_role.sagemaker_execution_role.arn
}