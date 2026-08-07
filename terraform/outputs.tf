output "s3_results_bucket_name" {
  description = "Name of the S3 bucket where task results are stored"
  value       = aws_s3_bucket.task_results.bucket
}

output "s3_results_bucket_arn" {
  description = "ARN of the S3 bucket where task results are stored"
  value       = aws_s3_bucket.task_results.arn
}

output "worker_iam_role_arn" {
  description = "ARN of the IAM role assumed by worker pods to access S3"
  value       = aws_iam_role.worker_role.arn
}
