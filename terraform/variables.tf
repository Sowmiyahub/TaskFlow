variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short name used to prefix all resources"
  type        = string
  default     = "taskflow"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "result_retention_days" {
  description = "Number of days to retain task results in S3 before expiring"
  type        = number
  default     = 30
}

# --- IAM Roles for Service Accounts (IRSA) inputs ---------------------------
# These come from your EKS cluster's OIDC provider. Populate them once the
# cluster exists, e.g. via `terraform_remote_state` or explicit tfvars.

variable "oidc_provider_arn" {
  description = "ARN of the EKS cluster's OIDC identity provider"
  type        = string
}

variable "oidc_provider_url" {
  description = "URL of the EKS cluster's OIDC identity provider (with https://)"
  type        = string
}

variable "k8s_namespace" {
  description = "Kubernetes namespace the worker service account lives in"
  type        = string
  default     = "taskflow"
}

variable "k8s_service_account" {
  description = "Kubernetes service account name used by worker pods"
  type        = string
  default     = "taskflow-worker"
}
