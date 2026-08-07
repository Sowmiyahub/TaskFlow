terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------------------
# S3 bucket that worker nodes write task results to.
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "task_results" {
  bucket = "${var.project_name}-results-${var.environment}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket_versioning" "task_results" {
  bucket = aws_s3_bucket.task_results.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "task_results" {
  bucket = aws_s3_bucket.task_results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "task_results" {
  bucket                  = aws_s3_bucket.task_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "task_results" {
  bucket = aws_s3_bucket.task_results.id

  rule {
    id     = "expire-old-results"
    status = "Enabled"

    expiration {
      days = var.result_retention_days
    }
  }
}

# ---------------------------------------------------------------------------
# IAM role assumed by worker pods (via IRSA) to write results to S3.
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "worker_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(var.oidc_provider_url, "https://", "")}:sub"
      values   = ["system:serviceaccount:${var.k8s_namespace}:${var.k8s_service_account}"]
    }
  }
}

resource "aws_iam_role" "worker_role" {
  name               = "${var.project_name}-worker-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.worker_assume_role.json

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

data "aws_iam_policy_document" "worker_s3_access" {
  statement {
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
    ]
    resources = ["${aws_s3_bucket.task_results.arn}/*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.task_results.arn]
  }
}

resource "aws_iam_policy" "worker_s3_access" {
  name   = "${var.project_name}-worker-s3-access-${var.environment}"
  policy = data.aws_iam_policy_document.worker_s3_access.json
}

resource "aws_iam_role_policy_attachment" "worker_s3_access" {
  role       = aws_iam_role.worker_role.name
  policy_arn = aws_iam_policy.worker_s3_access.arn
}
