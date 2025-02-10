data "aws_iam_policy_document" "glue_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_service_role" {
  name               = var.glue_role_name
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role_policy.json
}

resource "aws_iam_role_policy_attachment" "glue_s3_full_access" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_glue_service_role_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_sagemaker_full_access" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
}

data "aws_iam_policy_document" "sagemaker_assume_role_policy" {
  statement {
    effect = "Allow"

    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["sagemaker.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "sagemaker_athena_policy_doc" {
  statement {
    effect = "Allow"

    actions   = ["athena:GetWorkGroup"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "sagemaker_s3_policy_doc" {
  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket"
    ]

    resources = ["arn:aws:s3:::*"]
  }
}

resource "aws_iam_role" "sagemaker_execution_role" {
  name               = var.sagemaker_execution_role_name
  assume_role_policy = data.aws_iam_policy_document.sagemaker_assume_role_policy.json
}

resource "aws_iam_role_policy_attachment" "sagemaker_s3_full_access" {
  role       = aws_iam_role.sagemaker_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "sagemaker_glue_service_role_policy" {
  role       = aws_iam_role.sagemaker_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "sagemaker_sagemaker_full_access" {
  role       = aws_iam_role.sagemaker_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
}

resource "aws_iam_role_policy_attachment" "sagemaker_athena_full_access" {
  role       = aws_iam_role.sagemaker_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
}

resource "aws_iam_policy" "sagemaker_s3_policy" {
  name        = "AmazonSageMaker-S3AccessPolicy"
  description = "SageMaker policy for S3 access"
  policy      = data.aws_iam_policy_document.sagemaker_s3_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "sagemaker_s3_access" {
  role       = aws_iam_role.sagemaker_execution_role.name
  policy_arn = aws_iam_policy.sagemaker_s3_policy.arn
}

resource "aws_iam_policy" "sagemaker_athena_policy" {
  name        = "AmazonSageMaker-AthenaAccessPolicy"
  description = "Policy to allow Athena GetWorkGroup access"
  policy      = data.aws_iam_policy_document.sagemaker_athena_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "sagemaker_athena_access" {
  role       = aws_iam_role.sagemaker_execution_role.name
  policy_arn = aws_iam_policy.sagemaker_athena_policy.arn
}