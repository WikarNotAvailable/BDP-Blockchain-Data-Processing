variable "aws_region" {
  type        = string
  default     = "eu-north-1"
  description = "AWS region"
}

variable "glue_role_name" {
  type        = string
  default     = "GlueServiceRole"
  description = " IAM role for Glue"
}

variable "sagemaker_execution_role_name" {
  type        = string
  default     = "AmazonSageMaker-ExecutionRole-20250102T163562"
  description = " IAM role for Sagemaker execution"
}

variable "glue_jobs_default_arguments" {
  type = map(string)
  default = {
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://aws-glue-assets-982534349340-eu-north-1/sparkHistoryLogs/"
    "--enable-job-insights"              = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-glue-datacatalog"          = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://aws-glue-assets-982534349340-eu-north-1/temporary/"
    "--enable-auto-scaling"              = "true"
  }
}

variable "orphan_files_removal_workflow_name" {
  type        = string
  default     = "Orphan files removal"
  description = "Name of the orphan files removal Glue workflow."
}

variable "etl_workflow_name" {
  type        = string
  default     = "ETL"
  description = "Name of the etl Glue workflow."
}

variable "github_role_name" {
  type        = string
  default     = "GitHubOIDCWorkflowRole"
  description = "IAM role for Github integration name"
}