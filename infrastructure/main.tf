module "s3_buckets" {
  source = "./modules/s3_buckets"
}

module "iam" {
  source         = "./modules/iam"
  glue_role_name = var.glue_role_name
}

module "glue_catalog" {
  source                          = "./modules/glue_catalog"
  glue_role_arn                   = module.iam.glue_role_arn
  bdp_cleaned_transactions_bucket = module.s3_buckets.bdp_cleaned_transactions_bucket
  bdp_wallets_aggregations_bucket = module.s3_buckets.bdp_wallets_aggregations_bucket
  bdp_scaled_features_bucket      = module.s3_buckets.bdp_scaled_features_bucket
  bdp_unscaled_features_bucket    = module.s3_buckets.bdp_unscaled_features_bucket
}

module "iam_github_role" {
  source             = "./modules/iam_github_role"
  github_role_name   = var.github_role_name
  glue_script_bucket = module.s3_buckets.glue_scripts_bucket
}

module "iam_github_user" {
  source             = "./modules/iam_github_user"
  glue_script_bucket = module.s3_buckets.glue_scripts_bucket
}

module "glue_jobs" {
  source             = "./modules/glue_jobs"
  glue_script_bucket = module.s3_buckets.glue_scripts_bucket
  glue_role_arn      = module.iam.glue_role_arn
  default_arguments  = var.glue_jobs_default_arguments
}

module "glue_workflows" {
  source                         = "./modules/glue_workflows"
  etl_workflow_name              = var.etl_workflow_name
  transactions_cleaning_job_name = module.glue_jobs.transactions_cleaning_job_name
  wallets_aggregations_job_name  = module.glue_jobs.wallets_aggregations_job_name
  feature_scaling_job_name       = module.glue_jobs.feature_scaling_job_name
}