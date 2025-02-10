output "transactions_cleaning_job_name" {
  value       = aws_glue_job.transactions_cleaning.name
  description = "Name of the transactions_cleaning Glue job"
}

output "wallets_aggregations_job_name" {
  value       = aws_glue_job.wallets_aggregations.name
  description = "Name of the wallets_aggregations Glue job"
}

output "feature_scaling_job_name" {
  value       = aws_glue_job.feature_scaling.name
  description = "Name of the feature_scaling Glue job"
}

output "spearman_feature_selection_job_name" {
  value       = aws_glue_job.spearman_feature_selection.name
  description = "Name of the spearman_feature_selection Glue job"
}

output "convert_parquet_to_csv_job_name" {
  value       = aws_glue_job.convert_parquet_to_csv.name
  description = "Name of the convert_parquet_to_csv Glue job"
}

output "convert_features_to_recordio_job_name" {
  value       = aws_glue_job.convert_features_to_recordio.name
  description = "Name of the convert_features_to_recordio Glue job"
}

output "preprocessing_with_string_columns_job_name" {
  value       = aws_glue_job.preprocessing_with_string_columns.name
  description = "Name of the preprocessing_with_string_columns Glue job"
}

output "convert_parquet_to_csv_for_visualisation_job_name" {
  value       = aws_glue_job.convert_parquet_to_csv_for_visualisation.name
  description = "Name of the convert_parquet_to_csv_for_visualisation Glue job"
}