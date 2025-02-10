output "bdp_anomaly_detection_bucket" {
  value = aws_s3_bucket.bdp_anomaly_detection.bucket
}

output "bdp_athena_results_bucket" {
  value = aws_s3_bucket.bdp_athena_results.bucket
}

output "bdp_cleaned_transactions_bucket" {
  value = aws_s3_bucket.bdp_cleaned_transactions.bucket
}

output "bdp_feature_selection_bucket" {
  value = aws_s3_bucket.bdp_feature_selection.bucket
}

output "bdp_glue_scripts_bucket" {
  value = aws_s3_bucket.bdp_glue_scripts.bucket
}

output "bdp_inference_results_bucket" {
  value = aws_s3_bucket.bdp_inference_results.bucket
}

output "bdp_models_bucket" {
  value = aws_s3_bucket.bdp_models.bucket
}

output "bdp_recordio_bucket" {
  value = aws_s3_bucket.bdp_recordio.bucket
}

output "bdp_scaled_features_bucket" {
  value = aws_s3_bucket.bdp_scaled_features.bucket
}

output "bdp_scaled_features_inference_bucket" {
  value = aws_s3_bucket.bdp_scaled_features_inference.bucket
}

output "bdp_test_data_bucket" {
  value = aws_s3_bucket.bdp_test-data.bucket
}

output "bdp_unscaled_features_bucket" {
  value = aws_s3_bucket.bdp_unscaled_features.bucket
}

output "bdp_unscaled_features_inference_bucket" {
  value = aws_s3_bucket.bdp_unscaled_features_inference.bucket
}

output "bdp_wallets_aggregations_bucket" {
  value = aws_s3_bucket.bdp_wallets_aggregations.bucket
}