output "glue_scripts_bucket" {
  value = aws_s3_bucket.glue_scripts.bucket
}

output "bdp_anomaly_classification_bucket" {
  value = aws_s3_bucket.bdp_anomaly_classification.bucket
}

output "bdp_cleaned_transactions_bucket" {
  value = aws_s3_bucket.bdp_cleaned_transactions.bucket
}

output "bdp_scaled_features_bucket" {
  value = aws_s3_bucket.bdp_scaled_features.bucket
}

output "bdp_unscaled_features_bucket" {
  value = aws_s3_bucket.bdp_unscaled_features.bucket
}

output "bdp_metadata_bucket" {
  value = aws_s3_bucket.bdp_metadata.bucket
}

output "bdp_wallets_aggregations_bucket" {
  value = aws_s3_bucket.bdp_wallets_aggregations.bucket
}