resource "aws_s3_bucket" "bdp_anomaly_detection" {
  bucket = "bdp-anomaly-detection"
}

resource "aws_s3_bucket" "bdp_athena_results" {
  bucket = "bdp-athena-results"
}

resource "aws_s3_bucket" "bdp_cleaned_transactions" {
  bucket = "bdp-cleaned-transactions"
}

resource "aws_s3_bucket" "bdp_feature_selection" {
  bucket = "bdp-feature-selection"
}

resource "aws_s3_bucket" "bdp_glue_scripts" {
  bucket = "bdp-glue-scripts"
}

resource "aws_s3_bucket" "bdp_inference_results" {
  bucket = "bdp-inference-results"
}

resource "aws_s3_bucket" "bdp_models" {
  bucket = "bdp-models"
}

resource "aws_s3_bucket" "bdp_recordio" {
  bucket = "bdp-recordio"
}

resource "aws_s3_bucket" "bdp_scaled_features" {
  bucket = "bdp-scaled-features"
}

resource "aws_s3_bucket" "bdp_scaled_features_inference" {
  bucket = "bdp-scaled-features-inference"
}

resource "aws_s3_bucket" "bdp_test-data" {
  bucket = "bdp-test-data"
}

resource "aws_s3_bucket" "bdp_unscaled_features" {
  bucket = "bdp-unscaled-features"
}

resource "aws_s3_bucket" "bdp_unscaled_features_inference" {
  bucket = "bdp-unscaled-features-inference"
}

resource "aws_s3_bucket" "bdp_wallets_aggregations" {
  bucket = "bdp-wallets-aggregations"
}