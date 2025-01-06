resource "aws_s3_bucket" "glue_scripts" {
  bucket = "bdp-glue-scripts"
}

resource "aws_s3_bucket" "bdp_anomaly_classification" {
  bucket = "bdp-anomaly-classification"
}

resource "aws_s3_bucket" "bdp_cleaned_transactions" {
  bucket = "bdp-cleaned-transactions"
}

resource "aws_s3_bucket" "bdp_scaled_features" {
  bucket = "bdp-scaled-features"
}

resource "aws_s3_bucket" "bdp_unscaled_features" {
  bucket = "bdp-unscaled-features"
}

resource "aws_s3_bucket" "bdp_metadata" {
  bucket = "bdp-metadata"
}

resource "aws_s3_bucket" "bdp_wallets_aggregations" {
  bucket = "bdp-wallets-aggregations"
}
