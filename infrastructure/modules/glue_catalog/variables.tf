variable "bdp_cleaned_transactions_bucket" {
  type        = string
  description = "Cleaned transactions bucket name"
}

variable "bdp_wallets_aggregations_bucket" {
  type        = string
  description = "Wallets aggregations bucket name"
}

variable "bdp_scaled_features_bucket" {
  type        = string
  description = "Scaled features bucket name"
}

variable "bdp_unscaled_features_bucket" {
  type        = string
  description = "Unscaled features bucket name"
}

variable "glue_role_arn" {
  type        = string
  description = "ARN of IAM role for Glue"
}