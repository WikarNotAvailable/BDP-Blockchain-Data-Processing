locals {
  transactions_cleaning_arguments = {
    "--END_DATE"       = "2024-12-31"
    "--START_DATE"     = "2024-10-1"
    "--NETWORK_PREFIX" = "all"
  }

  iceberg_argument = {
    "--datalake-formats" = "iceberg"
  }

  converting_to_recordio_arguments = {
    "--extra-jars"                      = "s3://bdp-glue-scripts/sagemaker-spark_2.12-spark_3.3.0-1.4.6.dev0.jar"
    "--python-modules-installer-option" = "-r"
    "--additional-python-modules"       = "s3://bdp-glue-scripts/requirements.txt"
  }

  anomaly_classification_arguments = {
    "--QUANTILE" = 0.673
  }

}

resource "aws_glue_job" "transactions_cleaning" {
  name     = "Transactions cleaning"
  role_arn = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_script_bucket}/transactions_cleaning.py"
    python_version  = "3"
  }

  worker_type       = "G.2X"
  number_of_workers = 10
  glue_version      = "5.0"
  default_arguments = merge(var.default_arguments, local.iceberg_argument, local.transactions_cleaning_arguments)
  timeout           = 120
}

resource "aws_glue_job" "wallets_aggregations" {
  name     = "Wallets aggregations"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_script_bucket}/wallets_aggregations.py"
    python_version  = "3"
  }

  worker_type       = "G.2X"
  number_of_workers = 10
  glue_version      = "5.0"
  default_arguments = merge(var.default_arguments, local.iceberg_argument)
  timeout           = 120
}

resource "aws_glue_job" "feature_scaling" {
  name     = "Feature scaling"
  role_arn = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_script_bucket}/preprocessing.py"
    python_version  = "3"
  }

  worker_type       = "G.2X"
  number_of_workers = 10
  glue_version      = "5.0"
  default_arguments = merge(var.default_arguments, local.iceberg_argument)
  timeout           = 120
}

resource "aws_glue_job" "spearman_feature_selection" {
  name     = "Spearman feature selection"
  role_arn = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_script_bucket}/spearman.py"
    python_version  = "3"
  }

  worker_type       = "G.2X"
  number_of_workers = 10
  glue_version      = "5.0"
  default_arguments = var.default_arguments
  timeout           = 300
}

resource "aws_glue_job" "convert_parquet_to_csv" {
  name     = "Convert parquet to CSV"
  role_arn = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_script_bucket}/convert_features_to_csv.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 10
  glue_version      = "5.0"
  default_arguments = var.default_arguments
  timeout           = 480
}

resource "aws_glue_job" "convert_features_to_recordio" {
  name     = "Convert features to recordio"
  role_arn = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_script_bucket}/convert_features_to_recordio.py"
    python_version  = "3"
  }

  worker_type       = "G.2X"
  number_of_workers = 10
  glue_version      = "5.0"
  default_arguments = merge(var.default_arguments, local.converting_to_recordio_arguments)
  timeout           = 180
}

resource "aws_glue_job" "preprocessing_with_string_columns" {
  name     = "Preprocesssing with string columns"
  role_arn = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_script_bucket}/preprocessing_for_inference.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 10
  glue_version      = "5.0"
  default_arguments = var.default_arguments
  timeout           = 120
}

resource "aws_glue_job" "convert_parquet_to_csv_for_visualisation" {
  name     = "Convert parquet to csv for visualization"
  role_arn = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_script_bucket}/convert_features_to_csv_inference.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 10
  glue_version      = "5.0"
  default_arguments = var.default_arguments
  timeout           = 120
}

resource "aws_glue_job" "anomaly_classification" {
  name     = "Anomaly Classification"
  role_arn = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_script_bucket}/detect_anomaly.py"
    python_version  = "3"
  }

  worker_type       = "G.1X"
  number_of_workers = 10
  glue_version      = "5.0"
  default_arguments = merge(var.default_arguments, local.anomaly_classification_arguments)
  timeout           = 120
}