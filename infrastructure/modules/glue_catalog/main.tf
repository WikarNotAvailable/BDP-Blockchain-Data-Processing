resource "aws_glue_catalog_database" "bdp_db" {
  name = "bdp"
}

/*resource "aws_glue_catalog_table" "cleaned_transactions" {
  database_name = aws_glue_catalog_database.bdp_db.name
  name          = "cleaned_transactions"
  table_type    = "EXTERNAL_TABLE"

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
    }
  }
  //Commented because https://github.com/hashicorp/terraform-provider-aws/issues/36531
  partition_keys {
    name = "network_name"
    type = "string"
  }

  parameters = {
    "write.format.default"            = "parquet"
    "write.parquet.compression-codec" = "zstd"
  }

  storage_descriptor {
    location      = "s3://${var.bdp_cleaned_transactions_bucket}"
    input_format  = "org.apache.hadoop.mapred.FileInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed    = true

    ser_de_info {
      name                  = "cleaned_transactions_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }


    columns {
      name = "transaction_id"
      type = "string"
    }
    columns {
      name = "block_timestamp"
      type = "timestamp"
    }
    columns {
      name = "block_number"
      type = "bigint"
    }
    columns {
      name = "transaction_hash"
      type = "string"
    }
    columns {
      name = "transaction_index"
      type = "bigint"
    }
    columns {
      name = "fee"
      type = "double"
    }
    columns {
      name = "sender_address"
      type = "string"
    }
    columns {
      name = "receiver_address"
      type = "string"
    }
    columns {
      name = "total_transferred_value"
      type = "double"
    }
    columns {
      name = "total_input_value"
      type = "double"
    }
    columns {
      name = "sent_value"
      type = "double"
    }
    columns {
      name = "received_value"
      type = "double"
    }
    columns {
      name = "network_name"
      type = "string"
    }
  }
}*/

resource "aws_glue_catalog_table_optimizer" "cleaned_transactions_orphan_files_deletion_optimizer" {
  catalog_id    = "982534349340"
  database_name = aws_glue_catalog_database.bdp_db.name
  table_name    = "cleaned_transactions"
  type          = "orphan_file_deletion"

  configuration {
    role_arn = var.glue_role_arn
    enabled  = true

    orphan_file_deletion_configuration {
      iceberg_configuration {
        orphan_file_retention_period_in_days = 2
        location                             = "s3://${var.bdp_cleaned_transactions_bucket}"
      }
    }
  }
}

resource "aws_glue_catalog_table_optimizer" "cleaned_transactions_compaction_optimizer" {
  catalog_id    = "982534349340"
  database_name = aws_glue_catalog_database.bdp_db.name
  table_name    = "cleaned_transactions"
  type          = "compaction"

  configuration {
    role_arn = var.glue_role_arn
    enabled  = true
  }
}

/*resource "aws_glue_catalog_table" "wallets_aggregations" {
  database_name = aws_glue_catalog_database.bdp_db.name
  name          = "wallets_aggregations"

  table_type = "EXTERNAL_TABLE"
  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
    }
  }

  //Commented because https://github.com/hashicorp/terraform-provider-aws/issues/36531
  partition_keys {
    name = "network_name"
    type = "string"
  }

  parameters = {
    "write.format.default"            = "parquet",
    "write.parquet.compression-codec" = "zstd",
    "write.bucketed-columns"          = "address",
    "write.num-buckets"               = "2048"
  }

  storage_descriptor {
    location      = "s3://${var.bdp_wallets_aggregations_bucket}"
    input_format  = "org.apache.hadoop.mapred.FileInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed    = true

    ser_de_info {
      name                  = "wallets_aggregations_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "address"
      type = "string"
    }
    columns {
      name = "network_name"
      type = "string"
    }
    columns {
      name = "avg_sent_value"
      type = "double"
    }
    columns {
      name = "avg_received_value"
      type = "double"
    }
    columns {
      name = "avg_total_value_for_sender"
      type = "double"
    }
    columns {
      name = "avg_total_value_for_receiver"
      type = "double"
    }
    columns {
      name = "sum_sent_value"
      type = "double"
    }
    columns {
      name = "sum_received_value"
      type = "double"
    }
    columns {
      name = "sum_total_value_for_sender"
      type = "double"
    }
    columns {
      name = "sum_total_value_for_receiver"
      type = "double"
    }
    columns {
      name = "min_sent_value"
      type = "double"
    }
    columns {
      name = "min_received_value"
      type = "double"
    }
    columns {
      name = "min_total_value_for_sender"
      type = "double"
    }
    columns {
      name = "min_total_value_for_receiver"
      type = "double"
    }
    columns {
      name = "max_sent_value"
      type = "double"
    }
    columns {
      name = "max_received_value"
      type = "double"
    }
    columns {
      name = "max_total_value_for_sender"
      type = "double"
    }
    columns {
      name = "max_total_value_for_receiver"
      type = "double"
    }
    columns {
      name = "median_sent_value"
      type = "double"
    }
    columns {
      name = "median_received_value"
      type = "double"
    }
    columns {
      name = "median_total_value_for_sender"
      type = "double"
    }
    columns {
      name = "median_total_value_for_receiver"
      type = "double"
    }
    columns {
      name = "mode_sent_value"
      type = "double"
    }
    columns {
      name = "mode_received_value"
      type = "double"
    }
    columns {
      name = "mode_total_value_for_sender"
      type = "double"
    }
    columns {
      name = "mode_total_value_for_receiver"
      type = "double"
    }
    columns {
      name = "stddev_sent_value"
      type = "double"
    }
    columns {
      name = "stddev_received_value"
      type = "double"
    }
    columns {
      name = "stddev_total_value_for_sender"
      type = "double"
    }
    columns {
      name = "stddev_total_value_for_receiver"
      type = "double"
    }
    columns {
      name = "num_sent_transactions"
      type = "bigint"
    }
    columns {
      name = "num_received_transactions"
      type = "bigint"
    }
    columns {
      name = "avg_time_between_sent_transactions"
      type = "double"
    }
    columns {
      name = "avg_time_between_received_transactions"
      type = "double"
    }
    columns {
      name = "avg_outgoing_speed_count"
      type = "double"
    }
    columns {
      name = "avg_incoming_speed_count"
      type = "double"
    }
    columns {
      name = "avg_outgoing_speed_value"
      type = "double"
    }
    columns {
      name = "avg_incoming_speed_value"
      type = "double"
    }
    columns {
      name = "avg_outgoing_acceleration_count"
      type = "double"
    }
    columns {
      name = "avg_incoming_acceleration_count"
      type = "double"
    }
    columns {
      name = "avg_outgoing_acceleration_value"
      type = "double"
    }
    columns {
      name = "avg_incoming_acceleration_value"
      type = "double"
    }
    columns {
      name = "avg_fee_paid"
      type = "double"
    }
    columns {
      name = "total_fee_paid"
      type = "double"
    }
    columns {
      name = "min_fee_paid"
      type = "double"
    }
    columns {
      name = "max_fee_paid"
      type = "double"
    }
    columns {
      name = "activity_duration"
      type = "bigint"
    }
    columns {
      name = "first_transaction_timestamp"
      type = "timestamp"
    }
    columns {
      name = "last_transaction_timestamp"
      type = "timestamp"
    }
    columns {
      name = "unique_out_degree"
      type = "bigint"
    }
    columns {
      name = "unique_in_degree"
      type = "bigint"
    }

  }
}*/

resource "aws_glue_catalog_table_optimizer" "wallets_aggregations_orphan_files_deletion_optimizer" {
  catalog_id    = "982534349340"
  database_name = aws_glue_catalog_database.bdp_db.name
  table_name    = "wallets_aggregations"
  type          = "orphan_file_deletion"

  configuration {
    role_arn = var.glue_role_arn
    enabled  = true

    orphan_file_deletion_configuration {
      iceberg_configuration {
        orphan_file_retention_period_in_days = 2
        location                             = "s3://${var.bdp_wallets_aggregations_bucket}"
      }
    }
  }
}

resource "aws_glue_catalog_table_optimizer" "wallets_aggregations_compaction_optimizer" {
  catalog_id    = "982534349340"
  database_name = aws_glue_catalog_database.bdp_db.name
  table_name    = "wallets_aggregations"
  type          = "compaction"

  configuration {
    role_arn = var.glue_role_arn
    enabled  = true
  }
}

/*resource "aws_glue_catalog_table" "scaled_features" {
  database_name = aws_glue_catalog_database.bdp_db.name
  name          = "scaled_features"

  table_type = "EXTERNAL_TABLE"
  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
    }
  }

  //Commented because https://github.com/hashicorp/terraform-provider-aws/issues/36531
  partition_keys {
    name = "network_name"
    type = "boolean"
  }

  parameters = {
    "write.format.default"            = "parquet",
    "write.parquet.compression-codec" = "zstd"
  }

  storage_descriptor {
    location      = "s3://${var.bdp_scaled_features_bucket}"
    input_format  = "org.apache.hadoop.mapred.FileInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed    = true

    ser_de_info {
      name                  = "features_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "block_timestamp"
      type = "float"
    }
    columns {
      name = "block_number"
      type = "float"
    }
    columns {
      name = "transaction_index"
      type = "float"
    }
    columns {
      name = "fee"
      type = "float"
    }
    columns {
      name = "total_transferred_value"
      type = "float"
    }
    columns {
      name = "total_input_value"
      type = "float"
    }
    columns {
      name = "sent_value"
      type = "float"
    }
    columns {
      name = "received_value"
      type = "float"
    }
    columns {
      name = "network_name"
      type = "boolean"
    }
    columns {
      name = "avg_sent_value"
      type = "float"
    }
    columns {
      name = "avg_received_value"
      type = "float"
    }
    columns {
      name = "avg_total_value_for_sender"
      type = "float"
    }
    columns {
      name = "avg_total_value_for_receiver"
      type = "float"
    }
    columns {
      name = "sum_sent_value"
      type = "float"
    }
    columns {
      name = "sum_received_value"
      type = "float"
    }
    columns {
      name = "sum_total_value_for_sender"
      type = "float"
    }
    columns {
      name = "sum_total_value_for_receiver"
      type = "float"
    }
    columns {
      name = "min_sent_value"
      type = "float"
    }
    columns {
      name = "min_received_value"
      type = "float"
    }
    columns {
      name = "min_total_value_for_sender"
      type = "float"
    }
    columns {
      name = "min_total_value_for_receiver"
      type = "float"
    }
    columns {
      name = "max_sent_value"
      type = "float"
    }
    columns {
      name = "max_received_value"
      type = "float"
    }
    columns {
      name = "max_total_value_for_sender"
      type = "float"
    }
    columns {
      name = "max_total_value_for_receiver"
      type = "float"
    }
    columns {
      name = "median_sent_value"
      type = "float"
    }
    columns {
      name = "median_received_value"
      type = "float"
    }
    columns {
      name = "median_total_value_for_sender"
      type = "float"
    }
    columns {
      name = "median_total_value_for_receiver"
      type = "float"
    }
    columns {
      name = "mode_sent_value"
      type = "float"
    }
    columns {
      name = "mode_received_value"
      type = "float"
    }
    columns {
      name = "mode_total_value_for_sender"
      type = "float"
    }
    columns {
      name = "mode_total_value_for_receiver"
      type = "float"
    }
    columns {
      name = "stddev_sent_value"
      type = "float"
    }
    columns {
      name = "stddev_received_value"
      type = "float"
    }
    columns {
      name = "stddev_total_value_for_sender"
      type = "float"
    }
    columns {
      name = "stddev_total_value_for_receiver"
      type = "float"
    }
    columns {
      name = "num_sent_transactions"
      type = "float"
    }
    columns {
      name = "num_received_transactions"
      type = "float"
    }
    columns {
      name = "avg_time_between_sent_transactions"
      type = "float"
    }
    columns {
      name = "avg_time_between_received_transactions"
      type = "float"
    }
    columns {
      name = "avg_outgoing_speed_count"
      type = "float"
    }
    columns {
      name = "avg_incoming_speed_count"
      type = "float"
    }
    columns {
      name = "avg_outgoing_speed_value"
      type = "float"
    }
    columns {
      name = "avg_incoming_speed_value"
      type = "float"
    }
    columns {
      name = "avg_outgoing_acceleration_count"
      type = "float"
    }
    columns {
      name = "avg_incoming_acceleration_count"
      type = "float"
    }
    columns {
      name = "avg_outgoing_acceleration_value"
      type = "float"
    }
    columns {
      name = "avg_incoming_acceleration_value"
      type = "float"
    }
    columns {
      name = "avg_fee_paid"
      type = "float"
    }
    columns {
      name = "total_fee_paid"
      type = "float"
    }
    columns {
      name = "min_fee_paid"
      type = "float"
    }
    columns {
      name = "max_fee_paid"
      type = "float"
    }
    columns {
      name = "activity_duration_for_sender"
      type = "float"
    }
    columns {
      name = "first_transaction_timestamp_for_sender"
      type = "float"
    }
    columns {
      name = "last_transaction_timestamp_for_sender"
      type = "float"
    }
    columns {
      name = "activity_duration_for_receiver"
      type = "float"
    }
    columns {
      name = "first_transaction_timestamp_for_receiver"
      type = "float"
    }
    columns {
      name = "last_transaction_timestamp_for_receiver"
      type = "float"
    }
    columns {
      name = "unique_out_degree"
      type = "float"
    }
    columns {
      name = "unique_in_degree"
      type = "float"
    }

  }
}*/


resource "aws_glue_catalog_table_optimizer" "scaled_features_orphan_files_deletion_optimizer" {
  catalog_id    = "982534349340"
  database_name = aws_glue_catalog_database.bdp_db.name
  table_name    = "scaled_features"
  type          = "orphan_file_deletion"

  configuration {
    role_arn = var.glue_role_arn
    enabled  = true

    orphan_file_deletion_configuration {
      iceberg_configuration {
        orphan_file_retention_period_in_days = 2
        location                             = "s3://${var.bdp_scaled_features_bucket}"
      }
    }
  }
}

resource "aws_glue_catalog_table_optimizer" "scaled_features_compaction_optimizer" {
  catalog_id    = "982534349340"
  database_name = aws_glue_catalog_database.bdp_db.name
  table_name    = "scaled_features"
  type          = "compaction"

  configuration {
    role_arn = var.glue_role_arn
    enabled  = true
  }
}

/*resource "aws_glue_catalog_table" "unscaled_features" {
  database_name = aws_glue_catalog_database.bdp_db.name
  name          = "unscaled_features"

  table_type = "EXTERNAL_TABLE"
  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
    }
  }

  //Commented because https://github.com/hashicorp/terraform-provider-aws/issues/36531
  partition_keys {
    name = "network_name"
    type = "boolean"
  }

  parameters = {
    "write.format.default"            = "parquet",
    "write.parquet.compression-codec" = "zstd"
  }

  storage_descriptor {
    location      = "s3://${var.bdp_unscaled_features_bucket}"
    input_format  = "org.apache.hadoop.mapred.FileInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed    = true

    ser_de_info {
      name                  = "features_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "block_timestamp"
      type = "bigint"
    }
    columns {
      name = "block_number"
      type = "bigint"
    }
    columns {
      name = "transaction_index"
      type = "bigint"
    }
    columns {
      name = "fee"
      type = "float"
    }
    columns {
      name = "total_transferred_value"
      type = "float"
    }
    columns {
      name = "total_input_value"
      type = "float"
    }
    columns {
      name = "sent_value"
      type = "float"
    }
    columns {
      name = "received_value"
      type = "float"
    }
    columns {
      name = "network_name"
      type = "boolean"
    }
    columns {
      name = "avg_sent_value"
      type = "float"
    }
    columns {
      name = "avg_received_value"
      type = "float"
    }
    columns {
      name = "avg_total_value_for_sender"
      type = "float"
    }
    columns {
      name = "avg_total_value_for_receiver"
      type = "float"
    }
    columns {
      name = "sum_sent_value"
      type = "float"
    }
    columns {
      name = "sum_received_value"
      type = "float"
    }
    columns {
      name = "sum_total_value_for_sender"
      type = "float"
    }
    columns {
      name = "sum_total_value_for_receiver"
      type = "float"
    }
    columns {
      name = "min_sent_value"
      type = "float"
    }
    columns {
      name = "min_received_value"
      type = "float"
    }
    columns {
      name = "min_total_value_for_sender"
      type = "float"
    }
    columns {
      name = "min_total_value_for_receiver"
      type = "float"
    }
    columns {
      name = "max_sent_value"
      type = "float"
    }
    columns {
      name = "max_received_value"
      type = "float"
    }
    columns {
      name = "max_total_value_for_sender"
      type = "float"
    }
    columns {
      name = "max_total_value_for_receiver"
      type = "float"
    }
    columns {
      name = "median_sent_value"
      type = "float"
    }
    columns {
      name = "median_received_value"
      type = "float"
    }
    columns {
      name = "median_total_value_for_sender"
      type = "float"
    }
    columns {
      name = "median_total_value_for_receiver"
      type = "float"
    }
    columns {
      name = "mode_sent_value"
      type = "float"
    }
    columns {
      name = "mode_received_value"
      type = "float"
    }
    columns {
      name = "mode_total_value_for_sender"
      type = "float"
    }
    columns {
      name = "mode_total_value_for_receiver"
      type = "float"
    }
    columns {
      name = "stddev_sent_value"
      type = "float"
    }
    columns {
      name = "stddev_received_value"
      type = "float"
    }
    columns {
      name = "stddev_total_value_for_sender"
      type = "float"
    }
    columns {
      name = "stddev_total_value_for_receiver"
      type = "float"
    }
    columns {
      name = "num_sent_transactions"
      type = "bigint"
    }
    columns {
      name = "num_received_transactions"
      type = "bigint"
    }
    columns {
      name = "avg_time_between_sent_transactions"
      type = "float"
    }
    columns {
      name = "avg_time_between_received_transactions"
      type = "float"
    }
    columns {
      name = "avg_outgoing_speed_count"
      type = "float"
    }
    columns {
      name = "avg_incoming_speed_count"
      type = "float"
    }
    columns {
      name = "avg_outgoing_speed_value"
      type = "float"
    }
    columns {
      name = "avg_incoming_speed_value"
      type = "float"
    }
    columns {
      name = "avg_outgoing_acceleration_count"
      type = "float"
    }
    columns {
      name = "avg_incoming_acceleration_count"
      type = "float"
    }
    columns {
      name = "avg_outgoing_acceleration_value"
      type = "float"
    }
    columns {
      name = "avg_incoming_acceleration_value"
      type = "float"
    }
    columns {
      name = "avg_fee_paid"
      type = "float"
    }
    columns {
      name = "total_fee_paid"
      type = "float"
    }
    columns {
      name = "min_fee_paid"
      type = "float"
    }
    columns {
      name = "max_fee_paid"
      type = "float"
    }
    columns {
      name = "activity_duration_for_sender"
      type = "bigint"
    }
    columns {
      name = "first_transaction_timestamp_for_sender"
      type = "bigint"
    }
    columns {
      name = "last_transaction_timestamp_for_sender"
      type = "bigint"
    }
    columns {
      name = "activity_duration_for_receiver"
      type = "bigint"
    }
    columns {
      name = "first_transaction_timestamp_for_receiver"
      type = "bigint"
    }
    columns {
      name = "last_transaction_timestamp_for_receiver"
      type = "bigint"
    }
    columns {
      name = "unique_out_degree"
      type = "bigint"
    }
    columns {
      name = "unique_in_degree"
      type = "bigint"
    }

  }
}*/

resource "aws_glue_catalog_table_optimizer" "unscaled_features_orphan_files_deletion_optimizer" {
  catalog_id    = "982534349340"
  database_name = aws_glue_catalog_database.bdp_db.name
  table_name    = "unscaled_features"
  type          = "orphan_file_deletion"

  configuration {
    role_arn = var.glue_role_arn
    enabled  = true

    orphan_file_deletion_configuration {
      iceberg_configuration {
        orphan_file_retention_period_in_days = 2
        location                             = "s3://${var.bdp_unscaled_features_bucket}"
      }
    }
  }
}

resource "aws_glue_catalog_table_optimizer" "unscaled_features_compaction_optimizer" {
  catalog_id    = "982534349340"
  database_name = aws_glue_catalog_database.bdp_db.name
  table_name    = "unscaled_features"
  type          = "compaction"

  configuration {
    role_arn = var.glue_role_arn
    enabled  = true
  }
}