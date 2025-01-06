from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
    DoubleType,
    BooleanType,
    ArrayType,
    FloatType
)

eth_schema = StructType(
    [
        StructField("gas", LongType(), True),
        StructField("hash", StringType(), True),
        StructField("input", StringType(), True),
        StructField("nonce", LongType(), True),
        StructField("value", DoubleType(), True),
        StructField("block_number", LongType(), True),
        StructField("block_hash", StringType(), True),
        StructField("transaction_index", LongType(), True),
        StructField("from_address", StringType(), True),
        StructField("to_address", StringType(), True),
        StructField("gas_price", LongType(), True),
        StructField("receipt_cumulative_gas_used", LongType(), True),
        StructField("receipt_gas_used", LongType(), True),
        StructField("receipt_contract_address", StringType(), True),
        StructField("receipt_status", LongType(), True),
        StructField("receipt_effective_gas_price", LongType(), True),
        StructField("transaction_type", LongType(), True),
        StructField("max_fee_per_gas", LongType(), True),
        StructField("max_priority_fee_per_gas", LongType(), True),
        StructField("block_timestamp", TimestampType(), True),
        StructField("date", StringType(), True),
        StructField("last_modified", TimestampType(), True),
    ]
)

btc_inputs_column_schema = ArrayType(StructType(
    [
        StructField("index", LongType(), False),
        StructField("spent_transaction_hash", StringType(), True),
        StructField("spent_output_index", LongType(), True),
        StructField("script_asm", StringType(), True),
        StructField("script_hex", StringType(), True),
        StructField("sequence", LongType(), True),
        StructField("required_signatures", LongType(), True),
        StructField("type", StringType(), True),
        StructField("address", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("txinwitness", ArrayType(StringType()), True),
    ]
))

btc_outputs_column_schema = ArrayType(StructType(
    [
        StructField("index", LongType(), False),
        StructField("script_asm", StringType(), True),
        StructField("script_hex", StringType(), True),
        StructField("required_signatures", LongType(), True),
        StructField("type", StringType(), True),
        StructField("address", StringType(), True),
        StructField("value", DoubleType(), True),
    ]
))

btc_schema = StructType(
    [
        StructField("index", LongType(), False),
        StructField("hash", StringType(), False),
        StructField("size", LongType(), True),
        StructField("virtual_size", LongType(), True),
        StructField("version", LongType(), True),
        StructField("lock_time", LongType(), True),
        StructField("block_hash", StringType(), False),
        StructField("block_number", LongType(), False),
        StructField("block_timestamp", TimestampType(), False),
        StructField("input_count", LongType(), True),
        StructField("output_count", LongType(), True),
        StructField("input_value", DoubleType(), True),
        StructField("output_value", DoubleType(), True),
        StructField("is_coinbase", BooleanType(), True),
        StructField("fee", DoubleType(), True),
        StructField("last_modified", TimestampType(), True),
        StructField("date", StringType(), True),
        StructField("inputs", btc_inputs_column_schema, True),
        StructField("outputs", btc_outputs_column_schema, True),
    ]
)

transaction_schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("block_timestamp", TimestampType(), False),
        StructField("block_number", LongType(), False),
        StructField("transaction_hash", StringType(), False),
        StructField("transaction_index", LongType(), False),
        StructField("fee", DoubleType(), True),
        StructField("sender_address", StringType(), True),
        StructField("receiver_address", StringType(), True),
        StructField("total_transferred_value", DoubleType(), True),
        StructField("total_input_value", DoubleType(), True),
        StructField("sent_value", DoubleType(), True),
        StructField("received_value", DoubleType(), True),
        StructField("network_name", StringType(), True),
    ]
)

aggregations_schema = StructType(
    [
        StructField("address", StringType(), False),
        StructField("network_name", StringType(), False),
        
        StructField("avg_sent_value", DoubleType(), False),
        StructField("avg_received_value", DoubleType(), False),
        StructField("avg_total_value_for_sender", DoubleType(), False),
        StructField("avg_total_value_for_receiver", DoubleType(), False),

        StructField("sum_sent_value", DoubleType(), False),
        StructField("sum_received_value", DoubleType(), False),
        StructField("sum_total_value_for_sender", DoubleType(), False),
        StructField("sum_total_value_for_receiver", DoubleType(), False),

        StructField("min_sent_value", DoubleType(), False),
        StructField("min_received_value", DoubleType(), False),
        StructField("min_total_value_for_sender", DoubleType(), False),
        StructField("min_total_value_for_receiver", DoubleType(), False),

        StructField("max_sent_value", DoubleType(), False),
        StructField("max_received_value", DoubleType(), False),
        StructField("max_total_value_for_sender", DoubleType(), False),
        StructField("max_total_value_for_receiver", DoubleType(), False),

        StructField("median_sent_value", DoubleType(), False),
        StructField("median_received_value", DoubleType(), False),
        StructField("median_total_value_for_sender", DoubleType(), False),
        StructField("median_total_value_for_receiver", DoubleType(), False),

        StructField("mode_sent_value", DoubleType(), False),
        StructField("mode_received_value", DoubleType(), False),
        StructField("mode_total_value_for_sender", DoubleType(), False),
        StructField("mode_total_value_for_receiver", DoubleType(), False),

        StructField("stddev_sent_value", DoubleType(), False),
        StructField("stddev_received_value", DoubleType(), False),
        StructField("stddev_total_value_for_sender", DoubleType(), False),
        StructField("stddev_total_value_for_receiver", DoubleType(), False),

        StructField("num_sent_transactions", LongType(), False),
        StructField("num_received_transactions", LongType(), False),

        StructField("avg_time_between_sent_transactions", DoubleType(), False),
        StructField("avg_time_between_received_transactions", DoubleType(), False),

        StructField("avg_outgoing_speed_count", DoubleType(), False),
        StructField("avg_incoming_speed_count", DoubleType(), False),
        StructField("avg_outgoing_speed_value", DoubleType(), False),
        StructField("avg_incoming_speed_value", DoubleType(), False),

        StructField("avg_outgoing_acceleration_count", DoubleType(), False),
        StructField("avg_incoming_acceleration_count", DoubleType(), False),
        StructField("avg_outgoing_acceleration_value", DoubleType(), False),
        StructField("avg_incoming_acceleration_value", DoubleType(), False),

        StructField("avg_fee_paid", DoubleType(), False),
        StructField("total_fee_paid", DoubleType(), False),
        StructField("min_fee_paid", DoubleType(), False),
        StructField("max_fee_paid", DoubleType(), False),

        StructField("activity_duration", LongType(), False),
        StructField("first_transaction_timestamp", TimestampType(), False),
        StructField("last_transaction_timestamp", TimestampType(), False),

        StructField("unique_out_degree", LongType(), False),
        StructField("unique_in_degree", LongType(), False)
    ]
)

transaction_scaled_schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("block_timestamp", FloatType(), False),
        StructField("block_number", FloatType(), False),
        StructField("transaction_hash", StringType(), False),
        StructField("transaction_index", FloatType(), False),
        StructField("fee", FloatType(), True),
        StructField("sender_address", StringType(), True),
        StructField("receiver_address", StringType(), True),
        StructField("total_transferred_value", FloatType(), True),
        StructField("total_input_value", FloatType(), True),
        StructField("sent_value", FloatType(), True),
        StructField("received_value", FloatType(), True),
        StructField("network_name", StringType(), True),
    ]
)

aggregations_scaled_schema = StructType(
    [
        StructField("address", StringType(), False),
        StructField("network_name", StringType(), False),
        
        StructField("avg_sent_value", FloatType(), False),
        StructField("avg_received_value", FloatType(), False),
        StructField("avg_total_value_for_sender", FloatType(), False),
        StructField("avg_total_value_for_receiver", FloatType(), False),

        StructField("sum_sent_value", FloatType(), False),
        StructField("sum_received_value", FloatType(), False),
        StructField("sum_total_value_for_sender", FloatType(), False),
        StructField("sum_total_value_for_receiver", FloatType(), False),

        StructField("min_sent_value", FloatType(), False),
        StructField("min_received_value", FloatType(), False),
        StructField("min_total_value_for_sender", FloatType(), False),
        StructField("min_total_value_for_receiver", FloatType(), False),

        StructField("max_sent_value", FloatType(), False),
        StructField("max_received_value", FloatType(), False),
        StructField("max_total_value_for_sender", FloatType(), False),
        StructField("max_total_value_for_receiver", FloatType(), False),

        StructField("median_sent_value", FloatType(), False),
        StructField("median_received_value", FloatType(), False),
        StructField("median_total_value_for_sender", FloatType(), False),
        StructField("median_total_value_for_receiver", FloatType(), False),

        StructField("mode_sent_value", FloatType(), False),
        StructField("mode_received_value", FloatType(), False),
        StructField("mode_total_value_for_sender", FloatType(), False),
        StructField("mode_total_value_for_receiver", FloatType(), False),

        StructField("stddev_sent_value", FloatType(), False),
        StructField("stddev_received_value", FloatType(), False),
        StructField("stddev_total_value_for_sender", FloatType(), False),
        StructField("stddev_total_value_for_receiver", FloatType(), False),

        StructField("num_sent_transactions", FloatType(), False),
        StructField("num_received_transactions", FloatType(), False),

        StructField("avg_time_between_sent_transactions", FloatType(), False),
        StructField("avg_time_between_received_transactions", FloatType(), False),

        StructField("avg_outgoing_speed_count", FloatType(), False),
        StructField("avg_incoming_speed_count", FloatType(), False),
        StructField("avg_outgoing_speed_value", FloatType(), False),
        StructField("avg_incoming_speed_value", FloatType(), False),

        StructField("avg_outgoing_acceleration_count", FloatType(), False),
        StructField("avg_incoming_acceleration_count", FloatType(), False),
        StructField("avg_outgoing_acceleration_value", FloatType(), False),
        StructField("avg_incoming_acceleration_value", FloatType(), False),

        StructField("avg_fee_paid", FloatType(), False),
        StructField("total_fee_paid", FloatType(), False),
        StructField("min_fee_paid", FloatType(), False),
        StructField("max_fee_paid", FloatType(), False),

        StructField("activity_duration", FloatType(), False),
        StructField("first_transaction_timestamp", FloatType(), False),
        StructField("last_transaction_timestamp", FloatType(), False),

        StructField("unique_out_degree", FloatType(), False),
        StructField("unique_in_degree", FloatType(), False)
    ]
)

joined_scaled_schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("block_timestamp", FloatType(), False),
        StructField("block_number", FloatType(), False),
        StructField("transaction_hash", StringType(), False),
        StructField("transaction_index", FloatType(), False),
        StructField("fee", FloatType(), True),
        StructField("sender_address", StringType(), True),
        StructField("receiver_address", StringType(), True),
        StructField("total_transferred_value", FloatType(), True),
        StructField("total_input_value", FloatType(), True),
        StructField("sent_value", FloatType(), True),
        StructField("received_value", FloatType(), True),
        StructField("network_name", StringType(), True),
        
        StructField("avg_sent_value", FloatType(), False),
        StructField("avg_received_value", FloatType(), False),
        StructField("avg_total_value_for_sender", FloatType(), False),
        StructField("avg_total_value_for_receiver", FloatType(), False),

        StructField("sum_sent_value", FloatType(), False),
        StructField("sum_received_value", FloatType(), False),
        StructField("sum_total_value_for_sender", FloatType(), False),
        StructField("sum_total_value_for_receiver", FloatType(), False),

        StructField("min_sent_value", FloatType(), False),
        StructField("min_received_value", FloatType(), False),
        StructField("min_total_value_for_sender", FloatType(), False),
        StructField("min_total_value_for_receiver", FloatType(), False),

        StructField("max_sent_value", FloatType(), False),
        StructField("max_received_value", FloatType(), False),
        StructField("max_total_value_for_sender", FloatType(), False),
        StructField("max_total_value_for_receiver", FloatType(), False),

        StructField("median_sent_value", FloatType(), False),
        StructField("median_received_value", FloatType(), False),
        StructField("median_total_value_for_sender", FloatType(), False),
        StructField("median_total_value_for_receiver", FloatType(), False),

        StructField("mode_sent_value", FloatType(), False),
        StructField("mode_received_value", FloatType(), False),
        StructField("mode_total_value_for_sender", FloatType(), False),
        StructField("mode_total_value_for_receiver", FloatType(), False),

        StructField("stddev_sent_value", FloatType(), False),
        StructField("stddev_received_value", FloatType(), False),
        StructField("stddev_total_value_for_sender", FloatType(), False),
        StructField("stddev_total_value_for_receiver", FloatType(), False),

        StructField("num_sent_transactions", FloatType(), False),
        StructField("num_received_transactions", FloatType(), False),

        StructField("avg_time_between_sent_transactions", FloatType(), False),
        StructField("avg_time_between_received_transactions", FloatType(), False),

        StructField("avg_outgoing_speed_count", FloatType(), False),
        StructField("avg_incoming_speed_count", FloatType(), False),
        StructField("avg_outgoing_speed_value", FloatType(), False),
        StructField("avg_incoming_speed_value", FloatType(), False),

        StructField("avg_outgoing_acceleration_count", FloatType(), False),
        StructField("avg_incoming_acceleration_count", FloatType(), False),
        StructField("avg_outgoing_acceleration_value", FloatType(), False),
        StructField("avg_incoming_acceleration_value", FloatType(), False),

        StructField("avg_fee_paid", FloatType(), False),
        StructField("total_fee_paid", FloatType(), False),
        StructField("min_fee_paid", FloatType(), False),
        StructField("max_fee_paid", FloatType(), False),

        StructField("activity_duration_for_sender", FloatType(), False),
        StructField("first_transaction_timestamp_for_sender", FloatType(), False),
        StructField("last_transaction_timestamp_for_sender", FloatType(), False),
        
        StructField("activity_duration_for_receiver", FloatType(), False),
        StructField("first_transaction_timestamp_for_receiver", FloatType(), False),
        StructField("last_transaction_timestamp_for_receiver", FloatType(), False),

        StructField("unique_out_degree", FloatType(), False),
        StructField("unique_in_degree", FloatType(), False)
    ]
)

features_schema_scaled = StructType(
    [
        StructField("block_timestamp", FloatType(), False),
        StructField("block_number", FloatType(), False),
        #StructField("transaction_hash", LongType(), False),
        StructField("transaction_index", FloatType(), False),
        StructField("fee", FloatType(), True),
        #StructField("sender_address", LongType(), True),
       # StructField("receiver_address", LongType(), True),
        StructField("total_transferred_value", FloatType(), True),
        StructField("total_input_value", FloatType(), True),
        StructField("sent_value", FloatType(), True),
        StructField("received_value", FloatType(), True),
        StructField("network_name", BooleanType(), True),
        
        StructField("avg_sent_value", FloatType(), False),
        StructField("avg_received_value", FloatType(), False),
        StructField("avg_total_value_for_sender", FloatType(), False),
        StructField("avg_total_value_for_receiver", FloatType(), False),

        StructField("sum_sent_value", FloatType(), False),
        StructField("sum_received_value", FloatType(), False),
        StructField("sum_total_value_for_sender", FloatType(), False),
        StructField("sum_total_value_for_receiver", FloatType(), False),

        StructField("min_sent_value", FloatType(), False),
        StructField("min_received_value", FloatType(), False),
        StructField("min_total_value_for_sender", FloatType(), False),
        StructField("min_total_value_for_receiver", FloatType(), False),

        StructField("max_sent_value", FloatType(), False),
        StructField("max_received_value", FloatType(), False),
        StructField("max_total_value_for_sender", FloatType(), False),
        StructField("max_total_value_for_receiver", FloatType(), False),

        StructField("median_sent_value", FloatType(), False),
        StructField("median_received_value", FloatType(), False),
        StructField("median_total_value_for_sender", FloatType(), False),
        StructField("median_total_value_for_receiver", FloatType(), False),

        StructField("mode_sent_value", FloatType(), False),
        StructField("mode_received_value", FloatType(), False),
        StructField("mode_total_value_for_sender", FloatType(), False),
        StructField("mode_total_value_for_receiver", FloatType(), False),

        StructField("stddev_sent_value", FloatType(), False),
        StructField("stddev_received_value", FloatType(), False),
        StructField("stddev_total_value_for_sender", FloatType(), False),
        StructField("stddev_total_value_for_receiver", FloatType(), False),

        StructField("num_sent_transactions", FloatType(), False),
        StructField("num_received_transactions", FloatType(), False),

        StructField("avg_time_between_sent_transactions", FloatType(), False),
        StructField("avg_time_between_received_transactions", FloatType(), False),

        StructField("avg_outgoing_speed_count", FloatType(), False),
        StructField("avg_incoming_speed_count", FloatType(), False),
        StructField("avg_outgoing_speed_value", FloatType(), False),
        StructField("avg_incoming_speed_value", FloatType(), False),

        StructField("avg_outgoing_acceleration_count", FloatType(), False),
        StructField("avg_incoming_acceleration_count", FloatType(), False),
        StructField("avg_outgoing_acceleration_value", FloatType(), False),
        StructField("avg_incoming_acceleration_value", FloatType(), False),

        StructField("avg_fee_paid", FloatType(), False),
        StructField("total_fee_paid", FloatType(), False),
        StructField("min_fee_paid", FloatType(), False),
        StructField("max_fee_paid", FloatType(), False),

        StructField("activity_duration_for_sender", FloatType(), False),
        StructField("first_transaction_timestamp_for_sender", FloatType(), False),
        StructField("last_transaction_timestamp_for_sender", FloatType(), False),
        
        StructField("activity_duration_for_receiver", FloatType(), False),
        StructField("first_transaction_timestamp_for_receiver", FloatType(), False),
        StructField("last_transaction_timestamp_for_receiver", FloatType(), False),

        StructField("unique_out_degree", FloatType(), False),
        StructField("unique_in_degree", FloatType(), False)
    ]
)