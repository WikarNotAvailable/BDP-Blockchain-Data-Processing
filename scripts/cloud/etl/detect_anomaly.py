import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, when
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'QUANTILE'])
           
input_schema = StructType(
    [
        StructField("transaction_hash", StringType(), True),
        StructField("sender_address", StringType(), True),
        StructField("receiver_address", StringType(), True),
        StructField("block_timestamp_unscaled", TimestampType(), True),
        StructField("network_name", DoubleType(), True),
        StructField("cluster_id", DoubleType(), True),
        StructField("distance", DoubleType(), True),
    ]
)

columns_to_drop = ["cluster_id", "distance"]

def validate_params(quantile):
    if not quantile:
        print("ERROR: parameter quantile required")
        sys.exit(1)
        
def setup_blockchain_db(spark):
    spark.sql("""
    CREATE DATABASE IF NOT EXISTS bdp
    """)

def setup_iceberg_table(spark):
    spark.sql("""      
    CREATE TABLE IF NOT EXISTS glue_catalog.bdp.anomaly_detection (
    transaction_hash STRING,
    sender_address STRING,
    receiver_address STRING,
    block_timestamp_unscaled TIMESTAMP,
    network_name STRING,
    is_anomaly BOOLEAN
    )
    PARTITIONED BY (network_name, day(block_timestamp_unscaled))
    LOCATION 's3://bdp-anomaly-detection'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd'
    )
    """)
 
spark = (
    SparkSession.builder.appName("DataAggregations")    
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.parquet.mergeSchema", "true") # No need as we explicitly specify the schema
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://bdp-wallets-aggregations/") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.glue_catalog.glue.id", "982534349340") \
    .config("spark.sql.adaptive.enabled", "true") # Keep partitions in simmilar size
    .getOrCreate()
)
    
quantile = float(args["QUANTILE"])
validate_params(quantile)

glueContext = GlueContext(spark)
job = Job(glueContext)

job.init(args['JOB_NAME'], args)

df = spark.read.option("header", "false").schema(input_schema).csv("s3://bdp-inference-results/kmeans/part-00000-609399e4-4d11-42c0-94eb-3d82fbc5d896-c000.csv.out")

setup_blockchain_db(spark)
setup_iceberg_table(spark)
spark.sql("TRUNCATE TABLE glue_catalog.bdp.anomaly_detection")

try:
    threshold = df.approxQuantile("distance", [quantile], 0.001)[0] 
except:
    quantile = 0.673
    threshold = df.approxQuantile("distance", [quantile], 0.001)[0] 
    
df = df.withColumn("is_anomaly", when(col("distance") > threshold, True).otherwise(False))

df = df.drop(*columns_to_drop)
df = df.withColumn("network_name", when(col("network_name") == False, "ethereum").otherwise("bitcoin"))

glueContext.write_data_frame.from_catalog(
    frame=df,
    database="bdp",
    table_name="anomaly_detection",
    additional_options = {
        "useCatalogSchema": True,
        "useSparkDataSource": True
    }
)

job.commit()