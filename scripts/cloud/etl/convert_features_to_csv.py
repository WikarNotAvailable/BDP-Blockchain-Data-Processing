import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import boto3


def get_selected_columns():
    bucket_name = "bdp-feature-selection"
    file_key = "data/selected_columns.txt"

    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read().decode('utf-8')
    return file_content.splitlines()


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = (
    SparkSession.builder
        .appName("DataAggregations")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.sql.parquet.mergeSchema", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://bdp-scaled-features/")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.glue.id", "982534349340")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
)

glueContext = GlueContext(spark)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


columns_to_select = get_selected_columns()

features_df = glueContext.create_data_frame.from_catalog(
    database="bdp",
    table_name="scaled_features",
    additional_options = {
        "useCatalogSchema": True,
        "useSparkDataSource": True
    }
).select(*columns_to_select)

#fraction = 1_000_000/features_df.count()
#sampled_df = features_df.sample(withReplacement=False, fraction=fraction, seed=42)

features_df = features_df.withColumn("network_name", when(col("network_name") == True, 1.0).otherwise(0.0))


features_df.coalesce(1).write \
    .format("csv") \
    .option("header", "false") \
    .mode("append") \
    .save("s3://bdp-test-data/scaled/")

job.commit()
spark.stop()