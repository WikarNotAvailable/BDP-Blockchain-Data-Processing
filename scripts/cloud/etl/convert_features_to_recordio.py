import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array, array_to_vector
import sagemaker_pyspark
import boto3

def get_selected_columns():
    bucket_name = "bdp-feature-selection"
    file_key = "data/selected_columns.txt"

    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read().decode('utf-8')
    return file_content.splitlines()
    
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

classpath = ":".join(sagemaker_pyspark.classpath_jars())

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
        .config("spark.driver.extraClassPath", classpath)
        .config("spark.executor.extraClassPath", classpath)
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

features_df = features_df.withColumn("network_name", when(col("network_name") == True, 1.0).otherwise(0.0))

assembler = VectorAssembler(
    inputCols=columns_to_select,
    outputCol="features"
)

features_vector_df = assembler.transform(features_df)

dense_features_df = features_vector_df.withColumn(
    "features",
    array_to_vector(vector_to_array(col("features")))
)

dense_features_df.select("features").write \
    .format("sagemaker") \
    .option("recordio-protobuf", "true") \
    .option("featureDim", len(columns_to_select)) \
    .mode("overwrite") \
    .save("s3://bdp-recordio/train/")
    


job.commit()
spark.stop()