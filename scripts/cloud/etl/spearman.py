from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import io


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = (
    SparkSession.builder
        .appName("FeatureSelection")
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


features_df = glueContext.create_data_frame.from_catalog(
    database="bdp",
    table_name="scaled_features",
    additional_options = {
        "useCatalogSchema": True,
        "useSparkDataSource": True
    }
)

feature_cols = [col for col in features_df.columns]

df_vectorized = VectorAssembler(inputCols=feature_cols, outputCol="features").transform(features_df)
df_vectorized.cache()
correlation_matrix = Correlation.corr(df_vectorized, "features", method="spearman").head()[0].toArray()
correlation_matrix_np = np.array(correlation_matrix)
correlation_matrix_df = pd.DataFrame(correlation_matrix_np, index=feature_cols, columns=feature_cols)
df_vectorized.unpersist()

output_bucket = "bdp-feature-selection"

# Save correlation matrix to S3 as CSV
s3_client = boto3.client('s3')
correlation_csv_buffer = io.StringIO()
correlation_matrix_df.to_csv(correlation_csv_buffer)
s3_client.put_object(
    Bucket=output_bucket,
    Key="data/correlation_matrix.csv",
    Body=correlation_csv_buffer.getvalue()
)


threshold = 0.9 
to_remove = set()
for i in range(len(correlation_matrix_np)):
    for j in range(i+1, len(correlation_matrix_np)):
        if abs(correlation_matrix_np[i, j]) > threshold:
            to_remove.add(feature_cols[j])

selected_columns = [col for col in feature_cols if col not in to_remove]

# Save selected columns (only names) to S3 as a plain text file
selected_columns_buffer = io.StringIO()
selected_columns_buffer.write("\n".join(selected_columns))  # Write column names line by line
s3_client.put_object(
    Bucket=output_bucket,
    Key="data/selected_columns.txt",
    Body=selected_columns_buffer.getvalue(),
    ContentType="text/plain"
)

# Generate and save the heatmap plot
plt.figure(figsize=(12, 8))
sns.heatmap(correlation_matrix_df, cmap="coolwarm", fmt=".2f", vmin=-1, vmax=1, annot=False)
plt.title("Correlation Heatmap")
heatmap_buffer = io.BytesIO()
plt.savefig(heatmap_buffer, format='png', bbox_inches='tight')
heatmap_buffer.seek(0)
s3_client.put_object(
    Bucket=output_bucket,
    Key="data/correlation_heatmap.png",
    Body=heatmap_buffer,
    ContentType='image/png'
)

job.commit()
spark.stop()
