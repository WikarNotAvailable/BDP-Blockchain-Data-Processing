import os
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import monotonically_increasing_id
from matplotlib import pyplot as plt

def list_s3_files(bucket, prefix, suffix):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = []
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith(suffix):
                files.append(key)
    return files

def download_s3_file(bucket, key, download_path):
    s3 = boto3.client("s3")
    os.makedirs(os.path.dirname(download_path), exist_ok=True)
    s3.download_file(bucket, key, download_path)
    print(f"Downloaded {key} to {download_path}")

def download_results():
    bucket = "bdp-inference-results"
    download_dir = "data/results"

    for k in range(6, 7):
        prefix = f"kmeans/k_{k}/"
        suffix = ".csv.out"
        result_files = list_s3_files(bucket, prefix, suffix)
        s3_key = result_files[0]  # Assuming only one file per k
        local_filename = f"{k}.csv"
        download_path = os.path.join(download_dir, local_filename)
        download_s3_file(bucket, s3_key, download_path)       

def merge_results():
    results = []
    for k in range(2, 11):
        result_df = pd.read_csv(f"data/results/{k}.csv", usecols=[28, 29], names=[f"cluster_{k}", f"distance_{k}"])
        results.append(result_df)

    results_df = pd.concat(results, axis=1)
    results_df.to_csv("data/results.csv", index=False)

def check_num_clusters():
    for k in range(6, 7):
        results_df = spark.read.csv(f"data/results/{k}.csv", header=False, inferSchema=True)
        results_df = results_df.select(results_df.columns[28])
        
        num_clusters = results_df.distinct().count()
        print(f"Number of clusters for k={k}: {num_clusters}")

def silhouette_score():
    data_df = spark.read.csv("data/data.csv", header=True, inferSchema=True)
    results_df = spark.read.csv("data/results.csv", header=True, inferSchema=True)

    assembler = VectorAssembler(inputCols=data_df.columns , outputCol="features")
    data_features = assembler.transform(data_df)

    data_features = data_features.withColumn("index", monotonically_increasing_id())
    results_df = results_df.withColumn("index", monotonically_increasing_id())
    
    results_with_clusters = data_features.join(results_df, on="index", how="inner")

    for k in range(2, 11):
        cluster_col = f"cluster_{k}"

        if results_with_clusters.select(cluster_col).distinct().count() == 1:
            print(f"Skipping k={k} as there is only one cluster")
            continue
        
        evaluator = ClusteringEvaluator(predictionCol=cluster_col, featuresCol="features", metricName="silhouette")
        
        score = evaluator.evaluate(results_with_clusters)
        print(f"Silhouette score for k={k}: {score}")

from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col

def silhouette(k):
    data_df = spark.read.csv(f"data/results/{k}.csv", header=False, inferSchema=True)

    feature_cols = data_df.columns[:28]
    label_col = data_df.columns[28]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    data_features = assembler.transform(data_df)
    data_features = data_features.withColumn("prediction", col(label_col))

    evaluator = ClusteringEvaluator(predictionCol="prediction", featuresCol="features", metricName="silhouette")
    score = evaluator.evaluate(data_features)

    print(f"Silhouette score for k={k}: {score}")
    return score

def plot_scores(scores):
    plt.plot(range(2, 11), scores)
    plt.xlabel("Number of clusters")
    plt.ylabel("Silhouette score")
    plt.title("Silhouette score vs Number of clusters")
    plt.savefig("silhouette_score.png")


spark = (
    SparkSession.builder.appName("'SilhouetteScore'") 
    .config("spark.executor.memory", "16g")
    .config("spark.driver.memory", "8g")
    .getOrCreate()
)

#download_results()
check_num_clusters()

silhouette(6)
#score_list = []
#for k in range(2, 11):
#    score = silhouette(k)
#    score_list.append(score)

#plot_scores(score_list)

spark.stop()