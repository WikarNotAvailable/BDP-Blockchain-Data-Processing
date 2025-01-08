from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sklearn.metrics import confusion_matrix, classification_report, ConfusionMatrixDisplay, recall_score, f1_score, accuracy_score
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.feature_selection import SelectFromModel
from sklearn.model_selection import train_test_split
from scripts.local.shared.schemas import features_schema_scaled
import joblib
import numpy as np
import matplotlib.pyplot as plt

def feature_selection_with_random_forest(benchmark, features, label_column):
    benchmark_data = benchmark.select(features).toPandas()
    label_data = benchmark.select(col(label_column)).toPandas().values.ravel()

    X_train, X_test, y_train, y_test = train_test_split(
        benchmark_data, 
        label_data, 
        test_size=0.2,           
        stratify=label_data,              
        random_state=42
    )

    rf = RandomForestClassifier(
        n_estimators=690,
        class_weight='balanced_subsample',  
        random_state=69,
        n_jobs=-1 
    )
    rf.fit(X_train, y_train)

    sel_ = SelectFromModel(rf)
    sel_.fit(benchmark_data, label_data)

    selected_features = benchmark_data.columns[(sel_.get_support())].tolist()
    print(f"Selected features: {selected_features}")

    return selected_features

def train_isolation_forest(data, model, contamination, n_estimators, max_samples):
    if model is None:
        model = IsolationForest(
            n_estimators=n_estimators,
            contamination=contamination,
            random_state=69,
            max_samples=max_samples,
            warm_start=True,
            n_jobs=-1
        )
        model.fit(data)
    else:
        model.n_estimators += n_estimators
        model.fit(data)
    return model

def validate_model(model, test_data, selected_features, label_col):
    X_test = test_data.select(*selected_features).drop(label_col).toPandas()
    y_true = test_data.select(col(label_col)).toPandas().iloc[:, 0].map({True: -1, False: 1})

    y_pred = model.predict(X_test)
    cm = confusion_matrix(y_true, y_pred)
    print("Confusion Matrix:")
    print(cm)
    print("\nClassification Report:")
    print(classification_report(y_true, y_pred))

    tn, fp, fn, tp = cm.ravel()
    specificity = tn / (tn + fp)
    recall = recall_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred)
    accuracy = accuracy_score(y_true, y_pred)
    print(f"Specificity: {specificity}")
    print(f"Recall: {recall}")
    print(f"F1: {f1}")
    print(f"Accuracy: {accuracy}")
    
    disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=["Anomaly", "Normal"])

    plt.figure(figsize=(6, 6))
    disp.plot(cmap="Blues", colorbar=True)
    plt.title("Confusion Matrix")
    plt.show()

spark = (SparkSession.builder 
    .appName("BlockchainFeatureSelection") 
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
    .config("spark.executor.memory", "16g")
    .config("spark.driver.memory", "16g")
    .getOrCreate())

unscaled_parquet_file_path = "C:/Users/jakub/Desktop/Universe/Studia/Magisterka/PBL/Repository/data/historical/features/unscaled/"
unscaled_benchmark_file_path = "C:/Users/jakub/Desktop/Universe/Studia/Magisterka/PBL/Repository/data/benchmark/features/unscaled/"
label_column_name = "label"
features = [field.name for field in features_schema_scaled.fields if field.dataType.simpleString() == "float" ]

benchmark = spark.read.parquet(unscaled_benchmark_file_path)
selected_features = feature_selection_with_random_forest(benchmark, features, label_column_name)


data = spark.read.parquet(unscaled_parquet_file_path).select(*selected_features).limit(100000).na.drop().toPandas()
model = train_isolation_forest(data, None, contamination=0.0069, n_estimators=690, max_samples=69000)

joblib.dump(model, "isolation_forest_model.joblib")

selected_features.append(label_column_name)
validate_model(model, benchmark, selected_features, label_column_name)

spark.stop()