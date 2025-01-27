import pandas as pd
from sklearn.neighbors import LocalOutlierFactor
from sklearn.metrics import classification_report
import math
import warnings
from sklearn.exceptions import DataConversionWarning
warnings.filterwarnings(action='ignore', category=DataConversionWarning)

columns_to_keep = ['avg_time_between_received_transactions', 'avg_fee_paid', 'stddev_received_value', 'unique_in_degree', 'sum_total_value_for_receiver', 'fee', 'num_received_transactions', 'sum_sent_value', 'num_sent_transactions', 'network_name', 'activity_duration_for_sender', 'total_transferred_value', 'transaction_index', 'unique_out_degree', 'stddev_sent_value', 'last_transaction_timestamp_for_sender', 'avg_sent_value', 'block_timestamp', 'activity_duration_for_receiver', 'received_value', 'first_transaction_timestamp_for_receiver', 'sum_total_value_for_sender', 'sent_value', 'min_received_value', 'last_transaction_timestamp_for_receiver', 'min_total_value_for_receiver', 'avg_time_between_sent_transactions', 'first_transaction_timestamp_for_sender']

train_df = pd.read_parquet("data/historical/merged/merged_training_data.parquet")
train_df = train_df[columns_to_keep]
test_df =  pd.read_parquet("data/benchmark/testing")

oneModelLimit = 300000
fracToTrain = oneModelLimit/len(train_df)
models_number = math.floor(len(train_df)/oneModelLimit)

print("models number: " + str(models_number))

X_test = test_df[columns_to_keep]
y_test = test_df["label"]

contamination = 0.007 #0.005 0.4 false 0.8 true
for i in range(10):
    estimators = []
    for j in range(models_number):
        X_train = train_df[j * oneModelLimit : (j+1) * oneModelLimit]
        n_neighbors = 20
        lof = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=contamination, novelty=True)
        lof.fit(X_train)
        estimators.append((lof,f'lof{j}'))

    predictions = []
    for i in range(len(estimators)):
        outlier_predictions = lof.predict(X_test)
        outlier_predictions = pd.Series(outlier_predictions).map({-1: True, 1: False})
        predictions.append(outlier_predictions)

    df = pd.concat(predictions, axis=1)
    final_predictions = df.mode(axis=1)[0]
    
    print("Classification Report:\n", classification_report(y_test, final_predictions))
    print("contamination: " + str(contamination))
    
    contamination += 0.0001
