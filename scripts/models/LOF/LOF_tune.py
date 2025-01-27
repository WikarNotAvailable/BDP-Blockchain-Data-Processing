import pandas as pd
from sklearn.neighbors import LocalOutlierFactor
from sklearn.metrics import confusion_matrix
from sklearn.metrics import confusion_matrix, recall_score, accuracy_score
from statistics import mean

columns_to_keep = [
    "block_timestamp",
    "transaction_index",
    "fee",
    "total_transferred_value",
    "sent_value",
    "received_value",
    "network_name",
    "avg_sent_value",
    "avg_received_value",
    "sum_sent_value",
    "sum_received_value",
    "sum_total_value_for_sender",
    "sum_total_value_for_receiver",
    "min_sent_value",
    "min_received_value",
    "min_total_value_for_receiver",
    "mode_received_value",
    "stddev_sent_value",
    "stddev_received_value",
    "stddev_total_value_for_receiver",
    "num_sent_transactions",
    "num_received_transactions",
    "avg_time_between_sent_transactions",
    "avg_time_between_received_transactions",
    "avg_outgoing_speed_count",
    "avg_incoming_speed_count",
    "avg_outgoing_speed_value",
    "avg_incoming_speed_value",
    "avg_outgoing_acceleration_count",
    "avg_incoming_acceleration_count",
    "avg_outgoing_acceleration_value",
    "avg_incoming_acceleration_value",
    "avg_fee_paid",
    "total_fee_paid",
    "activity_duration_for_sender",
    "first_transaction_timestamp_for_sender",
    "last_transaction_timestamp_for_sender",
    "activity_duration_for_receiver",
    "first_transaction_timestamp_for_receiver",
    "last_transaction_timestamp_for_receiver",
    "unique_out_degree",
    "unique_in_degree"
]

train_df = pd.read_parquet("data/historical/merged/merged_training_data.parquet")
train_df = train_df[columns_to_keep]
test_df =  pd.read_parquet("data/benchmark/testing")

oneModelLimit = 300000
fracToTrain = oneModelLimit/len(train_df)

X_test = test_df[columns_to_keep]
y_test = test_df["label"]

contamination = 0.01
for i in range(20):
    iterations = []
    for j in range(10):
        X_train = train_df.sample(frac=fracToTrain)
        n_neighbors = 20

        lof = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=contamination, novelty=True)
        lof.fit(X_train)
        outlier_predictions = lof.predict(X_test)
        outlier_predictions = pd.Series(outlier_predictions).map({-1: True, 1: False})

        tn, fp, fn, tp = confusion_matrix(y_test, outlier_predictions).ravel()
        specificity = tn / (tn + fp)
        recall = recall_score(y_test, outlier_predictions)
        accuracy = accuracy_score(y_test, outlier_predictions)
        iterations.append({"specificity": specificity, "accuracy": accuracy, "recall": recall})
        
    avg_specificity = mean([iteration["specificity"] for iteration in iterations])
    avg_accuracy = mean([iteration["accuracy"] for iteration in iterations])
    avg_recall = mean([iteration["recall"] for iteration in iterations])
    
    print("contamination:" + str(contamination) + ", specificity:" + str(avg_specificity) + ", accuracy:" + str(avg_accuracy) + ", sensitivity:" + str(avg_recall))
    contamination += 0.005