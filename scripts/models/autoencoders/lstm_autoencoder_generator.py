import numpy as np
import tensorflow as tf
from tensorflow.keras.models import  Model
from tensorflow.keras.layers import  RepeatVector, Dense, Dropout, Input, LSTM, TimeDistributed
from tensorflow.keras.metrics import mae
from keras import regularizers
from sklearn.metrics import classification_report, confusion_matrix
import pandas as pd
import pyarrow.parquet as pq
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt

columns_to_keep = ['avg_time_between_received_transactions', 'avg_fee_paid', 'stddev_received_value', 'unique_in_degree', 'sum_total_value_for_receiver', 'fee', 'num_received_transactions', 'sum_sent_value', 'num_sent_transactions', 'network_name', 'activity_duration_for_sender', 'total_transferred_value', 'transaction_index', 'unique_out_degree', 'stddev_sent_value', 'last_transaction_timestamp_for_sender', 'avg_sent_value', 'block_timestamp', 'activity_duration_for_receiver', 'received_value', 'first_transaction_timestamp_for_receiver', 'sum_total_value_for_sender', 'sent_value', 'min_received_value', 'last_transaction_timestamp_for_receiver', 'min_total_value_for_receiver', 'avg_time_between_sent_transactions', 'first_transaction_timestamp_for_sender']

training_path = ("data/training/merged_training_data.parquet")
training_size = pq.ParquetFile(training_path).metadata.num_rows

def build_autoencoder(input_dim, seq_length):
    inputs = Input(shape=(seq_length, input_dim))
    
    encoded = LSTM(64, activation="relu", return_sequences=True, kernel_regularizer=regularizers.l1(l1=0.001))(inputs)
    encoded = LSTM(32, activation="relu", return_sequences=False)(encoded)
    encoded = Dropout(0.1)(encoded)
    repeated = RepeatVector(seq_length)(encoded)

    decoded = LSTM(32, activation="relu", return_sequences=True)(repeated)
    decoded = LSTM(64, activation="relu", return_sequences=True)(decoded)
    output = TimeDistributed(Dense(input_dim))(decoded)   

    autoencoder = Model(inputs, output)
    autoencoder.compile(optimizer="adam", loss="mae")
    return autoencoder

def create_sequences(data, seq_length):
        sequences = []
        for i in range(0, len(data) - seq_length + 1, seq_length):
            sequences.append(data[i:i + seq_length])
        return np.array(sequences)
    
class DataGenerator(tf.keras.utils.Sequence):
    def __init__(self, batch_size, start_index, end_index, dataset_path, sequence_length):
        self.batch_size = batch_size
        self.start_index = start_index
        self.end_index  = end_index
        self.dataset_path = dataset_path
        self.dataset_scan = pl.scan_parquet(dataset_path)
        self.sequence_length = sequence_length
        self.is_training = True

    def __len__(self):
        return np.floor(((self.end_index - self.start_index) / (self.batch_size * self.sequence_length))).astype(int)

    def __getitem__(self, index):
        data_batch = create_sequences(self.dataset_scan.slice((index * self.batch_size * self.sequence_length), (self.batch_size * self.sequence_length)).select(columns_to_keep).collect().to_numpy(), self.sequence_length)

        if(self.is_training):
            return data_batch, data_batch
        else:
            return data_batch

    def on_epoch_end(self):
        print("epoch finished")
        
    def set_for_predictions(self):
        self.is_training = False

def calculate_bic_laplace(model, reconstruction_errors, num_samples):
    num_params = model.count_params()
    
    b = np.mean(np.abs(reconstruction_errors))
    
    log_likelihood = -num_samples * np.log(2 * b) - (1 / b) * np.sum(np.abs(reconstruction_errors))
    
    bic = num_params * np.log(num_samples) - 2 * log_likelihood
    return bic

input_dim = 28
sequence_length = 32
batch_size = 256

test_df =  pd.read_parquet("data/testing/part-00000-4755c810-9cc9-433a-92ab-85feaed511dc-c000.zstd.parquet")
X_test = create_sequences(test_df.drop(columns=["label"])[columns_to_keep].to_numpy(dtype=float), sequence_length)
y_test = test_df["label"]

generator_train = DataGenerator(batch_size, 0, np.floor(training_size * 0.9).astype(int), training_path, sequence_length)
generator_val = DataGenerator(batch_size, np.floor(training_size * 0.9).astype(int), training_size, training_path, sequence_length)

autoencoder = build_autoencoder(input_dim, sequence_length)
autoencoder.fit(generator_train, epochs=10, steps_per_epoch=len(generator_train), validation_data=generator_val, validation_steps=len(generator_val))

test_reconstructions = autoencoder.predict(X_test)
test_mae = mae(test_reconstructions, X_test)

generator_train.set_for_predictions()
train_mae = []
train_reconstructions = []
for i in range(len(generator_train)):
    batch_item = generator_train.__getitem__(i)
    batch_prediction = autoencoder.predict(batch_item)
    train_mae.extend(mae(batch_item, batch_prediction)) 
    train_reconstructions.extend(batch_prediction)

threshold = np.percentile(train_mae, 85)

classification_results = test_mae > threshold
y_pred = np.array(classification_results).astype(int).reshape(-1)

print("Classification Report:")
print(classification_report(y_test, y_pred))

print("Confusion Matrix:")
print(confusion_matrix(y_test, y_pred))

cm = confusion_matrix(y_test, y_pred)
plt.figure(figsize=(8, 6))
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=["Class 0", "Class 1"], yticklabels=["Class 0", "Class 1"])
plt.show()
plt.savefig("plots/CNN.png", dpi=300, bbox_inches="tight")

bic_laplace = calculate_bic_laplace(autoencoder, train_reconstructions, np.floor(training_size * 0.9).astype(int))
print("BIC (Laplace Error Model):", bic_laplace)