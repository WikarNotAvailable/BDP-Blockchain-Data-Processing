import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Conv1D, MaxPooling1D, Flatten, Dense, Reshape, Input, BatchNormalization, Conv1DTranspose
from tensorflow.keras.metrics import mae
from sklearn.metrics import classification_report, confusion_matrix
import pandas as pd
import pyarrow.parquet as pq
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt

columns_to_keep = ['avg_time_between_received_transactions', 'avg_fee_paid', 'stddev_received_value', 'unique_in_degree', 'sum_total_value_for_receiver', 'fee', 'num_received_transactions', 'sum_sent_value', 'num_sent_transactions', 'network_name', 'activity_duration_for_sender', 'total_transferred_value', 'transaction_index', 'unique_out_degree', 'stddev_sent_value', 'last_transaction_timestamp_for_sender', 'avg_sent_value', 'block_timestamp', 'activity_duration_for_receiver', 'received_value', 'first_transaction_timestamp_for_receiver', 'sum_total_value_for_sender', 'sent_value', 'min_received_value', 'last_transaction_timestamp_for_receiver', 'min_total_value_for_receiver', 'avg_time_between_sent_transactions', 'first_transaction_timestamp_for_sender']

training_path = ("data/training/merged_training_data.parquet")
training_size = pq.ParquetFile(training_path).metadata.num_rows

def build_autoencoder(input_dim, latent_dim):
    model = Sequential()
    
    # Encoder
    model.add(Input(shape=(input_dim, )))
    model.add(Reshape((input_dim, 1)))
    model.add(Conv1D(128, 3, activation='relu', padding='same'))
    model.add(BatchNormalization())
    model.add(MaxPooling1D(2, padding='same'))
    model.add(Conv1D(128, 3, activation='relu', padding='same'))
    model.add(BatchNormalization())
    model.add(MaxPooling1D(2, padding='same'))
    model.add(Conv1D(latent_dim, 3, activation='relu', padding='same'))
    model.add(BatchNormalization())
    model.add(MaxPooling1D(2, padding='same'))

    # Decoder
    model.add(Conv1DTranspose(latent_dim, 3, strides=1, activation='relu', padding="same"))
    model.add(BatchNormalization())
    model.add(Conv1DTranspose(128, 3, strides=1, activation='relu', padding="same"))
    model.add(BatchNormalization())
    model.add(Conv1DTranspose(128, 3, strides=1, activation='relu', padding="same"))
    model.add(BatchNormalization())
    model.add(Flatten())
    model.add(Dense(input_dim))

    model.compile(optimizer='adam', loss='mae')
    return model

class DataGenerator(tf.keras.utils.Sequence):
    def __init__(self, batch_size, start_index, end_index, dataset_path):
        self.batch_size = batch_size
        self.start_index = start_index
        self.end_index  = end_index
        self.dataset_path = dataset_path
        self.dataset_scan = pl.scan_parquet(dataset_path)
        self.is_training = True
        
    def __len__(self):
        if(self.is_training):
            return np.floor(((self.end_index - self.start_index) / self.batch_size)).astype(int)
        else:
            return np.floor(((self.end_index - self.start_index) / (self.batch_size))).astype(int)      

    def __getitem__(self, index):      
        if(self.is_training):
            data_batch = self.dataset_scan.slice(index * batch_size, batch_size).select(columns_to_keep).collect().to_numpy()
            return data_batch, data_batch
        else:
            data_batch = self.dataset_scan.slice(index * batch_size, batch_size).select(columns_to_keep).collect().to_numpy()
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
     
test_df =  pd.read_parquet("data/testing/part-00000-4755c810-9cc9-433a-92ab-85feaed511dc-c000.zstd.parquet")
X_test = test_df.drop(columns=["label"])[columns_to_keep].to_numpy(dtype=float)
y_test = test_df["label"]

input_dim = 28
latent_dim = 32
batch_size = 1024

generator_train = DataGenerator(batch_size, 0, np.floor(training_size * 0.9).astype(int), training_path)
generator_val = DataGenerator(batch_size, np.floor(training_size * 0.9).astype(int), training_size, training_path)

autoencoder = build_autoencoder(input_dim, latent_dim)
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

threshold = np.percentile(train_mae, 95)

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