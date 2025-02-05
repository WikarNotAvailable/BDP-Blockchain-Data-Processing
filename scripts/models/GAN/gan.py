import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from sklearn.metrics import confusion_matrix, classification_report, f1_score, roc_auc_score, recall_score, accuracy_score
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

def test_discriminator(discriminator, X_test, y_test, threshold=0.5):
    discriminator.eval()
    
    with torch.no_grad():
        scores = discriminator(X_test.to(device)).cpu().numpy().ravel()

    y_pred = (scores > threshold).astype(int)

    cm = confusion_matrix(y_test, y_pred)
    print("Confusion Matrix:")
    print(cm)
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))

    tn, fp, fn, tp = cm.ravel()
    specificity = tn / (tn + fp)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Specificity: {specificity}")
    print(f"Recall: {recall}")
    print(f"F1: {f1}")
    print(f"Accuracy: {accuracy}")

    roc_auc = roc_auc_score(y_test, scores)
    print(f"ROC AUC: {roc_auc:.4f}")

    return scores

def find_best_threshold(scores, y_test):
    thresholds = np.linspace(0.05, 0.95, 10000)
    best_threshold = 0
    best_accuracy  = 0

    for t in thresholds:
        y_pred = (scores > t).astype(int)
        accuracy  = accuracy_score(y_test, y_pred)
        if accuracy  > best_accuracy :
            best_accuracy  = accuracy 
            best_threshold = t

    print(f"Best threshold: {best_threshold:.2f}, Accuracy : {best_accuracy :.4f}")
    return best_threshold

def test_gan(discriminator, X_test, y_test):

    scores = test_discriminator(discriminator, X_test, y_test)
    best_threshold = find_best_threshold(scores, y_test)
    test_discriminator(discriminator, X_test, y_test, threshold=best_threshold)

def compute_bic(discriminator, X, y):
    """
    Computes an approximate BIC for the discriminator model.
    """
    discriminator.eval()
    with torch.no_grad():
        probs = discriminator(X.to(device)).cpu().numpy().ravel()
    
    eps = 1e-6
    probs = np.clip(probs, eps, 1 - eps)
        
    log_likelihood = np.sum(y * np.log(probs) + (1-y) * np.log(1-probs))
    
    n = len(y)

    k = sum(p.numel() for p in discriminator.parameters() if p.requires_grad)
    
    bic = -2 * log_likelihood + k * np.log(n)
    print(f"\nBIC (Laplace approximation): {bic:.4f}")
    return bic


spark = (SparkSession.builder 
    .appName("BlockchainFeatureSelection") 
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false") # No need as we explicitly specify the schema
    .config("spark.executor.memory", "16g")
    .config("spark.driver.memory", "16g")
    .getOrCreate())

features = ['block_timestamp', 'block_number', 'min_received_value', 'min_total_value_for_sender', 
           'min_total_value_for_receiver', 'stddev_received_value', 'stddev_total_value_for_receiver', 
           'num_received_transactions', 'avg_time_between_received_transactions', 'avg_incoming_speed_count', 
           'avg_incoming_acceleration_count', 'min_fee_paid', 'first_transaction_timestamp_for_sender', 
           'activity_duration_for_receiver', 'first_transaction_timestamp_for_receiver', 'last_transaction_timestamp_for_receiver']

latent_dim = 16
input_dim = len(features)
hidden_dim = 128
batch_size = 64
num_epochs = 10
lr = 1e-4
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

class Generator(nn.Module):
    def __init__(self, latent_dim, input_dim, hidden_dim):
        super(Generator, self).__init__()
        self.net = nn.Sequential(
            nn.Linear(latent_dim, hidden_dim),
            nn.BatchNorm1d(hidden_dim),
            nn.LeakyReLU(0.2),
            nn.Linear(hidden_dim, hidden_dim),
            nn.BatchNorm1d(hidden_dim),
            nn.LeakyReLU(0.2),
            nn.Linear(hidden_dim, input_dim),
        )
    def forward(self, z):
        return self.net(z)

class Discriminator(nn.Module):
    def __init__(self, input_dim, hidden_dim):
        super(Discriminator, self).__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LeakyReLU(0.2),
            nn.Dropout(0.4),
            nn.Linear(hidden_dim, hidden_dim),
            nn.LeakyReLU(0.2),
            nn.Linear(hidden_dim, 1),
            nn.Sigmoid()
        )


    def forward(self, x):
        return self.net(x) 

train_df = spark.read.parquet("data/historical/features/scaled/").select(*features).limit(1000)
X_train = train_df.toPandas().values.astype(np.float32)

features.append('label')
test_df = spark.read.parquet("data/benchmark/features/scaled/").select(*features).toPandas()
X_test = test_df.drop(columns=["label"]).values.astype(np.float32)
y_test = test_df["label"].map({True: 1, False: 0}).values
X_test_torch = torch.tensor(X_test, dtype=torch.float32)

train_dataset = TensorDataset(torch.tensor(X_train))
train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

generator = Generator(latent_dim, input_dim, hidden_dim).to(device)
discriminator = Discriminator(input_dim, hidden_dim).to(device)

optim_G = optim.Adam(generator.parameters(), lr=lr, betas=(0.5, 0.999))
optim_D = optim.Adam(discriminator.parameters(), lr=lr, betas=(0.5, 0.999))

criterion = nn.BCELoss()

for epoch in range(num_epochs):
    for real_samples, in train_loader:
        real_samples = real_samples.to(device)
        batch_size_current = real_samples.size(0)

        real_labels = torch.ones(batch_size_current, 1).to(device)
        fake_labels = torch.zeros(batch_size_current, 1).to(device)

        outputs_real = discriminator(real_samples)
        d_loss_real = criterion(outputs_real, real_labels)

        z = torch.randn(batch_size_current, latent_dim).to(device)
        fake_samples = generator(z)
        outputs_fake = discriminator(fake_samples.detach())
        d_loss_fake = criterion(outputs_fake, fake_labels)

        d_loss = d_loss_real + d_loss_fake

        optim_D.zero_grad()
        d_loss.backward()
        optim_D.step()

        z = torch.randn(batch_size_current, latent_dim).to(device)
        fake_samples = generator(z)
        outputs_fake_for_G = discriminator(fake_samples)
        g_loss = criterion(outputs_fake_for_G, real_labels)

        optim_G.zero_grad()
        g_loss.backward()
        optim_G.step()

    print(f"Epoch [{epoch+1}/{num_epochs}] | d_loss: {d_loss.item():.4f}, g_loss: {g_loss.item():.4f}")

test_gan(discriminator, X_test_torch, y_test)
bic_value = compute_bic(discriminator, X_test_torch, y_test)
