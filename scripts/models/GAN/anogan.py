from pyspark.sql import SparkSession
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import TensorDataset, DataLoader
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix, classification_report, f1_score, precision_recall_curve
from tqdm import tqdm


DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

LATENT_DIM = 64
HIDDEN_DIM = 256
BATCH_SIZE = 64
EPOCHS = 10
LR = 2e-5
LAMBDA_GP = 10.0
ALPHA_ANOGAN = 0.8
ANO_STEPS = 3

spark = (
    SparkSession.builder
    .appName("BlockchainFeatureSelection")
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.mergeSchema", "false")
    .config("spark.executor.memory", "16g")
    .config("spark.driver.memory", "16g")
    .getOrCreate()
)

features = ['block_timestamp', 'min_sent_value', 'network_name', 'avg_sent_value', 'mode_total_value_for_sender', 
            'min_received_value', 'total_transferred_value', 'avg_time_between_sent_transactions', 'mode_sent_value', 
            'transaction_index', 'fee', 'avg_received_value', 'avg_fee_paid']

train_df = spark.read.parquet("data/historical/features/scaled/").select(*features).limit(100)
X_train = train_df.toPandas().values.astype(np.float32)

features.append('label')
test_df = spark.read.parquet("data/benchmark/features/scaled/").select(*features).toPandas()
X_test = test_df.drop(columns=["label"]).values.astype(np.float32)

y_test = test_df["label"].map({True: 1, False: 0}).values

train_dataset = TensorDataset(torch.tensor(X_train))
train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)

class Generator(nn.Module):
    def __init__(self, latent_dim, out_dim, hidden_dim=128):
        super(Generator, self).__init__()
        self.net = nn.Sequential(
            nn.Linear(latent_dim, hidden_dim),
            nn.LeakyReLU(0.2),
            nn.Linear(hidden_dim, hidden_dim),
            nn.BatchNorm1d(hidden_dim),
            nn.LeakyReLU(0.2),
            nn.Linear(hidden_dim, hidden_dim),
            nn.LeakyReLU(0.2),
            nn.Linear(hidden_dim, out_dim),
        )
        
    def forward(self, z):
        return self.net(z)

class Discriminator(nn.Module):
    def __init__(self, in_dim, hidden_dim=256):
        super(Discriminator, self).__init__()
        
        self.feature_extractor = nn.Sequential(
            nn.Linear(in_dim, hidden_dim),
            nn.LeakyReLU(0.2),
            nn.Linear(hidden_dim, hidden_dim),
            nn.Dropout(0.2),
            nn.LeakyReLU(0.2),
            nn.Linear(hidden_dim, hidden_dim),
            nn.LeakyReLU(0.2),
        )
        self.out_layer = nn.Linear(hidden_dim, 1)
        
    def forward(self, x):
        features = self.feature_extractor(x)
        return self.out_layer(features)
    
    def forward_features(self, x):
        return self.feature_extractor(x)

def gradient_penalty(discriminator, real_samples, fake_samples):

    batch_size = real_samples.size(0)
    alpha = torch.rand(batch_size, 1, device=DEVICE)
    alpha = alpha.expand_as(real_samples)

    interpolated = alpha * real_samples + ((1 - alpha) * fake_samples)
    interpolated.requires_grad_(True)

    d_interpolated = discriminator(interpolated)
    grad = torch.autograd.grad(
        outputs=d_interpolated,
        inputs=interpolated,
        grad_outputs=torch.ones_like(d_interpolated),
        create_graph=True,
        retain_graph=True
    )[0]
    grad_penalty = ((grad.norm(2, dim=1) - 1) ** 2).mean()
    return grad_penalty

def train_wgan_gp(generator, discriminator, data_loader, epochs=10, lr=1e-4, lambda_gp=10.0, critic_iters=5):

    optim_G = optim.Adam(generator.parameters(), lr=lr, betas=(0.5, 0.999))
    optim_D = optim.Adam(discriminator.parameters(), lr=lr, betas=(0.5, 0.999))
    
    generator.train()
    discriminator.train()
    
    for epoch in range(epochs):
        for i, (real_batch,) in enumerate(data_loader):
            real_batch = real_batch.to(DEVICE)
            batch_size = real_batch.size(0)

            for _ in range(critic_iters):
                z = torch.randn(batch_size, LATENT_DIM, device=DEVICE)
                fake_batch = generator(z).detach()
                
                d_real = discriminator(real_batch)
                d_fake = discriminator(fake_batch)
                
                wass_distance = d_real.mean() - d_fake.mean()
                
                gp = gradient_penalty(discriminator, real_batch, fake_batch)
                
                d_loss = -wass_distance + lambda_gp * gp
                
                optim_D.zero_grad()
                d_loss.backward()
                optim_D.step()
            

            z = torch.randn(batch_size, LATENT_DIM, device=DEVICE)
            fake_batch = generator(z)
            d_fake_for_G = discriminator(fake_batch)
            
            g_loss = - d_fake_for_G.mean()
            
            optim_G.zero_grad()
            g_loss.backward()
            optim_G.step()
            
        print(f"[Epoch {epoch+1}/{epochs}] D_loss: {d_loss.item():.4f} | "
              f"G_loss: {g_loss.item():.4f} | Wdist: {wass_distance.item():.4f}")


def anogan_score(x, generator, discriminator, alpha=0.8, z_dim=16, lr=1e-2, steps=500):

    generator.eval()
    discriminator.eval()
    
    z_optim = torch.randn((1, z_dim), device=DEVICE, requires_grad=True)
    optimizer_z = optim.Adam([z_optim], lr=lr)
    
    with torch.no_grad():
        real_feat = discriminator.forward_features(x)
    
    for step in range(steps):
        optimizer_z.zero_grad()
        
        gen_sample = generator(z_optim)
        
        recon_loss = torch.mean(torch.abs(x - gen_sample))
        
        gen_feat = discriminator.forward_features(gen_sample)
        feat_loss = torch.mean(torch.abs(real_feat - gen_feat))
        
        loss = alpha * recon_loss + (1 - alpha) * feat_loss
        loss.backward()
        optimizer_z.step()
    
    anomaly_score = loss.item()
    return anomaly_score, z_optim.detach()

def find_best_threshold(y_test, anomaly_scores):

    precision, recall, thresholds = precision_recall_curve(y_test, anomaly_scores)
    
    f1_scores = 2 * (precision * recall) / (precision + recall + 1e-10)
    
    best_idx = np.argmax(f1_scores)
    best_threshold = thresholds[best_idx]
    best_f1 = f1_scores[best_idx]
    
    return best_threshold, best_f1

    
in_dim = X_train.shape[1]
generator = Generator(LATENT_DIM, out_dim=in_dim, hidden_dim=HIDDEN_DIM).to(DEVICE)
discriminator = Discriminator(in_dim=in_dim, hidden_dim=HIDDEN_DIM).to(DEVICE)

print("\n=== Training WGAN-GP ===")
train_wgan_gp(
    generator, 
    discriminator, 
    data_loader=train_loader, 
    epochs=EPOCHS, 
    lr=LR, 
    lambda_gp=LAMBDA_GP, 
    critic_iters=5
)

torch.save(generator.state_dict(), "generator3.pth")
torch.save(discriminator.state_dict(), "discriminator3.pth")


print("\n=== Faza testowa: obliczanie anomaly score (AnoGAN) ===")
X_test_torch = torch.tensor(X_test, dtype=torch.float32, device=DEVICE)

anomaly_scores = []
print("Obliczam anomaly score dla każdej próbki w X_test ...")

for i in tqdm(range(len(X_test_torch))):
    x = X_test_torch[i].unsqueeze(0) 
    score, _ = anogan_score(
        x, 
        generator, 
        discriminator, 
        alpha=ALPHA_ANOGAN, 
        z_dim=LATENT_DIM, 
        lr=1e-2, 
        steps=ANO_STEPS
    )
    anomaly_scores.append(score)

anomaly_scores = np.array(anomaly_scores)
np.savetxt("data3.csv", anomaly_scores, delimiter=",", fmt="%d")

anomaly_scores = np.loadtxt("data3.csv", delimiter=",", dtype=int)
#threshold = np.percentile(anomaly_scores, 80) 
threshold, best_f1 = find_best_threshold(y_test, anomaly_scores)

y_pred = (anomaly_scores > threshold).astype(int)
    
cm = confusion_matrix(y_test, y_pred)
plt.figure(figsize=(8, 6))
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=["Class 0", "Class 1"], yticklabels=["Class 0", "Class 1"])

print("Classification Report:\n", classification_report(y_test, y_pred))