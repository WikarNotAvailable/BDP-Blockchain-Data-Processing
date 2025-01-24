import pandas as pd
from sklearn.metrics import classification_report
import matplotlib.pyplot as plt
import seaborn as sns

# Load the data
data_file = "test_data.csv.out"
df = pd.read_csv(data_file, header=None, names=["label", "cluster", "distance"])

# Ensure the label column is in boolean format
df["label"] = df["label"].astype(bool)

# Define the range of quantiles to test
quantile_values = [x / 1000 for x in range(500, 1000)]  # From 0.500 to 0.999 in steps of 0.001

# Store results for each quantile
results = []

for quantile in quantile_values:
    # Calculate the threshold
    threshold = df["distance"].quantile(quantile)
    
    # Mark anomalies based on the threshold
    df["is_anomaly"] = df["distance"] > threshold

    # Generate classification report
    report = classification_report(
        df["label"], df["is_anomaly"],
        target_names=["Normal", "Anomaly"],
        labels=[False, True],  # Explicitly define the label order
        output_dict=True
    )

    # Extract relevant metrics
    macro_avg_recall = report["macro avg"]["recall"]
    weighted_avg_recall = report["weighted avg"]["recall"]

    # Store results
    results.append({
        "quantile": quantile,
        "threshold": threshold,
        "macro_avg_recall": macro_avg_recall,
        "weighted_avg_recall": weighted_avg_recall,
        "avg_recall": (macro_avg_recall + weighted_avg_recall) / 2,
        "classification_report_dict": report
    })

# Convert results to a DataFrame
results_df = pd.DataFrame(results)

# Find the best quantile based on the average recall
results_df["avg_recall"] = (results_df["macro_avg_recall"] + results_df["weighted_avg_recall"]) / 2
best_result = results_df.loc[results_df["avg_recall"].idxmax()]

# Extract the best classification report in dictionary format
best_report_dict = best_result["classification_report_dict"]

# Convert the dictionary classification report to a formatted string
df["is_anomaly"] = df["distance"] > best_result["threshold"]
formatted_report = classification_report(
    df["label"], df["is_anomaly"], target_names=["Normal", "Anomaly"], labels=[False, True]
)

# Print the detailed report
report_text = f"""
Detailed Report:

Best Quantile: {best_result['quantile']}
Threshold: {best_result['threshold']}

Metrics:
- Macro Avg Recall: {best_result['macro_avg_recall']:.4f}
- Weighted Avg Recall: {best_result['weighted_avg_recall']:.4f}
- Average Recall: {best_result['avg_recall']:.4f}

Classification Report:
{formatted_report}

Full results saved to: quantile_analysis_results.csv
"""
print(report_text)

# Save results to a CSV file
results_df.to_csv("quantile_analysis_results.csv", index=False)

# Generate and save confusion matrix for the best quantile
best_threshold = best_result["threshold"]
conf_matrix = pd.crosstab(df["label"], df["is_anomaly"], rownames=["True"], colnames=["Predicted"], dropna=False)

plt.figure(figsize=(8, 6))
sns.heatmap(conf_matrix, annot=True, fmt="d", cmap="Blues", xticklabels=["Normal", "Anomaly"], yticklabels=["Normal", "Anomaly"])
plt.xlabel("Predicted Label")
plt.ylabel("True Label")
plt.title(f"Confusion Matrix (Quantile {best_result['quantile']:.3f}, Threshold {best_threshold:.2f})")
plt.savefig("best_confusion_matrix.png")
plt.close()
