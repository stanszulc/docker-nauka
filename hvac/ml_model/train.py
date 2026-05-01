"""
train.py — Random Forest training on AI4I 2020 dataset
Run locally, then scp hvac_rf_model.pkl to Oracle VM:
    scp -i ~/ssh-key.key hvac_rf_model.pkl ubuntu@92.5.14.76:~/docker-nauka/hvac/ml_model/

Dataset: https://www.kaggle.com/datasets/stephanmatzka/predictive-maintenance-dataset-ai4i-2020
Place ai4i2020.csv in the same directory as this script.
"""

import os
import sys
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import LabelEncoder

# ── Config ────────────────────────────────────────────────────────────────────
CSV_PATH   = os.getenv("CSV_PATH",   "ai4i2020.csv")
MODEL_OUT  = os.getenv("MODEL_OUT",  "hvac_rf_model.pkl")
N_TREES    = int(os.getenv("N_TREES",    "100"))   # keep small for VM
MAX_DEPTH  = int(os.getenv("MAX_DEPTH",  "12"))
RANDOM_STATE = 42

# Feature columns (must match consumer.py FEATURE_COLS)
FEATURE_COLS = ["Air temperature [K]", "Process temperature [K]",
                "Rotational speed [rpm]", "Torque [Nm]", "Tool wear [min]"]
TARGET_COL   = "Machine failure"

# Failure type label encoding for multiclass
FAILURE_TYPE_COL = "Failure Type"
LABEL_MAP = {
    "No Failure":              0,
    "Heat Dissipation Failure": 1,
    "Overstrain Failure":       2,
    "Power Failure":            3,
    "Tool Wear Failure":        4,
    "Random Failures":          5,
}


def load_data(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"ERROR: {path} not found.")
        print("Download from: https://www.kaggle.com/datasets/stephanmatzka/predictive-maintenance-dataset-ai4i-2020")
        sys.exit(1)
    df = pd.read_csv(path)
    print(f"Loaded {len(df)} rows, columns: {list(df.columns)}")
    return df


def train(df: pd.DataFrame):
    X = df[FEATURE_COLS].values

    # Encode failure type as integer class
    if FAILURE_TYPE_COL in df.columns:
        y = df[FAILURE_TYPE_COL].map(LABEL_MAP).fillna(0).astype(int).values
        print(f"Class distribution:\n{pd.Series(y).value_counts().sort_index()}")
    else:
        # Fallback: binary (failure / no failure)
        y = df[TARGET_COL].values
        print("Using binary target (Machine failure)")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=RANDOM_STATE, stratify=y
    )

    print(f"\nTraining Random Forest: n_estimators={N_TREES}, max_depth={MAX_DEPTH}")
    clf = RandomForestClassifier(
        n_estimators=N_TREES,
        max_depth=MAX_DEPTH,
        class_weight="balanced",   # handles imbalanced failure classes
        n_jobs=-1,
        random_state=RANDOM_STATE,
    )
    clf.fit(X_train, y_train)

    # Evaluation
    y_pred = clf.predict(X_test)
    print("\nClassification report:")
    print(classification_report(y_test, y_pred,
          target_names=[k for k, v in sorted(LABEL_MAP.items(), key=lambda x: x[1])]))

    # Feature importance
    print("\nFeature importance:")
    for feat, imp in sorted(zip(FEATURE_COLS, clf.feature_importances_),
                            key=lambda x: -x[1]):
        print(f"  {feat:<40} {imp:.4f}")

    return clf


def main():
    print("=== HVAC Random Forest Training ===")
    df = load_data(CSV_PATH)

    clf = train(df)

    # Save model
    joblib.dump(clf, MODEL_OUT)
    size_kb = os.path.getsize(MODEL_OUT) / 1024
    print(f"\nModel saved: {MODEL_OUT} ({size_kb:.1f} KB)")
    print(f"\nDeploy to VM:")
    print(f"  scp -i ~/ssh-key-2026-03-11.key {MODEL_OUT} ubuntu@92.5.14.76:~/docker-nauka/hvac/ml_model/")


if __name__ == "__main__":
    main()
