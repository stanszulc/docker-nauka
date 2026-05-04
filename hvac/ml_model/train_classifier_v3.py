"""
train_classifier.py
===================
Trenuje XGBoost klasyfikator do detekcji pre-failure.
Target: label 0=normal, 1=pre_failure (5 min przed awarią)
Ignoruje klasę 2 (awaria aktywna) — wtedy już za późno.

Wyjście: hvac_classifier.pkl

Uruchomienie:
    python train_classifier.py
"""

import numpy as np
import pandas as pd
import joblib
from pathlib import Path

from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (classification_report, confusion_matrix,
                             roc_auc_score, precision_recall_curve)
from sklearn.preprocessing import LabelEncoder

# ── Config ────────────────────────────────────────────────────────────────────
CSV_PATH     = "hvac_training_v3.csv"
MODEL_OUT    = "hvac_classifier_v3.pkl"
WINDOW       = 20     # rolling window (20 ticks × 10s = 200s historii)
RANDOM_STATE = 42
TEST_SIZE    = 0.2

# Surowe sensory + velocity features (bez device_profile i accum!)
BASE_FEATURES = [
    'air_temp', 'proc_temp', 'rpm', 'torque',
    'vibration', 'delta_temp', 'power_w',
    'proc_temp_velocity', 'rpm_velocity',
    'torque_velocity', 'vibration_velocity',
]


# ── Feature engineering ───────────────────────────────────────────────────────
def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().sort_values(['sequence_id', 'step'])

    grp = df.groupby('sequence_id')

    for feat in BASE_FEATURES:
        df[f'{feat}_rmean'] = grp[feat].transform(
            lambda x: x.rolling(WINDOW, min_periods=1).mean()
        )
        df[f'{feat}_rgrad'] = grp[feat].transform(
            lambda x: x.diff().rolling(WINDOW, min_periods=1).mean()
        )
        df[f'{feat}_rstd'] = grp[feat].transform(
            lambda x: x.rolling(WINDOW, min_periods=1).std().fillna(0)
        )

    # Uptime jako cecha — licznik sekund od startu sesji
    df['uptime_norm'] = df['uptime_seconds'] / 1800.0  # normalizacja do ~30 min
    df['buffer_fill_ratio'] = df['buffer_fill_ratio'].clip(0, 1) if 'buffer_fill_ratio' in df.columns else 0.5

    # buffer_fill_ratio — ile próbek w buforze / max okno (0=zimny start, 1=pełny bufor)
    if 'buffer_fill_ratio' not in df.columns:
        df['buffer_fill_ratio'] = df.groupby('sequence_id')['step'].transform(
            lambda x: (x + 1).clip(upper=20) / 20.0
        )

    # Proximity features
    df['hdf_margin']     = df['delta_temp'] - 8.6
    df['pwf_low_margin'] = df['power_w'] - 3500
    df['pwf_high_margin']= 9000 - df['power_w']
    df['pwf_margin']     = df[['pwf_low_margin', 'pwf_high_margin']].min(axis=1)

    # Brak tool_wear w HVAC — usunięto osf_margin i twf_margin

    return df


def get_feature_cols(df: pd.DataFrame) -> list:
    cols = ['uptime_norm', 'buffer_fill_ratio',
            'hdf_margin', 'pwf_margin']

    for feat in BASE_FEATURES:
        cols += [f'{feat}_rmean', f'{feat}_rgrad', f'{feat}_rstd']

    return [c for c in cols if c in df.columns]


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"Loading {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH)
    print(f"  {len(df)} rows, {df['sequence_id'].nunique()} sequences")

    # Trenujemy tylko na klasach 0 i 1 — ignorujemy aktywną awarię (klasa 2)
    df = df[df['label'].isin([0, 1])].copy()
    print(f"  After filtering (0+1 only): {len(df)} rows")
    print(f"\nLabel distribution:")
    print(df['label'].value_counts().sort_index().rename({0: 'Normal', 1: 'Pre-failure'}))

    print("\nEngineering features...")
    df = engineer_features(df)
    feature_cols = get_feature_cols(df)
    print(f"  {len(feature_cols)} features")

    # Split by sequence_id (no data leakage)
    seq_ids = df['sequence_id'].unique()
    train_ids, test_ids = train_test_split(
        seq_ids, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    train = df[df['sequence_id'].isin(train_ids)]
    test  = df[df['sequence_id'].isin(test_ids)]

    X_train = train[feature_cols]
    y_train = train['label']
    X_test  = test[feature_cols]
    y_test  = test['label']

    print(f"\n  Train: {len(X_train)} rows ({len(train_ids)} sequences)")
    print(f"  Test:  {len(X_test)} rows  ({len(test_ids)} sequences)")

    # Balans klas — scale_pos_weight dla pre_failure
    n_normal     = (y_train == 0).sum()
    n_prefailure = (y_train == 1).sum()
    scale_pos_weight = n_normal / n_prefailure
    print(f"\n  scale_pos_weight: {scale_pos_weight:.2f} (balans klas)")

    print(f"\nTraining XGBoost classifier...")
    model = XGBClassifier(
        n_estimators=500,
        max_depth=8,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        scale_pos_weight=scale_pos_weight,
        random_state=RANDOM_STATE,
        n_jobs=-1,
        tree_method='hist',
        early_stopping_rounds=30,
        eval_metric='auc',
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=50,
    )

    # ── Evaluation ─────────────────────────────────────────────────────────────
    y_pred      = model.predict(X_test)
    y_pred_prob = model.predict_proba(X_test)[:, 1]

    print(f"\n=== Evaluation ===")
    print(classification_report(y_test, y_pred,
                                 target_names=['Normal', 'Pre-failure']))

    print("Confusion matrix:")
    cm = confusion_matrix(y_test, y_pred)
    print(f"  TN={cm[0,0]:6d}  FP={cm[0,1]:6d}")
    print(f"  FN={cm[1,0]:6d}  TP={cm[1,1]:6d}")

    auc = roc_auc_score(y_test, y_pred_prob)
    print(f"\nROC-AUC: {auc:.4f}")

    # Precision-Recall przy różnych progach
    precision, recall, thresholds = precision_recall_curve(y_test, y_pred_prob)
    print(f"\nPrecision-Recall przy różnych progach:")
    for threshold in [0.3, 0.4, 0.5, 0.6, 0.7]:
        idx = np.argmin(np.abs(thresholds - threshold))
        print(f"  threshold={threshold:.1f}: "
              f"precision={precision[idx]:.3f} recall={recall[idx]:.3f}")

    # Feature importance
    importance = pd.Series(model.feature_importances_, index=feature_cols)
    print(f"\nTop 10 features:")
    print(importance.nlargest(10).to_string())

    # ── Save ───────────────────────────────────────────────────────────────────
    bundle = {
        'model':         model,
        'feature_cols':  feature_cols,
        'window':        WINDOW,
        'base_features': BASE_FEATURES,
        'model_type':    'classifier',
        'version':       '2.0',
        'threshold':     0.5,   # próg klasyfikacji (można dostroić)
    }
    joblib.dump(bundle, MODEL_OUT)
    size_kb = Path(MODEL_OUT).stat().st_size / 1024
    print(f"\nModel saved: {MODEL_OUT} ({size_kb:.0f} KB)")


if __name__ == '__main__':
    main()
