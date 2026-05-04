"""
train_v5.py
===========
Trenuje XGBoost klasyfikator na danych v5.

Zmiany względem v4:
1. load_to_temp_ratio w BASE_FEATURES
2. Trend features (trend_sl, trend_ml) — short-long, mid-long
3. std_mid — zmienność w oknie 200s
4. min_child_weight 5→3 (lepiej dla rzadkich pre_failure HDF)
5. Brak własnego engineer_features — rolling już w CSV z generatora
6. Proximity features: thermal_instability, mechanical_instability

Wyjście: hvac_classifier_v5.pkl

Uruchomienie:
    python train_v5.py
"""

import numpy as np
import pandas as pd
import joblib
from pathlib import Path

from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (classification_report, confusion_matrix,
                             roc_auc_score, precision_recall_curve)

# ── Config ────────────────────────────────────────────────────────────────────
CSV_PATH     = "hvac_training_v5.csv"
MODEL_OUT    = "hvac_classifier_v5.pkl"
RANDOM_STATE = 42
TEST_SIZE    = 0.2

WIN_SHORT = 5
WIN_MID   = 20
WIN_LONG  = 60

# Sensory bazowe — bez rolling (rolling już w CSV)
BASE_SENSORS = [
    'air_temp', 'proc_temp', 'rpm', 'torque',
    'vibration', 'delta_temp', 'power_w',
    'proc_temp_velocity', 'rpm_velocity',
    'torque_velocity', 'vibration_velocity',
    'load_to_temp_ratio',
]


def get_feature_cols(df: pd.DataFrame) -> list:
    cols = []

    # 1. Surowe sensory
    cols += [s for s in BASE_SENSORS if s in df.columns]

    # 2. Rolling features z generatora (ma_short/mid/long, trend_sl/ml, std_mid)
    rolling_suffixes = ['_ma_short', '_ma_mid', '_ma_long',
                        '_trend_sl', '_trend_ml', '_std_mid']
    for s in BASE_SENSORS:
        for suf in rolling_suffixes:
            col = f'{s}{suf}'
            if col in df.columns:
                cols.append(col)

    # 3. Uptime i buffer fill
    for c in ['uptime_norm', 'buffer_fill_short', 'buffer_fill_mid', 'buffer_fill_long']:
        if c in df.columns:
            cols.append(c)

    # 4. Proximity features (obliczone poniżej)
    proximity = [
        'hdf_margin', 'pwf_margin',
        'thermal_instability', 'mechanical_instability',
        'load_ratio_trend',
    ]
    cols += [c for c in proximity if c in df.columns]

    return cols


def engineer_proximity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tylko cechy pochodne których nie ma w CSV z generatora.
    Rolling stats już są — tu liczymy cross-features i proximity.
    """
    df = df.copy()

    # Uptime norm
    df['uptime_norm'] = df['uptime_seconds'] / 1800.0

    # Proximity do progów awarii
    df['hdf_margin']  = df['delta_temp'] - 8.6          # ujemny = blisko HDF
    df['pwf_low']     = df['power_w'] - 3500
    df['pwf_high']    = 9000 - df['power_w']
    df['pwf_margin']  = df[['pwf_low', 'pwf_high']].min(axis=1)

    # Thermal instability — std proc_temp * delta_temp std
    # Oba dostępne z generatora jako *_std_mid
    if 'proc_temp_std_mid' in df.columns and 'delta_temp_std_mid' in df.columns:
        df['thermal_instability'] = df['proc_temp_std_mid'] * df['delta_temp_std_mid']
    elif 'proc_temp_std_mid' in df.columns:
        df['thermal_instability'] = df['proc_temp_std_mid']

    # Mechanical instability — vibration std * torque std
    if 'vibration_std_mid' in df.columns and 'torque_std_mid' in df.columns:
        df['mechanical_instability'] = df['vibration_std_mid'] * df['torque_std_mid']
    elif 'vibration_std_mid' in df.columns:
        df['mechanical_instability'] = df['vibration_std_mid']

    # load_ratio_trend — trend load_to_temp_ratio (short - long)
    # to samo co load_to_temp_ratio_trend_sl z generatora — alias dla czytelności
    if 'load_to_temp_ratio_trend_sl' in df.columns:
        df['load_ratio_trend'] = df['load_to_temp_ratio_trend_sl']

    return df


def main():
    print(f"Loading {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH)
    print(f"  {len(df)} rows, {df['sequence_id'].nunique()} sequences")
    print(f"  Columns in CSV: {len(df.columns)}")

    # Tylko klasy 0 i 1 — ignorujemy aktywną awarię (klasa 2)
    df = df[df['label'].isin([0, 1])].copy()
    print(f"  After filtering (0+1 only): {len(df)} rows")
    print(f"\nLabel distribution:")
    print(df['label'].value_counts().sort_index().rename({0: 'Normal', 1: 'Pre-failure'}))

    print("\nEngineering proximity features...")
    df = engineer_proximity(df)

    feature_cols = get_feature_cols(df)
    print(f"  {len(feature_cols)} features total")

    # Podział na train/test po sequence_id — bez data leakage
    seq_ids = df['sequence_id'].unique()
    train_ids, test_ids = train_test_split(
        seq_ids, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    train = df[df['sequence_id'].isin(train_ids)]
    test  = df[df['sequence_id'].isin(test_ids)]

    X_train = train[feature_cols].fillna(0)
    y_train = train['label']
    X_test  = test[feature_cols].fillna(0)
    y_test  = test['label']

    print(f"\n  Train: {len(X_train)} rows ({len(train_ids)} sequences)")
    print(f"  Test:  {len(X_test)} rows  ({len(test_ids)} sequences)")

    # Balans klas
    n_normal     = (y_train == 0).sum()
    n_prefailure = (y_train == 1).sum()
    scale_pos_weight = n_normal / n_prefailure
    print(f"\n  scale_pos_weight: {scale_pos_weight:.2f}")

    print(f"\nTraining XGBoost v5...")
    model = XGBClassifier(
        n_estimators=600,
        max_depth=8,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=3,       # było 5 → lepiej dla rzadkich HDF
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

    cm = confusion_matrix(y_test, y_pred)
    print(f"Confusion matrix:")
    print(f"  TN={cm[0,0]:6d}  FP={cm[0,1]:6d}")
    print(f"  FN={cm[1,0]:6d}  TP={cm[1,1]:6d}")

    auc = roc_auc_score(y_test, y_pred_prob)
    print(f"\nROC-AUC: {auc:.4f}")

    # Precision-Recall przy różnych progach
    precision, recall, thresholds = precision_recall_curve(y_test, y_pred_prob)
    print(f"\nPrecision-Recall przy różnych progach:")
    for threshold in [0.3, 0.35, 0.4, 0.5, 0.6, 0.7]:
        idx = np.argmin(np.abs(thresholds - threshold))
        print(f"  threshold={threshold:.2f}: "
              f"precision={precision[idx]:.3f}  recall={recall[idx]:.3f}")

    # Per typ awarii (jeśli failure_type w test)
    if 'failure_type' in test.columns and 'anomaly_mode' in test.columns:
        print(f"\nRecall per typ awarii (pre-failure):")
        pre_test = test[test['label'] == 1].copy()
        pre_test['pred'] = model.predict(pre_test[feature_cols].fillna(0))
        for mode in ['HDF', 'PWF', 'CLOG', 'BEARING']:
            subset = pre_test[pre_test['anomaly_mode'] == mode]
            if len(subset) == 0:
                continue
            tp = (subset['pred'] == 1).sum()
            fn = (subset['pred'] == 0).sum()
            r  = tp / (tp + fn) if (tp + fn) > 0 else 0
            print(f"  {mode:8s}: Recall={r:.1%}  TP={tp}  FN={fn}")

    # Top features
    importance = pd.Series(model.feature_importances_, index=feature_cols)
    print(f"\nTop 15 features:")
    print(importance.nlargest(15).to_string())

    # ── Save ───────────────────────────────────────────────────────────────────
    bundle = {
        'model':          model,
        'feature_cols':   feature_cols,
        'base_sensors':   BASE_SENSORS,
        'win_short':      WIN_SHORT,
        'win_mid':        WIN_MID,
        'win_long':       WIN_LONG,
        'model_type':     'classifier',
        'version':        '5.0',
        'threshold':      0.5,
    }
    joblib.dump(bundle, MODEL_OUT)
    size_kb = Path(MODEL_OUT).stat().st_size / 1024
    print(f"\nModel saved: {MODEL_OUT} ({size_kb:.0f} KB)")


if __name__ == '__main__':
    main()
