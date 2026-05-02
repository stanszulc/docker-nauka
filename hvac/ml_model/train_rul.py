"""
train_rul.py
============
Trenuje XGBoost Regressor do predykcji RUL (Remaining Useful Life).
Wejście:  hvac_training_data.csv (z generate_training_data.py)
Wyjście:  hvac_rul_model.pkl

Uruchomienie:
    pip install xgboost scikit-learn pandas numpy joblib
    python train_rul.py

Po wytrenowaniu skopiuj model na VM:
    scp -i ~/ssh-key-2026-03-11.key hvac_rul_model.pkl ubuntu@92.5.14.76:~/docker-nauka/hvac/ml_model/
    cd ~/docker-nauka/hvac && docker-compose restart hvac_consumer
"""

import numpy as np
import pandas as pd
import joblib
import argparse
from pathlib import Path

from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split, GroupShuffleSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import LabelEncoder

# ── Config ────────────────────────────────────────────────────────────────────
CSV_PATH    = "hvac_training_data.csv"
MODEL_OUT   = "hvac_rul_model.pkl"
WINDOW      = 3      # rolling window size (3 ticks × 10s = 30s history)
TEST_SIZE   = 0.2
RANDOM_STATE = 42

# Base sensor features
BASE_FEATURES = [
    'air_temp', 'proc_temp', 'rpm', 'torque',
    'tool_wear', 'vibration', 'delta_temp', 'power_w'
]

# ── Feature engineering ───────────────────────────────────────────────────────
def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add rolling statistics per sequence.
    Window of 3 ticks = 30 seconds of history.
    """
    df = df.copy().sort_values(['sequence_id', 'step'])

    # Label encode product_type (L=0, M=1, H=2)
    le = LabelEncoder()
    df['product_type_enc'] = le.fit_transform(df['product_type'])

    grp = df.groupby('sequence_id')

    for feat in BASE_FEATURES:
        # Rolling mean (smoothed current value)
        df[f'{feat}_rmean'] = grp[feat].transform(
            lambda x: x.rolling(WINDOW, min_periods=1).mean()
        )
        # Rolling gradient (rate of change) — key for RUL prediction
        df[f'{feat}_rgrad'] = grp[feat].transform(
            lambda x: x.diff().rolling(WINDOW, min_periods=1).mean()
        )
        # Rolling std (variability — important for bearing/vibration)
        df[f'{feat}_rstd'] = grp[feat].transform(
            lambda x: x.rolling(WINDOW, min_periods=1).std().fillna(0)
        )

    # Derived features
    df['proc_temp_rate'] = df['proc_temp_rgrad']          # K per 10s
    df['rpm_rate']       = df['rpm_rgrad']                 # rpm per 10s
    df['torque_rate']    = df['torque_rgrad']              # Nm per 10s
    df['vibration_rate'] = df['vibration_rgrad']           # mm/s² per 10s

    # Proximity to HDF threshold (delta_temp - 8.6, clamped)
    df['hdf_margin'] = df['delta_temp'] - 8.6

    # Proximity to PWF thresholds
    df['pwf_low_margin']  = df['power_w'] - 3500
    df['pwf_high_margin'] = 9000 - df['power_w']
    df['pwf_margin']      = df[['pwf_low_margin', 'pwf_high_margin']].min(axis=1)

    # OSF proximity (tool_wear × torque vs limit)
    osf_limits = {'L': 13000, 'M': 12000, 'H': 11000}
    df['osf_limit']  = df['product_type'].map(osf_limits)
    df['osf_margin'] = df['osf_limit'] - df['tool_wear'] * df['torque']

    # TWF proximity (tool_wear to 200)
    df['twf_margin'] = 200 - df['tool_wear']

    # Step (how far into the sequence)
    df['step_norm'] = df.groupby('sequence_id')['step'].transform(
        lambda x: x / x.max() if x.max() > 0 else 0
    )

    return df


def get_feature_cols(df: pd.DataFrame) -> list:
    """Return all feature column names."""
    cols = ['product_type_enc', 'step_norm',
            'hdf_margin', 'pwf_margin', 'osf_margin', 'twf_margin',
            'proc_temp_rate', 'rpm_rate', 'torque_rate', 'vibration_rate']

    for feat in BASE_FEATURES:
        cols += [f'{feat}_rmean', f'{feat}_rgrad', f'{feat}_rstd']

    # Keep only columns that exist
    return [c for c in cols if c in df.columns]


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',  default=CSV_PATH)
    parser.add_argument('--output', default=MODEL_OUT)
    parser.add_argument('--trees',  type=int, default=300)
    parser.add_argument('--depth',  type=int, default=8)
    args = parser.parse_args()

    print(f"Loading {args.input}...")
    df = pd.read_csv(args.input)
    print(f"  {len(df)} rows, {df['sequence_id'].nunique()} sequences")

    print("Engineering features...")
    df = engineer_features(df)
    feature_cols = get_feature_cols(df)
    print(f"  {len(feature_cols)} features")

    # Split by sequence_id (no data leakage between train/test)
    seq_ids = df['sequence_id'].unique()
    train_ids, test_ids = train_test_split(
        seq_ids, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    train = df[df['sequence_id'].isin(train_ids)]
    test  = df[df['sequence_id'].isin(test_ids)]

    X_train = train[feature_cols]
    y_train = train['rul_seconds']
    X_test  = test[feature_cols]
    y_test  = test['rul_seconds']

    print(f"  Train: {len(X_train)} rows ({len(train_ids)} sequences)")
    print(f"  Test:  {len(X_test)} rows  ({len(test_ids)} sequences)")

    print(f"\nTraining XGBoost (n_estimators={args.trees}, max_depth={args.depth})...")
    model = XGBRegressor(
        n_estimators=args.trees,
        max_depth=args.depth,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=RANDOM_STATE,
        n_jobs=-1,
        tree_method='hist',     # fast
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=50,
    )

    # ── Evaluation ────────────────────────────────────────────────────────────
    y_pred = model.predict(X_test)
    y_pred = np.clip(y_pred, 0, None)  # RUL can't be negative

    mae  = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))

    print(f"\n=== Evaluation ===")
    print(f"MAE:  {mae:.1f}s  ({mae/60:.1f} min)")
    print(f"RMSE: {rmse:.1f}s  ({rmse/60:.1f} min)")

    # Per-RUL-bucket accuracy (important: how good near failure?)
    buckets = [(0, 60), (60, 300), (300, 600), (600, 1200), (1200, 9999)]
    print(f"\nMAE by RUL bucket:")
    for lo, hi in buckets:
        mask = (y_test >= lo) & (y_test < hi)
        if mask.sum() > 0:
            bucket_mae = mean_absolute_error(y_test[mask], y_pred[mask])
            label = f"<{hi//60}min" if hi < 9999 else f">{lo//60}min"
            print(f"  RUL {label:>10}: MAE={bucket_mae:.0f}s ({bucket_mae/60:.1f}min)  n={mask.sum()}")

    # Feature importance
    importance = pd.Series(model.feature_importances_, index=feature_cols)
    print(f"\nTop 10 features:")
    print(importance.nlargest(10).to_string())

    # ── Save model ────────────────────────────────────────────────────────────
    bundle = {
        'model':        model,
        'feature_cols': feature_cols,
        'window':       WINDOW,
        'base_features': BASE_FEATURES,
        'version':      '1.0',
    }
    joblib.dump(bundle, args.output)
    size_kb = Path(args.output).stat().st_size / 1024
    print(f"\nModel saved: {args.output} ({size_kb:.0f} KB)")
    print(f"\nDeploy to VM:")
    print(f"  scp -i ~/ssh-key-2026-03-11.key {args.output} ubuntu@92.5.14.76:~/docker-nauka/hvac/ml_model/")
    print(f"  cd ~/docker-nauka/hvac && docker-compose restart hvac_consumer")


if __name__ == '__main__':
    main()
