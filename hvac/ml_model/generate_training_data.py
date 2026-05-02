"""
generate_training_data.py
=========================
Generuje syntetyczne sekwencje czasowe dla modelu XGBoost RUL.
Fizyka identyczna z apką HTML — wzory AI4I 2020 + losowe profile urządzeń.

Każda sekwencja = ścieżka od normalnej pracy do awarii.
Próbkowanie co 10 sekund (jak w apce po zmianie bufora).

Wyjście: hvac_training_data.csv

Uruchomienie:
    pip install numpy pandas
    python generate_training_data.py [--sequences 5000] [--output hvac_training_data.csv]
"""

import numpy as np
import pandas as pd
import argparse
from pathlib import Path

np.random.seed(42)

# ── AI4I 2020 exact ranges ───────────────────────────────────────────────────
R = {
    'air_temp':  {'min': 295.3, 'max': 304.5, 'mean': 300.0, 'std': 2.0},
    'proc_temp': {'min': 305.7, 'max': 313.8, 'mean': 310.0, 'std': 1.48},
    'rpm':       {'min': 1168,  'max': 2886,  'mean': 1538,  'std': 179},
    'torque':    {'min': 3.8,   'max': 76.6,  'mean': 40.0,  'std': 9.97},
    'tool_wear': {'min': 0,     'max': 253,   'mean': 108,   'std': 63},
    'vibration': {'min': 0.01,  'max': 2.0,   'mean': 0.04,  'std': 0.02},
}

OSF_LIMIT = {'L': 13000, 'M': 12000, 'H': 11000}
PRODUCT_TYPES = ['L', 'L', 'L', 'L', 'L', 'L', 'M', 'M', 'M', 'H']  # 60/30/10%

TICK = 10        # sekund na próbkę (zgodnie z buforem apki)
MAX_STEPS = 300  # max próbek w sekwencji (300 × 10s = 50 minut)


# ── Helpers ──────────────────────────────────────────────────────────────────
def clamp(v, a, b):
    return max(a, min(b, v))


def gauss(mean, std, lo, hi):
    return clamp(mean + np.random.randn() * std, lo, hi)


def normal_state(product_type='L'):
    return {
        'type':      product_type,
        'air_temp':  gauss(R['air_temp']['mean'],  R['air_temp']['std'],  R['air_temp']['min'],  R['air_temp']['max']),
        'proc_temp': gauss(R['proc_temp']['mean'], R['proc_temp']['std'], R['proc_temp']['min'], R['proc_temp']['max']),
        'rpm':       gauss(R['rpm']['mean'],        R['rpm']['std'],        R['rpm']['min'],        R['rpm']['max']),
        'torque':    gauss(R['torque']['mean'],     R['torque']['std'],     R['torque']['min'],     R['torque']['max']),
        'tool_wear': gauss(R['tool_wear']['mean'],  R['tool_wear']['std'],  R['tool_wear']['min'],  R['tool_wear']['max']),
        'vibration': gauss(R['vibration']['mean'],  R['vibration']['std'],  R['vibration']['min'],  R['vibration']['max']),
    }


# ── Device profile — same formula as HTML generateDeviceProfile() ────────────
def generate_device_profile():
    def rv(nominal, std):
        return clamp(nominal * (1 + np.random.randn() * std), nominal * 0.4, nominal * 2.2)

    return {
        'heatRate':    rv(0.07,  0.30),
        'heatDecay':   rv(120,   0.25),
        'heatRpmDrop': rv(1.2,   0.35),
        'clogTorque':  rv(0.20,  0.30),
        'clogTemp':    rv(0.04,  0.30),
        'clogRpm':     rv(0.6,   0.35),
        'bearingAmp':  rv(0.007, 0.35),
        'bearingMax':  rv(1.8,   0.20),
        'driftAir':    rv(0.020, 0.30),
        'driftProc':   rv(0.022, 0.30),
        'wearRate':    rv(0.50,  0.35),
        'powerDrop':   rv(7.0,   0.30),
        'powerDecay':  rv(25,    0.25),
    }


# ── Failure detection — mirror of HTML detectFailures() ──────────────────────
def detect_failures(s):
    failures = []
    dT = s['proc_temp'] - s['air_temp']
    power = s['torque'] * (s['rpm'] * 2 * np.pi / 60)
    osl = OSF_LIMIT.get(s['type'], 13000)

    if dT < 8.6 and s['rpm'] < 1380:               failures.append('HDF')
    if power < 3500 or power > 9000:               failures.append('PWF')
    if s['tool_wear'] * s['torque'] > osl:         failures.append('OSF')
    if 200 <= s['tool_wear'] <= 240:               failures.append('TWF')
    return failures


# ── Single tick — mirror of HTML interval logic (but uses TICK seconds) ──────
def tick(state, accum, mode, profile, base_rpm):
    s = dict(state)
    N = 0.03

    # 1. Sensor noise (scaled by TICK — 10 ticks worth of noise)
    scale = np.sqrt(TICK)
    s['air_temp']  = clamp(s['air_temp']  + np.random.randn() * N * R['air_temp']['std']  * scale, R['air_temp']['min'],  R['air_temp']['max'])
    s['proc_temp'] = clamp(s['proc_temp'] + np.random.randn() * N * R['proc_temp']['std'] * scale, R['proc_temp']['min'], R['proc_temp']['max'])
    s['torque']    = clamp(s['torque']    + np.random.randn() * N * R['torque']['std']    * scale, R['torque']['min'],    R['torque']['max'])
    s['vibration'] = clamp(s['vibration'] + np.random.randn() * N * 0.01                 * scale, R['vibration']['min'], R['vibration']['max'])

    # 2. Mean-reversion (each tick = 10 seconds)
    MR = 0.025 * TICK
    if mode not in ('HEAT', 'CLOG', 'DRIFT'):
        s['proc_temp'] = clamp(s['proc_temp'] + (R['proc_temp']['mean'] - s['proc_temp']) * MR, R['proc_temp']['min'], R['proc_temp']['max'])
    if mode != 'DRIFT':
        s['air_temp']  = clamp(s['air_temp']  + (R['air_temp']['mean']  - s['air_temp'])  * MR, R['air_temp']['min'],  R['air_temp']['max'])
    if mode not in ('CLOG', 'BEARING'):
        s['torque']    = clamp(s['torque']    + (R['torque']['mean']    - s['torque'])    * MR, R['torque']['min'],    R['torque']['max'])
    if mode != 'BEARING':
        s['vibration'] = clamp(s['vibration'] + (0.03 - s['vibration']) * 0.08 * TICK,    R['vibration']['min'], 0.07)

    s['rpm']       = clamp(base_rpm + np.random.randn() * N * R['rpm']['std'], R['rpm']['min'], R['rpm']['max'])

    if mode != 'WEAR':
        s['tool_wear'] = clamp(s['tool_wear'] + 0.04 * TICK, R['tool_wear']['min'], R['tool_wear']['max'])

    # 3. Anomaly curves (accumulator counts in TICKS, rate scaled by TICK seconds)
    pr = profile

    if mode == 'HEAT':
        accum['HEAT'] = accum.get('HEAT', 0) + 1
        rate = pr['heatRate'] * np.exp(accum['HEAT'] / (pr['heatDecay'] / TICK)) * TICK
        s['proc_temp'] = clamp(s['proc_temp'] + rate + np.random.randn() * 0.015, R['proc_temp']['min'], R['proc_temp']['max'])
        s['rpm']       = clamp(s['rpm'] - pr['heatRpmDrop'] * TICK + np.random.randn() * 3, R['rpm']['min'], R['rpm']['max'])

    elif mode == 'CLOG':
        accum['CLOG'] = accum.get('CLOG', 0) + 1
        s['torque']    = clamp(s['torque']    + pr['clogTorque'] * TICK + np.random.randn() * 0.04, R['torque']['min'],    R['torque']['max'])
        s['proc_temp'] = clamp(s['proc_temp'] + pr['clogTemp']   * TICK + np.random.randn() * 0.01, R['proc_temp']['min'], R['proc_temp']['max'])
        s['rpm']       = clamp(s['rpm']       - pr['clogRpm']    * TICK + np.random.randn() * 2,    R['rpm']['min'],       R['rpm']['max'])

    elif mode == 'BEARING':
        accum['BEARING'] = accum.get('BEARING', 0) + 1
        amp = clamp(0.04 + accum['BEARING'] * pr['bearingAmp'], 0.04, pr['bearingMax'])
        s['vibration'] = clamp(amp * (0.7 + 0.3 * np.sin(accum['BEARING'] * 0.9)) + abs(np.random.randn() * 0.015), 0.01, 2.0)
        s['torque']    = clamp(s['torque'] + np.random.randn() * 0.35, R['torque']['min'], R['torque']['max'])

    elif mode == 'DRIFT':
        accum['DRIFT'] = accum.get('DRIFT', 0) + 1
        s['air_temp']  = clamp(s['air_temp']  + pr['driftAir']  * TICK + np.random.randn() * 0.004, R['air_temp']['min'],  R['air_temp']['max'])
        s['proc_temp'] = clamp(s['proc_temp'] + pr['driftProc'] * TICK + np.random.randn() * 0.004, R['proc_temp']['min'], R['proc_temp']['max'])

    elif mode == 'WEAR':
        accum['WEAR'] = accum.get('WEAR', 0) + 1
        s['tool_wear'] = clamp(s['tool_wear'] + pr['wearRate'] * TICK, R['tool_wear']['min'], R['tool_wear']['max'])

    elif mode == 'POWER':
        accum['POWER'] = accum.get('POWER', 0) + 1
        drop = pr['powerDrop'] * np.exp(-accum['POWER'] / (pr['powerDecay'] / TICK)) * TICK + 0.8
        s['rpm']    = clamp(s['rpm']    - drop + np.random.randn() * 4,         R['rpm']['min'],    R['rpm']['max'])
        s['torque'] = clamp(s['torque'] + (np.random.random() - 0.5) * 2.5,     R['torque']['min'], R['torque']['max'])

    elif mode == 'HDF_DIRECT':
        # HDF: rpm drops below 1380 AND delta_temp drops below 8.6K
        # Aggressive: both conditions must be met quickly
        accum['HDF'] = accum.get('HDF', 0) + 1
        # RPM drops ~3/s -> 30/tick -> reaches 1380 in ~(1538-1380)/30 = 5 ticks
        drop = (3.0 + np.random.randn() * 0.3) * TICK
        s['rpm'] = clamp(s['rpm'] - drop, R['rpm']['min'], R['rpm']['max'])
        # proc_temp cools aggressively toward air_temp + 7K (below 8.6K threshold)
        # 25% per tick -> reaches target in ~4 ticks
        target_temp = s['air_temp'] + 7.5
        cooling_rate = 0.25
        s['proc_temp'] = clamp(
            s['proc_temp'] + (target_temp - s['proc_temp']) * cooling_rate,
            R['proc_temp']['min'], R['proc_temp']['max']
        )

    return s


# ── Simulate one sequence ─────────────────────────────────────────────────────
def simulate_sequence(seq_id, mode, product_type=None):
    """
    Simulate a single run-to-failure sequence.
    Returns list of dicts (one per tick) with rul_seconds added.
    """
    if product_type is None:
        product_type = np.random.choice(PRODUCT_TYPES)

    profile  = generate_device_profile()
    state    = normal_state(product_type)
    # For HDF_DIRECT: low tool_wear + rpm already degraded toward threshold
    if mode == 'HDF_DIRECT':
        state['tool_wear'] = gauss(30, 10, 0, 60)
        state['rpm']       = gauss(1500, 80, 1350, 1650)  # close to 1380 threshold
        state['proc_temp'] = gauss(308.5, 0.5, 307.0, 309.5)  # close to air+8.6
    # For WEAR/OSF: start with moderate tool_wear to reach threshold faster
    elif mode == 'WEAR':
        state['tool_wear'] = gauss(80, 20, 30, 150)
    accum    = {}
    base_rpm = state['rpm'] if mode == 'HDF_DIRECT' else gauss(R['rpm']['mean'], R['rpm']['std'] * 0.5, R['rpm']['min'], R['rpm']['max'])

    # Warm-up phase: run normally for 10-30 ticks before triggering anomaly
    warmup = np.random.randint(10, 31)
    for _ in range(warmup):
        state = tick(state, accum, 'NONE', profile, base_rpm)

    rows = []
    failure_step = None

    for step in range(MAX_STEPS):
        state = tick(state, accum, mode, profile, base_rpm)
        failures = detect_failures(state)

        if failures and failure_step is None:
            failure_step = step  # first tick with active failure

        power = state['torque'] * (state['rpm'] * 2 * np.pi / 60)
        dT    = state['proc_temp'] - state['air_temp']

        rows.append({
            'sequence_id':   seq_id,
            'step':          step,
            't_seconds':     step * TICK,
            'product_type':  product_type,
            'anomaly_mode':  mode,
            'air_temp':      round(state['air_temp'],  2),
            'proc_temp':     round(state['proc_temp'], 2),
            'rpm':           int(state['rpm']),
            'torque':        round(state['torque'],    2),
            'tool_wear':     int(state['tool_wear']),
            'vibration':     round(state['vibration'], 3),
            'delta_temp':    round(dT, 2),
            'power_w':       round(power, 1),
            'failure_type':  ','.join(failures) if failures else 'None',
            'has_failure':   1 if failures else 0,
            # Profile params — useful for analysis and feature engineering
            'pr_heatRate':   round(profile['heatRate'],   4),
            'pr_heatDecay':  round(profile['heatDecay'],  1),
            'pr_clogTorque': round(profile['clogTorque'], 4),
            'pr_wearRate':   round(profile['wearRate'],   4),
        })

        # Stop generating after failure is confirmed (5 consecutive ticks with failure)
        if failure_step is not None and step >= failure_step + 5:
            break

    if not rows:
        return []

    # Calculate RUL: seconds from each step to first failure
    # If no failure occurred in sequence, rul = remaining time (open-ended — skip)
    if failure_step is None:
        return []  # no failure = no RUL label, skip

    failure_t = failure_step * TICK
    for row in rows:
        rul = failure_t - row['t_seconds']
        row['rul_seconds'] = max(0, rul)

    return rows


# ── Main ──────────────────────────────────────────────────────────────────────
ANOMALY_MODES = ['HEAT', 'CLOG', 'BEARING', 'WEAR', 'POWER', 'DRIFT', 'HDF_DIRECT']

# Distribution of failure modes (roughly matching AI4I 2020 frequencies)
# HDF = 115 cases, PWF = 95, OSF = 98, TWF = 120 in AI4I dataset
MODE_WEIGHTS = {
    'HDF_DIRECT': 0.30,  # HDF — most common: rpm drops + poor heat dissipation
    'POWER':      0.25,  # PWF
    'WEAR':       0.22,  # OSF + TWF
    'CLOG':       0.10,  # mixed HDF+OSF
    'BEARING':    0.07,  # vibration-related
    'HEAT':       0.03,  # thermal burn (extreme heat)
    'DRIFT':      0.03,  # sensor drift
}

def main():
    parser = argparse.ArgumentParser(description='Generate HVAC training data')
    parser.add_argument('--sequences', type=int, default=5000, help='Number of sequences to generate')
    parser.add_argument('--output',    type=str, default='hvac_training_data.csv')
    args = parser.parse_args()

    print(f"Generating {args.sequences} sequences...")
    modes   = list(MODE_WEIGHTS.keys())
    weights = list(MODE_WEIGHTS.values())

    all_rows = []
    seq_id   = 0
    skipped  = 0

    for i in range(args.sequences):
        if i % 500 == 0:
            print(f"  {i}/{args.sequences} sequences... ({len(all_rows)} rows so far)")

        mode         = np.random.choice(modes, p=weights)
        product_type = np.random.choice(PRODUCT_TYPES)
        rows         = simulate_sequence(seq_id, mode, product_type)

        if rows:
            all_rows.extend(rows)
            seq_id += 1
        else:
            skipped += 1

    df = pd.DataFrame(all_rows)
    df.to_csv(args.output, index=False)

    print(f"\n=== Done ===")
    print(f"Sequences generated: {seq_id} (skipped {skipped} with no failure)")
    print(f"Total rows: {len(df)}")
    print(f"Output: {args.output}")
    print(f"\nFailure type distribution:")
    print(df[df['has_failure']==1]['failure_type'].value_counts())
    print(f"\nRUL stats:")
    print(df['rul_seconds'].describe())
    print(f"\nSample row:")
    print(df[df['rul_seconds']>0].head(1).to_string())


if __name__ == '__main__':
    main()
