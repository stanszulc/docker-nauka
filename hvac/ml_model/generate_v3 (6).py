"""
generate_v5.py
==============
Generator danych treningowych v5.

Zmiany względem v4:
1. load_to_temp_ratio — nowa cecha (power_w / delta_temp) — klucz do HDF
2. Rolling averages na 3 oknach: SHORT=5, MID=20, LONG=60 ticków
   + trend = MA_short - MA_long (sygnał narastania awarii)
3. HDF init — szerszy zakres startowy (trudniejsze przypadki)
4. Labeling window: PRE_FAILURE_WINDOW 30→50 ticków (8 min)
5. Sekwencja kończy się failure_step+20 zamiast +5 (więcej kontekstu)

Wyjście: hvac_training_v5.csv
"""

import numpy as np
import pandas as pd
import argparse
import uuid


np.random.seed(42)

# ── Zakresy sensorów HVAC ─────────────────────────────────────────────────────
R = {
    'air_temp':  {'min': 295.3, 'max': 304.5, 'mean': 300.0, 'std': 2.0},
    'proc_temp': {'min': 305.7, 'max': 313.8, 'mean': 310.0, 'std': 1.48},
    'rpm':       {'min': 1168,  'max': 2886,  'mean': 1538,  'std': 179},
    'torque':    {'min': 3.8,   'max': 76.6,  'mean': 40.0,  'std': 9.97},
    'vibration': {'min': 0.01,  'max': 2.0,   'mean': 0.04,  'std': 0.02},
}

PRODUCT_TYPES = ['L', 'L', 'L', 'L', 'L', 'L', 'M', 'M', 'M', 'H']
TICK = 10         # sekund na próbkę
MAX_STEPS = 300   # max próbek (300 × 10s = 50 minut)

# ── Zmiana #4: szersze okno etykietowania ─────────────────────────────────────
PRE_FAILURE_WINDOW = 50   # było 30 → teraz ~8 min przed awarią

# Okna kroczące — trzy poziomy
WIN_SHORT = 5    #  50s — szybkie zmiany, spike'i
WIN_MID   = 20   # 200s — trend lokalny (poprzednie WINDOW)
WIN_LONG  = 60   # 600s — baseline urządzenia

MODE_WEIGHTS = {
    'HDF':     0.30,
    'PWF':     0.30,
    'CLOG':    0.25,
    'BEARING': 0.15,
}

# Cechy bazowe — sensory + pochodne
BASE_SENSORS = [
    'air_temp', 'proc_temp', 'rpm', 'torque',
    'vibration', 'delta_temp', 'power_w',
    'proc_temp_velocity', 'rpm_velocity',
    'torque_velocity', 'vibration_velocity',
    'load_to_temp_ratio',   # NOWE
]


def clamp(v, a, b):
    return max(a, min(b, v))


def gauss(mean, std, lo, hi):
    return clamp(mean + np.random.randn() * std, lo, hi)


def normal_state():
    return {
        'air_temp':  gauss(R['air_temp']['mean'],  R['air_temp']['std'],  R['air_temp']['min'],  R['air_temp']['max']),
        'proc_temp': gauss(R['proc_temp']['mean'], R['proc_temp']['std'], R['proc_temp']['min'], R['proc_temp']['max']),
        'rpm':       gauss(R['rpm']['mean'],        R['rpm']['std'],        R['rpm']['min'],        R['rpm']['max']),
        'torque':    gauss(R['torque']['mean'],     R['torque']['std'],     R['torque']['min'],     R['torque']['max']),
        'vibration': gauss(R['vibration']['mean'],  R['vibration']['std'],  R['vibration']['min'],  R['vibration']['max']),
    }


def generate_device_profile():
    def rv(nominal, std):
        return clamp(nominal * (1 + np.random.randn() * std), nominal * 0.4, nominal * 2.2)
    return {
        # Cel: ~15-20 min narastania awarii
        'hdfCoolRate':   rv(0.012, 0.30),  # bylo 0.25
        'hdfRpmDrop':    rv(0.079, 0.35),  # bylo 3.0
        'pwfDirection':  1 if np.random.random() > 0.5 else -1,
        'pwfRpmDrop':    rv(0.185, 0.30),  # bylo 4.0
        'pwfTorqueDrop': rv(0.015, 0.35),  # bylo 0.8
        'clogTorque':    rv(0.004, 0.30),  # bylo 0.25
        'clogRpmRise':   rv(0.80,  0.35),  # bylo 1.5
        'clogTempRise':  rv(0.001, 0.30),  # bylo 0.05
        'bearingAmp':    rv(0.008, 0.35),  # bez zmian
        'bearingMax':    rv(1.8,   0.20),
    }


def detect_failures(s):
    failures = []
    dT    = s['proc_temp'] - s['air_temp']
    power = s['torque'] * (s['rpm'] * 2 * np.pi / 60)
    if dT < 8.6 and s['rpm'] < 1380:
        failures.append('HDF')
    if power < 3500 or power > 9000:
        failures.append('PWF')
    if s['torque'] > 60 or dT > 13.5:
        failures.append('CLOG')
    if s['vibration'] > 0.8:
        failures.append('BEARING')
    return failures


def tick(state, accum, mode, profile):
    s  = dict(state)
    N  = 0.03
    sc = np.sqrt(TICK)
    pr = profile

    s['air_temp']  = clamp(s['air_temp']  + np.random.randn() * N * R['air_temp']['std']  * sc, R['air_temp']['min'],  R['air_temp']['max'])
    s['proc_temp'] = clamp(s['proc_temp'] + np.random.randn() * N * R['proc_temp']['std'] * sc, R['proc_temp']['min'], R['proc_temp']['max'])
    s['torque']    = clamp(s['torque']    + np.random.randn() * N * R['torque']['std']    * sc, R['torque']['min'],    R['torque']['max'])
    s['vibration'] = clamp(s['vibration'] + np.random.randn() * N * 0.01                 * sc, R['vibration']['min'], R['vibration']['max'])
    s['rpm']       = clamp(s['rpm']       + np.random.randn() * N * R['rpm']['std'],            R['rpm']['min'],       R['rpm']['max'])

    MR = 0.025 * TICK
    if mode not in ('HDF', 'CLOG'):
        s['proc_temp'] = clamp(s['proc_temp'] + (R['proc_temp']['mean'] - s['proc_temp']) * MR, R['proc_temp']['min'], R['proc_temp']['max'])
    if mode != 'CLOG':
        s['torque']    = clamp(s['torque']    + (R['torque']['mean']    - s['torque'])    * MR, R['torque']['min'],    R['torque']['max'])
    if mode != 'BEARING':
        s['vibration'] = clamp(s['vibration'] + (0.03 - s['vibration']) * 0.08 * TICK,    R['vibration']['min'], 0.07)
    s['air_temp'] = clamp(s['air_temp'] + (R['air_temp']['mean'] - s['air_temp']) * MR, R['air_temp']['min'], R['air_temp']['max'])

    if mode == 'HDF':
        accum['HDF'] = accum.get('HDF', 0) + 1
        ac    = accum['HDF']
        accel = 1 + (ac / 20) ** 1.5
        drop  = pr['hdfRpmDrop'] * TICK * accel
        s['rpm'] = clamp(s['rpm'] - drop, R['rpm']['min'], R['rpm']['max'])
        target = s['air_temp'] + 7.5
        s['proc_temp'] = clamp(
            s['proc_temp'] + (target - s['proc_temp']) * pr['hdfCoolRate'],
            R['proc_temp']['min'], R['proc_temp']['max']
        )

    elif mode == 'PWF':
        accum['PWF'] = accum.get('PWF', 0) + 1
        ac    = accum['PWF']
        accel = 1 + (ac / 25) ** 1.5
        if pr['pwfDirection'] == -1:
            s['rpm']    = clamp(s['rpm']    - pr['pwfRpmDrop']    * TICK * accel, R['rpm']['min'],    R['rpm']['max'])
            s['torque'] = clamp(s['torque'] - pr['pwfTorqueDrop'] * TICK * accel, R['torque']['min'], R['torque']['max'])
        else:
            s['rpm']    = clamp(s['rpm']    + pr['pwfRpmDrop']    * TICK * accel, R['rpm']['min'],    R['rpm']['max'])
            s['torque'] = clamp(s['torque'] + pr['pwfTorqueDrop'] * TICK * accel, R['torque']['min'], R['torque']['max'])

    elif mode == 'CLOG':
        accum['CLOG'] = accum.get('CLOG', 0) + 1
        ac    = accum['CLOG']
        accel = 1 + (ac / 30) ** 2
        s['torque']    = clamp(s['torque']    + pr['clogTorque']   * TICK * accel, R['torque']['min'],    R['torque']['max'])
        s['rpm']       = clamp(s['rpm']       + pr['clogRpmRise']  * TICK * accel, R['rpm']['min'],       R['rpm']['max'])
        s['proc_temp'] = clamp(s['proc_temp'] + pr['clogTempRise'] * TICK * accel, R['proc_temp']['min'], R['proc_temp']['max'])

    elif mode == 'BEARING':
        accum['BEARING'] = accum.get('BEARING', 0) + 1
        ac  = accum['BEARING']
        amp = clamp(0.04 + ac * pr['bearingAmp'], 0.04, pr['bearingMax'])
        s['vibration'] = clamp(
            amp * (0.7 + 0.3 * np.sin(ac * 0.9)) + abs(np.random.randn() * 0.015),
            0.01, 2.0
        )
        s['torque'] = clamp(s['torque'] + 0.05 * ac, R['torque']['min'], R['torque']['max'])

    return s


# ── Rolling stats helper ──────────────────────────────────────────────────────
class RollingBuffer:
    """Trzyma historię sensorów dla bieżącej sekwencji — liczy MA i trend."""

    def __init__(self):
        # numpy array zamiast deque — szybsze slicing
        self.bufs = {s: np.empty(WIN_LONG, dtype=np.float32) for s in BASE_SENSORS}
        self.counts = {s: 0 for s in BASE_SENSORS}

    def push(self, row: dict):
        for s in BASE_SENSORS:
            if s in row:
                c = self.counts[s]
                if c < WIN_LONG:
                    self.bufs[s][c] = row[s]
                else:
                    # przesunięcie o 1 w lewo
                    self.bufs[s][:-1] = self.bufs[s][1:]
                    self.bufs[s][-1]  = row[s]
                self.counts[s] = min(c + 1, WIN_LONG)

    def build_rolling_row(self, step: int) -> dict:
        """Zwraca słownik z rolling features dla wszystkich sensorów."""
        out = {}
        for s in BASE_SENSORS:
            n   = self.counts[s]
            buf = self.bufs[s][:n]   # tylko wypełnione

            if n == 0:
                short, mid, long_, std_mid = 0.0, 0.0, 0.0, 0.0
            else:
                # slicing raz — bez list()
                short  = float(buf[-WIN_SHORT:].mean()) if n >= WIN_SHORT else float(buf.mean())
                mid    = float(buf[-WIN_MID:].mean())   if n >= WIN_MID   else float(buf.mean())
                long_  = float(buf.mean())
                std_mid = float(buf[-WIN_MID:].std())   if n >= WIN_MID   else float(buf.std()) if n > 1 else 0.0

            out[f'{s}_ma_short']  = short
            out[f'{s}_ma_mid']    = mid
            out[f'{s}_ma_long']   = long_
            out[f'{s}_trend_sl']  = short - long_
            out[f'{s}_trend_ml']  = mid   - long_
            out[f'{s}_std_mid']   = std_mid

        out['buffer_fill_short'] = min(1.0, (step + 1) / WIN_SHORT)
        out['buffer_fill_mid']   = min(1.0, (step + 1) / WIN_MID)
        out['buffer_fill_long']  = min(1.0, (step + 1) / WIN_LONG)
        return out


# ── Normal sequence ───────────────────────────────────────────────────────────
def simulate_normal_sequence(seq_id, steps=None):
    if steps is None:
        steps = np.random.randint(80, 200)

    session_id = str(uuid.uuid4())
    profile    = generate_device_profile()
    state      = normal_state()
    accum      = {}
    rb         = RollingBuffer()

    rows       = []
    prev_state = dict(state)
    temp_drift = np.random.randn() * 0.3

    pseudo_start = np.random.randint(20, steps - 20) if steps > 40 else -1
    pseudo_len   = np.random.randint(3, 8)
    pseudo_type  = np.random.choice(['torque_spike', 'rpm_drop', 'vibration_bump'])

    for step in range(steps):
        prev_state = dict(state)
        state = tick(state, accum, 'NONE', profile)

        state['air_temp']  = clamp(state['air_temp']  + temp_drift * 0.01, R['air_temp']['min'],  R['air_temp']['max'])
        state['proc_temp'] = clamp(state['proc_temp'] + temp_drift * 0.01, R['proc_temp']['min'], R['proc_temp']['max'])

        in_pseudo = pseudo_start > 0 and pseudo_start <= step < pseudo_start + pseudo_len
        if in_pseudo:
            if pseudo_type == 'torque_spike':
                state['torque'] = clamp(state['torque'] + np.random.uniform(5, 15), R['torque']['min'], 58.0)
            elif pseudo_type == 'rpm_drop':
                state['rpm'] = clamp(state['rpm'] - np.random.uniform(50, 150), 1400.0, R['rpm']['max'])
            elif pseudo_type == 'vibration_bump':
                state['vibration'] = clamp(state['vibration'] + np.random.uniform(0.1, 0.4), R['vibration']['min'], 0.75)

        power = state['torque'] * (state['rpm'] * 2 * np.pi / 60)
        dT    = state['proc_temp'] - state['air_temp']
        ltr   = power / (abs(dT) + 1e-3)   # load_to_temp_ratio

        base_row = {
            'air_temp':             round(state['air_temp'],   2),
            'proc_temp':            round(state['proc_temp'],  2),
            'rpm':                  int(state['rpm']),
            'torque':               round(state['torque'],     2),
            'vibration':            round(state['vibration'],  3),
            'delta_temp':           round(dT,    2),
            'power_w':              round(power, 1),
            'proc_temp_velocity':   round(state['proc_temp'] - prev_state['proc_temp'], 4),
            'rpm_velocity':         round(state['rpm']       - prev_state['rpm'],       4),
            'torque_velocity':      round(state['torque']    - prev_state['torque'],    4),
            'vibration_velocity':   round(state['vibration'] - prev_state['vibration'], 4),
            'load_to_temp_ratio':   round(ltr, 3),
        }
        rb.push(base_row)
        rolling = rb.build_rolling_row(step)

        rows.append({
            'sequence_id':    seq_id,
            'session_id':     session_id,
            'step':           step,
            't_seconds':      step * TICK,
            'uptime_seconds': step * TICK,
            'product_type':   'L',
            'anomaly_mode':   'NONE',
            **base_row,
            **rolling,
            'failure_type':   'None',
            'has_failure':    0,
            'rul_seconds':    9999,
            'label':          0,
        })
    return rows


# ── Anomaly sequence ──────────────────────────────────────────────────────────
def simulate_sequence(seq_id, mode):
    product_type = np.random.choice(PRODUCT_TYPES)
    session_id   = str(uuid.uuid4())
    profile      = generate_device_profile()
    accum        = {}
    rb           = RollingBuffer()

    # ── Zmiana #3: szerszy zakres startowy HDF ────────────────────────────────
    state = normal_state()
    if mode == 'HDF':
        state['rpm']       = gauss(1700, 180, 1168, 2200)   # było gauss(1500,60,...)
        state['proc_temp'] = gauss(309.5, 1.0, 307.0, 312.0)  # było gauss(308.5,0.5,...)

    warmup = np.random.randint(70, 130)  # min 700s > WIN_LONG=600s
    for _ in range(warmup):
        state = tick(state, accum, 'NONE', profile)

    rows         = []
    failure_step = None
    prev_state   = dict(state)

    for step in range(MAX_STEPS):
        prev_state = dict(state)
        state      = tick(state, accum, mode, profile)
        failures   = detect_failures(state)

        if failures and failure_step is None:
            failure_step = step

        power = state['torque'] * (state['rpm'] * 2 * np.pi / 60)
        dT    = state['proc_temp'] - state['air_temp']
        ltr   = power / (abs(dT) + 1e-3)   # load_to_temp_ratio

        base_row = {
            'air_temp':             round(state['air_temp'],   2),
            'proc_temp':            round(state['proc_temp'],  2),
            'rpm':                  int(state['rpm']),
            'torque':               round(state['torque'],     2),
            'vibration':            round(state['vibration'],  3),
            'delta_temp':           round(dT,    2),
            'power_w':              round(power, 1),
            'proc_temp_velocity':   round(state['proc_temp'] - prev_state['proc_temp'], 4),
            'rpm_velocity':         round(state['rpm']       - prev_state['rpm'],       4),
            'torque_velocity':      round(state['torque']    - prev_state['torque'],    4),
            'vibration_velocity':   round(state['vibration'] - prev_state['vibration'], 4),
            'load_to_temp_ratio':   round(ltr, 3),
        }
        rb.push(base_row)
        rolling = rb.build_rolling_row(step)

        rows.append({
            'sequence_id':    seq_id,
            'session_id':     session_id,
            'step':           step,
            't_seconds':      step * TICK,
            'uptime_seconds': step * TICK,
            'product_type':   product_type,
            'anomaly_mode':   mode,
            **base_row,
            **rolling,
            'failure_type':   ','.join(failures) if failures else 'None',
            'has_failure':    1 if failures else 0,
        })

        # ── Zmiana #5: failure_step+20 zamiast +5 ────────────────────────────
        if failure_step is not None and step >= failure_step + 20:
            break

    if not rows or failure_step is None:
        return []

    # ── Zmiana #4: PRE_FAILURE_WINDOW=50 ─────────────────────────────────────
    failure_t = failure_step * TICK
    for row in rows:
        rul = failure_t - row['t_seconds']
        row['rul_seconds'] = max(0, rul)

        if row['has_failure'] == 1:
            row['label'] = 2
        elif rul <= PRE_FAILURE_WINDOW * TICK:
            row['label'] = 1
        else:
            row['label'] = 0

    return rows


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sequences', type=int, default=5000)
    parser.add_argument('--output',    type=str, default='hvac_training_v5.csv')
    args = parser.parse_args()

    modes   = list(MODE_WEIGHTS.keys())
    weights = list(MODE_WEIGHTS.values())

    print(f"Generating {args.sequences} sequences...")
    print(f"  PRE_FAILURE_WINDOW: {PRE_FAILURE_WINDOW} ticków = {PRE_FAILURE_WINDOW*TICK}s")
    print(f"  Rolling windows: SHORT={WIN_SHORT}, MID={WIN_MID}, LONG={WIN_LONG} ticków")

    all_rows = []
    seq_id   = 0
    skipped  = 0

    n_normal_only = int(args.sequences * 0.6)
    n_anomaly     = args.sequences - n_normal_only

    print(f"  Normal-only: {n_normal_only} (60%)")
    print(f"  Anomaly:     {n_anomaly} (40%)")

    for i in range(n_normal_only):
        rows = simulate_normal_sequence(seq_id, steps=np.random.randint(40, 80))
        all_rows.extend(rows)
        seq_id += 1

    for i in range(n_anomaly):
        if i % 500 == 0:
            print(f"  Anomaly {i}/{n_anomaly}... ({len(all_rows)} rows)")

        mode = np.random.choice(modes, p=weights)
        rows = simulate_sequence(seq_id, mode)

        if rows:
            all_rows.extend(rows)
            seq_id += 1
        else:
            skipped += 1

    df = pd.DataFrame(all_rows)
    df.to_csv(args.output, index=False)

    print(f"\n=== Done ===")
    print(f"Sequences: {seq_id} (skipped {skipped})")
    print(f"Total rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(f"\nNowe kolumny rolling (przykład proc_temp):")
    rolling_cols = [c for c in df.columns if 'proc_temp_ma' in c or 'proc_temp_trend' in c]
    print(f"  {rolling_cols}")
    print(f"\nLabel distribution:")
    print(df['label'].value_counts().sort_index().rename({0: 'Normal', 1: 'Pre-failure', 2: 'Awaria'}))
    print(f"\nAnomaly modes:")
    print(df.groupby('anomaly_mode')['sequence_id'].nunique())
    print(f"\nOutput: {args.output}")


if __name__ == '__main__':
    main()
