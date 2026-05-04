"""
generate_v3.py
==============
Generator danych treningowych v3 — prawdziwe awarie HVAC.
Usunięto tool_wear, TWF, OSF (to cechy CNC, nie HVAC).

4 typy awarii HVAC:
- HDF: Heat Dissipation Failure — przegrzanie, delta_temp spada, rpm spada
- PWF: Power Failure — pobór mocy poza zakresem (<3500W lub >9000W)
- CLOG: Zapchany filtr — torque rośnie, rpm rośnie, delta_temp rośnie
- BEARING: Uszkodzenie łożysk — vibration rośnie sinusoidalnie

Wyjście: hvac_training_v3.csv

Uruchomienie:
    python generate_v3.py --sequences 5000
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
TICK = 10        # sekund na próbkę
MAX_STEPS = 300  # max próbek (300 × 10s = 50 minut)
PRE_FAILURE_WINDOW = 30  # ticków przed awarią = "pre_failure" (30 × 10s = 5 min)

# Wagi trybów anomalii
MODE_WEIGHTS = {
    'HDF':     0.30,
    'PWF':     0.30,
    'CLOG':    0.25,
    'BEARING': 0.15,
}


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
    """Losowe parametry urządzenia — każda maszyna jest trochę inna."""
    def rv(nominal, std):
        return clamp(nominal * (1 + np.random.randn() * std), nominal * 0.4, nominal * 2.2)
    return {
        # HDF — szybkość chłodzenia proc_temp i spadku rpm
        'hdfCoolRate':   rv(0.25, 0.30),   # jak szybko proc_temp opada do air_temp
        'hdfRpmDrop':    rv(3.0,  0.35),   # rpm/tick
        # PWF — kierunek awarii zasilania (za mało lub za dużo mocy)
        'pwfDirection':  1 if np.random.random() > 0.5 else -1,  # +1=overvolt, -1=undervolt
        'pwfRpmDrop':    rv(4.0,  0.30),
        'pwfTorqueDrop': rv(0.8,  0.35),
        # CLOG — narastanie zatkania
        'clogTorque':    rv(0.25, 0.30),   # Nm/tick
        'clogRpmRise':   rv(1.5,  0.35),   # rpm/tick (wentylator pracuje ciężej)
        'clogTempRise':  rv(0.05, 0.30),   # K/tick
        # BEARING — amplituda wibracji
        'bearingAmp':    rv(0.008, 0.35),
        'bearingMax':    rv(1.8,   0.20),
    }


def detect_failures(s):
    """Warunki awarii HVAC — bez tool_wear."""
    failures = []
    dT    = s['proc_temp'] - s['air_temp']
    power = s['torque'] * (s['rpm'] * 2 * np.pi / 60)

    # HDF: zła dyssypacja ciepła — delta_temp za mała I rpm za niskie
    if dT < 8.6 and s['rpm'] < 1380:
        failures.append('HDF')

    # PWF: pobór mocy poza zakresem
    if power < 3500 or power > 9000:
        failures.append('PWF')

    # CLOG: zapchany filtr — torque za wysoki LUB delta_temp za duża
    if s['torque'] > 60 or dT > 13.5:
        failures.append('CLOG')

    # BEARING: wibracje za wysokie
    if s['vibration'] > 0.8:
        failures.append('BEARING')

    return failures


def tick(state, accum, mode, profile):
    """Jeden tick symulacji — identyczna fizyka jak HTML twin."""
    s  = dict(state)
    N  = 0.03
    sc = np.sqrt(TICK)
    pr = profile

    # Szum sensorów
    s['air_temp']  = clamp(s['air_temp']  + np.random.randn() * N * R['air_temp']['std']  * sc, R['air_temp']['min'],  R['air_temp']['max'])
    s['proc_temp'] = clamp(s['proc_temp'] + np.random.randn() * N * R['proc_temp']['std'] * sc, R['proc_temp']['min'], R['proc_temp']['max'])
    s['torque']    = clamp(s['torque']    + np.random.randn() * N * R['torque']['std']    * sc, R['torque']['min'],    R['torque']['max'])
    s['vibration'] = clamp(s['vibration'] + np.random.randn() * N * 0.01                 * sc, R['vibration']['min'], R['vibration']['max'])
    s['rpm']       = clamp(s['rpm']       + np.random.randn() * N * R['rpm']['std'],            R['rpm']['min'],       R['rpm']['max'])

    # Mean-reversion — powrót do normalnych wartości gdy brak anomalii
    MR = 0.025 * TICK
    if mode not in ('HDF', 'CLOG'):
        s['proc_temp'] = clamp(s['proc_temp'] + (R['proc_temp']['mean'] - s['proc_temp']) * MR, R['proc_temp']['min'], R['proc_temp']['max'])
    if mode != 'CLOG':
        s['torque']    = clamp(s['torque']    + (R['torque']['mean']    - s['torque'])    * MR, R['torque']['min'],    R['torque']['max'])
    if mode != 'BEARING':
        s['vibration'] = clamp(s['vibration'] + (0.03 - s['vibration']) * 0.08 * TICK,    R['vibration']['min'], 0.07)
    s['air_temp'] = clamp(s['air_temp'] + (R['air_temp']['mean'] - s['air_temp']) * MR, R['air_temp']['min'], R['air_temp']['max'])

    # ── Krzywe anomalii (nieliniowy boost — degradacja przyspiesza) ──────────

    if mode == 'HDF':
        # Wentylator zwalnia + proc_temp chłodzi się do air_temp (brak dyssypacji)
        accum['HDF'] = accum.get('HDF', 0) + 1
        ac = accum['HDF']
        accel = 1 + (ac / 20) ** 1.5   # nieliniowy boost
        drop = pr['hdfRpmDrop'] * TICK * accel
        s['rpm'] = clamp(s['rpm'] - drop, R['rpm']['min'], R['rpm']['max'])
        # proc_temp powoli opada do air_temp (brak chłodzenia = wyrównanie temp)
        target = s['air_temp'] + 7.5
        s['proc_temp'] = clamp(
            s['proc_temp'] + (target - s['proc_temp']) * pr['hdfCoolRate'],
            R['proc_temp']['min'], R['proc_temp']['max']
        )

    elif mode == 'PWF':
        # Awaria zasilania — rpm i torque spadają (lub skaczą przy overvolt)
        accum['PWF'] = accum.get('PWF', 0) + 1
        ac = accum['PWF']
        accel = 1 + (ac / 25) ** 1.5
        if pr['pwfDirection'] == -1:
            # Undervolt — moc spada poniżej 3500W
            s['rpm']    = clamp(s['rpm']    - pr['pwfRpmDrop']    * TICK * accel, R['rpm']['min'],    R['rpm']['max'])
            s['torque'] = clamp(s['torque'] - pr['pwfTorqueDrop'] * TICK * accel, R['torque']['min'], R['torque']['max'])
        else:
            # Overvolt — moc rośnie powyżej 9000W
            s['rpm']    = clamp(s['rpm']    + pr['pwfRpmDrop']    * TICK * accel, R['rpm']['min'],    R['rpm']['max'])
            s['torque'] = clamp(s['torque'] + pr['pwfTorqueDrop'] * TICK * accel, R['torque']['min'], R['torque']['max'])

    elif mode == 'CLOG':
        # Zapchany filtr — torque rośnie, rpm rośnie, temperatura rośnie
        accum['CLOG'] = accum.get('CLOG', 0) + 1
        ac = accum['CLOG']
        accel = 1 + (ac / 30) ** 2   # silny nieliniowy boost
        s['torque']    = clamp(s['torque']    + pr['clogTorque']   * TICK * accel, R['torque']['min'],    R['torque']['max'])
        s['rpm']       = clamp(s['rpm']       + pr['clogRpmRise']  * TICK * accel, R['rpm']['min'],       R['rpm']['max'])
        s['proc_temp'] = clamp(s['proc_temp'] + pr['clogTempRise'] * TICK * accel, R['proc_temp']['min'], R['proc_temp']['max'])

    elif mode == 'BEARING':
        # Uszkodzenie łożysk — wibracje rosną sinusoidalnie z rosnącą amplitudą
        accum['BEARING'] = accum.get('BEARING', 0) + 1
        ac = accum['BEARING']
        amp = clamp(0.04 + ac * pr['bearingAmp'], 0.04, pr['bearingMax'])
        s['vibration'] = clamp(
            amp * (0.7 + 0.3 * np.sin(ac * 0.9)) + abs(np.random.randn() * 0.015),
            0.01, 2.0
        )
        # Łożyska też powodują lekki wzrost torque
        s['torque'] = clamp(s['torque'] + 0.05 * ac, R['torque']['min'], R['torque']['max'])

    return s


def simulate_normal_sequence(seq_id, steps=60):
    """Sekwencja tylko normalnej pracy — bez żadnej anomalii. Label=0 przez cały czas."""
    session_id = str(uuid.uuid4())
    profile    = generate_device_profile()
    state      = normal_state()
    accum      = {}

    rows = []
    prev_state = dict(state)
    for step in range(steps):
        prev_state = dict(state)
        state = tick(state, accum, 'NONE', profile)

        power = state['torque'] * (state['rpm'] * 2 * np.pi / 60)
        dT    = state['proc_temp'] - state['air_temp']

        rows.append({
            'sequence_id':         seq_id,
            'session_id':          session_id,
            'step':                step,
            't_seconds':           step * TICK,
            'uptime_seconds':      step * TICK,
            'buffer_fill_ratio':   min(1.0, (step + 1) / 20.0),
            'product_type':        'L',
            'anomaly_mode':        'NONE',
            'air_temp':            round(state['air_temp'],   2),
            'proc_temp':           round(state['proc_temp'],  2),
            'rpm':                 int(state['rpm']),
            'torque':              round(state['torque'],     2),
            'vibration':           round(state['vibration'],  3),
            'delta_temp':          round(dT,    2),
            'power_w':             round(power, 1),
            'proc_temp_velocity':  round(state['proc_temp'] - prev_state['proc_temp'], 4),
            'rpm_velocity':        round(state['rpm'] - prev_state['rpm'], 4),
            'torque_velocity':     round(state['torque'] - prev_state['torque'], 4),
            'vibration_velocity':  round(state['vibration'] - prev_state['vibration'], 4),
            'failure_type':        'None',
            'has_failure':         0,
            'rul_seconds':         9999,
            'label':               0,
        })
    return rows


def simulate_sequence(seq_id, mode):
    product_type = np.random.choice(PRODUCT_TYPES)
    session_id   = str(uuid.uuid4())
    profile      = generate_device_profile()
    state        = normal_state()
    accum        = {}

    # Inicjalizacja stanu dla HDF — start bliżej progu
    if mode == 'HDF':
        state['rpm']      = gauss(1500, 60, 1380, 1650)
        state['proc_temp']= gauss(308.5, 0.5, 307.0, 309.5)

    # Warmup — normalna praca przed anomalią
    warmup = np.random.randint(10, 31)
    for _ in range(warmup):
        state = tick(state, accum, 'NONE', profile)

    rows = []
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

        # Velocity features — kluczowe dla XGBoost żeby "widzieć" trend
        proc_temp_velocity = state['proc_temp'] - prev_state['proc_temp']
        rpm_velocity       = state['rpm']       - prev_state['rpm']
        torque_velocity    = state['torque']    - prev_state['torque']
        vibration_velocity = state['vibration'] - prev_state['vibration']

        rows.append({
            'sequence_id':          seq_id,
            'session_id':           session_id,
            'step':                 step,
            't_seconds':            step * TICK,
            'uptime_seconds':       step * TICK,
            'buffer_fill_ratio':    min(1.0, (step + 1) / 20.0),  # 0→1 jak bufor się wypełnia
            'product_type':         product_type,
            'anomaly_mode':         mode,
            # Surowe sensory (tylko prawdziwe czujniki HVAC)
            'air_temp':             round(state['air_temp'],   2),
            'proc_temp':            round(state['proc_temp'],  2),
            'rpm':                  int(state['rpm']),
            'torque':               round(state['torque'],     2),
            'vibration':            round(state['vibration'],  3),
            # Pochodne
            'delta_temp':           round(dT,    2),
            'power_w':              round(power, 1),
            # Velocity features
            'proc_temp_velocity':   round(proc_temp_velocity,  4),
            'rpm_velocity':         round(rpm_velocity,         4),
            'torque_velocity':      round(torque_velocity,      4),
            'vibration_velocity':   round(vibration_velocity,   4),
            # Etykiety
            'failure_type':         ','.join(failures) if failures else 'None',
            'has_failure':          1 if failures else 0,
        })

        # Zakończ sekwencję 5 ticków po pierwszej awarii
        if failure_step is not None and step >= failure_step + 5:
            break

    if not rows or failure_step is None:
        return []

    # RUL i label
    failure_t = failure_step * TICK
    for row in rows:
        rul = failure_t - row['t_seconds']
        row['rul_seconds'] = max(0, rul)

        if row['has_failure'] == 1:
            row['label'] = 2                        # awaria aktywna
        elif rul <= PRE_FAILURE_WINDOW * TICK:
            row['label'] = 1                        # pre_failure (5 min przed)
        else:
            row['label'] = 0                        # normalna praca

    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sequences', type=int, default=5000)
    parser.add_argument('--output',    type=str, default='hvac_training_v3.csv')
    args = parser.parse_args()

    modes   = list(MODE_WEIGHTS.keys())
    weights = list(MODE_WEIGHTS.values())

    print(f"Generating {args.sequences} sequences...")
    all_rows = []
    seq_id   = 0
    skipped  = 0

    # 20% sekwencji to czysto normalna praca — bez anomalii
    n_normal_only = args.sequences // 5
    n_anomaly     = args.sequences - n_normal_only

    print(f"  Normal-only sequences: {n_normal_only}")
    print(f"  Anomaly sequences:     {n_anomaly}")

    # Normal-only sequences
    for i in range(n_normal_only):
        rows = simulate_normal_sequence(seq_id, steps=np.random.randint(40, 80))
        all_rows.extend(rows)
        seq_id += 1

    # Anomaly sequences
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
    print(f"\nLabel distribution:")
    print(df['label'].value_counts().sort_index().rename({0: 'Normal', 1: 'Pre-failure', 2: 'Awaria'}))
    print(f"\nFailure types:")
    print(df[df['has_failure']==1]['failure_type'].value_counts())
    print(f"\nAnomaly modes:")
    print(df.groupby('anomaly_mode')['sequence_id'].nunique())
    print(f"\nOutput: {args.output}")


if __name__ == '__main__':
    main()
