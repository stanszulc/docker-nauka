"""
hvac_simulator.py
=================
Symulator 25 urządzeń HVAC (15 anomaly + 10 normalOnly) — działa na VM bez przeglądarki.
Identyczna fizyka jak hvac_simulator_25.html.

Zmiany v3:
- CLOG: liniowe narastanie torque 0.667 Nm/tick ±1 (20 Nm w 5 min)

Zmiany v2:
- RPM std: 179 → 50 (stabilny silnik w normalnej pracy, unikanie strefy HDF)
- proc_temp std: 1.48 → 0.5 (stabilna temperatura, unikanie strefy HDF/CLOG)
- air_temp std: 2.0 → 0.5 (stabilna temperatura otoczenia)
- Szum sensorów w tick_state skalowany do nowych std

Uruchomienie:
    python3 hvac_simulator.py

Zatrzymanie:
    Ctrl+C lub kill PID
"""

import os
import json
import math
import time
import uuid
import random
import logging
import signal
import sys
from datetime import datetime, timezone

import requests
import numpy as np

# ── Config ────────────────────────────────────────────────────────────────────
SERVER_URL   = os.getenv("SERVER_URL",   "http://localhost:8002")
TICK         = 10       # sekund per krok
SEND_EVERY   = 1        # wysyłaj co N ticków
LOG_LEVEL    = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [hvac_sim] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Zakresy sensorów ──────────────────────────────────────────────────────────
# ZMIANA v2: zmniejszono std dla RPM, proc_temp i air_temp
# żeby urządzenia normalOnly nie wpadały losowo w strefy awaryjne
#
# Progi awarii HDF: rpm < 1380 AND dT < 8.6  → margines RPM: (1538-1380)/50 = 3.2σ ✅
# Poprzednio:                                  → margines RPM: (1538-1380)/179 = 0.88σ ❌
# Próg CLOG: dT > 13.5                        → margines dT: (13.5-10)/0.5 = 7σ ✅
# Poprzednio:                                  → margines dT: (13.5-10)/1.48 = 2.4σ ⚠️
R = {
    'air_temp':  {'min': 295.3, 'max': 304.5, 'mean': 300.0, 'std': 0.5},   # było: 2.0
    'proc_temp': {'min': 305.7, 'max': 313.8, 'mean': 310.0, 'std': 0.5},   # było: 1.48
    'rpm':       {'min': 1168,  'max': 2886,  'mean': 1538,  'std': 50},    # było: 179
    'torque':    {'min': 3.8,   'max': 76.6,  'mean': 40.0,  'std': 3.0},   # bylo: 9.97
    'vibration': {'min': 0.01,  'max': 2.0,   'mean': 0.04,  'std': 0.02},  # bez zmian
}

# Progi awarii
FAILURE_THRESHOLDS = {
    'HDF':     lambda s: (s['proc_temp'] - s['air_temp']) < 8.6 and s['rpm'] < 1380,
    'PWF':     lambda s: (s['torque'] * (s['rpm'] * 2 * math.pi / 60)) < 3500 or
                         (s['torque'] * (s['rpm'] * 2 * math.pi / 60)) > 9000,
    'CLOG':    lambda s: s['torque'] > 60 or (s['proc_temp'] - s['air_temp']) > 13.5,
    'BEARING': lambda s: s['vibration'] > 0.8,
}

MODES        = ['HDF', 'PWF', 'CLOG', 'BEARING']
MODE_WEIGHTS = [0.25, 0.25, 0.15, 0.35]

# Urządzenia: SIM_01-15 anomaly, SIM_16-25 normalOnly
DEVICES = [
    {'id': f'SIM_{i:02d}',
     'lat': round(50.0521 + (i-1)*0.007 + random.uniform(-0.003, 0.003), 4),
     'lng': round(19.9345 + (i-1)*0.005 + random.uniform(-0.003, 0.003), 4),
     'normal_only': i > 15}
    for i in range(1, 26)
]


def clamp(v, a, b):
    return max(a, min(b, v))


def gauss(mean, std, lo, hi):
    return clamp(mean + random.gauss(0, 1) * std, lo, hi)


def normal_state():
    return {
        'air_temp':  gauss(R['air_temp']['mean'],  R['air_temp']['std'],  R['air_temp']['min'],  R['air_temp']['max']),
        'proc_temp': gauss(R['proc_temp']['mean'], R['proc_temp']['std'], R['proc_temp']['min'], R['proc_temp']['max']),
        'rpm':       gauss(R['rpm']['mean'],        R['rpm']['std'],        R['rpm']['min'],        R['rpm']['max']),
        'torque':    gauss(R['torque']['mean'],     R['torque']['std'],     R['torque']['min'],     R['torque']['max']),
        'vibration': gauss(R['vibration']['mean'],  R['vibration']['std'],  R['vibration']['min'],  R['vibration']['max']),
    }


def generate_profile():
    def rv(nominal, std):
        return clamp(nominal * (1 + random.gauss(0, 1) * std), nominal * 0.4, nominal * 2.2)
    return {
        'heatRate':    rv(0.012, 0.30),
        'heatDecay':   rv(120,   0.25),
        'heatRpmDrop': rv(0.079, 0.35),
        'clogTorque':  rv(0.004, 0.30),
        'clogTemp':    rv(0.001, 0.30),
        'clogRpm':     rv(0.10,  0.35),
        'bearingAmp':  rv(0.008, 0.35),
        'bearingMax':  rv(1.8,   0.20),
        'driftAir':    rv(0.020, 0.30),
        'driftProc':   rv(0.022, 0.30),
        'powerDrop':   rv(0.185, 0.30),
        'powerDecay':  rv(25,    0.25),
    }


def detect_failures(s):
    return [f for f, check in FAILURE_THRESHOLDS.items() if check(s)]


def tick_state(state, accum, mode, profile, base_rpm=None):
    """Jeden krok symulacji."""
    s  = dict(state)
    pr = profile
    N  = 0.03
    sc = math.sqrt(TICK)

    # Szum sensorów — skalowany do nowych std
    s['air_temp']  = clamp(s['air_temp']  + random.gauss(0,1) * N * R['air_temp']['std']  * sc, R['air_temp']['min'],  R['air_temp']['max'])
    s['proc_temp'] = clamp(s['proc_temp'] + random.gauss(0,1) * N * R['proc_temp']['std'] * sc, R['proc_temp']['min'], R['proc_temp']['max'])
    s['torque']    = clamp(s['torque']    + random.gauss(0,1) * N * R['torque']['std']    * sc, R['torque']['min'],    R['torque']['max'])
    s['vibration'] = clamp(s['vibration'] + random.gauss(0,1) * N * 0.01                 * sc, R['vibration']['min'], R['vibration']['max'])
    s['rpm']       = clamp(s['rpm']       + random.gauss(0,1) * N * R['rpm']['std'],            R['rpm']['min'],       R['rpm']['max'])

    # Mean-reversion
    MR = 0.025 * TICK
    if mode not in ('HDF', 'CLOG'):
        s['proc_temp'] = clamp(s['proc_temp'] + (R['proc_temp']['mean'] - s['proc_temp']) * MR, R['proc_temp']['min'], R['proc_temp']['max'])
    if mode != 'CLOG':
        s['torque']    = clamp(s['torque']    + (R['torque']['mean']    - s['torque'])    * MR, R['torque']['min'],    R['torque']['max'])
    if mode != 'BEARING':
        s['vibration'] = clamp(s['vibration'] + (0.03 - s['vibration']) * 0.08 * TICK,    R['vibration']['min'], 0.07)
    s['air_temp'] = clamp(s['air_temp'] + (R['air_temp']['mean'] - s['air_temp']) * MR, R['air_temp']['min'], R['air_temp']['max'])
    # RPM mean-reversion gdy brak anomalii
    if mode == 'NONE':
        s['rpm'] = clamp(s['rpm'] + (R['rpm']['mean'] - s['rpm']) * MR, R['rpm']['min'], R['rpm']['max'])

    # Anomalie
    if mode == 'HDF':
        accum['HDF'] = accum.get('HDF', 0) + 1
        ac    = accum['HDF']
        accel = 1 + (ac / 20) ** 1.5
        s['rpm'] = clamp(s['rpm'] - pr['heatRpmDrop'] * TICK * accel, R['rpm']['min'], R['rpm']['max'])
        target = s['air_temp'] + 7.5
        s['proc_temp'] = clamp(s['proc_temp'] + (target - s['proc_temp']) * pr['heatRate'], R['proc_temp']['min'], R['proc_temp']['max'])

    elif mode == 'PWF':
        accum['PWF'] = accum.get('PWF', 0) + 1
        ac    = accum['PWF']
        accel = 1 + (ac / 25) ** 1.5
        s['rpm']    = clamp(s['rpm']    - pr['powerDrop']  * TICK * accel, R['rpm']['min'],    R['rpm']['max'])
        s['torque'] = clamp(s['torque'] - pr['powerDrop'] * 0.08 * TICK * accel, R['torque']['min'], R['torque']['max'])

    elif mode == 'CLOG':
        # ZMIANA v3: liniowe narastanie torque 20 Nm w 30 tickach (5 min)
        # + szum ±1 Nm — wyraźny sygnał dla modelu ML
        accum['CLOG'] = accum.get('CLOG', 0) + 1
        s['torque']    = clamp(s['torque']    + 0.667 + random.gauss(0, 1) * 1.0, R['torque']['min'], R['torque']['max'])
        s['rpm']       = clamp(s['rpm']       - 0.5   + random.gauss(0, 1) * 0.5, R['rpm']['min'],    R['rpm']['max'])
        s['proc_temp'] = clamp(s['proc_temp'] + 0.05  + random.gauss(0, 1) * 0.1, R['proc_temp']['min'], R['proc_temp']['max'])

    elif mode == 'BEARING':
        accum['BEARING'] = accum.get('BEARING', 0) + 1
        ac  = accum['BEARING']
        amp = clamp(0.04 + ac * pr['bearingAmp'], 0.04, pr['bearingMax'])
        s['vibration'] = clamp(amp * (0.7 + 0.3 * math.sin(ac * 0.9)) + abs(random.gauss(0,1) * 0.015), 0.01, 2.0)
        s['torque']    = clamp(s['torque'] + 0.05 * ac, R['torque']['min'], R['torque']['max'])

    return s


class DeviceSimulator:
    """Stan jednego urządzenia HVAC."""

    def __init__(self, device_cfg):
        self.id          = device_cfg['id']
        self.lat         = device_cfg['lat']
        self.lng         = device_cfg['lng']
        self.normal_only = device_cfg['normal_only']

        self.session_id  = str(uuid.uuid4())
        self.profile     = generate_profile()
        self.state       = normal_state()
        self.accum       = {}

        self.uptime      = 0
        self.phase       = 'warmup'
        self.mode        = None
        self.warmup_left = random.randint(70, 130) * TICK

        # Pseudo-anomalia dla normalOnly
        self.pseudo_active   = False
        self.pseudo_type     = None
        self.pseudo_duration = 0
        self.pseudo_timer    = 0

    def step(self):
        """Jeden tick — zwraca dict z danymi do wysłania."""

        if self.phase == 'warmup':
            self.state = tick_state(self.state, self.accum, 'NONE', self.profile)
            self.warmup_left -= TICK
            if self.warmup_left <= 0:
                if self.normal_only:
                    self.phase = 'normal_long'
                else:
                    self.phase  = 'anomaly'
                    self.mode   = random.choices(MODES, weights=MODE_WEIGHTS)[0]
                    log.info("Device %s → anomaly mode %s", self.id, self.mode)

        elif self.phase == 'anomaly':
            self.state = tick_state(self.state, self.accum, self.mode, self.profile)
            fails = detect_failures(self.state)
            if fails:
                self.phase = 'failure'
                log.warning("Device %s → FAILURE %s", self.id, fails)

        elif self.phase == 'failure':
            self.state = tick_state(self.state, self.accum, self.mode, self.profile)
            if self.accum.get(self.mode, 0) > 3:
                self._service()

        elif self.phase == 'normal_long':
            self.state = tick_state(self.state, self.accum, 'NONE', self.profile)
            # Pseudo-anomalia
            self.pseudo_timer += TICK
            if not self.pseudo_active and self.pseudo_timer > 60 + random.random() * 120:
                self.pseudo_active   = True
                self.pseudo_duration = random.randint(10, 30)
                self.pseudo_timer    = 0
                self.pseudo_type     = random.choice(['torque_spike', 'rpm_drop', 'vibration_bump'])

            if self.pseudo_active:
                if self.pseudo_type == 'torque_spike':
                    self.state['torque'] = min(self.state['torque'] + 0.1, 46.0)
                elif self.pseudo_type == 'rpm_drop':
                    # ZMIANA v2: rpm_drop zatrzymuje się na 1500 (powyżej progu HDF 1380)
                    self.state['rpm'] = max(self.state['rpm'] - 1, 1500.0)
                elif self.pseudo_type == 'vibration_bump':
                    self.state['vibration'] = min(self.state['vibration'] + 0.002, 0.30)
                self.pseudo_duration -= TICK
                if self.pseudo_duration <= 0:
                    self.pseudo_active = False

        self.uptime += TICK

        fails  = detect_failures(self.state)
        dT     = self.state['proc_temp'] - self.state['air_temp']
        power  = self.state['torque'] * (self.state['rpm'] * 2 * math.pi / 60)

        return {
            'device_id':      self.id,
            'session_id':     self.session_id,
            'lat':            self.lat,
            'lng':            self.lng,
            'air_temp':       round(self.state['air_temp'],   2),
            'proc_temp':      round(self.state['proc_temp'],  2),
            'rpm':            int(self.state['rpm']),
            'torque':         round(self.state['torque'],     2),
            'vibration':      round(self.state['vibration'],  3),
            'delta_temp':     round(dT,    2),
            'power_w':        round(power, 1),
            'ml_score':       0.0,
            'failure_type':   ','.join(fails) if fails else 'None',
            'severity':       'CRITICAL' if fails else 'OK',
            'event_type':     'telemetry',
            'uptime_seconds': self.uptime,
            'ts':             datetime.now(timezone.utc).isoformat(),
        }

    def _service(self):
        """Reset po awarii."""
        log.info("Device %s → SERVICE resolved=%s", self.id, self.mode)
        self.session_id  = str(uuid.uuid4())
        self.profile     = generate_profile()
        self.state       = normal_state()
        self.accum       = {}
        self.uptime      = 0
        self.phase       = 'warmup'
        self.warmup_left = random.randint(70, 130) * TICK
        self.mode        = None

    def service_payload(self):
        return {
            'device_id':        self.id,
            'lat':              self.lat,
            'lng':              self.lng,
            'air_temp':         round(self.state['air_temp'], 2),
            'proc_temp':        round(self.state['proc_temp'], 2),
            'rpm':              int(self.state['rpm']),
            'torque':           round(self.state['torque'], 2),
            'vibration':        round(self.state['vibration'], 3),
            'ml_score':         0.0,
            'failure_type':     'None',
            'severity':         'OK',
            'event_type':       'service',
            'resolved_failure': self.mode or 'RESET',
            'uptime_seconds':   0,
            'ts':               datetime.now(timezone.utc).isoformat(),
        }


SEND_BATCH: list = []
BATCH_SIZE = 50

def flush_batch(timeout: float = 5.0) -> int:
    global SEND_BATCH
    if not SEND_BATCH:
        return 0
    batch = SEND_BATCH[:]
    SEND_BATCH = []
    try:
        r = requests.post(f"{SERVER_URL}/events/batch",
                          json=batch,
                          timeout=timeout)
        if r.status_code in (200, 201, 202):
            return len(batch)
        log.warning("Batch error %d: %s", r.status_code, r.text[:100])
        return 0
    except Exception as e:
        log.warning("Batch send error: %s", e)
        return 0

def send_event(payload: dict, timeout: float = 3.0) -> bool:
    global SEND_BATCH
    SEND_BATCH.append(payload)
    if len(SEND_BATCH) >= BATCH_SIZE:
        sent = flush_batch()
        return sent > 0
    return True


def main():
    log.info("HVAC Simulator v3 | %d devices | server=%s | tick=%ds",
             len(DEVICES), SERVER_URL, TICK)
    log.info("CLOG: liniowe 0.667 Nm/tick ±1 (20 Nm w 5 min)")
    log.info("Sensor std: rpm=%.0f (było 179), proc_temp=%.2f (było 1.48), air_temp=%.2f (było 2.0)",
             R['rpm']['std'], R['proc_temp']['std'], R['air_temp']['std'])

    devices = [DeviceSimulator(d) for d in DEVICES]

    running = True
    def handle_signal(sig, frame):
        nonlocal running
        log.info("Shutdown signal received")
        running = False
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT,  handle_signal)

    tick_count = 0
    sent_ok    = 0
    sent_err   = 0

    while running:
        start = time.time()
        tick_count += 1

        for dev in devices:
            payload = dev.step()
            ok = send_event(payload)
            if ok:
                sent_ok += 1
            else:
                sent_err += 1

        if tick_count % 10 == 0:
            anomaly_devs = [d.id for d in devices if d.phase in ('anomaly', 'failure')]
            log.info("Tick %d | sent ok=%d err=%d | anomaly=%s",
                     tick_count, sent_ok, sent_err,
                     ','.join(anomaly_devs) if anomaly_devs else 'none')

        elapsed = time.time() - start
        sleep_t = max(0, TICK - elapsed)
        time.sleep(sleep_t)

    log.info("Simulator stopped after %d ticks", tick_count)


if __name__ == '__main__':
    main()
