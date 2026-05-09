"""
hvac_simulator.py v6
====================
Symulator urządzeń HVAC z przeprojektowaną fizyką.

Zmiany v6:
- Temperatury w Celsjuszach (nie Kelwinach)
- 6 typów defektów per czujnik: vibration↑, rpm↓, rpm↑, torque↑, torque↓, temp↑
- Wyraźne strefy: normalny → bufor → pre-alarm → alarm → defekt
- Brak mieszania defektów (każde urządzenie ma jeden typ sygnału)
- FIX: failure_ticks — urządzenie zostaje w fazie failure min. 12 ticków (2 min)

Parametry normalnej pracy:
  proc_temp:  60°C ±5
  rpm:        1500 ±50
  torque:     40 Nm ±3
  vibration:  0.5 mm/s ±0.2
  air_temp:   20°C ±2

Progi per czujnik:
  vibration:  normalny <1.8 | bufor 1.8-2.5 | pre-alarm >2.5 | defekt >7.1
  rpm↓:       normalny 1400-1600 | bufor 1300-1400 | pre-alarm <1300 | defekt <1000
  rpm↑:       normalny 1400-1600 | bufor 1600-1700 | pre-alarm >1700 | defekt >2000
  torque↑:    normalny 35-45 | bufor 45-47 | pre-alarm >49 | defekt >65
  torque↓:    normalny 35-45 | bufor 33-35 | pre-alarm <33 | defekt <15
  temp↑:      normalny 50-70 | bufor 70-75 | pre-alarm >75 | defekt >100
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
TICK         = 10
SEND_EVERY   = 1
LOG_LEVEL    = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [hvac_sim] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Zakresy sensorów (Celsjusze) ──────────────────────────────────────────────
R = {
    'air_temp':  {'min': 15.0,  'max': 30.0,  'mean': 20.0, 'std': 2.0},
    'proc_temp': {'min': 45.0,  'max': 110.0, 'mean': 60.0, 'std': 5.0},
    'rpm':       {'min': 800,   'max': 2200,  'mean': 1500, 'std': 50},
    'torque':    {'min': 10.0,  'max': 70.0,  'mean': 40.0, 'std': 3.0},
    'vibration': {'min': 0.1,   'max': 10.0,  'mean': 0.5,  'std': 0.2},
}

# ── Typy defektów ─────────────────────────────────────────────────────────────
FAULT_TYPES   = ['vibration_high', 'rpm_low', 'rpm_high', 'torque_high', 'torque_low', 'temp_high']
FAULT_WEIGHTS = [1/6, 1/6, 1/6, 1/6, 1/6, 1/6]

# ── Progi defektów ────────────────────────────────────────────────────────────
FAULT_THRESHOLDS = {
    'vibration_high': lambda s: s['vibration'] > 7.1,
    'rpm_low':        lambda s: s['rpm'] < 1000,
    'rpm_high':       lambda s: s['rpm'] > 2000,
    'torque_high':    lambda s: s['torque'] > 65,
    'torque_low':     lambda s: s['torque'] < 15,
    'temp_high':      lambda s: s['proc_temp'] > 100,
}

# Urządzenia: SIM_01-15 anomaly, SIM_16-25 normalOnly
DEVICES = [
    {'id': f'SIM_{i:02d}',
     'lat': round(50.0521 + (i-1)*0.007 + random.uniform(-0.003, 0.003), 4),
     'lng': round(19.9345 + (i-1)*0.005 + random.uniform(-0.003, 0.003), 4),
     'normal_only': i > 15}
    for i in range(1, 26)
]

# ── Czas w fazie failure przed SERVICE ───────────────────────────────────────
FAILURE_TICKS_MIN = 12  # 12 × 10s = 120s = 2 minuty


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
        return clamp(nominal * (1 + random.gauss(0, 1) * std), nominal * 0.5, nominal * 2.0)
    return {
        'vib_rate':  rv(0.20,  0.20),
        'rpm_rate':  rv(2.0,   0.20),
        'torq_rate': rv(0.80,  0.20),
        'temp_rate': rv(0.60,  0.20),
    }


def detect_fault(s, fault_type):
    return FAULT_THRESHOLDS[fault_type](s)


def tick_state(state, mode, profile):
    s  = dict(state)
    pr = profile
    N  = 0.03
    sc = math.sqrt(TICK)

    # Szum sensorów
    s['air_temp']  = clamp(s['air_temp']  + random.gauss(0,1) * N * R['air_temp']['std']  * sc, R['air_temp']['min'],  R['air_temp']['max'])
    s['proc_temp'] = clamp(s['proc_temp'] + random.gauss(0,1) * N * R['proc_temp']['std'] * sc, R['proc_temp']['min'], R['proc_temp']['max'])
    s['torque']    = clamp(s['torque']    + random.gauss(0,1) * N * R['torque']['std']    * sc, R['torque']['min'],    R['torque']['max'])
    s['vibration'] = clamp(s['vibration'] + random.gauss(0,1) * N * R['vibration']['std'] * sc, R['vibration']['min'], R['vibration']['max'])
    s['rpm']       = clamp(s['rpm']       + random.gauss(0,1) * N * R['rpm']['std'],            R['rpm']['min'],       R['rpm']['max'])

    # Mean-reversion — wyłączona dla aktywnego czujnika defektu
    MR = 0.025 * TICK
    if mode != 'temp_high':
        s['proc_temp'] = clamp(s['proc_temp'] + (R['proc_temp']['mean'] - s['proc_temp']) * MR, R['proc_temp']['min'], R['proc_temp']['max'])
    if mode not in ('torque_high', 'torque_low'):
        s['torque']    = clamp(s['torque']    + (R['torque']['mean']    - s['torque'])    * MR, R['torque']['min'],    R['torque']['max'])
    if mode != 'vibration_high':
        s['vibration'] = clamp(s['vibration'] + (R['vibration']['mean'] - s['vibration']) * MR, R['vibration']['min'], R['vibration']['max'])
    if mode not in ('rpm_low', 'rpm_high'):
        s['rpm']       = clamp(s['rpm']       + (R['rpm']['mean']       - s['rpm'])       * MR, R['rpm']['min'],       R['rpm']['max'])
    s['air_temp'] = clamp(s['air_temp'] + (R['air_temp']['mean'] - s['air_temp']) * MR, R['air_temp']['min'], R['air_temp']['max'])

    # Defekty — liniowy przyrost
    if mode == 'vibration_high':
        s['vibration'] = clamp(s['vibration'] + pr['vib_rate']  + random.gauss(0,1) * 0.02, R['vibration']['min'], 10.0)
    elif mode == 'rpm_low':
        s['rpm']       = clamp(s['rpm']       - pr['rpm_rate']  + random.gauss(0,1) * 2.0,  R['rpm']['min'],       R['rpm']['max'])
    elif mode == 'rpm_high':
        s['rpm']       = clamp(s['rpm']       + pr['rpm_rate']  + random.gauss(0,1) * 2.0,  R['rpm']['min'],       R['rpm']['max'])
    elif mode == 'torque_high':
        s['torque']    = clamp(s['torque']    + pr['torq_rate'] + random.gauss(0,1) * 0.1,  R['torque']['min'],    70.0)
    elif mode == 'torque_low':
        s['torque']    = clamp(s['torque']    - pr['torq_rate'] + random.gauss(0,1) * 0.1,  10.0,                  R['torque']['max'])
    elif mode == 'temp_high':
        s['proc_temp'] = clamp(s['proc_temp'] + pr['temp_rate'] + random.gauss(0,1) * 0.2,  R['proc_temp']['min'], 110.0)

    return s


class DeviceSimulator:
    def __init__(self, device_cfg):
        self.id          = device_cfg['id']
        self.lat         = device_cfg['lat']
        self.lng         = device_cfg['lng']
        self.normal_only = device_cfg['normal_only']

        self.session_id  = str(uuid.uuid4())
        self.profile     = generate_profile()
        self.state       = normal_state()

        self.uptime        = 0
        self.phase         = 'warmup'
        self.mode          = None
        self.warmup_left   = random.randint(70, 130) * TICK
        self.failure_ticks = 0  # licznik ticków w fazie failure

        self.pseudo_active   = False
        self.pseudo_type     = None
        self.pseudo_duration = 0
        self.pseudo_timer    = 0

    def step(self):
        if self.phase == 'warmup':
            self.state = tick_state(self.state, 'NONE', self.profile)
            self.warmup_left -= TICK
            if self.warmup_left <= 0:
                if self.normal_only:
                    self.phase = 'normal_long'
                else:
                    self.phase = 'anomaly'
                    self.mode  = random.choices(FAULT_TYPES, weights=FAULT_WEIGHTS)[0]
                    log.info("Device %s → fault mode %s", self.id, self.mode)

        elif self.phase == 'anomaly':
            self.state = tick_state(self.state, self.mode, self.profile)
            if detect_fault(self.state, self.mode):
                self.phase         = 'failure'
                self.failure_ticks = 0
                log.warning("Device %s → FAILURE %s", self.id, self.mode)

        elif self.phase == 'failure':
            self.state = tick_state(self.state, self.mode, self.profile)
            self.failure_ticks += 1
            if self.failure_ticks >= FAILURE_TICKS_MIN:
                self._service()

        elif self.phase == 'normal_long':
            self.state = tick_state(self.state, 'NONE', self.profile)
            self.pseudo_timer += TICK
            if not self.pseudo_active and self.pseudo_timer > 60 + random.random() * 120:
                self.pseudo_active   = True
                self.pseudo_duration = random.randint(10, 30)
                self.pseudo_timer    = 0
                self.pseudo_type     = random.choice(['torque_spike', 'rpm_drop', 'vibration_bump', 'temp_spike'])

            if self.pseudo_active:
                if self.pseudo_type == 'torque_spike':
                    self.state['torque']    = min(self.state['torque']    + 0.1,  48.0)
                elif self.pseudo_type == 'rpm_drop':
                    self.state['rpm']       = max(self.state['rpm']       - 1,  1350.0)
                elif self.pseudo_type == 'vibration_bump':
                    self.state['vibration'] = min(self.state['vibration'] + 0.01, 1.5)
                elif self.pseudo_type == 'temp_spike':
                    self.state['proc_temp'] = min(self.state['proc_temp'] + 0.2,  73.0)
                self.pseudo_duration -= TICK
                if self.pseudo_duration <= 0:
                    self.pseudo_active = False

        self.uptime += TICK

        active_faults = [f for f, check in FAULT_THRESHOLDS.items() if check(self.state)]

        return {
            'device_id':      self.id,
            'ml_score':       0.0,
            'session_id':     self.session_id,
            'lat':            self.lat,
            'lng':            self.lng,
            'air_temp':       round(self.state['air_temp'],   2),
            'proc_temp':      round(self.state['proc_temp'],  2),
            'rpm':            int(self.state['rpm']),
            'torque':         round(self.state['torque'],     2),
            'vibration':      round(self.state['vibration'],  3),
            'failure_type':   ','.join(active_faults) if active_faults else 'None',
            'severity':       'CRITICAL' if active_faults else 'OK',
            'event_type':     'telemetry',
            'uptime_seconds': self.uptime,
            'ts':             datetime.now(timezone.utc).isoformat(),
        }

    def _service(self):
        log.info("Device %s → SERVICE resolved=%s after %d failure ticks",
                 self.id, self.mode, self.failure_ticks)
        self.session_id    = str(uuid.uuid4())
        self.profile       = generate_profile()
        self.state         = normal_state()
        self.uptime        = 0
        self.phase         = 'warmup'
        self.warmup_left   = random.randint(70, 130) * TICK
        self.mode          = None
        self.failure_ticks = 0

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
        r = requests.post(f"{SERVER_URL}/events/batch", json=batch, timeout=timeout)
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
    log.info("HVAC Simulator v6 | %d devices | server=%s | tick=%ds",
             len(DEVICES), SERVER_URL, TICK)
    log.info("Fault types: %s", FAULT_TYPES)
    log.info("Temps in Celsius | vibration in mm/s RMS")
    log.info("Failure hold: %d ticks = %ds before SERVICE",
             FAILURE_TICKS_MIN, FAILURE_TICKS_MIN * TICK)

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
            fault_devs = [d.id for d in devices if d.phase in ('anomaly', 'failure')]
            log.info("Tick %d | sent ok=%d err=%d | fault=%s",
                     tick_count, sent_ok, sent_err,
                     ','.join(fault_devs) if fault_devs else 'none')

        elapsed = time.time() - start
        sleep_t = max(0, TICK - elapsed)
        time.sleep(sleep_t)

    log.info("Simulator stopped after %d ticks", tick_count)


if __name__ == "__main__":
    main()
