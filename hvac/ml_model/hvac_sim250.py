"""
hvac_simulator.py
=================
Python symulator 25 urządzeń HVAC — działa na VM bez przeglądarki.
Identyczna fizyka jak hvac_simulator_25.html.

Uruchomienie:
    python hvac_simulator.py --url http://92.5.14.76:8002 --interval 10

Jako daemon (działa po zamknięciu SSH):
    nohup python hvac_simulator.py --url http://92.5.14.76:8002 > sim.log 2>&1 &
"""

import argparse
import math
import logging
import random
import signal
import time
import uuid

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [sim] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Fizyka ────────────────────────────────────────────────────────────────────
R = {
    'air_temp':  {'min': 295.3, 'max': 304.5, 'mean': 300.0, 'std': 2.0},
    'proc_temp': {'min': 305.7, 'max': 313.8, 'mean': 310.0, 'std': 1.48},
    'rpm':       {'min': 1168,  'max': 2886,  'mean': 1538,  'std': 179},
    'torque':    {'min': 3.8,   'max': 76.6,  'mean': 40.0,  'std': 9.97},
    'vibration': {'min': 0.01,  'max': 2.0,   'mean': 0.04,  'std': 0.02},
}

ANOMALY_MODES    = ['HDF_DIRECT', 'PWF', 'CLOG', 'BEARING']
MODE_WEIGHTS     = [0.30, 0.30, 0.25, 0.15]
FAILURE_DURATION = 30  # sekund w fazie failure przed SERVICE

DEVICES = [
    {'id': f'SIM_{i:03d}',
     'lat': round(50.0521 + (i * 0.0037) % 0.08, 4),
     'lng': round(19.9345 + (i * 0.0051) % 0.08, 4),
     'normalOnly': i > 175}   # 175 anomaly + 75 normalOnly
    for i in range(1, 251)    # 250 urządzeń
]


def clamp(v, a, b): return max(a, min(b, v))
def randn(): return random.gauss(0, 1)
def gauss(m, s, a, b): return clamp(m + randn() * s, a, b)


def pick_mode():
    r, acc = random.random(), 0
    for mode, w in zip(ANOMALY_MODES, MODE_WEIGHTS):
        acc += w
        if r < acc: return mode
    return 'HDF_DIRECT'


def generate_profile():
    def rv(n, s): return clamp(n * (1 + randn() * s), n * 0.4, n * 2.2)
    return {
        'heatRate':    rv(0.012, 0.30), 'heatDecay':  rv(120,   0.25),
        'heatRpmDrop': rv(0.079, 0.35), 'clogTorque': rv(0.004, 0.30),
        'clogTemp':    rv(0.001, 0.30), 'clogRpm':    rv(0.10,  0.35),
        'bearingAmp':  rv(0.007, 0.35), 'bearingMax': rv(1.8,   0.20),
        'powerDrop':   rv(0.185, 0.30), 'powerDecay': rv(25,    0.25),
    }


def normal_state():
    return {k: gauss(R[k]['mean'], R[k]['std'], R[k]['min'], R[k]['max'])
            for k in R}


def detect_failures(s):
    f, dT = [], s['proc_temp'] - s['air_temp']
    pw = s['torque'] * (s['rpm'] * 2 * math.pi / 60)
    if dT < 8.6 and s['rpm'] < 1380:   f.append('HDF')
    if pw < 3500 or pw > 9000:          f.append('PWF')
    if s['torque'] > 60 or dT > 13.5:  f.append('CLOG')
    if s['vibration'] > 0.8:            f.append('BEARING')
    return f


def make_device(cfg):
    return {**cfg, 'state': normal_state(), 'profile': generate_profile(),
            'accum': {}, 'phase': 'warmup', 'mode': 'NONE', 'fails': [],
            'uptime': 0, 'warmup_timer': 0, 'failure_timer': 0,
            'pseudo_timer': 0, 'pseudo_active': False,
            'pseudo_duration': 0, 'pseudo_type': 'none',
            'session_id': str(uuid.uuid4())}


def tick_device(dev, tick):
    pr, s, m, ac = dev['profile'], dev['state'], dev['mode'], dev['accum']
    N = 0.03
    nx = {
        'air_temp':  clamp(s['air_temp']  + randn()*N*R['air_temp']['std'],  R['air_temp']['min'],  R['air_temp']['max']),
        'proc_temp': clamp(s['proc_temp'] + randn()*N*R['proc_temp']['std'], R['proc_temp']['min'], R['proc_temp']['max']),
        'torque':    clamp(s['torque']    + randn()*N*R['torque']['std'],    R['torque']['min'],    R['torque']['max']),
        'vibration': clamp(s['vibration'] + randn()*N*0.01,                 R['vibration']['min'], R['vibration']['max']),
        'rpm':       clamp(s['rpm']       + randn()*N*R['rpm']['std'],       R['rpm']['min'],       R['rpm']['max']),
    }
    MR = 0.025 * tick
    if m not in ('HEAT','CLOG','DRIFT'):
        nx['proc_temp'] = clamp(nx['proc_temp']+(R['proc_temp']['mean']-nx['proc_temp'])*MR, R['proc_temp']['min'], R['proc_temp']['max'])
    if m != 'DRIFT':
        nx['air_temp'] = clamp(nx['air_temp']+(R['air_temp']['mean']-nx['air_temp'])*MR, R['air_temp']['min'], R['air_temp']['max'])
    if m not in ('CLOG','BEARING'):
        nx['torque'] = clamp(nx['torque']+(R['torque']['mean']-nx['torque'])*MR, R['torque']['min'], R['torque']['max'])
    if m != 'BEARING':
        nx['vibration'] = clamp(nx['vibration']+(0.03-nx['vibration'])*0.08*tick, R['vibration']['min'], 0.07)

    if m == 'HDF_DIRECT':
        ac['HDF'] = ac.get('HDF',0)+1
        nx['rpm'] = clamp(nx['rpm']-(0.079*tick+randn()*0.05), R['rpm']['min'], R['rpm']['max'])
        nx['proc_temp'] = clamp(nx['proc_temp']+(nx['air_temp']+7.5-nx['proc_temp'])*0.012, R['proc_temp']['min'], R['proc_temp']['max'])
    elif m == 'HEAT':
        ac['HEAT'] = ac.get('HEAT',0)+1
        rate = pr['heatRate']*math.exp(ac['HEAT']/(pr['heatDecay']/tick))*tick
        nx['proc_temp'] = clamp(nx['proc_temp']+rate, R['proc_temp']['min'], R['proc_temp']['max'])
        nx['rpm'] = clamp(nx['rpm']-pr['heatRpmDrop']*tick, R['rpm']['min'], R['rpm']['max'])
    elif m == 'CLOG':
        ac['CLOG'] = ac.get('CLOG',0)+1
        nx['torque']    = clamp(nx['torque']   +pr['clogTorque']*tick, R['torque']['min'],    R['torque']['max'])
        nx['proc_temp'] = clamp(nx['proc_temp']+pr['clogTemp']*tick,   R['proc_temp']['min'], R['proc_temp']['max'])
        nx['rpm']       = clamp(nx['rpm']      -pr['clogRpm']*tick,    R['rpm']['min'],       R['rpm']['max'])
    elif m == 'BEARING':
        ac['BEARING'] = ac.get('BEARING',0)+1
        amp = clamp(0.04+ac['BEARING']*pr['bearingAmp'], 0.04, pr['bearingMax'])
        nx['vibration'] = clamp(amp*(0.7+0.3*math.sin(ac['BEARING']*0.9)), 0.01, 2.0)
    elif m == 'PWF':
        ac['PWF'] = ac.get('PWF',0)+1
        drop = pr['powerDrop']*math.exp(-ac['PWF']/(pr['powerDecay']/tick))*tick+0.015
        nx['rpm']    = clamp(nx['rpm']-drop, R['rpm']['min'], R['rpm']['max'])
        nx['torque'] = clamp(nx['torque']+(random.random()-0.5)*2.5, R['torque']['min'], R['torque']['max'])

    dev['state'] = nx
    dev['fails'] = detect_failures(nx)
    dev['uptime'] += tick


def update_phase(dev, tick):
    phase = dev['phase']
    is_service = False

    if phase == 'warmup':
        dev['warmup_timer'] += tick
        if dev['warmup_timer'] >= 700 + random.random()*600:
            dev['phase'] = 'normal_long' if dev['normalOnly'] else 'anomaly'
            if not dev['normalOnly']:
                dev['mode'] = pick_mode()
                if dev['mode'] == 'HDF_DIRECT':
                    dev['state']['rpm']       = gauss(1500, 60, 1380, 1620)
                    dev['state']['proc_temp'] = gauss(308.5, 0.5, 307.0, 309.5)
            dev['warmup_timer'] = 0

    elif phase == 'anomaly':
        if dev['fails']:
            dev['phase'] = 'failure'
            dev['failure_timer'] = 0

    elif phase == 'failure':
        dev['failure_timer'] += tick
        if dev['failure_timer'] >= FAILURE_DURATION:
            dev['phase'] = 'service'

    elif phase == 'service':
        dev.update({'phase': 'warmup', 'mode': 'NONE', 'warmup_timer': 0,
                    'uptime': 0, 'state': normal_state(), 'accum': {},
                    'profile': generate_profile(), 'fails': [],
                    'session_id': str(uuid.uuid4())})
        is_service = True

    elif phase == 'normal_long':
        dev['pseudo_timer'] += tick
        if not dev['pseudo_active'] and dev['pseudo_timer'] > 60+random.random()*120:
            dev['pseudo_active']   = True
            dev['pseudo_duration'] = 10+random.random()*20
            dev['pseudo_timer']    = 0
            dev['pseudo_type']     = random.choice(['torque_spike','rpm_drop','vibration_bump'])
        if dev['pseudo_active']:
            s = dev['state']
            if dev['pseudo_type'] == 'torque_spike':
                s['torque']    = min(s['torque']+0.1, 46.0)
            elif dev['pseudo_type'] == 'rpm_drop':
                s['rpm']       = max(s['rpm']-1, 1500)
            elif dev['pseudo_type'] == 'vibration_bump':
                s['vibration'] = min(s['vibration']+0.002, 0.30)
            dev['pseudo_duration'] -= tick
            if dev['pseudo_duration'] <= 0:
                dev['pseudo_active'] = False

    return is_service


def build_payload(dev, is_service):
    s  = dev['state']
    f  = dev['fails']
    dT = s['proc_temp'] - s['air_temp']
    pw = s['torque'] * (s['rpm'] * 2 * math.pi / 60)
    sc = dev.get('score', 0.05)
    ts = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

    base = {'device_id': dev['id'], 'lat': dev['lat'], 'lng': dev['lng'],
            'air_temp': round(s['air_temp'],2), 'proc_temp': round(s['proc_temp'],2),
            'rpm': int(s['rpm']), 'torque': round(s['torque'],2),
            'vibration': round(s['vibration'],3), 'uptime_seconds': round(dev['uptime'],1),
            'session_id': dev['session_id'], 'ts': ts}

    if is_service:
        return {**base, 'ml_score': 0, 'failure_type': 'None', 'severity': 'OK',
                'event_type': 'service', 'resolved_failure': ','.join(f) if f else 'RESET'}

    sev = 'CRITICAL' if sc >= 0.7 else 'WARNING' if sc >= 0.4 else 'OK'
    return {**base, 'delta_temp': round(dT,2), 'power_w': round(pw,1),
            'ml_score': round(sc,3), 'failure_type': ','.join(f) if f else 'None',
            'severity': sev, 'event_type': 'telemetry'}


def send_event(url, payload):
    try:
        r = requests.post(f"{url}/event", json=payload, timeout=5)
        return r.status_code == 200
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url',      default='http://92.5.14.76:8002')
    parser.add_argument('--interval', type=int, default=10)
    args = parser.parse_args()

    log.info("HVAC Simulator 250 | url=%s interval=%ds", args.url, args.interval)

    devices = [make_device(d) for d in DEVICES]
    running = True
    def handle_signal(sig, frame):
        nonlocal running; running = False
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT,  handle_signal)

    ok_cnt = err_cnt = cycle = 0
    tick = args.interval

    while running:
        t0 = time.time()
        cycle += 1

        for dev in devices:
            tick_device(dev, tick)
            is_service = update_phase(dev, tick)
            payload = build_payload(dev, is_service)

            if send_event(args.url, payload):
                ok_cnt += 1
            else:
                err_cnt += 1

            if is_service:
                log.info("SERVICE | device=%s", dev['id'])
            elif dev['fails']:
                log.warning("FAILURE | device=%s fails=%s uptime=%.0fs",
                            dev['id'], ','.join(dev['fails']), dev['uptime'])

        if cycle % 6 == 0:
            phases = {}
            for d in devices:
                phases[d['phase']] = phases.get(d['phase'], 0) + 1
            log.info("cycle=%d ok=%d err=%d phases=%s", cycle, ok_cnt, err_cnt, phases)

        time.sleep(max(0, tick - (time.time() - t0)))

    log.info("Stopped. ok=%d err=%d", ok_cnt, err_cnt)


if __name__ == '__main__':
    main()
