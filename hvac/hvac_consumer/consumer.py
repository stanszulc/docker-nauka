"""
hvac_consumer v5 — Single-event ML Consumer
Zmiany vs v4s:
- build_features zgodny z src/features.py (model v8)
- Dodane: baseline_delta, above_threshold, margins, pre_alarm, escalation
- is_close_to_fault, min_margin_to_fault
- Usunięte: uptime_norm (nie ma w v8)
"""

import os
import json
import time
import logging
import signal
import sys
from collections import deque
from datetime import datetime, timezone

import joblib
import numpy as np
import psycopg2
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka:9092")
TOPIC_TELEMETRY  = os.getenv("TOPIC_TELEMETRY",  "hvac_telemetry")
TOPIC_ALERTS     = os.getenv("TOPIC_ALERTS",     "hvac_alerts")
TOPIC_STATUS     = os.getenv("TOPIC_STATUS",     "hvac_status")
CONSUMER_GROUP   = os.getenv("CONSUMER_GROUP",   "hvac_ml_group")
POSTGRES_DSN     = os.getenv("POSTGRES_DSN",     "postgresql://kafka:kafka@postgres:5432/events")
MODEL_PATH       = os.getenv("MODEL_PATH",       "/app/model/hvac_rf_model.pkl")
ALERT_THRESHOLD  = float(os.getenv("ALERT_THRESHOLD",  "0.5"))
CONSECUTIVE_MIN  = int(os.getenv("CONSECUTIVE_MIN",    "1"))
RETENTION_HOURS  = int(os.getenv("RETENTION_HOURS",    "168"))
LOG_LEVEL        = os.getenv("LOG_LEVEL",        "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [hvac_consumer] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Rolling windows ───────────────────────────────────────────────────────────
WIN_SHORT    = 5
WIN_MID      = 20
WIN_LONG     = 60
BASELINE_LEN = 30

BASE_SENSORS = [
    'air_temp', 'proc_temp', 'rpm', 'torque', 'vibration',
    'delta_temp', 'power_w', 'load_to_temp_ratio',
]

# ── Progi fizyczne ────────────────────────────────────────────────────────────
FAULT_THRESHOLDS = {
    'vibration_high': 0.75,
    'rpm_high':       2000,
    'rpm_low':        1000,
    'torque_high':    65,
    'torque_low':     15,
    'temp_high':      100,
}

PRE_ALARM_THRESHOLDS = {
    'vibration_high': 0.35,
    'rpm_high':       1700,
    'rpm_low':        1300,
    'torque_high':    49,
    'torque_low':     33,
    'temp_high':      75,
}

# ── Database schema ───────────────────────────────────────────────────────────
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS hvac_metrics (
    id               BIGSERIAL    PRIMARY KEY,
    device_id        VARCHAR(50)  NOT NULL,
    ts               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    server_ts        TIMESTAMPTZ,
    lat              DOUBLE PRECISION,
    lng              DOUBLE PRECISION,
    air_temp         REAL,
    proc_temp        REAL,
    rpm              INTEGER,
    torque           REAL,
    vibration        REAL,
    ml_score         REAL,
    failure_type     VARCHAR(50),
    severity         VARCHAR(10),
    app_severity     VARCHAR(10),
    is_pre_failure   SMALLINT     DEFAULT 0,
    fail_probability REAL,
    uptime_seconds   REAL,
    session_id       VARCHAR(36)
);
ALTER TABLE hvac_metrics ADD COLUMN IF NOT EXISTS is_pre_failure   SMALLINT DEFAULT 0;
ALTER TABLE hvac_metrics ADD COLUMN IF NOT EXISTS fail_probability REAL;
ALTER TABLE hvac_metrics ADD COLUMN IF NOT EXISTS uptime_seconds   REAL;
ALTER TABLE hvac_metrics ADD COLUMN IF NOT EXISTS session_id       VARCHAR(36);

CREATE TABLE IF NOT EXISTS hvac_alerts_log (
    id               BIGSERIAL    PRIMARY KEY,
    device_id        VARCHAR(50)  NOT NULL,
    ts               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    lat              DOUBLE PRECISION,
    lng              DOUBLE PRECISION,
    ml_score         REAL,
    failure_type     VARCHAR(50),
    severity         VARCHAR(10),
    event_type       VARCHAR(20)  DEFAULT 'telemetry',
    resolved_failure VARCHAR(50),
    raw_event        JSONB
);

CREATE TABLE IF NOT EXISTS hvac_device_status (
    device_id         VARCHAR(50)  PRIMARY KEY,
    lat               DOUBLE PRECISION,
    lng               DOUBLE PRECISION,
    online            BOOLEAN      DEFAULT TRUE,
    last_seen         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_severity     VARCHAR(10)  DEFAULT 'OK',
    last_failure_type VARCHAR(50)  DEFAULT 'None',
    uptime_seconds    REAL         DEFAULT 0
);
ALTER TABLE hvac_device_status ADD COLUMN IF NOT EXISTS uptime_seconds REAL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_hvac_metrics_device_ts ON hvac_metrics (device_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_hvac_metrics_ts        ON hvac_metrics (ts DESC);
CREATE INDEX IF NOT EXISTS idx_hvac_alerts_ts         ON hvac_alerts_log (ts DESC);
CREATE INDEX IF NOT EXISTS idx_hvac_alerts_device     ON hvac_alerts_log (device_id);
CREATE INDEX IF NOT EXISTS idx_hvac_alerts_event_type ON hvac_alerts_log (event_type);
"""

# ── Per-device rolling buffer ─────────────────────────────────────────────────
class DeviceBuffer:
    def __init__(self):
        # Rolling buffers per sensor
        self.bufs   = {s: deque(maxlen=WIN_LONG) for s in BASE_SENSORS}
        self.step   = 0

        # Baseline (pierwsze BASELINE_LEN ticków)
        self.baseline        = {}
        self.baseline_counts = {s: 0 for s in BASE_SENSORS}
        self.baseline_sums   = {s: 0.0 for s in BASE_SENSORS}
        self.baseline_frozen = False

        # Velocity (poprzednia wartość)
        self.prev = {s: None for s in ['proc_temp', 'rpm', 'torque', 'vibration']}

        # Pre-alarm cumsums
        self.cumsum = {
            'vib': 0, 'rpm_high': 0, 'rpm_low': 0,
            'torque_high': 0, 'torque_low': 0, 'temp': 0,
        }

        # Escalation buffer (ostatnie 5 wartości)
        self.esc_bufs = {s: deque(maxlen=6) for s in
                         ['vibration', 'torque', 'rpm', 'proc_temp']}

    def push(self, sensor_row: dict):
        fa = FAULT_THRESHOLDS
        pa = PRE_ALARM_THRESHOLDS

        for s in BASE_SENSORS:
            val = float(sensor_row.get(s, 0.0))
            self.bufs[s].append(val)

            # Baseline
            if not self.baseline_frozen:
                self.baseline_sums[s]   += val
                self.baseline_counts[s] += 1

        # Freeze baseline po BASELINE_LEN tickach
        if not self.baseline_frozen and self.step >= BASELINE_LEN - 1:
            for s in BASE_SENSORS:
                n = self.baseline_counts[s]
                self.baseline[s] = self.baseline_sums[s] / n if n > 0 else 0.0
            self.baseline_frozen = True

        # Velocity
        for s in ['proc_temp', 'rpm', 'torque', 'vibration']:
            val = float(sensor_row.get(s, 0.0))
            self.esc_bufs[s].append(val)
            self.prev[s] = val

        # Pre-alarm cumsums
        vib    = float(sensor_row.get('vibration', 0.0))
        rpm    = float(sensor_row.get('rpm', 1500.0))
        torque = float(sensor_row.get('torque', 40.0))
        temp   = float(sensor_row.get('proc_temp', 60.0))

        self.cumsum['vib']        += int(vib    > pa['vibration_high'])
        self.cumsum['rpm_high']   += int(rpm    > pa['rpm_high'])
        self.cumsum['rpm_low']    += int(rpm    < pa['rpm_low'])
        self.cumsum['torque_high']+= int(torque > pa['torque_high'])
        self.cumsum['torque_low'] += int(torque < pa['torque_low'])
        self.cumsum['temp']       += int(temp   > pa['temp_high'])

        self.step += 1

    def _rolling(self, sensor: str):
        buf = list(self.bufs[sensor])
        n   = len(buf)
        if n == 0:
            return 0.0, 0.0, 0.0, 0.0
        arr      = np.array(buf, dtype=np.float32)
        short    = float(arr[-WIN_SHORT:].mean()) if n >= WIN_SHORT else float(arr.mean())
        mid      = float(arr[-WIN_MID:].mean())   if n >= WIN_MID   else float(arr.mean())
        long_    = float(arr.mean())
        std_mid  = float(arr[-WIN_MID:].std())    if n >= WIN_MID   else (float(arr.std()) if n > 1 else 0.0)
        return short, mid, long_, std_mid

    def _escalation(self, sensor: str) -> float:
        buf = list(self.esc_bufs[sensor])
        if len(buf) < 2:
            return 0.0
        return (buf[-1] - buf[0]) / max(len(buf) - 1, 1)

    def build_features(self, uptime_seconds: float) -> dict:
        fa = FAULT_THRESHOLDS
        pa = PRE_ALARM_THRESHOLDS
        feats = {}

        # Raw sensors
        for s in BASE_SENSORS:
            buf = list(self.bufs[s])
            feats[s] = float(buf[-1]) if buf else 0.0

        # Rolling per BASE_SENSORS
        for s in BASE_SENSORS:
            short, mid, long_, std_mid = self._rolling(s)
            feats[f'{s}_ma_short']  = short
            feats[f'{s}_ma_mid']    = mid
            feats[f'{s}_ma_long']   = long_
            feats[f'{s}_trend_sl']  = short - long_
            feats[f'{s}_trend_ml']  = mid   - long_
            feats[f'{s}_std_mid']   = std_mid

        # Velocity diff(5) — uproszczony jako diff ostatniej i pierwszej w buforze
        for s in ['proc_temp', 'rpm', 'torque', 'vibration']:
            buf = list(self.esc_bufs[s])
            vel = (buf[-1] - buf[0]) / max(len(buf) - 1, 1) if len(buf) >= 2 else 0.0
            feats[f'{s}_velocity'] = vel
            # Rolling velocity
            feats[f'{s}_velocity_ma_short'] = vel
            feats[f'{s}_velocity_std_mid']  = 0.0

        # Baseline delta
        bl = self.baseline if self.baseline_frozen else {s: feats[s] for s in BASE_SENSORS}
        feats['rpm_delta_baseline']       = feats['rpm']       - bl.get('rpm', feats['rpm'])
        feats['vibration_delta_baseline'] = feats['vibration'] - bl.get('vibration', feats['vibration'])
        feats['temp_delta_baseline']      = feats['proc_temp'] - bl.get('proc_temp', feats['proc_temp'])
        feats['torque_delta_baseline']    = feats['torque']    - bl.get('torque', feats['torque'])
        feats['power_delta_baseline']     = feats['power_w']   - bl.get('power_w', feats['power_w'])

        for s in ['rpm_delta_baseline', 'vibration_delta_baseline',
                  'temp_delta_baseline', 'torque_delta_baseline']:
            feats[f'{s}_ma_short'] = feats[s]
            feats[f'{s}_ma_mid']   = feats[s]

        # Above/below threshold
        feats['vibration_above_prealarm'] = max(0.0, feats['vibration'] - pa['vibration_high'])
        feats['vibration_above_failure']  = max(0.0, feats['vibration'] - fa['vibration_high'])
        feats['rpm_above_prealarm']       = max(0.0, feats['rpm']       - pa['rpm_high'])
        feats['rpm_above_failure']        = max(0.0, feats['rpm']       - fa['rpm_high'])
        feats['rpm_below_prealarm']       = max(0.0, pa['rpm_low']      - feats['rpm'])
        feats['rpm_below_failure']        = max(0.0, fa['rpm_low']      - feats['rpm'])
        feats['torque_above_prealarm']    = max(0.0, feats['torque']    - pa['torque_high'])
        feats['torque_above_failure']     = max(0.0, feats['torque']    - fa['torque_high'])
        feats['torque_below_prealarm']    = max(0.0, pa['torque_low']   - feats['torque'])
        feats['torque_below_failure']     = max(0.0, fa['torque_low']   - feats['torque'])
        feats['temp_above_prealarm']      = max(0.0, feats['proc_temp'] - pa['temp_high'])
        feats['temp_above_failure']       = max(0.0, feats['proc_temp'] - fa['temp_high'])

        # Buffer fill
        n = self.step
        feats['buffer_fill_short'] = min(1.0, n / WIN_SHORT)
        feats['buffer_fill_mid']   = min(1.0, n / WIN_MID)
        feats['buffer_fill_long']  = min(1.0, n / WIN_LONG)

        # Margins
        feats['hdf_margin']               = feats['delta_temp'] - 8.6
        feats['pwf_low']                  = feats['power_w'] - 3500
        feats['pwf_high']                 = 9000 - feats['power_w']
        feats['pwf_margin']               = min(feats['pwf_low'], feats['pwf_high'])
        feats['margin_vibration_fault']   = fa['vibration_high'] - feats['vibration']
        feats['margin_torque_high_fault'] = fa['torque_high']    - feats['torque']
        feats['margin_torque_low_fault']  = feats['torque']      - fa['torque_low']
        feats['margin_temp_fault']        = fa['temp_high']      - feats['proc_temp']
        feats['margin_rpm_high_fault']    = fa['rpm_high']       - feats['rpm']
        feats['margin_rpm_low_fault']     = feats['rpm']         - fa['rpm_low']

        margins = [
            feats['margin_vibration_fault'],   feats['margin_torque_high_fault'],
            feats['margin_torque_low_fault'],  feats['margin_temp_fault'],
            feats['margin_rpm_high_fault'],    feats['margin_rpm_low_fault'],
        ]
        feats['min_margin_to_fault'] = min(margins)
        feats['is_close_to_fault']   = int(feats['min_margin_to_fault'] < 5)

        # Instability
        feats['thermal_instability']    = feats['proc_temp_std_mid'] * feats.get('delta_temp_std_mid', 0.0)
        feats['mechanical_instability'] = feats['vibration_std_mid'] * feats['torque_std_mid']
        feats['load_ratio_trend']       = feats['load_to_temp_ratio_trend_sl']

        # Pre-alarm binary + cumsum
        vib    = feats['vibration']
        rpm    = feats['rpm']
        torque = feats['torque']
        temp   = feats['proc_temp']

        feats['in_pre_alarm_vib']                = int(vib    > pa['vibration_high'])
        feats['in_pre_alarm_vib_cumsum']         = self.cumsum['vib']
        feats['in_pre_alarm_rpm_high']           = int(rpm    > pa['rpm_high'])
        feats['in_pre_alarm_rpm_high_cumsum']    = self.cumsum['rpm_high']
        feats['in_pre_alarm_rpm_low']            = int(rpm    < pa['rpm_low'])
        feats['in_pre_alarm_rpm_low_cumsum']     = self.cumsum['rpm_low']
        feats['in_pre_alarm_torque_high']        = int(torque > pa['torque_high'])
        feats['in_pre_alarm_torque_high_cumsum'] = self.cumsum['torque_high']
        feats['in_pre_alarm_torque_low']         = int(torque < pa['torque_low'])
        feats['in_pre_alarm_torque_low_cumsum']  = self.cumsum['torque_low']
        feats['in_pre_alarm_temp']               = int(temp   > pa['temp_high'])
        feats['in_pre_alarm_temp_cumsum']        = self.cumsum['temp']

        # Escalation
        feats['vibration_escalation'] = self._escalation('vibration')
        feats['torque_escalation']    = self._escalation('torque')
        feats['rpm_escalation']       = self._escalation('rpm')
        feats['proc_temp_escalation'] = self._escalation('proc_temp')

        # Interaction features
        feats['vibration_x_proc_temp']          = feats['vibration'] * feats['proc_temp']
        feats['rpm_x_torque']                   = feats['rpm']       * feats['torque']
        feats['vibration_x_proc_temp_ma_short'] = feats['vibration_x_proc_temp']
        feats['vibration_x_proc_temp_ma_mid']   = feats['vibration_x_proc_temp']
        feats['vibration_x_proc_temp_std_mid']  = 0.0
        feats['rpm_x_torque_ma_short']          = feats['rpm_x_torque']
        feats['rpm_x_torque_ma_mid']            = feats['rpm_x_torque']
        feats['rpm_x_torque_std_mid']           = 0.0

        return feats

    def reset(self):
        self.__init__()


# ── Global state ──────────────────────────────────────────────────────────────
DEVICE_BUFFERS: dict = {}
DEVICE_UPTIME:  dict = {}
DEVICE_STREAK:  dict = {}


def get_or_create_buffer(device_id: str) -> DeviceBuffer:
    if device_id not in DEVICE_BUFFERS:
        DEVICE_BUFFERS[device_id] = DeviceBuffer()
    return DEVICE_BUFFERS[device_id]


def update_streak(device_id: str, is_pre_failure_raw: int) -> int:
    if is_pre_failure_raw == 1:
        DEVICE_STREAK[device_id] = DEVICE_STREAK.get(device_id, 0) + 1
    else:
        DEVICE_STREAK[device_id] = 0
    return 1 if DEVICE_STREAK.get(device_id, 0) >= CONSECUTIVE_MIN else 0


def reset_streak(device_id: str):
    DEVICE_STREAK[device_id] = 0


# ── Model ─────────────────────────────────────────────────────────────────────
def load_model(path: str):
    if not os.path.exists(path):
        log.warning("Model not found at %s", path)
        return None
    bundle = joblib.load(path)
    log.info("Model loaded: %s | version=%s features=%d threshold=%.2f",
             path, bundle.get('version', '?'),
             len(bundle.get('feature_cols', [])),
             bundle.get('threshold', 0.5))
    return bundle


def infer(bundle, event: dict, uptime_seconds: float) -> tuple:
    failure_type = event.get("failure_type", "None")
    app_score    = event.get("ml_score", 0.0)
    if bundle is None:
        return app_score, failure_type, 0, app_score
    try:
        device_id = event["device_id"]
        buf       = get_or_create_buffer(device_id)

        # Policz pochodne fizyczne
        proc_temp = float(event.get("proc_temp", 60.0))
        air_temp  = float(event.get("air_temp",  300.0))
        rpm       = float(event.get("rpm",       1500))
        torque    = float(event.get("torque",    40.0))
        vibration = float(event.get("vibration", 0.03))

        dT    = proc_temp - air_temp
        power = torque * (rpm * 2 * np.pi / 60)
        ltr   = power / (abs(dT) + 1e-3)

        sensor_row = {
            'air_temp':          air_temp,
            'proc_temp':         proc_temp,
            'rpm':               rpm,
            'torque':            torque,
            'vibration':         vibration,
            'delta_temp':        dT,
            'power_w':           power,
            'load_to_temp_ratio': ltr,
        }
        buf.push(sensor_row)
        feats     = buf.build_features(uptime_seconds)
        feat_cols = bundle['feature_cols']
        X         = np.array([[feats.get(c, 0.0) for c in feat_cols]])

        proba              = bundle['model'].predict_proba(X)[0]
        fail_prob          = float(proba[1])
        threshold          = float(os.getenv("ALERT_THRESHOLD", str(bundle.get("threshold", 0.5))))
        is_pre_failure_raw = 1 if fail_prob >= threshold else 0
        is_pre_failure     = update_streak(device_id, is_pre_failure_raw)

        return round(fail_prob, 4), failure_type, is_pre_failure, round(fail_prob, 4)
    except Exception as e:
        log.error("Inference error: %s", e)
        return app_score, failure_type, 0, app_score


def score_to_severity(score: float) -> str:
    if score >= 0.7: return "CRITICAL"
    if score >= 0.4: return "WARNING"
    return "OK"


# ── Postgres ──────────────────────────────────────────────────────────────────
def connect_postgres(retries: int = 10):
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            conn.autocommit = False
            log.info("PostgreSQL connected")
            return conn
        except Exception as e:
            log.warning("Postgres not ready (%d/%d): %s", attempt + 1, retries, e)
            time.sleep(3)
    sys.exit(1)


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    log.info("Schema ready")


def get_uptime(device_id: str) -> float:
    now_ts = time.time()
    if device_id not in DEVICE_UPTIME:
        DEVICE_UPTIME[device_id] = {'uptime': 0.0, 'last_ts': now_ts}
    tracker = DEVICE_UPTIME[device_id]
    delta = max(0.0, now_ts - tracker['last_ts'])
    tracker['uptime'] += delta
    tracker['last_ts'] = now_ts
    return tracker['uptime']


def reset_uptime(device_id: str):
    DEVICE_UPTIME[device_id] = {'uptime': 0.0, 'last_ts': time.time()}
    if device_id in DEVICE_BUFFERS:
        DEVICE_BUFFERS[device_id].reset()
    reset_streak(device_id)


def save_event(conn, event: dict, ml_score, failure_type, is_pre_failure,
               fail_prob, uptime, severity):
    device_id = event.get("device_id", "unknown")
    ts_val    = event.get("ts", datetime.now(timezone.utc).isoformat())
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO hvac_metrics
                  (device_id, ts, lat, lng,
                   air_temp, proc_temp, rpm, torque, vibration,
                   ml_score, failure_type, severity, app_severity,
                   is_pre_failure, fail_probability, uptime_seconds, session_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                device_id, ts_val,
                event.get("lat"), event.get("lng"),
                event.get("air_temp"), event.get("proc_temp"),
                event.get("rpm"), event.get("torque"), event.get("vibration"),
                ml_score, failure_type, severity,
                event.get("severity", "OK"),
                is_pre_failure, fail_prob,
                round(uptime, 1), event.get("session_id"),
            ))

            cur.execute("""
                INSERT INTO hvac_device_status
                  (device_id, lat, lng, last_seen, last_severity,
                   last_failure_type, uptime_seconds)
                VALUES (%s,%s,%s,NOW(),%s,%s,%s)
                ON CONFLICT (device_id) DO UPDATE SET
                  lat=EXCLUDED.lat, lng=EXCLUDED.lng, last_seen=NOW(),
                  last_severity=EXCLUDED.last_severity,
                  last_failure_type=EXCLUDED.last_failure_type,
                  uptime_seconds=EXCLUDED.uptime_seconds, online=TRUE
            """, (device_id, event.get("lat"), event.get("lng"),
                  severity, failure_type, round(uptime, 1)))

            if is_pre_failure or ml_score >= ALERT_THRESHOLD:
                cur.execute("""
                    INSERT INTO hvac_alerts_log
                      (device_id, ts, lat, lng, ml_score, failure_type,
                       severity, event_type, raw_event)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    device_id, ts_val,
                    event.get("lat"), event.get("lng"),
                    ml_score, failure_type, severity,
                    event.get("event_type", "telemetry"),
                    json.dumps(event),
                ))
        conn.commit()
    except Exception as e:
        log.error("DB error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass


# ── Kafka ─────────────────────────────────────────────────────────────────────
def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers":       KAFKA_BOOTSTRAP,
        "group.id":                CONSUMER_GROUP,
        "auto.offset.reset":       "latest",
        "enable.auto.commit":      True,
        "auto.commit.interval.ms": 5000,
        "session.timeout.ms":      30000,
        "heartbeat.interval.ms":   10000,
    })


def ensure_topics():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    topics = [
        NewTopic(TOPIC_TELEMETRY, num_partitions=1, replication_factor=1),
        NewTopic(TOPIC_ALERTS,    num_partitions=1, replication_factor=1),
        NewTopic(TOPIC_STATUS,    num_partitions=1, replication_factor=1),
    ]
    futures = admin.create_topics(topics)
    for topic, future in futures.items():
        try:
            future.result()
        except Exception as e:
            log.warning("Topic %s: %s", topic, e)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("hvac_consumer v5 | kafka=%s group=%s", KAFKA_BOOTSTRAP, CONSUMER_GROUP)

    running = True
    def handle_signal(sig, frame):
        nonlocal running
        log.info("Shutdown signal received")
        running = False
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT,  handle_signal)

    conn  = connect_postgres()
    ensure_schema(conn)
    model = load_model(MODEL_PATH)

    for attempt in range(10):
        try:
            ensure_topics()
            break
        except Exception as e:
            log.warning("Kafka not ready (%d): %s", attempt + 1, e)
            time.sleep(3)

    consumer = create_consumer()
    consumer.subscribe([TOPIC_TELEMETRY, TOPIC_STATUS])
    log.info("Subscribed | topics: %s, %s", TOPIC_TELEMETRY, TOPIC_STATUS)

    msg_count      = 0
    last_retention = time.time()

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Consumer error: %s", msg.error())
                continue

            topic = msg.topic()
            try:
                event = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                log.warning("Decode error: %s", e)
                continue

            try:
                device_id  = event.get("device_id", "unknown")
                event_type = event.get("event_type", "telemetry")

                if topic == TOPIC_TELEMETRY:
                    if event_type == "service":
                        reset_uptime(device_id)
                        with conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO hvac_device_status
                                  (device_id, lat, lng, last_seen, last_severity,
                                   last_failure_type, uptime_seconds)
                                VALUES (%s,%s,%s,NOW(),'OK','None',0)
                                ON CONFLICT (device_id) DO UPDATE SET
                                  last_seen=NOW(), last_severity='OK',
                                  last_failure_type='None', uptime_seconds=0,
                                  online=TRUE
                            """, (device_id, event.get("lat"), event.get("lng")))
                        conn.commit()
                        log.info("SERVICE | device=%s resolved=%s",
                                 device_id, event.get('resolved_failure'))
                    else:
                        event_uptime = event.get("uptime_seconds")
                        if event_uptime is not None:
                            uptime = float(event_uptime)
                            DEVICE_UPTIME[device_id] = {
                                'uptime': uptime, 'last_ts': time.time()
                            }
                        else:
                            uptime = get_uptime(device_id)

                        ml_score, failure_type, is_pre_failure, fail_prob = \
                            infer(model, event, uptime)
                        severity = score_to_severity(ml_score)

                        save_event(conn, event, ml_score, failure_type,
                                   is_pre_failure, fail_prob, uptime, severity)

                        if is_pre_failure:
                            log.warning(
                                "PRE-FAILURE | device=%s prob=%.2f streak=%d "
                                "failure=%s uptime=%.0fs",
                                device_id, fail_prob,
                                DEVICE_STREAK.get(device_id, 0),
                                failure_type, uptime,
                            )

                        msg_count += 1
                        if msg_count % 100 == 0:
                            log.info("Processed %d events", msg_count)

            except Exception as e:
                log.error("Processing error: %s", e)
                try:
                    conn.rollback()
                except Exception:
                    pass

            if time.time() - last_retention > 3600:
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"DELETE FROM hvac_metrics "
                            f"WHERE ts < NOW() - INTERVAL '{RETENTION_HOURS} hours'"
                        )
                    conn.commit()
                    last_retention = time.time()
                except Exception as e:
                    log.warning("Retention failed: %s", e)

    finally:
        log.info("Closing. Total processed: %d", msg_count)
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
