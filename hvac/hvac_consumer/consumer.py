"""
hvac_consumer v3 — ML Consumer
Reads from Kafka hvac_telemetry, runs XGBoost v5 classifier inference,
writes enriched records to PostgreSQL.

Changes vs v2:
- Rolling buffer na 3 oknach: SHORT=5, MID=20, LONG=60 ticków
- load_to_temp_ratio = power_w / (delta_temp + 1e-3)
- trend_sl = ma_short - ma_long, trend_ml = ma_mid - ma_long
- thermal_instability, mechanical_instability cross-features
- proximity: hdf_margin, pwf_margin, load_ratio_trend
- buffer_fill_short/mid/long
- Kompatybilny z hvac_classifier_v5.pkl
"""

import os
import json
import time
import logging
import signal
import sys
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
RETENTION_HOURS  = int(os.getenv("RETENTION_HOURS",    "24"))
LOG_LEVEL        = os.getenv("LOG_LEVEL",        "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [hvac_consumer] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Rolling window config (musi zgadzać się z generate_v5.py) ─────────────────
WIN_SHORT = 5
WIN_MID   = 20
WIN_LONG  = 60

BASE_SENSORS = [
    'air_temp', 'proc_temp', 'rpm', 'torque',
    'vibration', 'delta_temp', 'power_w',
    'proc_temp_velocity', 'rpm_velocity',
    'torque_velocity', 'vibration_velocity',
    'load_to_temp_ratio',
]

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
CREATE INDEX IF NOT EXISTS idx_hvac_alerts_event_type ON hvac_alerts_log (event_type, ts DESC);
"""

RETENTION_SQL = "DELETE FROM hvac_metrics WHERE ts < NOW() - INTERVAL '{hours} hours';"

# ── Per-device rolling buffer ─────────────────────────────────────────────────
class DeviceBuffer:
    """
    Trzyma historię WIN_LONG próbek per urządzenie.
    Liczy rolling features identycznie jak generate_v5.py.
    """
    def __init__(self):
        self.bufs   = {s: np.zeros(WIN_LONG, dtype=np.float32) for s in BASE_SENSORS}
        self.counts = {s: 0 for s in BASE_SENSORS}
        self.step   = 0

    def push(self, sensor_row: dict):
        for s in BASE_SENSORS:
            val = sensor_row.get(s, 0.0)
            c   = self.counts[s]
            if c < WIN_LONG:
                self.bufs[s][c] = val
            else:
                self.bufs[s][:-1] = self.bufs[s][1:]
                self.bufs[s][-1]  = val
            self.counts[s] = min(c + 1, WIN_LONG)
        self.step += 1

    def build_features(self, uptime_seconds: float) -> dict:
        feats = {}

        # Surowe sensory (ostatnia wartość)
        for s in BASE_SENSORS:
            n = self.counts[s]
            feats[s] = float(self.bufs[s][n-1]) if n > 0 else 0.0

        # Rolling features per sensor
        for s in BASE_SENSORS:
            n   = self.counts[s]
            buf = self.bufs[s][:n]

            if n == 0:
                short, mid, long_, std_mid = 0.0, 0.0, 0.0, 0.0
            else:
                short   = float(buf[-WIN_SHORT:].mean()) if n >= WIN_SHORT else float(buf.mean())
                mid     = float(buf[-WIN_MID:].mean())   if n >= WIN_MID   else float(buf.mean())
                long_   = float(buf.mean())
                std_mid = float(buf[-WIN_MID:].std())    if n >= WIN_MID   else (float(buf.std()) if n > 1 else 0.0)

            feats[f'{s}_ma_short']  = short
            feats[f'{s}_ma_mid']    = mid
            feats[f'{s}_ma_long']   = long_
            feats[f'{s}_trend_sl']  = short - long_
            feats[f'{s}_trend_ml']  = mid   - long_
            feats[f'{s}_std_mid']   = std_mid

        # Buffer fill ratios
        n_steps = self.step
        feats['buffer_fill_short'] = min(1.0, n_steps / WIN_SHORT)
        feats['buffer_fill_mid']   = min(1.0, n_steps / WIN_MID)
        feats['buffer_fill_long']  = min(1.0, n_steps / WIN_LONG)

        # Uptime
        feats['uptime_norm'] = uptime_seconds / 1800.0

        # Proximity features
        delta_temp = feats.get('delta_temp', 10.0)
        power_w    = feats.get('power_w', 6500.0)
        feats['hdf_margin']  = delta_temp - 8.6
        feats['pwf_low']     = power_w - 3500
        feats['pwf_high']    = 9000 - power_w
        feats['pwf_margin']  = min(feats['pwf_low'], feats['pwf_high'])

        # Cross-features
        pt_std  = feats.get('proc_temp_std_mid', 0.0)
        dt_std  = feats.get('delta_temp_std_mid', 0.0)
        vib_std = feats.get('vibration_std_mid', 0.0)
        tor_std = feats.get('torque_std_mid', 0.0)
        feats['thermal_instability']    = pt_std * dt_std
        feats['mechanical_instability'] = vib_std * tor_std
        feats['load_ratio_trend']       = feats.get('load_to_temp_ratio_trend_sl', 0.0)

        return feats

    def reset(self):
        self.bufs   = {s: np.zeros(WIN_LONG, dtype=np.float32) for s in BASE_SENSORS}
        self.counts = {s: 0 for s in BASE_SENSORS}
        self.step   = 0


# ── Global state ──────────────────────────────────────────────────────────────
DEVICE_BUFFERS: dict[str, DeviceBuffer] = {}
DEVICE_UPTIME:  dict = {}


def get_or_create_buffer(device_id: str) -> DeviceBuffer:
    if device_id not in DEVICE_BUFFERS:
        DEVICE_BUFFERS[device_id] = DeviceBuffer()
    return DEVICE_BUFFERS[device_id]


# ── Model ─────────────────────────────────────────────────────────────────────
def load_model(path: str):
    if not os.path.exists(path):
        log.warning("Model not found at %s — using heuristic ml_score", path)
        return None
    bundle = joblib.load(path)
    log.info("Model loaded: %s | version=%s features=%d threshold=%.2f",
             path,
             bundle.get('version', '?'),
             len(bundle.get('feature_cols', [])),
             bundle.get('threshold', 0.5))
    return bundle


def infer(bundle, event: dict, uptime_seconds: float) -> tuple:
    """Returns (ml_score, failure_type, is_pre_failure, fail_probability)."""
    failure_type = event.get("failure_type", "None")
    app_score    = event.get("ml_score", 0.0)

    if bundle is None:
        return app_score, failure_type, 0, app_score

    try:
        device_id = event["device_id"]
        buf       = get_or_create_buffer(device_id)

        # Oblicz pochodne
        dT    = event.get("proc_temp", 310.0) - event.get("air_temp", 300.0)
        power = event.get("torque", 40.0) * (event.get("rpm", 1538) * 2 * np.pi / 60)
        ltr   = power / (abs(dT) + 1e-3)

        # Velocity — różnica względem ostatniej próbki w buforze
        prev_proc_temp  = float(buf.bufs['proc_temp'][buf.counts['proc_temp']-1])  if buf.counts['proc_temp']  > 0 else event.get("proc_temp",  310.0)
        prev_rpm        = float(buf.bufs['rpm'][buf.counts['rpm']-1])              if buf.counts['rpm']        > 0 else event.get("rpm",         1538)
        prev_torque     = float(buf.bufs['torque'][buf.counts['torque']-1])        if buf.counts['torque']     > 0 else event.get("torque",       40.0)
        prev_vibration  = float(buf.bufs['vibration'][buf.counts['vibration']-1])  if buf.counts['vibration']  > 0 else event.get("vibration",    0.03)

        sensor_row = {
            'air_temp':             event.get("air_temp",   300.0),
            'proc_temp':            event.get("proc_temp",  310.0),
            'rpm':                  float(event.get("rpm",  1538)),
            'torque':               event.get("torque",      40.0),
            'vibration':            event.get("vibration",   0.03),
            'delta_temp':           dT,
            'power_w':              power,
            'proc_temp_velocity':   event.get("proc_temp",  310.0) - prev_proc_temp,
            'rpm_velocity':         float(event.get("rpm",  1538))  - prev_rpm,
            'torque_velocity':      event.get("torque",      40.0)  - prev_torque,
            'vibration_velocity':   event.get("vibration",   0.03)  - prev_vibration,
            'load_to_temp_ratio':   ltr,
        }

        buf.push(sensor_row)
        feats     = buf.build_features(uptime_seconds)
        feat_cols = bundle['feature_cols']
        X         = np.array([[feats.get(c, 0.0) for c in feat_cols]])

        proba          = bundle['model'].predict_proba(X)[0]
        fail_prob      = float(proba[1])
        threshold      = bundle.get('threshold', 0.5)
        is_pre_failure = 1 if fail_prob >= threshold else 0
        ml_score       = fail_prob

        return round(ml_score, 4), failure_type, is_pre_failure, round(fail_prob, 4)

    except Exception as e:
        log.error("Inference error: %s", e)
        return app_score, failure_type, 0, app_score


def score_to_severity(score: float) -> str:
    if score >= 0.7:  return "CRITICAL"
    if score >= 0.4:  return "WARNING"
    return "OK"


# ── SQL ───────────────────────────────────────────────────────────────────────
INSERT_METRIC = """
INSERT INTO hvac_metrics
  (device_id, ts, server_ts, lat, lng,
   air_temp, proc_temp, rpm, torque, vibration,
   ml_score, failure_type, severity, app_severity,
   is_pre_failure, fail_probability, uptime_seconds, session_id)
VALUES
  (%(device_id)s, %(ts)s, %(server_ts)s, %(lat)s, %(lng)s,
   %(air_temp)s, %(proc_temp)s, %(rpm)s, %(torque)s, %(vibration)s,
   %(ml_score)s, %(failure_type)s, %(severity)s, %(app_severity)s,
   %(is_pre_failure)s, %(fail_probability)s, %(uptime_seconds)s, %(session_id)s)
"""

INSERT_ALERT = """
INSERT INTO hvac_alerts_log
  (device_id, ts, lat, lng, ml_score, failure_type, severity,
   event_type, resolved_failure, raw_event)
VALUES
  (%(device_id)s, %(ts)s, %(lat)s, %(lng)s, %(ml_score)s,
   %(failure_type)s, %(severity)s,
   %(event_type)s, %(resolved_failure)s, %(raw_event)s)
"""

UPSERT_STATUS = """
INSERT INTO hvac_device_status
  (device_id, lat, lng, last_seen, last_severity, last_failure_type, uptime_seconds)
VALUES
  (%(device_id)s, %(lat)s, %(lng)s, NOW(), %(severity)s, %(failure_type)s, %(uptime)s)
ON CONFLICT (device_id) DO UPDATE SET
  lat               = EXCLUDED.lat,
  lng               = EXCLUDED.lng,
  last_seen         = NOW(),
  last_severity     = EXCLUDED.last_severity,
  last_failure_type = EXCLUDED.last_failure_type,
  uptime_seconds    = EXCLUDED.uptime_seconds,
  online            = TRUE
"""


# ── Postgres helpers ──────────────────────────────────────────────────────────
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


def run_retention(conn):
    with conn.cursor() as cur:
        cur.execute(RETENTION_SQL.format(hours=RETENTION_HOURS))
    conn.commit()


def restore_uptime(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (device_id) device_id, uptime_seconds, ts
                FROM hvac_metrics WHERE uptime_seconds IS NOT NULL
                ORDER BY device_id, ts DESC
            """)
            for row in cur.fetchall():
                dev_id, last_uptime, last_ts = row
                if last_uptime is not None:
                    DEVICE_UPTIME[dev_id] = {
                        'uptime': float(last_uptime),
                        'last_ts': last_ts.timestamp()
                    }
                    log.info("Restored uptime | device=%s uptime=%.0fs", dev_id, last_uptime)
    except Exception as e:
        log.warning("Could not restore uptime: %s", e)


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


# ── Kafka ─────────────────────────────────────────────────────────────────────
def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers":     KAFKA_BOOTSTRAP,
        "group.id":              CONSUMER_GROUP,
        "auto.offset.reset":     "latest",
        "enable.auto.commit":    False,
        "session.timeout.ms":    30000,
        "heartbeat.interval.ms": 10000,
        "max.poll.interval.ms":  300000,
    })


def ensure_topics():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    topics = [
        NewTopic(TOPIC_TELEMETRY, num_partitions=3, replication_factor=1,
                 config={"retention.ms": "3600000", "retention.bytes": "104857600"}),
        NewTopic(TOPIC_ALERTS,    num_partitions=1, replication_factor=1,
                 config={"retention.ms": "604800000", "retention.bytes": "10485760"}),
        NewTopic(TOPIC_STATUS,    num_partitions=1, replication_factor=1,
                 config={"retention.ms": "3600000", "retention.bytes": "10485760"}),
    ]
    futures = admin.create_topics(topics)
    for topic, future in futures.items():
        try:
            future.result()
        except Exception as e:
            if "TopicExistsException" not in str(type(e)):
                log.warning("Topic %s: %s", topic, e)


# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    log.info("hvac_consumer v3 | kafka=%s model=%s", KAFKA_BOOTSTRAP, MODEL_PATH)

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
    log.info("Subscribed to topics: %s, %s", TOPIC_TELEMETRY, TOPIC_STATUS)

    restore_uptime(conn)

    last_retention = time.time()
    msg_count      = 0

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
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.warning("Cannot decode message: %s", e)
                consumer.commit(message=msg)
                continue

            try:
                if topic == TOPIC_TELEMETRY:
                    event_type = event.get("event_type", "telemetry")
                    device_id  = event.get("device_id", "unknown")

                    if event_type == "service":
                        reset_uptime(device_id)
                        with conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO hvac_device_status
                                  (device_id, lat, lng, last_seen, last_severity,
                                   last_failure_type, uptime_seconds)
                                VALUES (%(device_id)s, %(lat)s, %(lng)s, NOW(), 'OK', 'None', 0)
                                ON CONFLICT (device_id) DO UPDATE SET
                                  last_seen=NOW(), last_severity='OK',
                                  last_failure_type='None', uptime_seconds=0, online=TRUE
                            """, {"device_id": device_id,
                                  "lat": event.get("lat"),
                                  "lng": event.get("lng")})
                        conn.commit()
                        log.info("SERVICE | device=%s resolved=%s",
                                 device_id, event.get('resolved_failure'))

                    else:
                        # uptime z eventu ma priorytet
                        event_uptime = event.get("uptime_seconds")
                        if event_uptime is not None:
                            uptime = float(event_uptime)
                            DEVICE_UPTIME[device_id] = {'uptime': uptime, 'last_ts': time.time()}
                        else:
                            uptime = get_uptime(device_id)

                        ml_score, failure_type, is_pre_failure, fail_prob = infer(model, event, uptime)
                        severity = score_to_severity(ml_score)

                        row = {
                            "device_id":        device_id,
                            "ts":               event.get("ts", datetime.now(timezone.utc).isoformat()),
                            "server_ts":        datetime.fromtimestamp(event["server_ts"], tz=timezone.utc)
                                                if "server_ts" in event else None,
                            "lat":              event.get("lat"),
                            "lng":              event.get("lng"),
                            "air_temp":         event.get("air_temp"),
                            "proc_temp":        event.get("proc_temp"),
                            "rpm":              event.get("rpm"),
                            "torque":           event.get("torque"),
                            "vibration":        event.get("vibration"),
                            "ml_score":         ml_score,
                            "failure_type":     failure_type,
                            "severity":         severity,
                            "app_severity":     event.get("severity", "OK"),
                            "is_pre_failure":   is_pre_failure,
                            "fail_probability": fail_prob,
                            "uptime_seconds":   round(uptime, 1),
                            "session_id":       event.get("session_id"),
                        }

                        with conn.cursor() as cur:
                            cur.execute(INSERT_METRIC, row)
                            cur.execute(UPSERT_STATUS, {
                                "device_id":    device_id,
                                "lat":          event.get("lat"),
                                "lng":          event.get("lng"),
                                "severity":     severity,
                                "failure_type": failure_type,
                                "uptime":       round(uptime, 1),
                            })
                            if ml_score >= ALERT_THRESHOLD or is_pre_failure:
                                cur.execute(INSERT_ALERT, {
                                    "device_id":        device_id,
                                    "ts":               row["ts"],
                                    "lat":              event.get("lat"),
                                    "lng":              event.get("lng"),
                                    "ml_score":         ml_score,
                                    "failure_type":     failure_type,
                                    "severity":         severity,
                                    "event_type":       event_type,
                                    "resolved_failure": event.get("resolved_failure"),
                                    "raw_event":        json.dumps(event),
                                })
                        conn.commit()

                        if is_pre_failure:
                            log.warning("PRE-FAILURE | device=%s prob=%.2f failure=%s uptime=%.0fs",
                                        device_id, fail_prob, failure_type, uptime)

                        msg_count += 1
                        if msg_count % 100 == 0:
                            log.info("Processed %d events | %s score=%.3f pre_fail=%d",
                                     msg_count, device_id, ml_score, is_pre_failure)

                elif topic == TOPIC_STATUS:
                    with conn.cursor() as cur:
                        cur.execute(UPSERT_STATUS, {
                            "device_id":    event.get("device_id"),
                            "lat":          event.get("lat"),
                            "lng":          event.get("lng"),
                            "severity":     "OK",
                            "failure_type": "None",
                            "uptime":       0,
                        })
                    conn.commit()

                consumer.commit(message=msg)

            except psycopg2.OperationalError as e:
                log.error("Postgres error: %s — reconnecting", e)
                conn = connect_postgres()
                ensure_schema(conn)
            except Exception as e:
                log.error("Processing error: %s", e)
                try:
                    conn.rollback()
                except Exception:
                    pass

            if time.time() - last_retention > 3600:
                try:
                    run_retention(conn)
                    last_retention = time.time()
                except Exception as e:
                    log.warning("Retention failed: %s", e)

    finally:
        log.info("Closing consumer and DB connection")
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
