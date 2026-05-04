"""
hvac_consumer v2 — ML Consumer
Reads from Kafka hvac_telemetry, runs XGBoost classifier inference,
writes enriched records to PostgreSQL.

Changes vs v1:
- Removed tool_wear, product_type (CNC-specific)
- New failure types: HDF, PWF, CLOG, BEARING
- Classifier model (is_pre_failure 0/1) instead of RUL regressor
- uptime_seconds — continuous counter per device (resets on SERVICE)
- session_id support
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
ALERT_THRESHOLD  = float(os.getenv("ALERT_THRESHOLD",  "0.4"))
RETENTION_HOURS  = int(os.getenv("RETENTION_HOURS",    "24"))
LOG_LEVEL        = os.getenv("LOG_LEVEL",        "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [hvac_consumer] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

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

# ── ML Model ──────────────────────────────────────────────────────────────────
DEVICE_BUFFERS: dict = {}
WINDOW = 20
DEVICE_UPTIME: dict = {}

BASE_FEATURES = [
    'air_temp', 'proc_temp', 'rpm', 'torque',
    'vibration', 'delta_temp', 'power_w',
    'proc_temp_velocity', 'rpm_velocity',
    'torque_velocity', 'vibration_velocity',
]


def load_model(path: str):
    if not os.path.exists(path):
        log.warning("Model not found at %s — using heuristic ml_score", path)
        return None
    bundle = joblib.load(path)
    log.info("Model loaded: %s | type=%s features=%d",
             path, bundle.get('model_type', 'unknown'), len(bundle.get('feature_cols', [])))
    return bundle


def _engineer_row(buf: list, uptime_seconds: float = 0.0) -> dict:
    n = len(buf)
    feats = {'uptime_norm': uptime_seconds / 1800.0}

    for feat in BASE_FEATURES:
        vals = [r.get(feat, 0.0) for r in buf]
        feats[f'{feat}_rmean'] = float(np.mean(vals))
        feats[f'{feat}_rgrad'] = float(np.mean(np.diff(vals))) if n > 1 else 0.0
        feats[f'{feat}_rstd']  = float(np.std(vals)) if n > 1 else 0.0

    last = buf[-1]
    feats['proc_temp_rate'] = feats['proc_temp_rgrad']
    feats['rpm_rate']       = feats['rpm_rgrad']
    feats['torque_rate']    = feats['torque_rgrad']
    feats['vibration_rate'] = feats['vibration_rgrad']
    feats['hdf_margin']     = last.get('delta_temp', 10.0) - 8.6
    feats['pwf_margin']     = min(last.get('power_w', 6500) - 3500,
                                  9000 - last.get('power_w', 6500))
    return feats


def infer(bundle, event: dict, uptime_seconds: float) -> tuple:
    """Returns (ml_score, failure_type, is_pre_failure, fail_probability)."""
    failure_type = event.get("failure_type", "None")
    app_score    = event.get("ml_score", 0.0)

    if bundle is None:
        return app_score, failure_type, 0, app_score

    try:
        device_id = event["device_id"]
        dT    = event.get("proc_temp", 310) - event.get("air_temp", 300)
        power = event.get("torque", 40) * (event.get("rpm", 1538) * 2 * np.pi / 60)

        prev = DEVICE_BUFFERS.get(device_id, [{}])[-1] if DEVICE_BUFFERS.get(device_id) else {}

        sensor_row = {
            "air_temp":           event.get("air_temp",   300.0),
            "proc_temp":          event.get("proc_temp",  310.0),
            "rpm":                event.get("rpm",         1538),
            "torque":             event.get("torque",       40.0),
            "vibration":          event.get("vibration",   0.03),
            "delta_temp":         dT,
            "power_w":            power,
            "proc_temp_velocity": event.get("proc_temp", 310.0) - prev.get("proc_temp", 310.0),
            "rpm_velocity":       event.get("rpm",  1538) - prev.get("rpm",  1538),
            "torque_velocity":    event.get("torque", 40.0) - prev.get("torque", 40.0),
            "vibration_velocity": event.get("vibration", 0.03) - prev.get("vibration", 0.03),
        }

        if device_id not in DEVICE_BUFFERS:
            DEVICE_BUFFERS[device_id] = []
        buf = DEVICE_BUFFERS[device_id]
        buf.append(sensor_row)
        if len(buf) > WINDOW:
            buf.pop(0)

        feats     = _engineer_row(buf, uptime_seconds)
        feat_cols = bundle['feature_cols']
        X         = np.array([[feats.get(c, 0.0) for c in feat_cols]])

        model_type = bundle.get('model_type', 'regressor')

        if model_type == 'classifier':
            proba          = bundle['model'].predict_proba(X)[0]
            fail_prob      = float(proba[1])
            threshold      = bundle.get('threshold', 0.5)
            is_pre_failure = 1 if fail_prob >= threshold else 0
            ml_score       = fail_prob
        else:
            rul_seconds    = float(max(0.0, bundle['model'].predict(X)[0]))
            ml_score       = float(np.clip(1.0 - rul_seconds / 1800, 0.05, 0.95))
            is_pre_failure = 1 if ml_score > 0.6 else 0
            fail_prob      = ml_score

        return round(ml_score, 4), failure_type, is_pre_failure, round(fail_prob, 4)

    except Exception as e:
        log.error("Inference error: %s", e)
        return app_score, failure_type, 0, app_score


def score_to_severity(score: float) -> str:
    if score >= 0.7:  return "CRITICAL"
    if score >= 0.4:  return "WARNING"
    return "OK"


# ── SQL statements ────────────────────────────────────────────────────────────
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
        DEVICE_BUFFERS[device_id] = []


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


def main():
    log.info("hvac_consumer v2 | kafka=%s model=%s", KAFKA_BOOTSTRAP, MODEL_PATH)

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
    msg_count = 0

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
                                  last_seen='NOW()', last_severity='OK',
                                  last_failure_type='None', uptime_seconds=0, online=TRUE
                            """, {"device_id": device_id,
                                  "lat": event.get("lat"),
                                  "lng": event.get("lng")})
                        conn.commit()
                        log.info("SERVICE | device=%s resolved=%s",
                                 device_id, event.get('resolved_failure'))

                    else:
                        # Użyj uptime z eventu (symulator/twin) lub własnego licznika
                        event_uptime = event.get("uptime_seconds")
                        if event_uptime is not None:
                            uptime = float(event_uptime)
                            # Zsynchronizuj własny licznik
                            DEVICE_UPTIME[device_id] = {'uptime': uptime, 'last_ts': time.time()}
                        else:
                            uptime = get_uptime(device_id)
                        ml_score, failure_type, is_pre_failure, fail_prob = infer(model, event, uptime)
                        severity = score_to_severity(ml_score)

                        row = {
                            "device_id":       device_id,
                            "ts":              event.get("ts", datetime.now(timezone.utc).isoformat()),
                            "server_ts":       datetime.fromtimestamp(event["server_ts"], tz=timezone.utc)
                                               if "server_ts" in event else None,
                            "lat":             event.get("lat"),
                            "lng":             event.get("lng"),
                            "air_temp":        event.get("air_temp"),
                            "proc_temp":       event.get("proc_temp"),
                            "rpm":             event.get("rpm"),
                            "torque":          event.get("torque"),
                            "vibration":       event.get("vibration"),
                            "ml_score":        ml_score,
                            "failure_type":    failure_type,
                            "severity":        severity,
                            "app_severity":    event.get("severity", "OK"),
                            "is_pre_failure":  is_pre_failure,
                            "fail_probability": fail_prob,
                            "uptime_seconds":  round(uptime, 1),
                            "session_id":      event.get("session_id"),
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
