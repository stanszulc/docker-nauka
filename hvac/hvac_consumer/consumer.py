"""
hvac_consumer — ML Consumer
Reads from Kafka hvac_telemetry, runs Random Forest inference,
writes enriched records to PostgreSQL. Handles its own DB schema creation.
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
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, TopicPartition
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

# ── Feature order must match training data ────────────────────────────────────
# AI4I 2020: air_temp[K], proc_temp[K], rpm, torque[Nm], tool_wear[min]
FEATURE_COLS = ["air_temp", "proc_temp", "rpm", "torque", "tool_wear"]

# Failure type label mapping (matches AI4I 2020 target encoding)
FAILURE_LABELS = {0: "None", 1: "HDF", 2: "OSF", 3: "PWF", 4: "TWF", 5: "Random"}


# ── Database schema ───────────────────────────────────────────────────────────
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS hvac_metrics (
    id           BIGSERIAL    PRIMARY KEY,
    device_id    VARCHAR(50)  NOT NULL,
    ts           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    server_ts    TIMESTAMPTZ,
    lat          DOUBLE PRECISION,
    lng          DOUBLE PRECISION,
    product_type CHAR(1),
    air_temp     REAL,
    proc_temp    REAL,
    rpm          INTEGER,
    torque       REAL,
    tool_wear    INTEGER,
    vibration    REAL,
    ml_score     REAL,
    failure_type VARCHAR(30),
    severity     VARCHAR(10),
    app_severity VARCHAR(10),
    rul_seconds  REAL,
    rul_minutes  REAL
);

-- Add RUL columns if upgrading existing table
ALTER TABLE hvac_metrics ADD COLUMN IF NOT EXISTS rul_seconds REAL;
ALTER TABLE hvac_metrics ADD COLUMN IF NOT EXISTS rul_minutes REAL;

CREATE TABLE IF NOT EXISTS hvac_alerts_log (
    id               BIGSERIAL    PRIMARY KEY,
    device_id        VARCHAR(50)  NOT NULL,
    ts               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    lat              DOUBLE PRECISION,
    lng              DOUBLE PRECISION,
    ml_score         REAL,
    failure_type     VARCHAR(30),
    severity         VARCHAR(10),
    event_type       VARCHAR(20)  DEFAULT 'telemetry',
    resolved_failure VARCHAR(30),
    raw_event        JSONB
);

CREATE INDEX IF NOT EXISTS idx_hvac_alerts_event_type
    ON hvac_alerts_log (event_type, ts DESC);

CREATE TABLE IF NOT EXISTS hvac_device_status (
    device_id         VARCHAR(50)  PRIMARY KEY,
    lat               DOUBLE PRECISION,
    lng               DOUBLE PRECISION,
    online            BOOLEAN      DEFAULT TRUE,
    last_seen         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_severity     VARCHAR(10)  DEFAULT 'OK',
    last_failure_type VARCHAR(30)  DEFAULT 'None'
);

CREATE INDEX IF NOT EXISTS idx_hvac_metrics_device_ts
    ON hvac_metrics (device_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_hvac_alerts_ts
    ON hvac_alerts_log (ts DESC);
"""

RETENTION_SQL = """
DELETE FROM hvac_metrics
WHERE ts < NOW() - INTERVAL '{hours} hours';
"""


# ── PostgreSQL connection with retry ─────────────────────────────────────────
def connect_postgres(retries: int = 10) -> psycopg2.extensions.connection:
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            conn.autocommit = False
            log.info("PostgreSQL connected")
            return conn
        except Exception as e:
            log.warning("Postgres not ready (attempt %d/%d): %s", attempt + 1, retries, e)
            time.sleep(3)
    log.error("Cannot connect to PostgreSQL after %d attempts", retries)
    sys.exit(1)


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    log.info("Schema ready (hvac_metrics, hvac_alerts_log, hvac_device_status)")


def run_retention(conn):
    """Delete old raw telemetry — keeps DB lean."""
    with conn.cursor() as cur:
        cur.execute(RETENTION_SQL.format(hours=RETENTION_HOURS))
        deleted = cur.rowcount
    conn.commit()
    if deleted:
        log.info("Retention cleanup: deleted %d old rows from hvac_metrics", deleted)


# ── ML Model (XGBoost RUL) ────────────────────────────────────────────────────
# Per-device rolling buffer for feature engineering (window=3 ticks)
DEVICE_BUFFERS: dict = {}
WINDOW = 3
OSF_LIMITS = {'L': 13000, 'M': 12000, 'H': 11000}

def load_model(path: str):
    """Load XGBoost RUL model bundle. Returns None if not present."""
    if not os.path.exists(path):
        log.warning("Model not found at %s — using heuristic ml_score", path)
        return None
    bundle = joblib.load(path)
    log.info("RUL model loaded: %s | features=%d", path, len(bundle.get('feature_cols', [])))
    return bundle


def _engineer_row(buf: list, product_type: str) -> dict:
    """Build feature vector from rolling buffer. Mirrors train_rul.py logic."""
    n = len(buf)
    BASE = ['air_temp', 'proc_temp', 'rpm', 'torque', 'tool_wear', 'vibration', 'delta_temp', 'power_w']
    pt_enc = {'L': 0, 'M': 1, 'H': 2}.get(product_type, 0)
    feats = {'product_type_enc': pt_enc, 'step_norm': 0.5}

    for feat in BASE:
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
    feats['pwf_margin']     = min(last.get('power_w', 6500) - 3500, 9000 - last.get('power_w', 6500))
    osf_limit               = OSF_LIMITS.get(product_type, 13000)
    feats['osf_margin']     = osf_limit - last.get('tool_wear', 0) * last.get('torque', 40)
    feats['twf_margin']     = 200 - last.get('tool_wear', 0)
    return feats


def infer(bundle, event: dict) -> tuple:
    """
    Run XGBoost RUL inference.
    Returns (ml_score, failure_type, rul_seconds).
    """
    failure_type = event.get("failure_type", "None")
    app_score    = event.get("ml_score", 0.0)

    if bundle is None:
        return app_score, failure_type, None

    try:
        device_id    = event["device_id"]
        product_type = event.get("type", "L")
        dT    = event.get("proc_temp", 310) - event.get("air_temp", 300)
        power = event.get("torque", 40) * (event.get("rpm", 1538) * 2 * np.pi / 60)

        sensor_row = {
            "air_temp":   event.get("air_temp",  300.0),
            "proc_temp":  event.get("proc_temp", 310.0),
            "rpm":        event.get("rpm",        1538),
            "torque":     event.get("torque",      40.0),
            "tool_wear":  event.get("tool_wear",   108),
            "vibration":  event.get("vibration",  0.03),
            "delta_temp": dT,
            "power_w":    power,
        }

        if device_id not in DEVICE_BUFFERS:
            DEVICE_BUFFERS[device_id] = []
        buf = DEVICE_BUFFERS[device_id]
        buf.append(sensor_row)
        if len(buf) > WINDOW:
            buf.pop(0)

        feats     = _engineer_row(buf, product_type)
        feat_cols = bundle['feature_cols']
        X         = np.array([[feats.get(c, 0.0) for c in feat_cols]])

        rul_seconds = float(max(0.0, bundle['model'].predict(X)[0]))
        ml_score    = float(np.clip(1.0 - rul_seconds / 1800, 0.05, 0.95))

        return round(ml_score, 4), failure_type, round(rul_seconds, 1)

    except Exception as e:
        log.error("RUL inference error: %s", e)
        return app_score, failure_type, None


def score_to_severity(score: float) -> str:
    if score >= 0.7:
        return "CRITICAL"
    if score >= 0.4:
        return "WARNING"
    return "OK"


# ── Database writes ───────────────────────────────────────────────────────────
INSERT_METRIC = """
INSERT INTO hvac_metrics
  (device_id, ts, server_ts, lat, lng, product_type,
   air_temp, proc_temp, rpm, torque, tool_wear, vibration,
   ml_score, failure_type, severity, app_severity,
   rul_seconds, rul_minutes)
VALUES
  (%(device_id)s, %(ts)s, %(server_ts)s, %(lat)s, %(lng)s, %(product_type)s,
   %(air_temp)s, %(proc_temp)s, %(rpm)s, %(torque)s, %(tool_wear)s, %(vibration)s,
   %(ml_score)s, %(failure_type)s, %(severity)s, %(app_severity)s,
   %(rul_seconds)s, %(rul_minutes)s)
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
INSERT INTO hvac_device_status (device_id, lat, lng, last_seen, last_severity, last_failure_type)
VALUES (%(device_id)s, %(lat)s, %(lng)s, NOW(), %(severity)s, %(failure_type)s)
ON CONFLICT (device_id) DO UPDATE SET
  lat               = EXCLUDED.lat,
  lng               = EXCLUDED.lng,
  last_seen         = NOW(),
  last_severity     = EXCLUDED.last_severity,
  last_failure_type = EXCLUDED.last_failure_type,
  online            = TRUE
"""


def write_event(conn, event: dict, ml_score: float, failure_type: str, severity: str, rul_seconds=None):
    row = {
        "device_id":    event["device_id"],
        "ts":           event.get("ts", datetime.now(timezone.utc).isoformat()),
        "server_ts":    datetime.fromtimestamp(event["server_ts"], tz=timezone.utc)
                        if "server_ts" in event else None,
        "lat":          event.get("lat"),
        "lng":          event.get("lng"),
        "product_type": event.get("type"),
        "air_temp":     event.get("air_temp"),
        "proc_temp":    event.get("proc_temp"),
        "rpm":          event.get("rpm"),
        "torque":       event.get("torque"),
        "tool_wear":    event.get("tool_wear"),
        "vibration":    event.get("vibration"),
        "ml_score":     ml_score,
        "failure_type": failure_type,
        "severity":     severity,
        "app_severity": event.get("severity", "OK"),
        "rul_seconds":  rul_seconds,
        "rul_minutes":  round(rul_seconds / 60, 2) if rul_seconds is not None else None,
    }
    with conn.cursor() as cur:
        cur.execute(INSERT_METRIC, row)
        # Update device status table (used by Grafana Geomap)
        cur.execute(UPSERT_STATUS, {
            "device_id":    event["device_id"],
            "lat":          event.get("lat"),
            "lng":          event.get("lng"),
            "severity":     severity,
            "failure_type": failure_type if failure_type != "None" else "None",
        })
        # Write to alerts log if score exceeds threshold
        event_type = event.get("event_type", "telemetry")
        # Always log service events; log telemetry only when above threshold
        if ml_score >= ALERT_THRESHOLD or event_type == "service":
            cur.execute(INSERT_ALERT, {
                "device_id":       event["device_id"],
                "ts":              row["ts"],
                "lat":             event.get("lat"),
                "lng":             event.get("lng"),
                "ml_score":        ml_score,
                "failure_type":    failure_type,
                "severity":        severity,
                "event_type":      event_type,
                "resolved_failure": event.get("resolved_failure"),
                "raw_event":       json.dumps(event),
            })
    conn.commit()


# ── Kafka Consumer setup ──────────────────────────────────────────────────────
def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers":        KAFKA_BOOTSTRAP,
        "group.id":                 CONSUMER_GROUP,
        "auto.offset.reset":        "latest",   # don't replay history on restart
        "enable.auto.commit":       False,       # manual commit after DB write
        "session.timeout.ms":       30000,
        "heartbeat.interval.ms":    10000,
        "max.poll.interval.ms":     300000,
        "socket.timeout.ms":        10000,
    })


def ensure_topics():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    topics = [
        NewTopic(TOPIC_TELEMETRY, num_partitions=3, replication_factor=1,
                 config={"retention.ms": "3600000", "retention.bytes": "104857600"}),
        NewTopic(TOPIC_ALERTS,    num_partitions=1, replication_factor=1,
                 config={"retention.ms": "604800000", "retention.bytes": "10485760"}),
        NewTopic(TOPIC_STATUS,    num_partitions=1, replication_factor=1,
                 config={"retention.ms": "3600000",  "retention.bytes": "10485760"}),
    ]
    futures = admin.create_topics(topics)
    for topic, future in futures.items():
        try:
            future.result()
            log.info("Topic created: %s", topic)
        except Exception as e:
            if "TopicExistsException" not in str(type(e)):
                log.warning("Topic %s: %s", topic, e)


# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    log.info("hvac_consumer starting | kafka=%s model=%s", KAFKA_BOOTSTRAP, MODEL_PATH)

    # Graceful shutdown
    running = True
    def handle_signal(sig, frame):
        nonlocal running
        log.info("Shutdown signal received")
        running = False
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT,  handle_signal)

    # Connect Postgres
    conn = connect_postgres()
    ensure_schema(conn)

    # Load model (may be None if .pkl not present yet)
    model = load_model(MODEL_PATH)

    # Ensure Kafka topics exist
    for attempt in range(10):
        try:
            ensure_topics()
            break
        except Exception as e:
            log.warning("Kafka not ready (attempt %d): %s", attempt + 1, e)
            time.sleep(3)

    # Start consumer
    consumer = create_consumer()
    consumer.subscribe([TOPIC_TELEMETRY, TOPIC_STATUS])
    log.info("Subscribed to topics: %s, %s", TOPIC_TELEMETRY, TOPIC_STATUS)

    # Retention runs every hour
    last_retention = time.time()
    msg_count = 0

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
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
                    # Run ML inference
                    ml_score, failure_type, rul_seconds = infer(model, event)
                    severity = score_to_severity(ml_score)

                    write_event(conn, event, ml_score, failure_type, severity, rul_seconds)

                    if rul_seconds is not None and rul_seconds < 300:
                        log.warning("RUL alert | device=%s rul=%.0fs (%.1f min) failure=%s",
                                    event.get('device_id'), rul_seconds, rul_seconds/60, failure_type)

                    msg_count += 1
                    if msg_count % 100 == 0:
                        log.info("Processed %d events | last: %s score=%.3f sev=%s",
                                 msg_count, event.get("device_id"), ml_score, severity)

                elif topic == TOPIC_STATUS:
                    # Update device online status
                    with conn.cursor() as cur:
                        cur.execute(UPSERT_STATUS, {
                            "device_id": event.get("device_id"),
                            "lat":       event.get("lat"),
                            "lng":       event.get("lng"),
                            "severity":  "OK",
                        })
                    conn.commit()

                consumer.commit(message=msg)

            except psycopg2.OperationalError as e:
                log.error("Postgres write failed: %s — reconnecting", e)
                conn = connect_postgres()
                ensure_schema(conn)
            except Exception as e:
                log.error("Processing error for %s: %s", topic, e)
                try:
                    conn.rollback()
                except Exception:
                    pass

            # Periodic retention cleanup
            if time.time() - last_retention > 3600:
                try:
                    run_retention(conn)
                    last_retention = time.time()
                except Exception as e:
                    log.warning("Retention cleanup failed: %s", e)

    finally:
        log.info("Closing consumer and DB connection")
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
