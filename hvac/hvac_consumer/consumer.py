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
    app_severity VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS hvac_alerts_log (
    id           BIGSERIAL    PRIMARY KEY,
    device_id    VARCHAR(50)  NOT NULL,
    ts           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    lat          DOUBLE PRECISION,
    lng          DOUBLE PRECISION,
    ml_score     REAL,
    failure_type VARCHAR(30),
    severity     VARCHAR(10),
    raw_event    JSONB
);

CREATE TABLE IF NOT EXISTS hvac_device_status (
    device_id    VARCHAR(50)  PRIMARY KEY,
    lat          DOUBLE PRECISION,
    lng          DOUBLE PRECISION,
    online       BOOLEAN      DEFAULT TRUE,
    last_seen    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_severity VARCHAR(10) DEFAULT 'OK'
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


# ── ML Model ──────────────────────────────────────────────────────────────────
def load_model(path: str):
    """Load Random Forest model. Returns None if .pkl not present yet."""
    if not os.path.exists(path):
        log.warning("Model not found at %s — running without ML inference", path)
        return None
    model = joblib.load(path)
    log.info("Model loaded: %s | classes=%s", path, getattr(model, "classes_", "unknown"))
    return model


def infer(model, event: dict) -> tuple[float, str]:
    """
    Run Random Forest inference.
    Returns (ml_score, failure_type).
    Falls back to app-provided values if model not loaded.
    """
    if model is None:
        return event.get("ml_score", 0.0), event.get("failure_type", "None")

    try:
        features = np.array([[
            event["air_temp"],
            event["proc_temp"],
            event["rpm"],
            event["torque"],
            event["tool_wear"],
        ]])
        proba = model.predict_proba(features)[0]
        # ml_score = probability of any failure (1 - P(no failure))
        # Assumes class 0 = "No Failure" (matches AI4I 2020 encoding)
        no_fail_idx = list(model.classes_).index(0) if 0 in model.classes_ else 0
        ml_score = float(1.0 - proba[no_fail_idx])

        pred_class = model.predict(features)[0]
        failure_type = FAILURE_LABELS.get(int(pred_class), "None") if pred_class != 0 else "None"

        return round(ml_score, 4), failure_type
    except Exception as e:
        log.error("Inference error: %s", e)
        return event.get("ml_score", 0.0), event.get("failure_type", "None")


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
   ml_score, failure_type, severity, app_severity)
VALUES
  (%(device_id)s, %(ts)s, %(server_ts)s, %(lat)s, %(lng)s, %(product_type)s,
   %(air_temp)s, %(proc_temp)s, %(rpm)s, %(torque)s, %(tool_wear)s, %(vibration)s,
   %(ml_score)s, %(failure_type)s, %(severity)s, %(app_severity)s)
"""

INSERT_ALERT = """
INSERT INTO hvac_alerts_log
  (device_id, ts, lat, lng, ml_score, failure_type, severity, raw_event)
VALUES
  (%(device_id)s, %(ts)s, %(lat)s, %(lng)s, %(ml_score)s,
   %(failure_type)s, %(severity)s, %(raw_event)s)
"""

UPSERT_STATUS = """
INSERT INTO hvac_device_status (device_id, lat, lng, last_seen, last_severity)
VALUES (%(device_id)s, %(lat)s, %(lng)s, NOW(), %(severity)s)
ON CONFLICT (device_id) DO UPDATE SET
  lat           = EXCLUDED.lat,
  lng           = EXCLUDED.lng,
  last_seen     = NOW(),
  last_severity = EXCLUDED.last_severity,
  online        = TRUE
"""


def write_event(conn, event: dict, ml_score: float, failure_type: str, severity: str):
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
    }
    with conn.cursor() as cur:
        cur.execute(INSERT_METRIC, row)
        # Update device status table (used by Grafana Geomap)
        cur.execute(UPSERT_STATUS, {
            "device_id": event["device_id"],
            "lat":       event.get("lat"),
            "lng":       event.get("lng"),
            "severity":  severity,
        })
        # Write to alerts log if score exceeds threshold
        if ml_score >= ALERT_THRESHOLD:
            cur.execute(INSERT_ALERT, {
                "device_id":    event["device_id"],
                "ts":           row["ts"],
                "lat":          event.get("lat"),
                "lng":          event.get("lng"),
                "ml_score":     ml_score,
                "failure_type": failure_type,
                "severity":     severity,
                "raw_event":    json.dumps(event),
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
                    ml_score, failure_type = infer(model, event)
                    severity = score_to_severity(ml_score)

                    write_event(conn, event, ml_score, failure_type, severity)

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
