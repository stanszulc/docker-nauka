"""
hvac_consumer v4 — BATCH ML Consumer
Zmiany vs v3:
- Batch INSERT z execute_values (100 wierszy naraz) — znacznie szybszy
- Nowa grupa Kafka: hvac_ml_group_v4 (czyste offsety)
- BATCH_SIZE=100, BATCH_TIMEOUT=5s
- Cała logika ML (DeviceBuffer, infer) bez zmian vs v3
- Kompatybilny z hvac_classifier_v5.pkl (93 features)
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
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka:9092")
TOPIC_TELEMETRY  = os.getenv("TOPIC_TELEMETRY",  "hvac_telemetry")
TOPIC_ALERTS     = os.getenv("TOPIC_ALERTS",     "hvac_alerts")
TOPIC_STATUS     = os.getenv("TOPIC_STATUS",     "hvac_status")
CONSUMER_GROUP   = os.getenv("CONSUMER_GROUP",   "hvac_ml_group_v4")  # nowa grupa
POSTGRES_DSN     = os.getenv("POSTGRES_DSN",     "postgresql://kafka:kafka@postgres:5432/events")
MODEL_PATH       = os.getenv("MODEL_PATH",       "/app/model/hvac_rf_model.pkl")
ALERT_THRESHOLD  = float(os.getenv("ALERT_THRESHOLD",  "0.5"))
RETENTION_HOURS  = int(os.getenv("RETENTION_HOURS",    "168"))
LOG_LEVEL        = os.getenv("LOG_LEVEL",        "INFO")
BATCH_SIZE       = int(os.getenv("BATCH_SIZE",   "100"))
BATCH_TIMEOUT    = float(os.getenv("BATCH_TIMEOUT", "5.0"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [hvac_consumer] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Rolling window config ─────────────────────────────────────────────────────
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
"""

RETENTION_SQL = "DELETE FROM hvac_metrics WHERE ts < NOW() - INTERVAL '{hours} hours';"

# ── Per-device rolling buffer ─────────────────────────────────────────────────
class DeviceBuffer:
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
        for s in BASE_SENSORS:
            n = self.counts[s]
            feats[s] = float(self.bufs[s][n-1]) if n > 0 else 0.0

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

        n_steps = self.step
        feats['buffer_fill_short'] = min(1.0, n_steps / WIN_SHORT)
        feats['buffer_fill_mid']   = min(1.0, n_steps / WIN_MID)
        feats['buffer_fill_long']  = min(1.0, n_steps / WIN_LONG)
        feats['uptime_norm'] = uptime_seconds / 1800.0

        delta_temp = feats.get('delta_temp', 10.0)
        power_w    = feats.get('power_w', 6500.0)
        feats['hdf_margin']  = delta_temp - 8.6
        feats['pwf_low']     = power_w - 3500
        feats['pwf_high']    = 9000 - power_w
        feats['pwf_margin']  = min(feats['pwf_low'], feats['pwf_high'])

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
DEVICE_BUFFERS: dict = {}
DEVICE_UPTIME:  dict = {}


def get_or_create_buffer(device_id: str) -> DeviceBuffer:
    if device_id not in DEVICE_BUFFERS:
        DEVICE_BUFFERS[device_id] = DeviceBuffer()
    return DEVICE_BUFFERS[device_id]


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
        dT    = event.get("proc_temp", 310.0) - event.get("air_temp", 300.0)
        power = event.get("torque", 40.0) * (event.get("rpm", 1538) * 2 * np.pi / 60)
        ltr   = power / (abs(dT) + 1e-3)

        prev_proc_temp = float(buf.bufs['proc_temp'][buf.counts['proc_temp']-1]) if buf.counts['proc_temp'] > 0 else event.get("proc_temp", 310.0)
        prev_rpm       = float(buf.bufs['rpm'][buf.counts['rpm']-1])             if buf.counts['rpm']       > 0 else event.get("rpm", 1538)
        prev_torque    = float(buf.bufs['torque'][buf.counts['torque']-1])       if buf.counts['torque']    > 0 else event.get("torque", 40.0)
        prev_vibration = float(buf.bufs['vibration'][buf.counts['vibration']-1]) if buf.counts['vibration'] > 0 else event.get("vibration", 0.03)

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
        return round(fail_prob, 4), failure_type, is_pre_failure, round(fail_prob, 4)
    except Exception as e:
        log.error("Inference error: %s", e)
        return app_score, failure_type, 0, app_score


def score_to_severity(score: float) -> str:
    if score >= 0.7: return "CRITICAL"
    if score >= 0.4: return "WARNING"
    return "OK"


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


# ── Batch flush ───────────────────────────────────────────────────────────────
def flush_batch(conn, metrics_batch: list, status_map: dict, alerts_batch: list):
    if not metrics_batch:
        return
    try:
        with conn.cursor() as cur:
            # Batch INSERT metryk
            execute_values(cur, """
                INSERT INTO hvac_metrics
                  (device_id, ts, server_ts, lat, lng,
                   air_temp, proc_temp, rpm, torque, vibration,
                   ml_score, failure_type, severity, app_severity,
                   is_pre_failure, fail_probability, uptime_seconds, session_id)
                VALUES %s
            """, metrics_batch)

            # Status update per urządzenie
            for dev_id, stat in status_map.items():
                cur.execute("""
                    INSERT INTO hvac_device_status
                      (device_id, lat, lng, last_seen, last_severity, last_failure_type, uptime_seconds)
                    VALUES (%(d)s, %(lat)s, %(lng)s, NOW(), %(sev)s, %(fail)s, %(upt)s)
                    ON CONFLICT (device_id) DO UPDATE SET
                      lat=EXCLUDED.lat, lng=EXCLUDED.lng, last_seen=NOW(),
                      last_severity=EXCLUDED.last_severity,
                      last_failure_type=EXCLUDED.last_failure_type,
                      uptime_seconds=EXCLUDED.uptime_seconds, online=TRUE
                """, {'d': dev_id, 'lat': stat['lat'], 'lng': stat['lng'],
                      'sev': stat['sev'], 'fail': stat['fail'], 'upt': stat['upt']})

            # Alerty
            if alerts_batch:
                execute_values(cur, """
                    INSERT INTO hvac_alerts_log
                      (device_id, ts, lat, lng, ml_score, failure_type,
                       severity, event_type, resolved_failure, raw_event)
                    VALUES %s
                """, alerts_batch)

        conn.commit()
        log.info("💾 Batch saved: %d rows | alerts: %d", len(metrics_batch), len(alerts_batch))
    except Exception as e:
        log.error("❌ Batch DB error: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass


# ── Kafka ─────────────────────────────────────────────────────────────────────
def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers":     KAFKA_BOOTSTRAP,
        "group.id":              CONSUMER_GROUP,
        "auto.offset.reset":     "latest",
        "enable.auto.commit":    False,
        "session.timeout.ms":    60000,
        "heartbeat.interval.ms": 20000,
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


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("hvac_consumer v4 BATCH | kafka=%s group=%s batch=%d timeout=%.1fs",
             KAFKA_BOOTSTRAP, CONSUMER_GROUP, BATCH_SIZE, BATCH_TIMEOUT)

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

    restore_uptime(conn)

    # Batch bufory
    metrics_batch = []
    status_map    = {}
    alerts_batch  = []
    last_flush    = time.time()
    last_retention = time.time()
    msg_count     = 0

    try:
        while running:
            # Flush jeśli batch pełny lub timeout
            now = time.time()
            if metrics_batch and (len(metrics_batch) >= BATCH_SIZE or now - last_flush >= BATCH_TIMEOUT):
                flush_batch(conn, metrics_batch, status_map, alerts_batch)
                consumer.commit()
                metrics_batch = []
                status_map    = {}
                alerts_batch  = []
                last_flush    = now

            msg = consumer.poll(timeout=0.5)
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
                if topic == TOPIC_TELEMETRY:
                    event_type = event.get("event_type", "telemetry")
                    device_id  = event.get("device_id", "unknown")

                    if event_type == "service":
                        # Flush przed SERVICE żeby nie mieszać
                        if metrics_batch:
                            flush_batch(conn, metrics_batch, status_map, alerts_batch)
                            consumer.commit()
                            metrics_batch = []
                            status_map    = {}
                            alerts_batch  = []
                            last_flush    = time.time()

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
                        event_uptime = event.get("uptime_seconds")
                        if event_uptime is not None:
                            uptime = float(event_uptime)
                            DEVICE_UPTIME[device_id] = {'uptime': uptime, 'last_ts': time.time()}
                        else:
                            uptime = get_uptime(device_id)

                        ml_score, failure_type, is_pre_failure, fail_prob = infer(model, event, uptime)
                        severity = score_to_severity(ml_score)

                        ts_val = event.get("ts", datetime.now(timezone.utc).isoformat())
                        server_ts = datetime.fromtimestamp(event["server_ts"], tz=timezone.utc) \
                                    if "server_ts" in event else None

                        metrics_batch.append((
                            device_id, ts_val, server_ts,
                            event.get("lat"), event.get("lng"),
                            event.get("air_temp"), event.get("proc_temp"),
                            event.get("rpm"), event.get("torque"), event.get("vibration"),
                            ml_score, failure_type, severity,
                            event.get("severity", "OK"),
                            is_pre_failure, fail_prob,
                            round(uptime, 1), event.get("session_id"),
                        ))

                        status_map[device_id] = {
                            'lat': event.get("lat"), 'lng': event.get("lng"),
                            'sev': severity, 'fail': failure_type, 'upt': round(uptime, 1),
                        }

                        if ml_score >= ALERT_THRESHOLD or is_pre_failure:
                            alerts_batch.append((
                                device_id, ts_val,
                                event.get("lat"), event.get("lng"),
                                ml_score, failure_type, severity,
                                event_type, event.get("resolved_failure"),
                                json.dumps(event),
                            ))
                            if is_pre_failure:
                                log.warning("PRE-FAILURE | device=%s prob=%.2f failure=%s uptime=%.0fs",
                                            device_id, fail_prob, failure_type, uptime)

                        msg_count += 1
                        if msg_count % 500 == 0:
                            log.info("Processed %d events | batch=%d", msg_count, len(metrics_batch))

                elif topic == TOPIC_STATUS:
                    status_map[event.get("device_id", "?")] = {
                        'lat': event.get("lat"), 'lng': event.get("lng"),
                        'sev': 'OK', 'fail': 'None', 'upt': 0,
                    }

            except Exception as e:
                log.error("Processing error: %s", e)
                try:
                    conn.rollback()
                except Exception:
                    pass

            # Retention co godzinę
            if time.time() - last_retention > 3600:
                try:
                    run_retention(conn)
                    last_retention = time.time()
                except Exception as e:
                    log.warning("Retention failed: %s", e)

    finally:
        # Ostatni flush
        if metrics_batch:
            flush_batch(conn, metrics_batch, status_map, alerts_batch)
        log.info("Closing. Total processed: %d", msg_count)
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
