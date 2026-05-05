import os
import json
import time
import logging
import signal
from datetime import datetime, timezone
import joblib
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError

# ── Config (pobierane z Twojego pliku v3) ─────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_TELEMETRY = os.getenv("TOPIC_TELEMETRY", "hvac_telemetry")
CONSUMER_GROUP  = os.getenv("CONSUMER_GROUP", "hvac_ml_group_v4") # Nowa grupa dla pewności
POSTGRES_DSN    = os.getenv("POSTGRES_DSN", "postgresql://kafka:kafka@postgres:5432/events")
MODEL_PATH      = os.getenv("MODEL_PATH", "/app/model/hvac_rf_model.pkl")
BATCH_SIZE      = 100 
BATCH_TIMEOUT   = 5.0 # sekundy

logging.basicConfig(level=logging.INFO, format="%(asctime)s [consumer] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ... [Tu wstaw klasy DeviceBuffer i funkcje infer() z Twojego źródła, są bez zmian] ...

def flush_batch_to_db(conn, metrics_batch, status_updates):
    """Szybki zapis paczki do Postgresa."""
    if not metrics_batch:
        return
    
    try:
        with conn.cursor() as cur:
            # 1. Batch Insert Metryk
            query = """
            INSERT INTO hvac_metrics 
            (device_id, ts, server_ts, lat, lng, air_temp, proc_temp, rpm, torque, vibration, 
             ml_score, failure_type, severity, app_severity, is_pre_failure, fail_probability, 
             uptime_seconds, session_id) 
            VALUES %s
            """
            execute_values(cur, query, metrics_batch)

            # 2. Update statusów (ostatni stan urządzenia)
            for dev_id, stat in status_updates.items():
                cur.execute("""
                    INSERT INTO hvac_device_status 
                    (device_id, lat, lng, last_seen, last_severity, last_failure_type, uptime_seconds)
                    VALUES (%(d)s, %(lat)s, %(lng)s, NOW(), %(sev)s, %(fail)s, %(upt)s)
                    ON CONFLICT (device_id) DO UPDATE SET 
                    last_seen=NOW(), last_severity=EXCLUDED.last_severity, uptime_seconds=EXCLUDED.uptime_seconds
                """, {
                    'd': dev_id, 'lat': stat['lat'], 'lng': stat['lng'], 
                    'sev': stat['sev'], 'fail': stat['fail'], 'upt': stat['upt']
                })
        conn.commit()
        log.info(f"💾 Batch saved: {len(metrics_batch)} rows")
    except Exception as e:
        log.error(f"❌ DB Batch Error: {e}")
        conn.rollback()

def main():
    conn = psycopg2.connect(POSTGRES_DSN)
    model = joblib.load(MODEL_PATH) if os.path.exists(MODEL_PATH) else None
    
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False
    })
    consumer.subscribe([TOPIC_TELEMETRY])

    metrics_buffer = []
    status_map = {}
    last_flush = time.time()

    log.info("🚀 Consumer BATCH mode started...")

    try:
        while True:
            msg = consumer.poll(0.5)
            
            # Sprawdź czy czas na flush
            if (len(metrics_buffer) >= BATCH_SIZE) or (time.time() - last_flush > BATCH_TIMEOUT and metrics_buffer):
                flush_batch_to_db(conn, metrics_buffer, status_map)
                consumer.commit()
                metrics_buffer = []
                status_map = {}
                last_flush = time.time()

            if msg is None or msg.error(): continue

            event = json.loads(msg.value().decode("utf-8"))
            device_id = event.get("device_id", "unknown")
            
            # Logika ML (wykorzystuje Twoje funkcje infer)
            uptime = event.get("uptime_seconds", 0.0)
            ml_score, fail_type, is_pre, fail_prob = infer(model, event, uptime)
            sev = "CRITICAL" if ml_score > 0.7 else ("WARNING" if ml_score > 0.4 else "OK")

            # Przygotowanie krotki do execute_values
            row = (
                device_id, event.get("ts"), event.get("server_ts"), event.get("lat"), event.get("lng"),
                event.get("air_temp"), event.get("proc_temp"), event.get("rpm"), event.get("torque"), event.get("vibration"),
                ml_score, fail_type, sev, event.get("severity"), is_pre, fail_prob, uptime, event.get("session_id")
            )
            metrics_buffer.append(row)
            
            # Do update'u statusu bierzemy najnowszy stan
            status_map[device_id] = {
                'lat': event.get("lat"), 'lng': event.get("lng"), 
                'sev': sev, 'fail': fail_type, 'upt': uptime
            }

    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()
