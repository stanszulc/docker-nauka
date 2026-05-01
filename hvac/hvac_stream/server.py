"""
hvac_stream — API Gateway
Receives telemetry events from mobile app, validates against AI4I 2020 schema,
publishes to Kafka. Returns immediately (fire-and-forget pattern).
"""

import os
import json
import time
import logging
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ── Config from environment ──────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_TELEMETRY = os.getenv("TOPIC_TELEMETRY", "hvac_telemetry")
TOPIC_STATUS    = os.getenv("TOPIC_STATUS",    "hvac_status")
LOG_LEVEL       = os.getenv("LOG_LEVEL",       "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [hvac_stream] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── AI4I 2020 exact ranges (used for validation) ─────────────────────────────
AI4I_RANGES = {
    "air_temp":  (295.3, 304.5),
    "proc_temp": (305.7, 313.8),
    "rpm":       (1168,  2886),
    "torque":    (3.8,   76.6),
    "tool_wear": (0,     253),
    "vibration": (0.0,   2.0),
    "ml_score":  (0.0,   1.0),
    "lat":       (-90.0, 90.0),
    "lng":       (-180.0, 180.0),
}

# ── Pydantic models ───────────────────────────────────────────────────────────
class TelemetryEvent(BaseModel):
    """
    Telemetry payload from mobile app.
    All physical values must be within AI4I 2020 dataset ranges.
    """
    device_id:    str = Field(..., min_length=1, max_length=50, pattern=r"^[A-Z0-9_\-]+$")
    type:         Literal["L", "M", "H"]
    lat:          float = Field(..., ge=-90.0,  le=90.0)
    lng:          float = Field(..., ge=-180.0, le=180.0)
    air_temp:     float = Field(..., ge=295.3,  le=304.5,  description="Kelvin")
    proc_temp:    float = Field(..., ge=305.7,  le=313.8,  description="Kelvin")
    rpm:          int   = Field(..., ge=1168,   le=2886)
    torque:       float = Field(..., ge=3.8,    le=76.6,   description="Nm")
    tool_wear:    int   = Field(..., ge=0,      le=253,    description="minutes")
    vibration:    float = Field(..., ge=0.0,    le=2.0,    description="mm/s²")
    ml_score:     float = Field(..., ge=0.0,    le=1.0)
    failure_type: str   = Field(..., max_length=50)
    severity:     Literal["OK", "WARNING", "CRITICAL"]
    ts:           str   = Field(..., description="ISO 8601 timestamp from device")

    @field_validator("failure_type")
    @classmethod
    def validate_failure_type(cls, v: str) -> str:
        allowed = {"None", "HDF", "PWF", "OSF", "TWF"}
        parts = set(v.split(","))
        if not parts.issubset(allowed):
            raise ValueError(f"failure_type must be subset of {allowed}, got: {v}")
        return v


class StatusEvent(BaseModel):
    """Heartbeat from device — used for Geomap in Grafana."""
    device_id: str = Field(..., min_length=1, max_length=50)
    lat:       float = Field(..., ge=-90.0,  le=90.0)
    lng:       float = Field(..., ge=-180.0, le=180.0)
    online:    bool = True


# ── Kafka producer (module-level singleton) ───────────────────────────────────
producer: Producer | None = None


def get_producer() -> Producer:
    global producer
    if producer is None:
        producer = Producer({
            "bootstrap.servers":       KAFKA_BOOTSTRAP,
            "socket.timeout.ms":       5000,
            "message.send.max.retries": 3,
            "retry.backoff.ms":        200,
            # Delivery guarantee: wait for leader ack (balance between speed and safety)
            "acks":                    "1",
        })
    return producer


def delivery_callback(err, msg):
    if err:
        log.error("Kafka delivery failed | topic=%s err=%s", msg.topic(), err)
    else:
        log.debug("Delivered | topic=%s partition=%d offset=%d",
                  msg.topic(), msg.partition(), msg.offset())


def ensure_topics():
    """Create Kafka topics if they don't exist yet."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    topics = [
        NewTopic(TOPIC_TELEMETRY, num_partitions=3, replication_factor=1,
                 config={"retention.ms": "3600000",   # 1h
                         "retention.bytes": "104857600"}),  # 100 MB
        NewTopic(TOPIC_STATUS, num_partitions=1, replication_factor=1,
                 config={"retention.ms": "3600000",
                         "retention.bytes": "10485760"}),   # 10 MB
    ]
    futures = admin.create_topics(topics)
    for topic, future in futures.items():
        try:
            future.result()
            log.info("Topic created: %s", topic)
        except Exception as e:
            # TopicExistsException is expected on restart — not an error
            if "TopicExistsException" in str(type(e)):
                log.debug("Topic already exists: %s", topic)
            else:
                log.warning("Could not create topic %s: %s", topic, e)


# ── App lifecycle ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting hvac_stream | kafka=%s", KAFKA_BOOTSTRAP)
    # Retry Kafka connection (container startup race)
    for attempt in range(10):
        try:
            ensure_topics()
            get_producer()
            log.info("Kafka ready")
            break
        except Exception as e:
            log.warning("Kafka not ready (attempt %d/10): %s", attempt + 1, e)
            time.sleep(3)
    yield
    log.info("Shutting down — flushing Kafka producer")
    if producer:
        producer.flush(timeout=10)


app = FastAPI(
    title="HVAC Stream API",
    description="API Gateway: mobile app → Kafka telemetry pipeline",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow mobile app domain
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten to your HTTPS domain in production
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.post("/event", status_code=202)
async def receive_event(event: TelemetryEvent, request: Request):
    """
    Accept telemetry from mobile app.
    Returns 202 Accepted immediately — Kafka handles delivery asynchronously.
    """
    payload = event.model_dump()
    payload["server_ts"] = time.time()
    payload["client_ip"] = request.client.host if request.client else "unknown"

    try:
        get_producer().produce(
            topic=TOPIC_TELEMETRY,
            key=event.device_id,          # partition by device_id
            value=json.dumps(payload),
            callback=delivery_callback,
        )
        get_producer().poll(0)            # trigger delivery callbacks, non-blocking
    except Exception as e:
        log.error("Failed to produce event: %s", e)
        raise HTTPException(status_code=503, detail="Kafka unavailable")

    return {"status": "accepted", "device_id": event.device_id}


@app.post("/status", status_code=202)
async def receive_status(event: StatusEvent):
    """
    Heartbeat from device — published to hvac_status topic.
    Grafana Geomap reads this to show online/offline state.
    """
    payload = {**event.model_dump(), "server_ts": time.time()}
    try:
        get_producer().produce(
            topic=TOPIC_STATUS,
            key=event.device_id,
            value=json.dumps(payload),
            callback=delivery_callback,
        )
        get_producer().poll(0)
    except Exception as e:
        log.error("Failed to produce status: %s", e)
        raise HTTPException(status_code=503, detail="Kafka unavailable")

    return {"status": "accepted", "device_id": event.device_id}


@app.get("/health")
async def health():
    """Health check for Docker healthcheck and load balancers."""
    return {
        "status": "ok",
        "kafka": KAFKA_BOOTSTRAP,
        "topics": [TOPIC_TELEMETRY, TOPIC_STATUS],
    }


@app.get("/")
async def root():
    return {"service": "hvac_stream", "version": "1.0.0", "docs": "/docs"}
