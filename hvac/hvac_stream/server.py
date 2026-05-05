import os
import json
import time
import logging
from contextlib import asynccontextmanager
from typing import Literal, List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

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


class TelemetryEvent(BaseModel):
    device_id:        str   = Field(..., min_length=1, max_length=50)
    lat:              float = Field(..., ge=-90.0,  le=90.0)
    lng:              float = Field(..., ge=-180.0, le=180.0)
    air_temp:         float = Field(..., ge=290.0,  le=310.0)   # rozluźnione — awarie mogą wychodzić
    proc_temp:        float = Field(..., ge=300.0,  le=320.0)   # rozluźnione — CLOG/HDF przekraczają 313.8
    rpm:              int   = Field(..., ge=1000,   le=3000)    # rozluźnione
    torque:           float = Field(..., ge=0.0,    le=100.0)   # rozluźnione — CLOG > 76.6
    vibration:        float = Field(..., ge=0.0,    le=2.0)
    ml_score:         float = Field(..., ge=0.0,    le=1.0)
    failure_type:     str   = Field(..., max_length=50)
    severity:         Literal["OK", "WARNING", "CRITICAL"]
    ts:               str   = Field(...)
    event_type:       str   = Field("telemetry")
    uptime_seconds:   Optional[float] = Field(None)
    session_id:       Optional[str]   = Field(None)
    resolved_failure: Optional[str]   = Field(None)
    delta_temp:       Optional[float] = Field(None)
    power_w:          Optional[float] = Field(None)

    @field_validator("failure_type")
    @classmethod
    def validate_failure_type(cls, v: str) -> str:
        allowed = {"None", "HDF", "PWF", "CLOG", "BEARING", "RESET"}
        parts = set(v.split(","))
        if not parts.issubset(allowed):
            # Nie rzucaj błędu — po prostu akceptuj
            pass
        return v


producer: Producer | None = None


def get_producer() -> Producer:
    global producer
    if producer is None:
        producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "1",
            "queue.buffering.max.messages": 200000,
            "queue.buffering.max.ms": 50,
        })
    return producer


def delivery_callback(err, msg):
    if err:
        log.error("Kafka error: %s", err)


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_producer()
    log.info("hvac_stream started | kafka=%s", KAFKA_BOOTSTRAP)
    yield
    if producer:
        producer.flush(10)


app = FastAPI(title="HVAC Stream API v4", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.post("/event", status_code=202)
async def receive_event(event: TelemetryEvent):
    payload = event.model_dump()
    payload["server_ts"] = time.time()
    get_producer().produce(
        TOPIC_TELEMETRY,
        key=event.device_id,
        value=json.dumps(payload),
        callback=delivery_callback
    )
    get_producer().poll(0)
    return {"status": "accepted", "device_id": event.device_id}


@app.post("/events/batch", status_code=202)
async def receive_batch(events: List[TelemetryEvent]):
    """Batch endpoint — wysyła 100 eventów naraz."""
    p  = get_producer()
    ts = time.time()
    for event in events:
        payload = event.model_dump()
        payload["server_ts"] = ts
        p.produce(
            TOPIC_TELEMETRY,
            key=event.device_id,
            value=json.dumps(payload),
            callback=delivery_callback
        )
    p.poll(0)
    return {"status": "batch_accepted", "count": len(events)}


@app.get("/health")
async def health():
    return {"status": "ok", "kafka": KAFKA_BOOTSTRAP, "topics": [TOPIC_TELEMETRY, TOPIC_STATUS]}
