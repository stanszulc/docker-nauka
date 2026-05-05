import os
import json
import time
import logging
from contextlib import asynccontextmanager
from typing import Literal, List

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ── Config ──────────────────────────────────────────────────────────────────
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

# ── Models ───────────────────────────────────────────────────────────────────
class TelemetryEvent(BaseModel):
    device_id:    str   = Field(..., min_length=1, max_length=50, pattern=r"^[A-Z0-9_\-]+$")
    lat:          float = Field(..., ge=-90.0,  le=90.0)
    lng:          float = Field(..., ge=-180.0, le=180.0)
    air_temp:     float = Field(..., ge=295.3,  le=304.5)
    proc_temp:    float = Field(..., ge=305.7,  le=313.8)
    rpm:          int   = Field(..., ge=1168,   le=2886)
    torque:       float = Field(..., ge=3.8,    le=76.6)
    vibration:    float = Field(..., ge=0.0,    le=2.0)
    ml_score:     float = Field(..., ge=0.0,    le=1.0)
    failure_type: str   = Field(..., max_length=50)
    severity:     Literal["OK", "WARNING", "CRITICAL"]
    ts:           str   = Field(...)
    event_type:   str   = Field("telemetry")
    resolved_failure: str | None = Field(None)

    @field_validator("failure_type")
    @classmethod
    def validate_failure_type(cls, v: str) -> str:
        allowed = {"None", "HDF", "PWF", "CLOG", "BEARING"}
        parts = set(v.split(","))
        if not parts.issubset(allowed):
            raise ValueError(f"Allowed types: {allowed}")
        return v

class StatusEvent(BaseModel):
    device_id: str = Field(..., min_length=1, max_length=50)
    lat:       float = Field(...)
    lng:       float = Field(...)
    online:    bool  = True

# ── Kafka Logic ──────────────────────────────────────────────────────────────
producer: Producer | None = None

def get_producer() -> Producer:
    global producer
    if producer is None:
        producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "1",
            "queue.buffering.max.messages": 100000
        })
    return producer

def delivery_callback(err, msg):
    if err: log.error(f"Kafka error: {err}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    get_producer()
    yield
    if producer: producer.flush(10)

app = FastAPI(title="HVAC Stream API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.post("/event", status_code=202)
async def receive_event(event: TelemetryEvent, request: Request):
    payload = event.model_dump()
    payload["server_ts"] = time.time()
    get_producer().produce(TOPIC_TELEMETRY, key=event.device_id, value=json.dumps(payload), callback=delivery_callback)
    get_producer().poll(0)
    return {"status": "sent"}

@app.post("/events/batch", status_code=202)
async def receive_batch(events: List[TelemetryEvent], request: Request):
    """Kluczowy endpoint dla 1000 urządzeń."""
    p = get_producer()
    ts = time.time()
    for event in events:
        payload = event.model_dump()
        payload["server_ts"] = ts
        p.produce(TOPIC_TELEMETRY, key=event.device_id, value=json.dumps(payload), callback=delivery_callback)
    p.poll(0)
    return {"status": "batch_accepted", "count": len(events)}

@app.get("/health")
async def health(): return {"status": "ok"}
