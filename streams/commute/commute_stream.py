import time
import json
import requests
from datetime import datetime
from kafka import KafkaProducer

START = {"lat": 49.9712, "lon": 19.8423}
KONIEC = {"lat": 49.9980, "lon": 20.0900}

OSRM_URL = (
    f"http://router.project-osrm.org/route/v1/driving/"
    f"{START['lon']},{START['lat']};{KONIEC['lon']},{KONIEC['lat']}"
    f"?overview=false"
)

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Połączono z Kafka")
        break
    except Exception as e:
        print(f"⏳ Kafka nie gotowa, retry za 3s... {e}")
        time.sleep(3)

while True:
    try:
        r = requests.get(OSRM_URL, timeout=5)
        data = r.json()
        route = data["routes"][0]
        czas_s = route["duration"]
        dystans_m = route["distance"]

        event = {
            "trasa": "Radziszów PKP → Podłęże A4",
            "czas_min": round(czas_s / 60, 1),
            "dystans_km": round(dystans_m / 1000, 2),
            "timestamp": str(datetime.now()),
        }

        producer.send('commute_events', event)
        producer.flush()
        print(f"✅ Czas dojazdu: {event['czas_min']} min | {event['dystans_km']} km")

    except Exception as e:
        print(f"❌ Błąd OSRM: {e}")

    time.sleep(60)
