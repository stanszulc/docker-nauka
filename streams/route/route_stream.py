import time
import json
import requests
from datetime import datetime
from kafka import KafkaProducer

TRASY = [
    {
        "nazwa": "Skawina PKP → Wawel",
        "start": "19.8316,50.0154",
        "cel": "19.9357,50.0540"
    },
]

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
    for trasa in TRASY:
        try:
            url = (
                f"http://router.project-osrm.org/route/v1/driving/"
                f"{trasa['start']};{trasa['cel']}"
                f"?overview=false"
            )
            r = requests.get(url, timeout=5)
            data = r.json()
            route = data['routes'][0]

            event = {
                "nazwa":        trasa['nazwa'],
                "czas_min":     round(route['duration'] / 60, 1),
                "dystans_km":   round(route['distance'] / 1000, 1),
                "timestamp":    str(datetime.now()),
            }

            producer.send('route_events', event)
            producer.flush()
            print(f"✅ {trasa['nazwa']}: {event['czas_min']} min ({event['dystans_km']} km)")

        except Exception as e:
            print(f"❌ Błąd: {e}")

    time.sleep(300)  # co 5 minut