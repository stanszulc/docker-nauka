import time
import json
import requests
from datetime import datetime
from kafka import KafkaProducer

START_LAT = 49.9712
START_LON = 19.8423
KONIEC_LAT = 49.9980
KONIEC_LON = 20.0900

API_KEY = "olDzucBAob8qXdWEsOdHUlecy1Wjg2xW"

TOMTOM_URL = (
    f"https://api.tomtom.com/routing/1/calculateRoute/"
    f"{START_LAT},{START_LON}:{KONIEC_LAT},{KONIEC_LON}/json"
    f"?traffic=true&travelMode=car&computeTravelTimeFor=all&key={API_KEY}"
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
        r = requests.get(TOMTOM_URL, timeout=5)
        r.raise_for_status()
        data = r.json()

        route = data["routes"][0]["summary"]
        czas_s = route["travelTimeInSeconds"]
        czas_free_s = route.get("noTrafficTravelTimeInSeconds", czas_s)
        dystans_m = route["lengthInMeters"]

        event = {
            "trasa": "Radziszów PKP → Podłęże A4",
            "czas_min": round(czas_s / 60, 2),
            "czas_free_min": round(czas_free_s / 60, 2),
            "opoznienie_min": round((czas_s - czas_free_s) / 60, 2),
            "dystans_km": round(dystans_m / 1000, 2),
            "timestamp": str(datetime.now()),
        }

        producer.send('commute_events', event)
        producer.flush()
        print(f"✅ Czas: {event['czas_min']} min | free: {event['czas_free_min']} min | opóźnienie: {event['opoznienie_min']} min")

    except KeyError as e:
        print(f"❌ Brak pola w odpowiedzi TomTom: {e}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Błąd HTTP: {e}")
    except Exception as e:
        print(f"❌ Nieoczekiwany błąd: {e}")

    time.sleep(300)