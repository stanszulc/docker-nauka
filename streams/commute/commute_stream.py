import time
import json
import requests
from datetime import datetime
from kafka import KafkaProducer

TRASY = [
    {
        "nazwa": "Radziszow -> Podleze",
        "kierunek": "dojazd",
        "start_lat": 49.9712, "start_lon": 19.8423,
        "koniec_lat": 49.9980, "koniec_lon": 20.0900,
    },
    {
        "nazwa": "Podleze -> Radziszow",
        "kierunek": "powrot",
        "start_lat": 49.9980, "start_lon": 20.0900,
        "koniec_lat": 49.9712, "koniec_lon": 19.8423,
    },
]

API_KEY = "olDzucBAob8qXdWEsOdHUlecy1Wjg2xW"

def build_url(trasa):
    return (
        f"https://api.tomtom.com/routing/1/calculateRoute/"
        f"{trasa['start_lat']},{trasa['start_lon']}:{trasa['koniec_lat']},{trasa['koniec_lon']}/json"
        f"?traffic=true&travelMode=car&computeTravelTimeFor=all&key={API_KEY}"
    )

def get_sleep_time():
    hour = datetime.now().hour
    if 6 <= hour < 22:
        return 300
    else:
        return 1800

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Polaczono z Kafka")
        break
    except Exception as e:
        print(f"Kafka nie gotowa, retry za 3s... {e}")
        time.sleep(3)

while True:
    for trasa in TRASY:
        try:
            r = requests.get(build_url(trasa), timeout=5)
            r.raise_for_status()
            data = r.json()
            route = data["routes"][0]["summary"]
            czas_s = route["travelTimeInSeconds"]
            czas_free_s = route.get("noTrafficTravelTimeInSeconds", czas_s)
            dystans_m = route["lengthInMeters"]
            event = {
                "nazwa":          trasa["nazwa"],
                "kierunek":       trasa["kierunek"],
                "czas_min":       round(czas_s / 60, 2),
                "czas_free_min":  round(czas_free_s / 60, 2),
                "opoznienie_min": round((czas_s - czas_free_s) / 60, 2),
                "dystans_km":     round(dystans_m / 1000, 2),
                "timestamp":      str(datetime.now()),
            }
            producer.send('commute_events', event)
            producer.flush()
            print(f"{trasa['nazwa']}: {event['czas_min']} min | opoznienie: {event['opoznienie_min']} min")
        except KeyError as e:
            print(f"Brak pola TomTom [{trasa['nazwa']}]: {e}")
        except requests.exceptions.RequestException as e:
            print(f"Blad HTTP [{trasa['nazwa']}]: {e}")
        except Exception as e:
            print(f"Blad [{trasa['nazwa']}]: {e}")
    time.sleep(get_sleep_time())
