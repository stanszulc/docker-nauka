import time
import json
import requests
from datetime import datetime
from kafka import KafkaProducer

# Punkty pomiarowe w Krakowie
PUNKTY = [
    {"nazwa": "Rondo Grunwaldzkie", "lat": 50.0574, "lon": 19.9425},
    {"nazwa": "Al. Krasińskiego",   "lat": 50.0614, "lon": 19.9269},
    {"nazwa": "Rondo Mogilskie",    "lat": 50.0647, "lon": 19.9495},
    {"nazwa": "ul. Lubicz",         "lat": 50.0629, "lon": 19.9468},
    {"nazwa": "Rondo Ofiar Katynia","lat": 50.0702, "lon": 19.9001},
]

API_KEY = "UhkLbIJMhz9LagyQ1BYlLDyPyaLag6Lx"

def get_sleep_time():
    hour = datetime.now().hour
    if 6 <= hour < 22:
        return 120   # co 2 minuty w dzień
    else:
        return 1800  # co 30 minut w nocy

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
    for punkt in PUNKTY:
        try:
            url = (
                f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
                f"?point={punkt['lat']},{punkt['lon']}&key={API_KEY}"
            )
            r = requests.get(url, timeout=5)
            data = r.json().get("flowSegmentData", {})

            event = {
                "nazwa":            punkt["nazwa"],
                "lat":              punkt["lat"],
                "lon":              punkt["lon"],
                "predkosc_kmh":     data.get("currentSpeed", 0),
                "predkosc_free":    data.get("freeFlowSpeed", 0),
                "zatrzymania":      data.get("currentTravelTime", 0),
                "poziom_korkow":    round(1 - data.get("currentSpeed", 1) / max(data.get("freeFlowSpeed", 1), 1), 2),
                "timestamp":        str(datetime.now()),
            }

            producer.send('traffic_events', event)
            producer.flush()
            sleep_time = get_sleep_time()
            print(f"✅ {punkt['nazwa']}: {event['predkosc_kmh']} km/h (korki: {event['poziom_korkow']}) | następny odczyt za {sleep_time}s")

        except Exception as e:
            print(f"❌ Błąd dla {punkt['nazwa']}: {e}")

    time.sleep(get_sleep_time())