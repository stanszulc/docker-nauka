import time
import json
import random
import psycopg2
from datetime import datetime
from kafka import KafkaProducer

# ===== KONFIGURACJA GPS =====
# Centra handlowe w Krakowie (współrzędne przybliżone)
CLUSTERS = [
    (50.0617, 19.9373),  # Rynek Główny
    (50.0745, 19.9884),  # Galeria Krakowska
    (50.0419, 19.9517),  # Kazimierz
    (50.0925, 19.9787),  # Krowodrza
    (50.0121, 19.9164),  # Zakopianka
]

# 500 metrów w stopniach (~0.0045 stopnia)
DEVIATION = 0.0045

def generate_gps():
    """Losuje centrum handlowe i dodaje odchylenie ~500m"""
    lat, lon = random.choice(CLUSTERS)
    lat += random.uniform(-DEVIATION, DEVIATION)
    lon += random.uniform(-DEVIATION, DEVIATION)
    return round(lat, 6), round(lon, 6)

DEFAULT_PRICE_FACTOR = 1.0

def get_price_factor():
    try:
        conn = psycopg2.connect(
            dbname="events", user="kafka",
            password="kafka", host="postgres", port=5432
        )
        with conn.cursor() as cur:
            cur.execute("""
                SELECT price_factor FROM agent_actions
                ORDER BY timestamp DESC LIMIT 1
            """)
            row = cur.fetchone()
        conn.close()
        if row:
            return float(row[0])
    except Exception:
        pass
    return DEFAULT_PRICE_FACTOR

def get_purchase_probability(price_factor):
    if price_factor > 1.0:
        return 0.4
    elif price_factor < 1.0:
        return 0.85
    else:
        return 0.7

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Połączono z Kafka")
        break
    except Exception as e:
        print(f"Kafka nie gotowa, retry za 3s... {e}")
        time.sleep(3)

while True:
    price_factor = get_price_factor()
    purchase_prob = get_purchase_probability(price_factor)

    if random.random() < purchase_prob:
        base_amount = round(random.uniform(10, 500), 2)
        amount = round(base_amount * price_factor, 2)
        
        lat, lon = generate_gps()

        event = {
            "user_id": random.randint(1, 1000),
            "action": "purchase",
            "amount": amount,
            "price_factor": price_factor,
            "timestamp": str(datetime.now()),
            "gps": {
                "lat": lat,
                "lon": lon
            }
        }
        producer.send('purchase_events', event)
        producer.flush()
        print(f"Zakup: {amount} PLN | GPS: {lat}, {lon} | factor: {price_factor}")

    time.sleep(5)
