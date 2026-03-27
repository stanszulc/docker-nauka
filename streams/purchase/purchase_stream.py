import time
import json
import random
import psycopg2
from datetime import datetime
from kafka import KafkaProducer

# Domyslny price_factor jesli baza niedostepna
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
    # PRICE_UP (1.2) -> mniej zakupow, PROMO (0.8) -> wiecej zakupow
    if price_factor > 1.0:
        return 0.5   # droze -> mniej kupuja
    elif price_factor < 1.0:
        return 0.85  # promocja -> wiecej kupuja
    else:
        return 0.7   # normalnie

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
    price_factor = get_price_factor()
    purchase_prob = get_purchase_probability(price_factor)

    # Losuj czy ten user dokona zakupu
    if random.random() < purchase_prob:
        base_amount = round(random.uniform(10, 500), 2)
        amount = round(base_amount * price_factor, 2)

        event = {
            "user_id": random.randint(1, 1000),
            "action": "purchase",
            "amount": amount,
            "price_factor": price_factor,
            "timestamp": str(datetime.now()),
            "gps": {
                "lat": round(random.uniform(50.0, 50.1), 6),
                "lon": round(random.uniform(19.9, 20.0), 6)
            }
        }
        producer.send('purchase_events', event)
        producer.flush()
        print(f"Zakup: {amount} PLN (factor: {price_factor}, prob: {purchase_prob})")

    time.sleep(5)
