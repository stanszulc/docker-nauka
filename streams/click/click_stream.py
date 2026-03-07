# click_stream.py
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Połączono z Kafka")
        break
    except Exception as e:
        print(f"⏳ Kafka nie jest gotowa, próbuję ponownie za 3 sekundy... {e}")
        time.sleep(3)

while True:
    event = {
        "user_id": random.randint(1, 1000),
        "action": "click",
        "timestamp": str(datetime.now()),
        "gps": {"lat": round(random.uniform(50.0, 50.1), 6),
                "lon": round(random.uniform(19.9, 20.0), 6)}
    }
    producer.send('click_events', event)
    producer.flush()
    print(f"✅ Wysłano: {event}")
    time.sleep(3)