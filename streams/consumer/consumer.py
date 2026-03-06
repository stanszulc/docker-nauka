# consumer.py
import json
import time
import psycopg2
from kafka import KafkaConsumer

TOPICS = ['login_events', 'click_events', 'purchase_events']

# -------------------------
# Connect to PostgreSQL with retry
# -------------------------
while True:
    try:
        conn = psycopg2.connect(
            dbname="events",
            user="kafka",
            password="kafka",
            host="postgres",  # nazwa serwisu PostgreSQL w docker-compose
            port=5432
        )
        print("✅ Połączono z PostgreSQL")
        break
    except Exception as e:
        print(f"⏳ PostgreSQL nie jest gotowy, próbuję ponownie za 3 sekundy... {e}")
        time.sleep(3)

# -------------------------
# Create tables if not exist
# -------------------------
with conn.cursor() as cur:
    for table in TOPICS:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                data JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
    conn.commit()
print("✅ Tabele utworzone lub już istnieją")

# -------------------------
# Connect to Kafka with retry
# -------------------------
while True:
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers='kafka:9092',  # nazwa serwisu Kafka w docker-compose
            group_id='analytics',
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        print("✅ Połączono z Kafka")
        break
    except Exception as e:
        print(f"⏳ Kafka nie jest gotowa, próbuję ponownie za 3 sekundy... {e}")
        time.sleep(3)

# -------------------------
# Consume events and save to PostgreSQL
# -------------------------
print("⏳ Oczekiwanie na eventy...")
for msg in consumer:
    topic = msg.topic
    try:
        event = json.loads(msg.value)
    except json.JSONDecodeError:
        event = msg.value.decode()
    print("-----------")
    print(f"TOPIC: {topic}")
    print(f"EVENT: {event}")

    # Insert into PostgreSQL safely
    try:
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO {topic} (data) VALUES (%s)", (json.dumps(event),))
        conn.commit()
        print(f"✅ Zapisano event w tabeli {topic}")
    except Exception as e:
        print(f"❌ Błąd zapisu do PostgreSQL: {e}")
        conn.rollback()