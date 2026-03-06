from kafka import KafkaConsumer
import json
import psycopg2

# połączenie z PostgreSQL
conn = psycopg2.connect(
    host="postgres",
    database="events",
    user="kafka",
    password="kafka"
)
cur = conn.cursor()

# tworzymy tabelę jeśli nie istnieje
cur.execute("""
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    topic TEXT,
    event_data JSONB
)
""")
conn.commit()

# Kafka consumer
consumer = KafkaConsumer(
    'login_events',
    'click_events',
    'purchase_events',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer started...")

for message in consumer:
    topic = message.topic
    event = message.value

    print("-----------")
    print("TOPIC:", topic)
    print("EVENT:", event)

    cur.execute(
        "INSERT INTO events (topic, event_data) VALUES (%s, %s)",
        (topic, json.dumps(event))
    )
    conn.commit()