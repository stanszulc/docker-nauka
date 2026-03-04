# purchase_stream.py
import time
from datetime import datetime
import random

while True:
    event = {
        "user_id": random.randint(1, 1000),
        "action": "purchase",
        "amount": round(random.uniform(10, 500), 2),
        "timestamp": str(datetime.now()),
        "gps": {"lat": round(random.uniform(50.0, 50.1), 6),
                "lon": round(random.uniform(19.9, 20.0), 6)}
    }
    print(event)
    time.sleep(5)  # co 5 sekund nowy event