# login_stream.py
import time
from datetime import datetime
import random

while True:
    event = {
        "user_id": random.randint(1, 1000),
        "action": "login",
        "timestamp": str(datetime.now()),
        "gps": {"lat": round(random.uniform(50.0, 50.1), 6),
                "lon": round(random.uniform(19.9, 20.0), 6)}
    }
    print(event)
    time.sleep(2)  # co 2 sekundy nowy event