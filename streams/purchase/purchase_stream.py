import time
from datetime import datetime
import random

while True:
    amount = random.randint(20, 500)
    print(f"[{datetime.now()}] PURCHASE {amount} PLN")
    time.sleep(4)