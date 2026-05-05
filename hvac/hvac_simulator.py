import time
import json
import random
import uuid
import requests
import numpy as np
from datetime import datetime, timezone

# ── KONFIGURACJA ─────────────────────────────────────────────────────────────
# Jeśli symulator będzie w Dockerze, użyj nazwy kontenera: http://hvac-stream:8002
# Jeśli odpalasz go ręcznie na tej samej maszynie co Docker: http://localhost:8002
SERVER_URL = "http://localhost:8002/events/batch"
NUM_DEVICES = 1000
BATCH_SIZE = 100  # Wielkość paczki wysyłanej do API
INTERVAL = 10     # Co ile sekund każde urządzenie generuje odczyt

print(f"🚀 Start symulatora: {NUM_DEVICES} urządzeń, paczki po {BATCH_SIZE}")

# Inicjalizacja urządzeń (pozycja i ID)
devices = []
for i in range(NUM_DEVICES):
    devices.append({
        "id": f"HVAC-{1000 + i}",
        "lat": 50.06 + random.uniform(-0.05, 0.05),
        "lng": 19.94 + random.uniform(-0.05, 0.05),
        "session_id": str(uuid.uuid4())
    })

def generate_telemetry(device):
    """Generuje realistyczne dane HVAC zgodne ze schematem API."""
    # Symulujemy lekkie przegrzewanie się niektórych jednostek
    base_air = 298.0 + random.uniform(-2, 2)
    proc_temp = base_air + 10.0 + random.uniform(0, 5)
    
    # Szansa na awarię (ml_score)
    ml_score = random.random() if random.random() > 0.98 else random.uniform(0, 0.3)
    
    return {
        "device_id": device["id"],
        "lat": device["lat"],
        "lng": device["lng"],
        "air_temp": round(base_air, 1),
        "proc_temp": round(proc_temp, 1),
        "rpm": random.randint(1200, 2800),
        "torque": round(random.uniform(10, 50), 1),
        "vibration": round(random.uniform(0.01, 0.1), 3),
        "ml_score": round(ml_score, 2),
        "failure_type": "None" if ml_score < 0.5 else random.choice(["HDF", "PWF", "CLOG"]),
        "severity": "OK" if ml_score < 0.4 else ("WARNING" if ml_score < 0.7 else "CRITICAL"),
        "ts": datetime.now(timezone.utc).isoformat(),
        "event_type": "telemetry",
        "session_id": device["session_id"]
    }

def run_simulation():
    while True:
        start_time = time.time()
        all_events = [generate_telemetry(d) for d in devices]
        
        # Dzielimy 1000 eventów na paczki po 100
        for i in range(0, len(all_events), BATCH_SIZE):
            batch = all_events[i : i + BATCH_SIZE]
            try:
                response = requests.post(SERVER_URL, json=batch, timeout=5)
                if response.status_code == 202:
                    print(f"✅ Wysłano paczkę {i//BATCH_SIZE + 1} ({len(batch)} urządzeń)")
                else:
                    print(f"❌ Błąd API: {response.status_code} - {response.text}")
            except Exception as e:
                print(f"⚠️ Serwer nie odpowiada: {e}")
        
        # Czekamy do pełnego interwału
        elapsed = time.time() - start_time
        wait_time = max(0, INTERVAL - elapsed)
        print(f"😴 Koniec cyklu. Czekam {wait_time:.1f}s...")
        time.sleep(wait_time)

if __name__ == "__main__":
    run_simulation()
