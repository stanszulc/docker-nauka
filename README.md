# docker-nauka

Monorepo zawierające dwa niezależne systemy działające na Oracle Cloud VM (`92.5.14.76`):

| Podprojekt | Opis |
|---|---|
| [`streams/`](#-kraków-traffic-monitor) | Kraków Traffic Monitor — monitoring ruchu drogowego w czasie rzeczywistym |
| [`hvac/`](#️-hvac-predictive-maintenance) | HVAC Predictive Maintenance — predyktywne utrzymanie ruchu urządzeń HVAC |

Oba systemy współdzielą infrastrukturę Docker (Apache Kafka, PostgreSQL, Grafana) zdefiniowaną w głównym `docker-compose.yml`.

---

## 🏗️ Architektura wspólna

```
┌──────────────────────────────────────────────────────────────────┐
│                     Oracle Cloud VM  92.5.14.76                  │
│                                                                   │
│  ┌─────────────────────────┐   ┌─────────────────────────────┐  │
│  │   streams/              │   │   hvac/                     │  │
│  │   Kraków Traffic Monitor│   │   HVAC Predictive Maint.    │  │
│  │                         │   │                             │  │
│  │  TomTom API → Kafka     │   │  Symulator / Digital Twin   │  │
│  │  AI Agent (Gemini)      │   │  → FastAPI :8002 → Kafka    │  │
│  │  Grafana dashboards     │   │  XGBoost ML → alerts        │  │
│  └────────────┬────────────┘   └──────────────┬──────────────┘  │
│               │                               │                  │
│               ▼                               ▼                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │              Apache Kafka  (port 9092)                     │  │
│  │  traffic_events · commute · click · login · purchase       │  │
│  │  hvac_telemetry · hvac_alerts                              │  │
│  └────────────────────────────┬───────────────────────────────┘  │
│                               │                                  │
│                               ▼                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │              PostgreSQL  (port 5432)                       │  │
│  │  traffic_events · commute_events · click/login/purchase    │  │
│  │  hvac_metrics · hvac_alerts_log · hvac_device_status       │  │
│  └────────────────────────────┬───────────────────────────────┘  │
│                               │                                  │
│                               ▼                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │              Grafana  (port 3000)                          │  │
│  │   Traffic dashboard · HVAC dashboard                      │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 📁 Struktura repo

```
docker-nauka/
│
├── docker-compose.yml             # Wspólna infrastruktura (Kafka, PG, Grafana, serwisy)
│
├── streams/                       # ── Kraków Traffic Monitor ──────────────
│   ├── agent_raport/              # AI agent — codzienny raport dojazdowy
│   ├── click/                     # Generator kliknięć (co 3s)
│   ├── login/                     # Generator logowań (co 2s)
│   ├── purchase/                  # Generator zakupów (co 5s)
│   ├── traffic/                   # TomTom API — 5 punktów pomiarowych
│   ├── commute/                   # TomTom API — Radziszów ↔ Podłęże
│   ├── route/                     # Generator tras
│   ├── consumer/                  # Kafka → PostgreSQL consumer
│   └── grafana/
│       ├── dashboard.json
│       └── provisioning/
│           ├── datasources/
│           └── dashboards/
│
└── hvac/                          # ── HVAC Predictive Maintenance ─────────
    ├── hvac_simulator.py          # Symulator 25 urządzeń — fizyka v6
    ├── hvac_dtwinv4.html          # Digital Twin — interfejs operatora
    ├── hvac-monitor.html          # Dashboard floty (mapa + tabela statusów)
    ├── consumer.py                # Kafka consumer + klasyfikator ML
    ├── server.py                  # FastAPI /event /events/batch (port 8002)
    ├── hvac_classifier_v3.pkl     # Wagi modelu XGBoost v4 (nazwa historyczna)
    └── ml/
        ├── generate_v4.py         # Generator danych treningowych
        ├── train_classifier_v3.py # Trening XGBoost, eksport pkl
        └── hvac_training_v4.csv   # Zbiór treningowy (218 975 wierszy)
```

---

## 🚦 Kraków Traffic Monitor

System monitoringu ruchu drogowego w czasie rzeczywistym dla trasy Radziszów ↔ Podłęże oraz 5 punktów pomiarowych w Krakowie.

### Przepływ danych

```
TomTom API (co 5min)          Generatory syntetyczne
      │                        click / login / purchase
      ▼                               │
  commute_stream.py                   ▼
  traffic_stream.py ────────► Apache Kafka
                                      │
                                      ▼
                               consumer.py
                               (psycopg2)
                                      │
                                      ▼
                               PostgreSQL
                              /              \
                        Grafana           agent_raport/
                       dashboards          agent.py
                                       Gemini 2.5 Flash
                                       raport o 02:00 UTC
```

### Dane z TomTom API

**Punkty pomiarowe (`traffic_events`)** — co 2 min w dzień, co 30 min w nocy:

| Punkt | Lokalizacja |
|---|---|
| Al. Krasińskiego | Kraków centrum |
| Rondo Mogilskie | Kraków centrum |
| Rondo Ofiar Katynia | Kraków centrum |
| A4 Balice | Autostrada zachód |
| A4 Podłęże | Autostrada wschód |

**Trasa dojazdowa (`commute_events`)** — co 5 min w dzień, co 30 min w nocy:
- Radziszów PKP → Podłęże (`kierunek: dojazd`)
- Podłęże → Radziszów (`kierunek: powrót`)

Współrzędne: `50.0335, 20.2172`

### AI Agent (`agent_raport/agent.py`)

Codziennie o **02:00 UTC** analizuje ostatnie 8 tygodni danych historycznych (filtrowane po dniu tygodnia jutrzejszego dnia roboczego) i generuje raport w języku polskim:

- Podsumowanie dnia
- Analiza porannego dojazdu (szczyty, opóźnienia)
- Analiza powrotu wieczornego
- Najgorszy moment dnia
- **Rekomendacja**: Wyjazd z domu / Przyjazd do pracy / Wyjazd z pracy / Powrót do domu

Model: **Gemini 2.5 Flash** · retry 3× z 5-minutowymi przerwami.

### Grafana — panele Traffic

| Panel | Typ | Opis |
|---|---|---|
| Traffic Speed in Kraków | Time series | Prędkość km/h per punkt pomiarowy |
| Congestion Level | Gauge | 🟢 <25% · 🟡 25–50% · 🔴 >50% |
| Latest Events | Table | Ostatnie pomiary |
| Kraków Traffic Map | Geomap | Mapa z kolorowanymi punktami |
| Commute Time | Time series | Czas przejazdu Radziszów ↔ Podłęże per godzina |

---

## ⚙️ HVAC Predictive Maintenance

System predyktywnego utrzymania ruchu urządzeń HVAC oparty na cyfrowym bliźniaku, symulatorze fizyki i modelu XGBoost do wczesnego wykrywania awarii.

### Przepływ danych

```
hvac_simulator.py               hvac_dtwinv4.html
(25 urządzeń, tick=10s)        (Digital Twin, przeglądarka)
        │                               │
        └──────────┬────────────────────┘
                   ▼
          FastAPI  :8002
          /event  /events/batch
                   │
                   ▼
            Apache Kafka
           hvac_telemetry
                   │
                   ▼
            consumer.py
         rolling buffer (20)
         XGBoost predict_proba()
                   │
          ml_score > 0.5 ?
         /                  \
        TAK                 NIE
         │                    │
  hvac_alerts_log        hvac_metrics
         │
         ▼
      Grafana
   HVAC dashboard
```

### Symulator — fizyka v6

25 wirtualnych urządzeń HVAC. SIM_01–15 przechodzą pełny cykl awarii, SIM_16–25 symulują pracę normalną z pseudo-anomaliami (do testowania czułości modelu).

**Fazy życia urządzenia:**
```
warmup (70–130 ticków)
    └─► anomaly          ← liniowy przyrost wybranego sensora
            └─► failure  ← sensor przekracza próg
                    └─► service (min. 2 min) → warmup
```

**Sensory i zakresy normalne:**

| Sensor | Norma | Jednostka |
|---|---|---|
| `proc_temp` | 60 ± 5 | °C |
| `rpm` | 1500 ± 50 | obr/min |
| `torque` | 40 ± 3 | Nm |
| `vibration` | 0.5 ± 0.2 | mm/s RMS |
| `air_temp` | 20 ± 2 | °C |

**Progi defektów (zsynchronizowane HTML ↔ Python):**

| Fault | Pre-alarm | Failure |
|---|---|---|
| `vibration_high` | 1.8–2.5 mm/s | > 2.5 |
| `rpm_low` | 1300–1400 rpm | < 1300 |
| `rpm_high` | 1600–1700 rpm | > 1700 |
| `torque_high` | 45–49 Nm | > 49 |
| `torque_low` | 33–35 Nm | < 33 |
| `temp_high` | 70–100 °C | > 100 |

### Digital Twin — `hvac_dtwinv4.html`

Przeglądarkowy interfejs operatora dla pojedynczego urządzenia. Działa offline lub połączony z FastAPI.

- Animowany wentylator reagujący na RPM, drgania i temperaturę (efekt dymu przy `temp_high`)
- Ręczne toggle 6 trybów defektów
- Strefy alertów: **PRE-ALARM** (żółty) → **FAILURE / freeze** (czerwony) → **🔧 SERVICE**
- Gauge'y dla wszystkich 5 sensorów z kolorowymi progami
- Event `service` ze świeżymi wartościami sensorów (reset przed wysłaniem)

### Model ML — XGBoost v4

Binarny klasyfikator anomalii. Plik `hvac_classifier_v3.pkl` zawiera wagi v4 (nazwa historyczna).

**Dane treningowe:** 218 975 wierszy · 60% normalne + pseudo-anomalie

**Features:** `proc_temp` · `rpm` · `torque` · `vibration` · `air_temp` · `uptime_norm` · `buffer_fill_ratio`

**Wyniki:**

| Metryka | Wartość |
|---|---|
| ROC-AUC | 0.9995 |
| Precision | 98.9% |
| False Positives | ~6/h |

**Recall per fault:**

| Fault | Recall |
|---|---|
| `vibration_high` | 100% |
| `rpm_low` / `rpm_high` | ~80% |
| `torque_high` / `torque_low` | ~52% |
| `temp_high` | 45% |

### Dwa sygnały alarmowe

System generuje dwa niezależne sygnały — oba widoczne w Grafanie:

| | Sygnał fizyczny | Sygnał ML |
|---|---|---|
| Źródło | progi w symulatorze / Digital Twin | XGBoost w `consumer.py` |
| Pole | `severity`, `failure_type` w evencie | wpis w `hvac_alerts_log` |
| Grafana | kolumna `severity` w `hvac_metrics` | tabela `hvac_alerts_log` |
| Charakter | twardy próg, zero latencji | probabilistyczny, wcześniejsze wykrycie |

---

## 🛠️ Tech Stack

| Technologia | Wersja | Zastosowanie |
|---|---|---|
| Apache Kafka | 7.3.0 | Message broker, event streaming |
| Zookeeper | 7.3.0 | Zarządzanie klastrem Kafka |
| PostgreSQL | 15 | Przechowywanie danych |
| Grafana | 11.4.0 | Wizualizacja, dashboardy |
| Python | 3.11 | Generatory, consumer, AI agent, ML |
| FastAPI | latest | REST API dla HVAC (port 8002) |
| XGBoost | latest | Klasyfikator anomalii HVAC |
| Docker | latest | Konteneryzacja |
| TomTom API | v4 | Dane ruchu drogowego w czasie rzeczywistym |
| Google Gemini | 2.5 Flash | Generowanie raportów w języku naturalnym |

---

## 🚀 Uruchomienie

### Wymagania

- Docker + Docker Compose
- TomTom API key — [developer.tomtom.com](https://developer.tomtom.com)
- Google Gemini API key — [aistudio.google.com](https://aistudio.google.com/app/apikey)

### Konfiguracja

```bash
# streams/traffic/traffic_stream.py i streams/commute/commute_stream.py
API_KEY = "TWÓJ_KLUCZ_TOMTOM"

# docker-compose.yml
GEMINI_API_KEY=TWÓJ_KLUCZ_GEMINI
```

### Start

```bash
git clone https://github.com/stanszulc/docker-nauka.git
cd docker-nauka
docker-compose up -d
```

### Dostęp

| Serwis | Adres |
|---|---|
| Grafana | http://localhost:3000  (admin / admin) |
| FastAPI HVAC | http://localhost:8002 |
| FastAPI docs | http://localhost:8002/docs |

### Digital Twin

Otwórz `hvac/hvac_dtwinv4.html` bezpośrednio w przeglądarce. W polu *Server URL* wpisz `http://92.5.14.76:8002`.

---

## 🔧 Deployment — workflow (VM)

```bash
# Lokalnie
git add . && git commit -m "opis" && git push

# Na VM
ssh -i ~/ssh-key-2026-03-11.key ubuntu@92.5.14.76
cd /home/ubuntu/docker-nauka
git pull
docker-compose build --no-cache [nazwa_serwisu]
docker-compose up -d
```

---

## 🔒 Bezpieczeństwo

- Walidacja nazw tabel przez allowlist (ochrona przed SQL Injection)
- Retry loops — automatyczne reconnecty przy awarii połączenia
- Persistent volumes — dane PostgreSQL i konfiguracja Grafana przeżywają restart
- Klucze API wyłącznie w zmiennych środowiskowych

---

## ⚠️ Znane ograniczenia

| Projekt | Problem | Status |
|---|---|---|
| Traffic / AI Agent | Agent może rekomendować wyjazd skutkujący przyjazdem przed 07:00 | Do poprawy w prompcie |
| HVAC / ML | Niski recall `temp_high` (~45%) — wolne narastanie sygnału | Rozważany drugi model (LSTM) |
| HVAC / ML | Cold-start false alarms przy `uptime_norm=0` | Częściowo naprawione przez `buffer_fill_ratio` |
| HVAC / plik | `hvac_classifier_v3.pkl` zawiera wagi v4 — nazwa historyczna | Do zunifikowania przy kolejnym treningu |
