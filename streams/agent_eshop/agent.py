import os
import psycopg2
from google import genai
from datetime import datetime
import schedule
import time
import json

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

def get_conn():
    return psycopg2.connect(
        dbname="events", user="kafka",
        password="kafka", host="postgres", port=5432
    )

def create_table():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_actions (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT NOW(),
                decision VARCHAR(20),
                conversion_rate FLOAT,
                revenue_15min FLOAT,
                active_users INT,
                reasoning TEXT
            )
        """)
    conn.commit()
    conn.close()
    print("Tabela agent_actions gotowa")

def pobierz_sygnaly():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM click_events
                 WHERE created_at > NOW() - INTERVAL '15 minutes') AS clicks,
                (SELECT COUNT(*) FROM purchase_events
                 WHERE created_at > NOW() - INTERVAL '15 minutes') AS purchases,
                (SELECT COALESCE(SUM((data->>'amount')::float), 0)
                 FROM purchase_events
                 WHERE created_at > NOW() - INTERVAL '15 minutes') AS revenue,
                (SELECT COUNT(DISTINCT (data->>'user_id'))
                 FROM click_events
                 WHERE created_at > NOW() - INTERVAL '15 minutes') AS active_users,
                (SELECT COALESCE(SUM((data->>'amount')::float), 0)
                 FROM purchase_events
                 WHERE created_at > NOW() - INTERVAL '30 minutes'
                   AND created_at <= NOW() - INTERVAL '15 minutes') AS revenue_prev
        """)
        row = cur.fetchone()
    conn.close()

    clicks, purchases, revenue, active_users, revenue_prev = row
    conversion_rate = round(purchases / clicks * 100, 1) if clicks > 0 else 0
    revenue_trend = round(((revenue - revenue_prev) / revenue_prev * 100), 1) if revenue_prev > 0 else 0

    return {
        "clicks": clicks,
        "purchases": purchases,
        "revenue": round(revenue, 2),
        "revenue_prev": round(revenue_prev, 2),
        "revenue_trend_pct": revenue_trend,
        "active_users": active_users,
        "conversion_rate": conversion_rate
    }

def zapisz_decyzje(decision, sygnaly, reasoning):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO agent_actions
                (decision, conversion_rate, revenue_15min, active_users, reasoning)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            decision,
            sygnaly["conversion_rate"],
            sygnaly["revenue"],
            sygnaly["active_users"],
            reasoning
        ))
    conn.commit()
    conn.close()

def analizuj():
    print(f"\n--- Analiza: {datetime.now().strftime('%H:%M:%S')} ---")
    try:
        s = pobierz_sygnaly()
    except Exception as e:
        print(f"Blad bazy: {e}")
        return

    print(f"Clicks: {s['clicks']} | Purchases: {s['purchases']} | "
          f"Conversion: {s['conversion_rate']}% | Revenue: {s['revenue']} PLN | "
          f"Trend: {s['revenue_trend_pct']}% | Users: {s['active_users']}")

    prompt = f"""
Jestes agentem AI zarzadzajacym cenami w sklepie internetowym.
Przeanalizuj ponizsze sygnaly z ostatnich 15 minut i podejmij decyzje.

SYGNALY:
- Klikniecia: {s['clicks']}
- Zakupy: {s['purchases']}
- Wskaznik konwersji: {s['conversion_rate']}%
- Przychod (ostatnie 15 min): {s['revenue']} PLN
- Przychod (poprzednie 15 min): {s['revenue_prev']} PLN
- Trend przychodu: {s['revenue_trend_pct']}%
- Aktywni uzytkownicy: {s['active_users']}

ZASADY DECYZJI:
- PRICE_UP: konwersja > 40% LUB trend przychodu > 20% — wysoki popyt, mozna podniesc ceny
- PROMO: konwersja < 10% LUB trend przychodu < -20% — niski popyt, uruchom promocje
- NEUTRAL: pozostale przypadki — utrzymaj aktualne ceny

Odpowiedz TYLKO w formacie JSON:
{{
  "decision": "PRICE_UP" lub "PROMO" lub "NEUTRAL",
  "reasoning": "krotkie uzasadnienie po polsku, max 2 zdania"
}}
"""

    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash", contents=prompt
        )
        raw = response.text.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        result = json.loads(raw)
        decision = result.get("decision", "NEUTRAL")
        reasoning = result.get("reasoning", "")
    except Exception as e:
        print(f"Blad Gemini: {e}")
        decision = "NEUTRAL"
        reasoning = f"Blad agenta: {e}"

    print(f"DECYZJA: {decision}")
    print(f"UZASADNIENIE: {reasoning}")

    try:
        zapisz_decyzje(decision, s, reasoning)
        print("Zapisano do bazy")
    except Exception as e:
        print(f"Blad zapisu: {e}")

create_table()
analizuj()

schedule.every(1).hours.do(analizuj)

print("\nAgent e-shop uruchomiony, analiza co 5 minut...")
while True:
    schedule.run_pending()
    time.sleep(30)