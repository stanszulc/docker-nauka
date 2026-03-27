import psycopg2
from datetime import datetime
import schedule
import time

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
                price_factor FLOAT,
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

def podejmij_decyzje(s):
    if s["conversion_rate"] > 40 or s["revenue_trend_pct"] > 20:
        return "PRICE_UP", 1.2, "Wysoka konwersja lub rosnacy przychod - podwyzka cen o 20%"
    elif s["conversion_rate"] < 10 or s["revenue_trend_pct"] < -20:
        return "PROMO", 0.8, "Niska konwersja lub spadajacy przychod - promocja -20%"
    else:
        return "NEUTRAL", 1.0, "Stabilna sytuacja - brak zmian cen"

def zapisz_decyzje(decision, price_factor, reasoning, sygnaly):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO agent_actions
                (decision, conversion_rate, revenue_15min, active_users, price_factor, reasoning)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            decision,
            sygnaly["conversion_rate"],
            sygnaly["revenue"],
            sygnaly["active_users"],
            price_factor,
            reasoning
            "agent_sql"
        ))
    conn.commit()
    conn.close()

def analizuj():
    print(f"\n--- Agent SQL: {datetime.now().strftime('%H:%M:%S')} ---")
    try:
        s = pobierz_sygnaly()
    except Exception as e:
        print(f"Blad bazy: {e}")
        return

    print(f"Clicks: {s['clicks']} | Purchases: {s['purchases']} | "
          f"Conversion: {s['conversion_rate']}% | Revenue: {s['revenue']} PLN | "
          f"Trend: {s['revenue_trend_pct']}%")

    decision, price_factor, reasoning = podejmij_decyzje(s)

    print(f"DECYZJA: {decision} | price_factor: {price_factor}")
    print(f"POWOD: {reasoning}")

    try:
        zapisz_decyzje(decision, price_factor, reasoning, s)
        print("Zapisano do bazy")
    except Exception as e:
        print(f"Blad zapisu: {e}")

create_table()
analizuj()

schedule.every(5).minutes.do(analizuj)

print("\nAgent SQL uruchomiony, analiza co 5 minut...")
while True:
    schedule.run_pending()
    time.sleep(30)
