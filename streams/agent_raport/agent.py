import os
import psycopg2
from google import genai
from datetime import datetime, timedelta
import schedule
import time

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

def pobierz_dane():
    conn = psycopg2.connect(
        dbname="events",
        user="kafka",
        password="kafka",
        host="postgres",
        port=5432
    )
    wczoraj = (datetime.now() - timedelta(days=1)).date()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                data->>'kierunek',
                EXTRACT(hour FROM created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Warsaw')::int,
                avg((data->>'czas_min')::float),
                avg((data->>'opoznienie_min')::float),
                max((data->>'czas_min')::float)
            FROM commute_events
            WHERE DATE(created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Warsaw') = %s
              AND data->>'kierunek' IN ('dojazd', 'powrot')
            GROUP BY data->>'kierunek', EXTRACT(hour FROM created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Warsaw')
            ORDER BY data->>'kierunek', 2
        """, (wczoraj,))
        rows = cur.fetchall()
    conn.close()
    return rows, wczoraj

def generuj_raport():
    print(f"Generuje raport: {datetime.now()}")
    rows, data = pobierz_dane()
    if not rows:
        print("Brak danych za wczoraj")
        return

    dane_txt = f"Dane o ruchu dla trasy Radziszow - Podleze za dzien {data}:\n\n"
    for row in rows:
        kierunek, godzina, avg_czas, avg_opoznienie, max_czas = row
        dane_txt += (
            f"Kierunek: {kierunek}, Godzina: {godzina}:00, "
            f"Sredni czas: {round(avg_czas, 1)} min, "
            f"Srednie opoznienie: {round(avg_opoznienie, 1)} min, "
            f"Maks. czas: {round(max_czas, 1)} min\n"
        )

    prompt = f"""
Jestes asystentem analizujacym dane o ruchu drogowym.
Na podstawie ponizszych danych napisz dzienny raport w jezyku polskim.

Raport powinien zawierac:
1. Ogolne podsumowanie dnia
2. Analize dojazdu do pracy (rano)
3. Analize powrotu z pracy (popoludnie/wieczor)
4. Najgorszy moment dnia
5. Krotka rekomendacje na jutro

Pisz naturalnie, jak czlowiek - nie jak tabela danych.

{dane_txt}
"""

    response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
    raport = response.text

    print("\n" + "="*60)
    print(f"RAPORT DZIENNY - {data}")
    print("="*60)
    print(raport)
    print("="*60 + "\n")

generuj_raport()

schedule.every().day.at("07:00").do(generuj_raport)

print("Agent uruchomiony, czeka na 7:00...")
while True:
    schedule.run_pending()
    time.sleep(60)