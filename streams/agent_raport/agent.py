import os
import psycopg2
from google import genai
from datetime import datetime, timedelta
import schedule
import time

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

DAYS_PL = {
    0: "poniedziałek", 1: "wtorek", 2: "środa",
    3: "czwartek", 4: "piątek", 5: "sobota", 6: "niedziela"
}

def get_conn():
    return psycopg2.connect(
        dbname="events", user="kafka",
        password="kafka", host="postgres", port=5432
    )

def create_tables():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_reports (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT NOW(),
                data_raportu DATE,
                raport TEXT
            )
        """)
    conn.commit()
    conn.close()
    print("Tabela agent_reports gotowa")

def pobierz_dane(jutro_dow):
    """Pobiera dane historyczne dla konkretnego dnia tygodnia (ostatnie 8 tygodni)"""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                data->>'kierunek',
                EXTRACT(hour FROM created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Warsaw')::int AS godzina,
                avg((data->>'czas_min')::float) AS avg_czas,
                avg((data->>'opoznienie_min')::float) AS avg_opoznienie,
                count(*) AS liczba_pomiarow
            FROM commute_events
            WHERE
                EXTRACT(dow FROM created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Warsaw') = %s
                AND created_at >= NOW() - INTERVAL '8 weeks'
                AND data->>'kierunek' IN ('dojazd', 'powrot')
            GROUP BY data->>'kierunek', godzina
            ORDER BY data->>'kierunek', godzina
        """, (jutro_dow,))
        rows = cur.fetchall()
    conn.close()
    return rows

def zapisz_raport(data, raport):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO agent_reports (data_raportu, raport)
            VALUES (%s, %s)
        """, (data, raport))
    conn.commit()
    conn.close()
    print("Raport zapisany do bazy")

def generuj_raport():
    print(f"Generuje rekomendacje: {datetime.now()}")

    jutro = datetime.now() + timedelta(days=1)
    jutro_dow = jutro.weekday()  # 0=pon, 6=nie
    jutro_nazwa = DAYS_PL[jutro_dow]

    # Weekendy pomijamy
    if jutro_dow >= 5:
        print(f"Jutro {jutro_nazwa} — brak rekomendacji na weekend")
        return

    # PostgreSQL: 0=niedziela, 1=pon ... 6=sob (EXTRACT dow)
    pg_dow = (jutro_dow + 1) % 7

    try:
        rows = pobierz_dane(pg_dow)
    except Exception as e:
        print(f"Blad bazy danych: {e}")
        return

    if not rows:
        print("Brak danych historycznych dla tego dnia tygodnia")
        return

    # Buduj czytelne dane dla promptu
    dojazd = [(r[1], r[2], r[3], r[4]) for r in rows if r[0] == 'dojazd']
    powrot = [(r[1], r[2], r[3], r[4]) for r in rows if r[0] == 'powrot']

    def formatuj(lista, etykieta):
        txt = f"{etykieta}:\n"
        for godzina, avg_czas, avg_opoznienie, pomiary in lista:
            txt += (f"  {int(godzina):02d}:00 — średni czas: {round(avg_czas, 1)} min, "
                    f"opóźnienie: {round(avg_opoznienie, 1)} min "
                    f"(próbek: {int(pomiary)})\n")
        return txt

    dane_txt = formatuj(dojazd, "DOJAZD Skawina → Podłęże")
    dane_txt += "\n" + formatuj(powrot, "POWRÓT Podłęże → Skawina")

    prompt = f"""
Jesteś asystentem pomagającym zaplanować dojazd do pracy.

Jutro jest {jutro_nazwa.upper()} ({jutro.strftime('%d.%m.%Y')}).
Poniżej znajdują się dane historyczne dla poprzednich {jutro_nazwa}ów.

Założenia:
- Praca zaczyna się między 7:00 a 9:00 (elastyczne godziny)
- Praca trwa 8 godzin
- Trasa: Skawina → Podłęże (dojazd), Podłęże → Skawina (powrót)

Na podstawie danych odpowiedz:
1. O której godzinie najlepiej wyjechać rano? (podaj konkretną godzinę i uzasadnij krótko)
2. O której można spodziewać się najlepszego powrotu? (podaj przedział np. 15:30–16:30)
3. Jednozdaniowe podsumowanie jutrzejszego dnia

Pisz konkretnie i krótko. Nie przepisuj danych — wyciągnij wnioski.

{dane_txt}
"""

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash", contents=prompt
        )
        raport = response.text
    except Exception as e:
        print(f"Blad Gemini: {e}")
        return

    print("\n" + "="*60)
    print(f"REKOMENDACJA NA {jutro_nazwa.upper()} {jutro.strftime('%d.%m.%Y')}")
    print("="*60)
    print(raport)
    print("="*60 + "\n")

    try:
        zapisz_raport(jutro.date(), raport)
    except Exception as e:
        print(f"Blad zapisu raportu: {e}")

create_tables()
generuj_raport()
schedule.every().day.at("07:00").do(generuj_raport)
print("Agent uruchomiony, czeka na 7:00...")
while True:
    schedule.run_pending()
    time.sleep(60)
