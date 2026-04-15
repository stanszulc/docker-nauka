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

def call_gemini(prompt, retries=3, delay=300):
    for attempt in range(retries):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash", contents=prompt
            )
            return response.text
        except Exception as e:
            print(f"Blad Gemini (proba {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                print(f"Ponawiam za {delay//60} minut...")
                time.sleep(delay)
    return None

def pobierz_dane(jutro_dow):
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

def pobierz_dane_tygodnia():
    conn = get_conn()
    dzisiaj = datetime.now().date()
    poprzedni_poniedzialek = dzisiaj - timedelta(days=dzisiaj.weekday() + 7)
    poprzedni_piatek = poprzedni_poniedzialek + timedelta(days=4)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                EXTRACT(dow FROM created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Warsaw')::int AS dow,
                data->>'kierunek',
                EXTRACT(hour FROM created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Warsaw')::int AS godzina,
                avg((data->>'czas_min')::float) AS avg_czas,
                avg((data->>'opoznienie_min')::float) AS avg_opoznienie
            FROM commute_events
            WHERE
                DATE(created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Warsaw') BETWEEN %s AND %s
                AND data->>'kierunek' IN ('dojazd', 'powrot')
            GROUP BY dow, data->>'kierunek', godzina
            ORDER BY dow, data->>'kierunek', godzina
        """, (poprzedni_poniedzialek, poprzedni_piatek))
        rows = cur.fetchall()
    conn.close()
    return rows, poprzedni_poniedzialek, poprzedni_piatek

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
    jutro_dow = jutro.weekday()
    jutro_nazwa = DAYS_PL[jutro_dow]

    if jutro_dow >= 5:
        print(f"Jutro {jutro_nazwa} — brak rekomendacji na weekend")
        return

    pg_dow = (jutro_dow + 1) % 7

    try:
        rows = pobierz_dane(pg_dow)
    except Exception as e:
        print(f"Blad bazy danych: {e}")
        return

    if not rows:
        print("Brak danych historycznych dla tego dnia tygodnia")
        return

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

    raport = call_gemini(prompt)
    if not raport:
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

def generuj_raport_tygodniowy():
    print(f"Generuje raport tygodniowy: {datetime.now()}")

    try:
        rows, poniedzialek, piatek = pobierz_dane_tygodnia()
    except Exception as e:
        print(f"Blad bazy danych: {e}")
        return

    if not rows:
        print("Brak danych za poprzedni tydzień")
        return

    PG_DOW_PL = {1: "Poniedziałek", 2: "Wtorek", 3: "Środa", 4: "Czwartek", 5: "Piątek"}

    dane_txt = f"Dane z tygodnia {poniedzialek.strftime('%d.%m')} – {piatek.strftime('%d.%m.%Y')}:\n\n"

    for pg_dow, nazwa in PG_DOW_PL.items():
        dojazd = [(r[2], r[3], r[4]) for r in rows if r[0] == pg_dow and r[1] == 'dojazd']
        powrot = [(r[2], r[3], r[4]) for r in rows if r[0] == pg_dow and r[1] == 'powrot']

        if not dojazd and not powrot:
            continue

        dane_txt += f"### {nazwa}\n"
        if dojazd:
            dane_txt += "  Dojazd:\n"
            for godzina, avg_czas, avg_opoznienie in dojazd:
                dane_txt += f"    {int(godzina):02d}:00 — {round(avg_czas, 1)} min, opóźnienie: {round(avg_opoznienie, 1)} min\n"
        if powrot:
            dane_txt += "  Powrót:\n"
            for godzina, avg_czas, avg_opoznienie in powrot:
                dane_txt += f"    {int(godzina):02d}:00 — {round(avg_czas, 1)} min, opóźnienie: {round(avg_opoznienie, 1)} min\n"
        dane_txt += "\n"

    prompt = f"""
Jesteś asystentem analizującym dane o ruchu drogowym.

Poniżej są dane z poprzedniego tygodnia roboczego dla trasy Skawina ↔ Podłęże.
Założenia: praca 8 godzin, elastyczny start 7:00–9:00.

Wygeneruj tygodniowe podsumowanie w formacie markdown zawierające:
1. Krótki komentarz ogólny (2-3 zdania)
2. Tabelkę w formacie markdown:

| Dzień | Optymalny wyjazd | Czas dojazdu | Optymalny powrót | Czas powrotu |
|-------|-----------------|--------------|-----------------|--------------|
| Poniedziałek | 07:15 | 22 min | 15:30 | 24 min |
...

Optymalny = godzina z najmniejszym opóźnieniem.
Pisz zwięźle i konkretnie.

{dane_txt}
"""

    raport = call_gemini(prompt)
    if not raport:
        return

    print("\n" + "="*60)
    print(f"RAPORT TYGODNIOWY {poniedzialek.strftime('%d.%m')}–{piatek.strftime('%d.%m.%Y')}")
    print("="*60)
    print(raport)
    print("="*60 + "\n")

    try:
        zapisz_raport(piatek, raport)
    except Exception as e:
        print(f"Blad zapisu raportu: {e}")

create_tables()
generuj_raport()
schedule.every().day.at("07:00").do(generuj_raport)
schedule.every().saturday.at("07:00").do(generuj_raport_tygodniowy)
print("Agent uruchomiony, czeka na 7:00...")
while True:
    schedule.run_pending()
    time.sleep(60)
