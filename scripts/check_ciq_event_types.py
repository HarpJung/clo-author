"""Check what CIQ Key Dev event types exist on WRDS beyond what we pulled."""
import psycopg2, time

conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu",
    port=9737,
    dbname="wrds",
    user="harperjung",
    password="Wwjksnm9087yu!"
)
cur = conn.cursor()

# Check all Key Dev event types
print("--- All CIQ Key Dev Event Types ---")
time.sleep(2)
cur.execute("""
    SELECT keydeveventtypeid, keydeveventtypename, COUNT(*) as cnt
    FROM ciq_keydev.ciqkeydev
    GROUP BY keydeveventtypeid, keydeveventtypename
    ORDER BY cnt DESC
""")
rows = cur.fetchall()
print(f"{'ID':>4} {'Event Type':<60} {'Count':>12}")
print("-" * 78)
for r in rows:
    print(f"{r[0]:>4} {r[1]:<60} {r[2]:>12,}")

# Check what we already have
print("\n\n--- What we pulled (from our CSV) ---")
import pandas as pd
events = pd.read_csv("C:/Users/hjung/Documents/Claude/CorpAcct/Data/CIQ_Extract/06_observer_company_key_events.csv")
print(events["keydeveventtypename"].value_counts().to_string())

# Check distress-related types we might want
print("\n\n--- Distress/Material Events at Observer Companies ---")
time.sleep(2)

# Get our observer company IDs
obs = pd.read_csv("C:/Users/hjung/Documents/Claude/CorpAcct/Data/CIQ_Extract/01_observer_records.csv")
companyids = sorted(set(obs["companyid"].dropna().astype(int)))
companyid_str = ", ".join(str(c) for c in companyids[:500])  # first 500

# Check what event types exist for our observer companies
cur.execute(f"""
    SELECT keydeveventtypename, COUNT(*) as cnt
    FROM ciq_keydev.ciqkeydev
    WHERE companyid IN ({companyid_str})
    GROUP BY keydeveventtypename
    ORDER BY cnt DESC
""")
rows = cur.fetchall()
print(f"\n{'Event Type':<60} {'Count':>8}")
print("-" * 70)
for r in rows:
    print(f"{r[0]:<60} {r[1]:>8,}")

conn.close()
