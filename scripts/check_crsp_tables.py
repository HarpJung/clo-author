"""Check all CRSP tables for 2025 data availability."""
import psycopg2, time

conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu",
    port=9737,
    dbname="wrds",
    user="harperjung",
    password="Wwjksnm9087yu!"
)
cur = conn.cursor()

# Check various CRSP tables
tables = [
    "crsp.dsf",          # daily stock file (traditional)
    "crsp.dsf_v2",       # daily stock file v2 (newer)
    "crsp_a_stock.dsf",  # alternative path
    "crsp.dsi",          # daily index
    "crsp.msf",          # monthly stock file
]

for tbl in tables:
    time.sleep(1)
    try:
        cur.execute(f"SELECT MAX(date) as max_date FROM {tbl}")
        row = cur.fetchone()
        print(f"{tbl}: max date = {row[0]}")
    except Exception as e:
        conn.rollback()
        print(f"{tbl}: {str(e)[:80]}")

# Also check if crsp_a_stock schema has daily
time.sleep(1)
try:
    cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_name LIKE '%dsf%' OR table_name LIKE '%daily%'
        ORDER BY table_schema, table_name
    """)
    rows = cur.fetchall()
    print(f"\nTables with 'dsf' or 'daily' in name:")
    for r in rows:
        print(f"  {r[0]}.{r[1]}")
except Exception as e:
    conn.rollback()
    print(f"Schema search failed: {e}")

conn.close()
