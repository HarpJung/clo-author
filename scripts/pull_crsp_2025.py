"""Pull 2025 CRSP daily returns for portfolio companies."""

import psycopg2, csv, os, time

conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu",
    port=9737,
    dbname="wrds",
    user="harperjung",
    password="Wwjksnm9087yu!"
)
cur = conn.cursor()

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

# Load existing portfolio permnos
existing = set()
with open(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"), "r") as f:
    for r in csv.DictReader(f):
        p = r.get("permno", "").strip()
        if p:
            existing.add(int(float(p)))

permnos = sorted(existing)
print(f"Portfolio permnos: {len(permnos)}")

# First check what dates are available
print("\n--- Checking CRSP date range ---")
time.sleep(2)
cur.execute("SELECT MIN(date) as min_date, MAX(date) as max_date FROM crsp.dsf")
row = cur.fetchone()
print(f"CRSP dsf: {row[0]} to {row[1]}")

# Pull 2025 daily returns
print(f"\n--- Pulling 2025 daily returns for {len(permnos)} permnos ---")
time.sleep(3)

permno_str = ", ".join(str(p) for p in permnos)
cur.execute(f"""
    SELECT permno, date, ret, prc, vol, shrout
    FROM crsp.dsf
    WHERE permno IN ({permno_str})
    AND date >= '2025-01-01'
    ORDER BY permno, date
""")

rows = cur.fetchall()
cols = [desc[0] for desc in cur.description]
print(f"  Fetched {len(rows):,} rows")

if rows:
    # Save to new file
    outpath = os.path.join(panel_c_dir, "06b_portfolio_crsp_daily_2025.csv")
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    print(f"  Saved to {outpath}")

    # Also append to existing file
    existing_path = os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv")
    with open(existing_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)
    print(f"  Appended to {existing_path}")

    # Date range check
    dates = [r[1] for r in rows]
    print(f"  Date range: {min(dates)} to {max(dates)}")
    print(f"  Unique permnos with 2025 data: {len(set(r[0] for r in rows))}")
else:
    print("  No 2025 data found!")

conn.close()
print("\nDone.")
