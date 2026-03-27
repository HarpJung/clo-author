"""Pull 2025 CRSP daily returns from dsf_v2 table."""
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

# Load portfolio permnos
permnos = set()
with open(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"), "r") as f:
    for r in csv.DictReader(f):
        p = r.get("permno", "").strip()
        if p:
            permnos.add(int(float(p)))

permnos = sorted(permnos)
print(f"Portfolio permnos: {len(permnos)}")

# Pull from dsf_v2 (2025 data)
print(f"\n--- Pulling 2025 from crsp.dsf_v2 ---")
time.sleep(3)

permno_str = ", ".join(str(p) for p in permnos)
cur.execute(f"""
    SELECT permno, dlycaldt as date, dlyret as ret, dlyprc as prc, dlyvol as vol, shrout
    FROM crsp.dsf_v2
    WHERE permno IN ({permno_str})
    AND dlycaldt >= '2025-01-01'
    ORDER BY permno, dlycaldt
""")

rows = cur.fetchall()
cols = ["permno", "date", "ret", "prc", "vol", "shrout"]
print(f"  Fetched {len(rows):,} rows")

if rows:
    dates = [r[1] for r in rows]
    print(f"  Date range: {min(dates)} to {max(dates)}")
    print(f"  Unique permnos: {len(set(r[0] for r in rows))}")

    # Save standalone 2025 file
    outpath = os.path.join(panel_c_dir, "06b_portfolio_crsp_daily_2025.csv")
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    print(f"  Saved to {outpath}")

    # Append to existing daily file
    existing_path = os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv")
    with open(existing_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)
    print(f"  Appended {len(rows):,} rows to {existing_path}")
else:
    print("  No 2025 data found!")

conn.close()
print("\nDone.")
