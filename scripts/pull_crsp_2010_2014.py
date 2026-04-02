"""
Pull CRSP daily returns for 2010-2014 for portfolio stocks.
Connects to WRDS PostgreSQL, batches PERMNOs in groups of 500.
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import time
import pandas as pd
import psycopg2

# ── paths ──────────────────────────────────────────────────────────────
CROSSWALK = r"C:\Users\hjung\Documents\Claude\CorpAcct\Data\Panel_C_Network\03_portfolio_permno_crosswalk.csv"
OUTFILE   = r"C:\Users\hjung\Documents\Claude\CorpAcct\Data\Panel_C_Network\06c_portfolio_crsp_daily_2010_2014.csv"

# ── load PERMNO list ───────────────────────────────────────────────────
xw = pd.read_csv(CROSSWALK)
permnos = xw["permno"].dropna().astype(int).unique().tolist()
print(f"Unique PERMNOs to query: {len(permnos)}")

# ── WRDS connection ────────────────────────────────────────────────────
conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu",
    port=9737,
    dbname="wrds",
    user="harperjung",
    password="Wwjksnm9087yu!",
)
print("Connected to WRDS.")

# ── batch query ────────────────────────────────────────────────────────
BATCH_SIZE = 500
batches = [permnos[i : i + BATCH_SIZE] for i in range(0, len(permnos), BATCH_SIZE)]
print(f"Total batches: {len(batches)}")

frames = []
for idx, batch in enumerate(batches, 1):
    placeholders = ",".join(["%s"] * len(batch))
    sql = f"""
        SELECT permno, date, ret, prc, vol
        FROM crsp_a_stock.dsf
        WHERE permno IN ({placeholders})
          AND date >= '2010-01-01'
          AND date <  '2015-01-01'
    """
    df_batch = pd.read_sql(sql, conn, params=batch)
    frames.append(df_batch)
    print(f"  Batch {idx}/{len(batches)}: {len(df_batch):,} rows fetched")
    if idx < len(batches):
        time.sleep(3)

conn.close()
print("Connection closed.")

# ── combine and save ───────────────────────────────────────────────────
df = pd.concat(frames, ignore_index=True)
df = df.sort_values(["permno", "date"]).reset_index(drop=True)
df.to_csv(OUTFILE, index=False)

print(f"\nTotal rows: {len(df):,}")
print(f"Date range: {df['date'].min()} to {df['date'].max()}")
print(f"Unique PERMNOs in output: {df['permno'].nunique()}")
print(f"Saved to: {OUTFILE}")
