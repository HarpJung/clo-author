"""Pull ALL CIQ Key Dev events for observer companies. Every event type."""
import psycopg2, csv, os, time, sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd

conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu",
    port=9737,
    dbname="wrds",
    user="harperjung",
    password="Wwjksnm9087yu!"
)
cur = conn.cursor()

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")

# Load observer company IDs
obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
companyids = sorted(set(obs["companyid"].dropna().astype(int)))
print(f"Observer companies: {len(companyids):,}")

# Pull ALL events — no filter on event type
batch_size = 300
all_results = []

for batch_start in range(0, len(companyids), batch_size):
    batch = companyids[batch_start:batch_start + batch_size]
    cid_str = ", ".join(str(c) for c in batch)

    time.sleep(3)

    try:
        cur.execute(f"""
            SELECT k.keydevid, oe.objectid as companyid, k.announceddate, k.headline,
                   ct.keydeveventtypeid, ct.keydeveventtypename, ct.keydevcategoryname
            FROM ciq_keydev.ciqkeydevtoobjecttoeventtype oe
            JOIN ciq_keydev.ciqkeydev k ON oe.keydevid = k.keydevid
            JOIN ciq_keydev.ciqkeydevcategorytype ct ON oe.keydeveventtypeid = ct.keydeveventtypeid
            WHERE oe.objectid IN ({cid_str})
            ORDER BY oe.objectid, k.announceddate
        """)
        rows = cur.fetchall()
        all_results.extend(rows)
        n_batch = batch_start // batch_size + 1
        n_total = (len(companyids) + batch_size - 1) // batch_size
        print(f"  Batch {n_batch}/{n_total}: {len(rows):,} events (total: {len(all_results):,})")
    except Exception as e:
        print(f"  Batch error: {str(e)[:80]}")
        conn.rollback()

print(f"\nTotal events pulled: {len(all_results):,}")

# Save
cols = ["keydevid", "companyid", "announcedate", "headline",
        "keydeveventtypeid", "keydeveventtypename", "keydevcategoryname"]
outpath = os.path.join(ciq_dir, "06c_observer_all_events.csv")
with open(outpath, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    for r in all_results:
        writer.writerow(r)

print(f"Saved to {outpath}")

# Summary
df = pd.DataFrame(all_results, columns=cols)
print(f"\nUnique companies: {df['companyid'].nunique():,}")
print(f"Unique events (keydevid): {df['keydevid'].nunique():,}")
print(f"Date range: {df['announcedate'].min()} to {df['announcedate'].max()}")

print(f"\nBy category:")
for cat, n in df["keydevcategoryname"].value_counts().items():
    print(f"  {cat:<60} {n:>8,}")

print(f"\nAll event types:")
for et, n in df["keydeveventtypename"].value_counts().items():
    print(f"  {et:<60} {n:>8,}")

conn.close()
print("\nDone.")
