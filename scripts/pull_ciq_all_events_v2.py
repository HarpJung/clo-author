"""Pull ALL CIQ Key Dev events using wrds_keydev table.
Gets extra fields: situation, objectroletype, gvkey, companyname, sourcetypename.
"""
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

# Pull from wrds_keydev in batches
batch_size = 200  # smaller batches since more columns = more data
all_results = []

for batch_start in range(0, len(companyids), batch_size):
    batch = companyids[batch_start:batch_start + batch_size]
    cid_str = ", ".join(str(c) for c in batch)

    time.sleep(3)

    try:
        cur.execute(f"""
            SELECT keydevid, companyid, companyname, headline,
                   keydeveventtypeid, eventtype,
                   keydevtoobjectroletypeid, objectroletype,
                   announcedate, announcetime,
                   gvkey, sourcetypename
            FROM ciq_keydev.wrds_keydev
            WHERE companyid IN ({cid_str})
            ORDER BY companyid, announcedate
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
cols = ["keydevid", "companyid", "companyname", "headline",
        "keydeveventtypeid", "eventtype",
        "keydevtoobjectroletypeid", "objectroletype",
        "announcedate", "announcetime",
        "gvkey", "sourcetypename"]

outpath = os.path.join(ciq_dir, "06d_observer_all_events_full.csv")
with open(outpath, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    for r in all_results:
        writer.writerow(r)
print(f"Saved to {outpath}")

# Summary
df = pd.DataFrame(all_results, columns=cols)
print(f"\nUnique companies: {df['companyid'].nunique():,}")
print(f"Unique events: {df['keydevid'].nunique():,}")
print(f"Date range: {df['announcedate'].min()} to {df['announcedate'].max()}")
print(f"With gvkey: {df['gvkey'].notna().sum():,} ({df['gvkey'].notna().mean()*100:.1f}%)")

print(f"\nBy event type (top 30):")
for et, n in df["eventtype"].value_counts().head(30).items():
    print(f"  {et:<55} {n:>8,}")

print(f"\nBy object role type:")
for rt, n in df["objectroletype"].value_counts().items():
    print(f"  {rt:<40} {n:>8,}")

print(f"\nBy source type:")
for st, n in df["sourcetypename"].value_counts().items():
    print(f"  {st:<55} {n:>8,}")

# Cross-tab: event type x role type for M&A
print(f"\nM&A events by role type:")
ma = df[df["eventtype"].str.contains("M&A", na=False)]
for rt, n in ma["objectroletype"].value_counts().items():
    print(f"  {rt:<40} {n:>8,}")

conn.close()
print("\nDone.")
