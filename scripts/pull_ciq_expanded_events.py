"""Pull expanded CIQ Key Dev events for observer companies.
Focus on: distress/red flags, M&A, CEO/CFO changes, private placements.
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

# Event types we want to pull (by name)
target_events = [
    # Distress / Red Flags
    "Impairments/Write Offs",
    "Delayed SEC Filings",
    "Auditor Going Concern Doubts",
    "Auditor Changes",
    "Business Reorganizations",
    "Labor-related Announcements",
    "Halt/Resume of Operations - Unusual Events",
    "Regulatory Authority - Enforcement Actions",
    "Regulatory Authority – Enforcement Actions",
    "Bankruptcy - Filing",
    "Debt Defaults",
    "Delistings",
    "Dividend Cancellation or Suspension",
    "Delayed Earnings Announcements",
    "Corporate Guidance - Lowered",
    # M&A
    "M&A Transaction Announcements",
    "M&A Transaction Closings",
    "M&A Rumors and Discussions",
    "M&A Transaction Cancellations",
    "Considering Multiple Strategic Alternatives",
    "Seeking to Sell/Divest",
    "Seeking Acquisitions/Investments",
    # Executive changes (specific)
    "Executive Changes - CEO",
    "Executive Changes - CFO",
    # Transactions
    "Private Placements",
    "Follow-on Equity Offerings",
    "Debt Financing Related",
    "Fixed Income Offerings",
    # Investor activism
    "Investor Activism - Proxy/Voting Related",
    "Investor Activism - Activist Communication",
    "Investor Activism - Target Communication",
    "Investor Activism - Proposal Related",
    # Corporate structure
    "Board Meeting",
    "Changes in Company Bylaws/Rules",
    "Business Expansions",
    "Spin-Off/Split-Off",
    # Bankruptcy details
    "Bankruptcy - Other",
    "Bankruptcy - Asset Sale/Liquidation",
    "Bankruptcy - Reorganization",
    "Bankruptcy - Financing",
    "Bankruptcy - Emergence/Exit",
    "Bankruptcy - Conclusion",
]

# Pull in batches of company IDs (WRDS has query size limits)
batch_size = 500
all_results = []

for batch_start in range(0, len(companyids), batch_size):
    batch = companyids[batch_start:batch_start + batch_size]
    cid_str = ", ".join(str(c) for c in batch)

    time.sleep(3)  # rate limit

    try:
        cur.execute(f"""
            SELECT k.keydevid, oe.objectid as companyid, k.announceddate, k.headline,
                   ct.keydeveventtypeid, ct.keydeveventtypename, ct.keydevcategoryname
            FROM ciq_keydev.ciqkeydevtoobjecttoeventtype oe
            JOIN ciq_keydev.ciqkeydev k ON oe.keydevid = k.keydevid
            JOIN ciq_keydev.ciqkeydevcategorytype ct ON oe.keydeveventtypeid = ct.keydeveventtypeid
            WHERE oe.objectid IN ({cid_str})
            AND ct.keydeveventtypename IN ({', '.join("'" + e.replace("'", "''") + "'" for e in target_events)})
            ORDER BY oe.objectid, k.announceddate
        """)
        rows = cur.fetchall()
        all_results.extend(rows)
        print(f"  Batch {batch_start//batch_size + 1}/{(len(companyids) + batch_size - 1)//batch_size}: {len(rows):,} events (total: {len(all_results):,})")
    except Exception as e:
        print(f"  Batch error: {str(e)[:80]}")
        conn.rollback()

print(f"\nTotal events pulled: {len(all_results):,}")

# Save
cols = ["keydevid", "companyid", "announcedate", "headline", "keydeveventtypeid", "keydeveventtypename", "keydevcategoryname"]
outpath = os.path.join(ciq_dir, "06b_observer_expanded_events.csv")
with open(outpath, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    for r in all_results:
        writer.writerow(r)

print(f"Saved to {outpath}")

# Summary
df = pd.DataFrame(all_results, columns=cols)
print(f"\nEvent type breakdown:")
for et, n in df["keydeveventtypename"].value_counts().items():
    print(f"  {et:<55} {n:>6,}")

print(f"\nBy category:")
for cat, n in df["keydevcategoryname"].value_counts().items():
    print(f"  {cat:<55} {n:>6,}")

print(f"\nUnique companies: {df['companyid'].nunique():,}")
print(f"Date range: {df['announcedate'].min()} to {df['announcedate'].max()}")

conn.close()
print("\nDone.")
