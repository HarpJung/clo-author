"""Explore what's in the CIQ transaction table."""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd, os

ciq_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data/CIQ_Extract"
trans = pd.read_csv(os.path.join(ciq_dir, "10_observed_company_transactions.csv"))

print("=== ciq.ciqtransaction at our observed companies ===")
print(f"Columns: {list(trans.columns)}")
print(f"Total rows: {len(trans):,}")

# Sample rows
print("\nSAMPLE ROWS:")
for _, r in trans.sample(10, random_state=42).iterrows():
    cid = int(r["companyid"]) if pd.notna(r["companyid"]) else "?"
    ay = int(r["announcedyear"]) if pd.notna(r["announcedyear"]) else "?"
    am = int(r["announcedmonth"]) if pd.notna(r["announcedmonth"]) else "?"
    ad = int(r["announcedday"]) if pd.notna(r["announcedday"]) else "?"
    cy = int(r["closingyear"]) if pd.notna(r["closingyear"]) else "?"
    cm = int(r["closingmonth"]) if pd.notna(r["closingmonth"]) else "?"
    cd = int(r["closingday"]) if pd.notna(r["closingday"]) else "?"
    rn = r["roundnumber"] if pd.notna(r["roundnumber"]) else "?"
    sz = r["transactionsize"] if pd.notna(r["transactionsize"]) else "?"
    tid = int(r["transactionidtypeid"]) if pd.notna(r["transactionidtypeid"]) else "?"
    sid = int(r["statusid"]) if pd.notna(r["statusid"]) else "?"
    comment = str(r.get("comments", ""))[:200]

    print(f"  Company {cid} | Announced {ay}-{am}-{ad} | Closed {cy}-{cm}-{cd}")
    print(f"    Round: {rn} | Size: ${sz}M | TypeID: {tid} | StatusID: {sid}")
    if comment and comment not in ("None", "nan", ""):
        print(f"    Comment: {comment}")
    print()

# Summary
print("SUMMARY:")
print(f"  Unique companies: {trans['companyid'].nunique():,}")
print(f"  With round number: {trans['roundnumber'].notna().sum():,} ({trans['roundnumber'].notna().mean()*100:.1f}%)")
ts = trans["transactionsize"].notna().sum()
print(f"  With transaction size: {ts:,} ({ts/len(trans)*100:.1f}%)")
cm = trans["comments"].notna() & (trans["comments"].astype(str) != "None")
print(f"  With comments: {cm.sum():,}")

print(f"\nROUND NUMBERS:")
for rn, n in trans["roundnumber"].value_counts().head(20).items():
    print(f"  Round {rn}: {n:>5,}")

print(f"\nTRANSACTION TYPE IDs:")
for tid, n in trans["transactionidtypeid"].value_counts().items():
    print(f"  Type {int(tid)}: {n:>5,}")

print(f"\nSTATUS IDs:")
for sid, n in trans["statusid"].value_counts().items():
    print(f"  Status {int(sid)}: {n:>5,}")

# Transaction sizes
sized = trans[trans["transactionsize"].notna()]
if len(sized) > 0:
    print(f"\nTRANSACTION SIZES ($M):")
    print(f"  Mean: ${sized['transactionsize'].mean():.1f}M")
    print(f"  Median: ${sized['transactionsize'].median():.1f}M")
    print(f"  Min: ${sized['transactionsize'].min():.1f}M, Max: ${sized['transactionsize'].max():.1f}M")

# Comments examples
print(f"\nSAMPLE COMMENTS:")
comments = trans[cm]["comments"].sample(min(15, cm.sum()), random_state=42)
for c in comments:
    print(f"  {str(c)[:250]}")
    print()
