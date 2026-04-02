"""
Pull ALL available CIQ data for our observer network:
1. ciqcompanyrel - VC investment links (confirmed VC -> portfolio company)
2. ciqtransaction - deal-level data with dates and round numbers
3. ciqprotoprofunction - position start/end dates (solves time-matching!)
4. ciqpersonbiography - person bios for validation

This gives us the ground truth for:
- Which VC invested in which company (ciqcompanyrel)
- When the deal happened (ciqtransaction)
- When the observer started/ended each position (ciqprotoprofunction)
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import psycopg2
import pandas as pd
import os, time, csv

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")

conn = psycopg2.connect(host="wrds-pgdata.wharton.upenn.edu", port=9737,
                         dbname="wrds", user="harperjung", password="Wwjksnm9087yu!")
cur = conn.cursor()

# Load our observer and VC company IDs
obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
obs["companyid"] = obs["companyid"].astype(str).str.replace(".0", "", regex=False)

co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_cids = set(co[co["country"] == "United States"]["companyid"].astype(str).str.replace(".0", "", regex=False))

pos = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
pos["personid"] = pos["personid"].astype(str).str.replace(".0", "", regex=False)
pos["companyid"] = pos["companyid"].astype(str).str.replace(".0", "", regex=False)

vc_types = {"Private Investment Firm", "Public Investment Firm", "Private Fund"}
vc_positions = pos[pos["companytypename"].isin(vc_types)]
vc_companyids = sorted(set(vc_positions["companyid"]))
observer_personids = sorted(set(obs["personid"]))
observed_companyids = sorted(set(obs[obs["companyid"].isin(us_cids)]["companyid"]))

print("=" * 80)
print("PULL ALL CIQ RELATIONSHIP DATA")
print("=" * 80)
print(f"  VC company IDs: {len(vc_companyids):,}")
print(f"  Observer person IDs: {len(observer_personids):,}")
print(f"  Observed company IDs (US): {len(observed_companyids):,}")

batch_size = 500

def save_csv(rows, columns, filepath):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    print(f"  Saved: {len(rows):,} rows -> {os.path.basename(filepath)}")

# =====================================================================
# 1. COMPANY RELATIONSHIPS (VC -> portfolio company investments)
# =====================================================================
print("\n--- 1. Company Relationships (ciq.ciqcompanyrel) ---")
print("  Pulling ALL relationships where our VC firms are involved...")

rel_rows = []
for i in range(0, len(vc_companyids), batch_size):
    batch = vc_companyids[i:i+batch_size]
    id_str = ", ".join(batch)
    time.sleep(3)
    try:
        # VC as investor (companyid = VC, companyid2 = portfolio company)
        cur.execute(f"""
            SELECT r.companyrelid, r.companyid, r.companyid2,
                   r.companyreltypeid, r.percentownership, r.totalinvestment,
                   t.companyreltypename, t.currentflag, t.priorflag,
                   c2.companyname as portfolio_name,
                   ct2.companytypename as portfolio_type
            FROM ciq.ciqcompanyrel r
            JOIN ciq.ciqcompanyreltype t ON r.companyreltypeid = t.companyreltypeid
            LEFT JOIN ciq.ciqcompany c2 ON r.companyid2 = c2.companyid
            LEFT JOIN ciq.ciqcompanytype ct2 ON c2.companytypeid = ct2.companytypeid
            WHERE r.companyid IN ({id_str})
            AND r.companyreltypeid IN (1, 2, 25, 26)
        """)
        # 1=Current Investment, 2=Prior Investment, 25=Pending, 26=Cancelled
        rel_rows.extend(cur.fetchall())
    except Exception as e:
        print(f"  Batch error: {str(e)[:60]}")
        conn.rollback()
    if (i // batch_size + 1) % 5 == 0:
        print(f"  Batch {i//batch_size+1}: {len(rel_rows):,} total")

cols = ["companyrelid", "vc_companyid", "portfolio_companyid",
        "companyreltypeid", "percentownership", "totalinvestment",
        "companyreltypename", "currentflag", "priorflag",
        "portfolio_name", "portfolio_type"]
save_csv(rel_rows, cols, os.path.join(ciq_dir, "09_vc_portfolio_investments.csv"))

# Summary
rel_df = pd.DataFrame(rel_rows, columns=cols)
print(f"\n  Total VC investment relationships: {len(rel_df):,}")
print(f"  Unique VCs: {rel_df['vc_companyid'].nunique():,}")
print(f"  Unique portfolio companies: {rel_df['portfolio_companyid'].nunique():,}")
print(f"  By type:")
for t, n in rel_df["companyreltypename"].value_counts().items():
    print(f"    {t:<35} {n:>8,}")
print(f"  Portfolio types:")
for t, n in rel_df["portfolio_type"].value_counts().head(10).items():
    print(f"    {str(t):<35} {n:>8,}")

# How many are at our OBSERVED companies?
obs_set = set(str(int(c)) if pd.notna(c) else "" for c in rel_df["portfolio_companyid"])
overlap = obs_set & set(observed_companyids)
print(f"\n  Portfolio companies that are also our observed companies: {len(overlap):,}")

# =====================================================================
# 2. TRANSACTIONS (deal-level with dates)
# =====================================================================
print("\n\n--- 2. Transactions (ciq.ciqtransaction) ---")
print("  Pulling transactions involving our observed companies...")

# Transactions for observed companies
trans_rows = []
for i in range(0, len(observed_companyids), batch_size):
    batch = observed_companyids[i:i+batch_size]
    id_str = ", ".join(batch)
    time.sleep(3)
    try:
        cur.execute(f"""
            SELECT transactionid, transactionidtypeid, companyid,
                   announcedday, announcedmonth, announcedyear,
                   transactionsize,
                   closingday, closingmonth, closingyear,
                   roundnumber, statusid, comments, currencyid
            FROM ciq.ciqtransaction
            WHERE companyid IN ({id_str})
        """)
        trans_rows.extend(cur.fetchall())
    except Exception as e:
        print(f"  Batch error: {str(e)[:60]}")
        conn.rollback()
    if (i // batch_size + 1) % 3 == 0:
        print(f"  Batch {i//batch_size+1}: {len(trans_rows):,} total")

cols = ["transactionid", "transactionidtypeid", "companyid",
        "announcedday", "announcedmonth", "announcedyear",
        "transactionsize",
        "closingday", "closingmonth", "closingyear",
        "roundnumber", "statusid", "comments", "currencyid"]
save_csv(trans_rows, cols, os.path.join(ciq_dir, "10_observed_company_transactions.csv"))

if trans_rows:
    trans_df = pd.DataFrame(trans_rows, columns=cols)
    print(f"\n  Transactions at observed companies: {len(trans_df):,}")
    print(f"  Unique companies: {trans_df['companyid'].nunique():,}")
    print(f"  With round number: {trans_df['roundnumber'].notna().sum():,}")
    print(f"  Year range: {trans_df['announcedyear'].min()} to {trans_df['announcedyear'].max()}")
    print(f"  Round numbers:")
    for rn, n in trans_df["roundnumber"].value_counts().head(15).items():
        print(f"    {str(rn):<25} {n:>5,}")

# =====================================================================
# 3. PROFESSIONAL FUNCTIONS WITH DATES (position start/end dates!)
# =====================================================================
print("\n\n--- 3. Professional Functions with Dates (ciq.ciqprotoprofunction) ---")
print("  Pulling position dates for our observers...")

# First get proid -> personid mapping from ciqprofessional
# We need the proid to query protoprofunction
print("  Getting proid mapping...")
proid_rows = []
for i in range(0, len(observer_personids), batch_size):
    batch = observer_personids[i:i+batch_size]
    pid_str = ", ".join(batch)
    time.sleep(3)
    try:
        cur.execute(f"""
            SELECT proid, personid, companyid, title, boardflag,
                   currentproflag, currentboardflag
            FROM ciq.ciqprofessional
            WHERE personid IN ({pid_str})
        """)
        proid_rows.extend(cur.fetchall())
    except Exception as e:
        print(f"  Batch error: {str(e)[:60]}")
        conn.rollback()

proid_df = pd.DataFrame(proid_rows, columns=["proid", "personid", "companyid", "title",
                                               "boardflag", "currentproflag", "currentboardflag"])
proids = sorted(set(str(int(r[0])) for r in proid_rows if r[0]))
print(f"  PRO IDs for our observers: {len(proids):,}")

# Now pull protoprofunction for these proids (has start/end dates!)
print("  Pulling position dates...")
func_rows = []
for i in range(0, len(proids), batch_size):
    batch = proids[i:i+batch_size]
    id_str = ", ".join(batch)
    time.sleep(3)
    try:
        cur.execute(f"""
            SELECT ppf.protoprofunctionid, ppf.proid,
                   ppf.profunctionid, ppf.currentflag,
                   ppf.startday, ppf.startmonth, ppf.startyear,
                   ppf.endday, ppf.endmonth, ppf.endyear,
                   ppf.profunctionspecialty,
                   pf.profunctionname, pf.boardflag as func_boardflag
            FROM ciq.ciqprotoprofunction ppf
            LEFT JOIN ciq.ciqprofunction pf ON ppf.profunctionid = pf.profunctionid
            WHERE ppf.proid IN ({id_str})
        """)
        func_rows.extend(cur.fetchall())
    except Exception as e:
        print(f"  Batch error: {str(e)[:60]}")
        conn.rollback()
    n_batch = i // batch_size + 1
    n_total = (len(proids) + batch_size - 1) // batch_size
    if n_batch % 10 == 0:
        print(f"  Batch {n_batch}/{n_total}: {len(func_rows):,} total")

cols = ["protoprofunctionid", "proid", "profunctionid", "currentflag",
        "startday", "startmonth", "startyear",
        "endday", "endmonth", "endyear",
        "profunctionspecialty", "profunctionname", "func_boardflag"]
save_csv(func_rows, cols, os.path.join(ciq_dir, "11_observer_position_dates.csv"))

if func_rows:
    func_df = pd.DataFrame(func_rows, columns=cols)
    print(f"\n  Position function records: {len(func_df):,}")
    print(f"  With start year: {func_df['startyear'].notna().sum():,} ({func_df['startyear'].notna().mean()*100:.1f}%)")
    print(f"  With end year: {func_df['endyear'].notna().sum():,} ({func_df['endyear'].notna().mean()*100:.1f}%)")
    print(f"  Current positions: {(func_df['currentflag']==1).sum():,}")
    print(f"  Function types:")
    for fn, n in func_df["profunctionname"].value_counts().head(20).items():
        print(f"    {str(fn):<40} {n:>6,}")

    # Save the proid mapping too
    save_csv(proid_rows, ["proid", "personid", "companyid", "title", "boardflag",
                           "currentproflag", "currentboardflag"],
             os.path.join(ciq_dir, "11b_observer_proid_mapping.csv"))

# =====================================================================
# 4. PERSON BIOGRAPHIES (for validation)
# =====================================================================
print("\n\n--- 4. Person Biographies ---")
bio_rows = []
for i in range(0, len(observer_personids), batch_size):
    batch = observer_personids[i:i+batch_size]
    pid_str = ", ".join(batch)
    time.sleep(3)
    try:
        cur.execute(f"""
            SELECT personid, personbiography
            FROM ciq.ciqpersonbiography
            WHERE personid IN ({pid_str})
        """)
        bio_rows.extend(cur.fetchall())
    except Exception as e:
        print(f"  Batch error: {str(e)[:60]}")
        conn.rollback()

save_csv(bio_rows, ["personid", "personbiography"],
         os.path.join(ciq_dir, "12_observer_biographies.csv"))
print(f"  Biographies: {len(bio_rows):,} observers")

conn.close()

# =====================================================================
# SUMMARY
# =====================================================================
print(f"\n\n{'=' * 80}")
print("ALL PULLS COMPLETE")
print(f"{'=' * 80}")
for fname in sorted(os.listdir(ciq_dir)):
    if fname.startswith(("09", "10", "11", "12")) and fname.endswith(".csv"):
        fp = os.path.join(ciq_dir, fname)
        size = os.path.getsize(fp) / (1024 * 1024)
        n = sum(1 for _ in open(fp, "r", encoding="utf-8")) - 1
        print(f"  {fname:<50} {n:>8,} rows  ({size:.1f} MB)")

print("\nDone.")
