"""Pull Form 4 insider trades for our board observers.
Uses WRDS pre-built CIQ-to-TR crosswalk to match observer personids
to Thomson Reuters insider filing data.
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

# =====================================================================
# STEP 1: Get our observer personids from CIQ
# =====================================================================
print("--- Step 1: Load CIQ observer personids ---")
obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
ciq_personids = sorted(set(obs["personid"]))
print(f"  CIQ observer personids: {len(ciq_personids):,}")

# =====================================================================
# STEP 2: Use WRDS crosswalk to get TR insider personids
# =====================================================================
print("\n--- Step 2: CIQ -> TR crosswalk ---")
time.sleep(3)

# First check the crosswalk size and structure
cur.execute("SELECT COUNT(*) FROM wrdsapps_plink_trinsider_ciq.trinsider_ciq_link")
print(f"  Total crosswalk entries: {cur.fetchone()[0]:,}")

# Pull crosswalk for our observers
pid_str = ", ".join(f"'{p}'" for p in ciq_personids[:2000])
time.sleep(3)
cur.execute(f"""
    SELECT tr_personid, owner, ciq_personid, firstname, middlename, lastname, score, matchstyle
    FROM wrdsapps_plink_trinsider_ciq.trinsider_ciq_link
    WHERE ciq_personid::text IN ({pid_str})
""")
xwalk_rows = cur.fetchall()
xwalk_cols = ["tr_personid", "owner", "ciq_personid", "firstname", "middlename", "lastname", "score", "matchstyle"]
print(f"  Matched observers: {len(xwalk_rows):,}")

if len(ciq_personids) > 2000:
    pid_str2 = ", ".join(f"'{p}'" for p in ciq_personids[2000:])
    time.sleep(3)
    cur.execute(f"""
        SELECT tr_personid, owner, ciq_personid, firstname, middlename, lastname, score, matchstyle
        FROM wrdsapps_plink_trinsider_ciq.trinsider_ciq_link
        WHERE ciq_personid::text IN ({pid_str2})
    """)
    xwalk_rows.extend(cur.fetchall())
    print(f"  After second batch: {len(xwalk_rows):,}")

xwalk_df = pd.DataFrame(xwalk_rows, columns=xwalk_cols)
print(f"  Unique CIQ personids matched: {xwalk_df['ciq_personid'].nunique():,}")
print(f"  Unique TR personids: {xwalk_df['tr_personid'].nunique():,}")
print(f"  Match score distribution:")
for s, n in xwalk_df["score"].value_counts().sort_index().items():
    print(f"    score={s}: {n:,}")
print(f"  Match style:")
for s, n in xwalk_df["matchstyle"].value_counts().items():
    print(f"    {s}: {n:,}")

# Save crosswalk
xwalk_path = os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv")
xwalk_df.to_csv(xwalk_path, index=False)
print(f"  Saved crosswalk to {xwalk_path}")

# =====================================================================
# STEP 3: Pull Form 4 transactions for matched observers
# =====================================================================
print("\n--- Step 3: Pull Form 4 transactions ---")

tr_personids = sorted(set(str(int(p)) for p in xwalk_df["tr_personid"].dropna()))
print(f"  TR personids to query: {len(tr_personids):,}")

# Pull in batches
batch_size = 500
all_trades = []

for batch_start in range(0, len(tr_personids), batch_size):
    batch = tr_personids[batch_start:batch_start + batch_size]
    pid_batch = ", ".join(batch)
    time.sleep(3)

    try:
        cur.execute(f"""
            SELECT personid, owner, secid, ticker, cusip6, cusip2, cname,
                   rolecode1, rolecode2, formtype, trancode, acqdisp,
                   trandate, tprice, shares, sharesheld, ownership,
                   cleanse, shares_adj, tprice_adj, sectitle,
                   fdate, sigdate
            FROM tfn.table1
            WHERE personid IN ({pid_batch})
            AND formtype = '4'
            AND trandate >= '2015-01-01'
            ORDER BY personid, trandate
        """)
        rows = cur.fetchall()
        all_trades.extend(rows)
        n_batch = batch_start // batch_size + 1
        n_total = (len(tr_personids) + batch_size - 1) // batch_size
        print(f"  Batch {n_batch}/{n_total}: {len(rows):,} trades (total: {len(all_trades):,})")
    except Exception as e:
        print(f"  Batch error: {str(e)[:80]}")
        conn.rollback()

trade_cols = ["personid", "owner", "secid", "ticker", "cusip6", "cusip2", "cname",
              "rolecode1", "rolecode2", "formtype", "trancode", "acqdisp",
              "trandate", "tprice", "shares", "sharesheld", "ownership",
              "cleanse", "shares_adj", "tprice_adj", "sectitle",
              "fdate", "sigdate"]

print(f"\nTotal Form 4 trades: {len(all_trades):,}")

# Also pull table2 (derivative transactions)
print("\n--- Step 3b: Pull derivative transactions (table2) ---")
all_deriv = []

for batch_start in range(0, len(tr_personids), batch_size):
    batch = tr_personids[batch_start:batch_start + batch_size]
    pid_batch = ", ".join(batch)
    time.sleep(3)

    try:
        cur.execute(f"""
            SELECT personid, owner, secid, ticker, cusip6, cusip2, cname,
                   rolecode1, rolecode2, formtype, trancode, acqdisp,
                   trandate, derivative, xdate, shares, xprice, sprice,
                   derivheld, ownership, cleanse, shares_adj,
                   fdate, sigdate
            FROM tfn.table2
            WHERE personid IN ({pid_batch})
            AND formtype = '4'
            AND trandate >= '2015-01-01'
            ORDER BY personid, trandate
        """)
        rows = cur.fetchall()
        all_deriv.extend(rows)
        n_batch = batch_start // batch_size + 1
        n_total = (len(tr_personids) + batch_size - 1) // batch_size
        print(f"  Batch {n_batch}/{n_total}: {len(rows):,} deriv trades (total: {len(all_deriv):,})")
    except Exception as e:
        print(f"  Batch error: {str(e)[:80]}")
        conn.rollback()

deriv_cols = ["personid", "owner", "secid", "ticker", "cusip6", "cusip2", "cname",
              "rolecode1", "rolecode2", "formtype", "trancode", "acqdisp",
              "trandate", "derivative", "xdate", "shares", "xprice", "sprice",
              "derivheld", "ownership", "cleanse", "shares_adj",
              "fdate", "sigdate"]

print(f"Total derivative trades: {len(all_deriv):,}")

# =====================================================================
# STEP 4: Also check the full crosswalk table for company-level info
# =====================================================================
print("\n--- Step 4: Pull full crosswalk with company info ---")
time.sleep(3)

pid_str_all = ", ".join(f"'{p}'" for p in ciq_personids[:2000])
cur.execute(f"""
    SELECT tr_personid, owner, cname, secid, tr_ticker, tr_cusip,
           tr_lname, tr_fname, tr_mname, tr_minit,
           ciq_personid, companyid, companyname, firstname, lastname
    FROM wrdsapps_plink_trinsider_ciq.trinsider_ciq
    WHERE ciq_personid::text IN ({pid_str_all})
""")
full_xwalk = cur.fetchall()

if len(ciq_personids) > 2000:
    pid_str_all2 = ", ".join(f"'{p}'" for p in ciq_personids[2000:])
    time.sleep(3)
    cur.execute(f"""
        SELECT tr_personid, owner, cname, secid, tr_ticker, tr_cusip,
               tr_lname, tr_fname, tr_mname, tr_minit,
               ciq_personid, companyid, companyname, firstname, lastname
        FROM wrdsapps_plink_trinsider_ciq.trinsider_ciq
        WHERE ciq_personid::text IN ({pid_str_all2})
    """)
    full_xwalk.extend(cur.fetchall())

full_xwalk_cols = ["tr_personid", "owner", "cname", "secid", "tr_ticker", "tr_cusip",
                    "tr_lname", "tr_fname", "tr_mname", "tr_minit",
                    "ciq_personid", "companyid", "companyname", "firstname", "lastname"]
full_xwalk_df = pd.DataFrame(full_xwalk, columns=full_xwalk_cols)
print(f"  Full crosswalk: {len(full_xwalk_df):,} rows")

# =====================================================================
# STEP 5: Save everything
# =====================================================================
print("\n--- Step 5: Save ---")

# Trades
trades_df = pd.DataFrame(all_trades, columns=trade_cols)
trades_path = os.path.join(data_dir, "Form4", "observer_form4_trades.csv")
os.makedirs(os.path.join(data_dir, "Form4"), exist_ok=True)
trades_df.to_csv(trades_path, index=False)
print(f"  Trades: {len(trades_df):,} -> {trades_path}")

# Derivatives
deriv_df = pd.DataFrame(all_deriv, columns=deriv_cols)
deriv_path = os.path.join(data_dir, "Form4", "observer_form4_derivatives.csv")
deriv_df.to_csv(deriv_path, index=False)
print(f"  Derivatives: {len(deriv_df):,} -> {deriv_path}")

# Full crosswalk
full_xwalk_path = os.path.join(data_dir, "Form4", "observer_tr_ciq_full_crosswalk.csv")
full_xwalk_df.to_csv(full_xwalk_path, index=False)
print(f"  Full crosswalk: {len(full_xwalk_df):,} -> {full_xwalk_path}")

# =====================================================================
# STEP 6: Summary stats
# =====================================================================
print("\n--- Summary ---")
if len(trades_df) > 0:
    print(f"  Trades date range: {trades_df['trandate'].min()} to {trades_df['trandate'].max()}")
    print(f"  Unique persons: {trades_df['personid'].nunique():,}")
    print(f"  Unique companies: {trades_df['cname'].nunique():,}")
    print(f"\n  Transaction codes:")
    for tc, n in trades_df["trancode"].value_counts().head(10).items():
        print(f"    {tc}: {n:,}")
    print(f"\n  Role codes:")
    for rc, n in trades_df["rolecode1"].value_counts().head(10).items():
        print(f"    {rc}: {n:,}")
    print(f"\n  Acq/Disp:")
    for ad, n in trades_df["acqdisp"].value_counts().items():
        print(f"    {ad}: {n:,}")

    # Purchases vs Sales
    buys = trades_df[trades_df["trancode"] == "P"]
    sells = trades_df[trades_df["trancode"] == "S"]
    print(f"\n  Purchases: {len(buys):,} (total shares: {buys['shares'].sum():,.0f})")
    print(f"  Sales: {len(sells):,} (total shares: {sells['shares'].sum():,.0f})")

conn.close()
print("\nDone.")
