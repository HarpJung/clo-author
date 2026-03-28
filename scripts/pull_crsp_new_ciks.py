"""Pull CRSP daily returns + SIC codes for new portfolio CIKs
from the supplemented network."""
import psycopg2, csv, os, time, sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd

conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu", port=9737, dbname="wrds",
    user="harperjung", password="Wwjksnm9087yu!")
cur = conn.cursor()

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

# Load new CIKs
new_ciks_df = pd.read_csv(os.path.join(panel_c_dir, "new_ciks_for_crsp.csv"))
new_ciks = sorted(new_ciks_df["cik"].dropna().astype(int).tolist())
print(f"New CIKs to pull: {len(new_ciks):,}")

# =====================================================================
# STEP 1: Get PERMNO for new CIKs via CCM link
# =====================================================================
print("\n--- Step 1: CIK -> PERMNO crosswalk ---")
time.sleep(3)

cik_str = ", ".join(str(c) for c in new_ciks)
cur.execute(f"""
    SELECT DISTINCT b.cik, a.gvkey, a.lpermno as permno,
           a.linkdt, a.linkenddt, a.linktype, a.linkprim
    FROM crsp_a_ccm.ccmxpf_lnkhist a
    JOIN comp.company b ON a.gvkey = b.gvkey
    WHERE CAST(b.cik AS BIGINT) IN ({cik_str})
    AND a.linktype IN ('LU', 'LC')
    AND a.linkprim IN ('P', 'C')
    ORDER BY b.cik, a.gvkey
""")
xwalk_rows = cur.fetchall()
xwalk_df = pd.DataFrame(xwalk_rows, columns=["cik", "gvkey", "permno", "linkdt", "linkenddt", "linktype", "linkprim"])
print(f"  Crosswalk rows: {len(xwalk_df):,}")
print(f"  Unique CIKs with PERMNO: {xwalk_df['cik'].nunique():,} (of {len(new_ciks):,})")
print(f"  Unique PERMNOs: {xwalk_df['permno'].nunique():,}")

# Save crosswalk
new_xwalk_path = os.path.join(panel_c_dir, "03b_new_portfolio_permno_crosswalk.csv")
xwalk_df.to_csv(new_xwalk_path, index=False)

# =====================================================================
# STEP 2: Get SIC codes for new CIKs
# =====================================================================
print("\n--- Step 2: SIC codes ---")
time.sleep(3)

new_permnos = sorted(set(int(p) for p in xwalk_df["permno"].dropna()))
permno_str = ", ".join(str(p) for p in new_permnos)

cur.execute(f"""
    SELECT DISTINCT permno, siccd
    FROM crsp.stocknames
    WHERE permno IN ({permno_str})
    AND siccd IS NOT NULL AND siccd > 0
""")
sic_rows = cur.fetchall()
sic_df = pd.DataFrame(sic_rows, columns=["permno", "sic"])
sic_df = sic_df.drop_duplicates("permno", keep="first")
print(f"  PERMNOs with SIC: {len(sic_df):,}")

# Also get CIK for these
cik_sic = xwalk_df[["cik", "permno"]].drop_duplicates("permno").merge(sic_df, on="permno", how="inner")
new_sic_path = os.path.join(panel_c_dir, "05b_new_industry_codes.csv")
cik_sic.to_csv(new_sic_path, index=False)
print(f"  Saved to {new_sic_path}")

# =====================================================================
# STEP 3: Pull daily returns from CRSP dsf_v2 (2015-2025)
# =====================================================================
print(f"\n--- Step 3: Pull CRSP daily returns for {len(new_permnos):,} new PERMNOs ---")

batch_size = 300
all_returns = []

for batch_start in range(0, len(new_permnos), batch_size):
    batch = new_permnos[batch_start:batch_start + batch_size]
    p_str = ", ".join(str(p) for p in batch)
    time.sleep(3)

    try:
        # Use dsf for 2015-2024
        cur.execute(f"""
            SELECT permno, date, ret, prc, vol
            FROM crsp.dsf
            WHERE permno IN ({p_str})
            AND date >= '2015-01-01'
            ORDER BY permno, date
        """)
        rows = cur.fetchall()
        all_returns.extend(rows)
        n_batch = batch_start // batch_size + 1
        n_total = (len(new_permnos) + batch_size - 1) // batch_size
        print(f"  Batch {n_batch}/{n_total} (dsf): {len(rows):,} rows (total: {len(all_returns):,})")
    except Exception as e:
        print(f"  Batch error: {str(e)[:80]}")
        conn.rollback()

# Also pull 2025 from dsf_v2
print(f"\n  Pulling 2025 from dsf_v2...")
for batch_start in range(0, len(new_permnos), batch_size):
    batch = new_permnos[batch_start:batch_start + batch_size]
    p_str = ", ".join(str(p) for p in batch)
    time.sleep(3)

    try:
        cur.execute(f"""
            SELECT permno, dlycaldt as date, dlyret as ret, dlyprc as prc, dlyvol as vol
            FROM crsp.dsf_v2
            WHERE permno IN ({p_str})
            AND dlycaldt >= '2025-01-01'
            ORDER BY permno, dlycaldt
        """)
        rows = cur.fetchall()
        all_returns.extend(rows)
        n_batch = batch_start // batch_size + 1
        n_total = (len(new_permnos) + batch_size - 1) // batch_size
        print(f"  Batch {n_batch}/{n_total} (dsf_v2 2025): {len(rows):,} rows (total: {len(all_returns):,})")
    except Exception as e:
        print(f"  Batch error: {str(e)[:80]}")
        conn.rollback()

conn.close()

# Save
ret_df = pd.DataFrame(all_returns, columns=["permno", "date", "ret", "prc", "vol"])
ret_df = ret_df.drop_duplicates(subset=["permno", "date"])
new_ret_path = os.path.join(panel_c_dir, "06c_new_portfolio_crsp_daily.csv")
ret_df.to_csv(new_ret_path, index=False)
print(f"\n  Total new returns: {len(ret_df):,}")
print(f"  Unique PERMNOs: {ret_df['permno'].nunique():,}")
print(f"  Date range: {ret_df['date'].min()} to {ret_df['date'].max()}")
print(f"  Saved to {new_ret_path}")

# Append to main returns file
main_ret_path = os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv")
with open(main_ret_path, "a", newline="") as f:
    writer = csv.writer(f)
    for _, row in ret_df.iterrows():
        writer.writerow([row["permno"], row["date"], row["ret"], row["prc"], row["vol"]])
print(f"  Appended {len(ret_df):,} rows to {main_ret_path}")

# Also append crosswalk and SIC to main files
main_xwalk_path = os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv")
with open(main_xwalk_path, "a", newline="") as f:
    writer = csv.writer(f)
    for _, row in xwalk_df.iterrows():
        writer.writerow([row["cik"], row["gvkey"], row["permno"]])
print(f"  Appended {len(xwalk_df):,} rows to {main_xwalk_path}")

main_sic_path = os.path.join(panel_c_dir, "05_industry_codes.csv")
with open(main_sic_path, "a", newline="") as f:
    writer = csv.writer(f)
    for _, row in cik_sic.iterrows():
        writer.writerow([row["cik"], row["sic"]])
print(f"  Appended {len(cik_sic):,} rows to {main_sic_path}")

print("\n\nDone.")
