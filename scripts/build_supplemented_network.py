"""Build supplemented observer network from CIQ + BoardEx + Form 4.
Then pull CRSP returns for new portfolio companies and save unified network.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv
import psycopg2, time

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 100)
print("BUILD SUPPLEMENTED NETWORK")
print("=" * 100)

# =====================================================================
# STEP 1: Load original CIQ network
# =====================================================================
print("\n--- Step 1: Original CIQ network ---")
orig = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
orig["observer_personid"] = orig["observer_personid"].astype(str).str.replace(".0", "", regex=False)
orig["observed_companyid"] = orig["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
orig["portfolio_cik"] = pd.to_numeric(orig["portfolio_cik"], errors="coerce")
print(f"  Original: {len(orig):,} edges, {orig['observer_personid'].nunique():,} observers, "
      f"{orig['portfolio_cik'].nunique():,} portfolio CIKs")

# Observer -> observed companies mapping
obs_records = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs_records["personid"] = obs_records["personid"].astype(str).str.replace(".0", "", regex=False)
obs_records["companyid"] = obs_records["companyid"].astype(str).str.replace(".0", "", regex=False)
obs_to_companies = obs_records.groupby("personid")["companyid"].apply(set).to_dict()

# Industry mapping
industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))

ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0"), errors="coerce")
cid_to_cik = dict(zip(ciq_xwalk["cid"], ciq_xwalk["cik_int"]))

# =====================================================================
# STEP 2: BoardEx additions
# =====================================================================
print("\n--- Step 2: BoardEx additions ---")

bd_xwalk = pd.read_csv(os.path.join(data_dir, "BoardEx", "observer_boardex_crosswalk.csv"))
bd_xwalk["ciq_personid"] = bd_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
bd_did_to_ciq = dict(zip(bd_xwalk["directorid"], bd_xwalk["ciq_personid"]))

bd_pos = pd.read_csv(os.path.join(data_dir, "BoardEx", "observer_boardex_positions.csv"))
bd_co = pd.read_csv(os.path.join(data_dir, "BoardEx", "observer_boardex_companies.csv"))

# Filter to public company board positions
bd_board = bd_pos[
    (bd_pos["brdposition"] == "Yes") |
    (bd_pos["rolename"].str.contains("Director|Board|Chairman|CEO|CFO|Officer", case=False, na=False))
].copy()
bd_board = bd_board[bd_board["orgtype"].isin(["Quoted", "Listed"])]
bd_board["ciq_personid"] = bd_board["directorid"].map(bd_did_to_ciq)
bd_board = bd_board.dropna(subset=["ciq_personid"])

# Get CIK
bd_co_cik = dict(zip(bd_co["companyid"], bd_co["cikcode"]))
bd_board["cik"] = bd_board["companyid"].map(bd_co_cik)
bd_board["cik"] = pd.to_numeric(bd_board["cik"], errors="coerce")
bd_board = bd_board.dropna(subset=["cik"])

# Build edges from BoardEx
bd_edges = []
for _, row in bd_board.iterrows():
    obs_pid = row["ciq_personid"]
    cik = int(row["cik"])
    observed_companies = obs_to_companies.get(obs_pid, set())
    for obs_cid in observed_companies:
        bd_edges.append({
            "observer_personid": obs_pid,
            "observed_companyid": obs_cid,
            "portfolio_cik": cik,
            "portfolio_companyname": row["companyname"],
            "portfolio_title": row["rolename"],
            "source": "BoardEx",
        })

bd_edges_df = pd.DataFrame(bd_edges)
print(f"  BoardEx edges (before dedup): {len(bd_edges_df):,}")

# =====================================================================
# STEP 3: Form 4 additions
# =====================================================================
print("\n--- Step 3: Form 4 additions ---")

trades = pd.read_csv(os.path.join(data_dir, "Form4", "observer_form4_trades.csv"))
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
tr_xwalk["tr_personid"] = tr_xwalk["tr_personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk["ciq_personid"] = tr_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
tr_to_ciq = dict(zip(tr_xwalk["tr_personid"], tr_xwalk["ciq_personid"]))
trades["ciq_personid"] = trades["personid"].map(tr_to_ciq)
trades = trades.dropna(subset=["ciq_personid"])

# Get CIK from CUSIP
conn = psycopg2.connect(host="wrds-pgdata.wharton.upenn.edu", port=9737, dbname="wrds",
                         user="harperjung", password="Wwjksnm9087yu!")
cur = conn.cursor()
time.sleep(3)
cur.execute("SELECT DISTINCT ncusip, permno FROM crsp.stocknames WHERE ncusip IS NOT NULL AND permno IS NOT NULL")
cusip_to_permno = {r[0]: r[1] for r in cur.fetchall()}

# Also get CIK from CRSP
time.sleep(3)
cur.execute("""SELECT DISTINCT b.cik, a.lpermno as permno
               FROM crsp_a_ccm.ccmxpf_lnkhist a
               JOIN comp.company b ON a.gvkey = b.gvkey
               WHERE a.linktype IN ('LU','LC') AND a.linkprim IN ('P','C')
               AND b.cik IS NOT NULL""")
permno_to_cik = {}
for r in cur.fetchall():
    try:
        permno_to_cik[int(r[1])] = int(r[0])
    except:
        pass
conn.close()

trades["cusip8"] = trades["cusip6"].astype(str).str.strip() + trades["cusip2"].astype(str).str.strip()
trades["permno"] = trades["cusip8"].map(cusip_to_permno)
trades["cik"] = trades["permno"].map(permno_to_cik)

# Unique Form 4 connections with CIK
f4_unique = trades[["ciq_personid", "cname", "cik"]].dropna(subset=["cik"]).drop_duplicates()
f4_unique["cik"] = f4_unique["cik"].astype(int)

f4_edges = []
for _, row in f4_unique.iterrows():
    obs_pid = row["ciq_personid"]
    cik = row["cik"]
    observed_companies = obs_to_companies.get(obs_pid, set())
    for obs_cid in observed_companies:
        f4_edges.append({
            "observer_personid": obs_pid,
            "observed_companyid": obs_cid,
            "portfolio_cik": cik,
            "portfolio_companyname": row["cname"],
            "portfolio_title": "Form 4 Insider",
            "source": "Form4",
        })

f4_edges_df = pd.DataFrame(f4_edges)
print(f"  Form 4 edges (before dedup): {len(f4_edges_df):,}")

# =====================================================================
# STEP 4: Combine and deduplicate
# =====================================================================
print("\n--- Step 4: Combine and deduplicate ---")

# Standardize original edges
orig_std = orig[["observer_personid", "observed_companyid", "portfolio_cik",
                  "portfolio_companyname", "portfolio_title"]].copy()
orig_std["source"] = "CIQ"
orig_std["portfolio_cik"] = orig_std["portfolio_cik"].astype("Int64")

# Combine all
all_edges = pd.concat([orig_std, bd_edges_df, f4_edges_df], ignore_index=True)
all_edges["portfolio_cik"] = pd.to_numeric(all_edges["portfolio_cik"], errors="coerce").astype("Int64")

# Deduplicate on (observer, observed_company, portfolio_cik)
before = len(all_edges)
all_edges = all_edges.drop_duplicates(subset=["observer_personid", "observed_companyid", "portfolio_cik"])
print(f"  Before dedup: {before:,}")
print(f"  After dedup: {len(all_edges):,}")

# Add same_industry flag
all_edges["same_industry"] = (
    all_edges["observed_companyid"].map(cid_to_cik).map(cik_to_sic2) ==
    all_edges["portfolio_cik"].map(cik_to_sic2)
).astype(int)

# =====================================================================
# STEP 5: Summary and comparison
# =====================================================================
print(f"\n\n{'='*100}")
print("NETWORK COMPARISON")
print(f"{'='*100}")

print(f"\n  {'Metric':<40} {'Original':>12} {'Supplemented':>14} {'Change':>12}")
print(f"  {'-'*78}")

o_obs = orig_std["observer_personid"].nunique()
s_obs = all_edges["observer_personid"].nunique()
print(f"  {'Unique observers':<40} {o_obs:>12,} {s_obs:>14,} {'+' + str(s_obs-o_obs):>12}")

o_observed = orig_std["observed_companyid"].nunique()
s_observed = all_edges["observed_companyid"].nunique()
print(f"  {'Unique observed companies':<40} {o_observed:>12,} {s_observed:>14,} {'+' + str(s_observed-o_observed):>12}")

o_port = orig_std["portfolio_cik"].nunique()
s_port = all_edges["portfolio_cik"].nunique()
print(f"  {'Unique portfolio CIKs':<40} {o_port:>12,} {s_port:>14,} {'+' + str(s_port-o_port):>12}")

o_edges = len(orig_std)
s_edges = len(all_edges)
print(f"  {'Total edges':<40} {o_edges:>12,} {s_edges:>14,} {'+' + str(s_edges-o_edges):>12}")

o_same = orig_std.merge(all_edges[["observer_personid", "observed_companyid", "portfolio_cik", "same_industry"]].drop_duplicates(),
                         on=["observer_personid", "observed_companyid", "portfolio_cik"], how="left")["same_industry"].sum()
s_same = all_edges["same_industry"].sum()
print(f"  {'Same-industry edges':<40} {int(o_same):>12,} {int(s_same):>14,} {'+' + str(int(s_same-o_same)):>12}")

print(f"\n  By source:")
for src, n in all_edges["source"].value_counts().items():
    print(f"    {src:<20} {n:>8,} edges")

# =====================================================================
# STEP 6: Save supplemented network
# =====================================================================
print(f"\n--- Step 6: Save ---")

outpath = os.path.join(panel_c_dir, "02b_supplemented_network_edges.csv")
all_edges.to_csv(outpath, index=False)
print(f"  Saved to {outpath}")
print(f"  Total edges: {len(all_edges):,}")

# =====================================================================
# STEP 7: Check which new portfolio CIKs need CRSP returns
# =====================================================================
print(f"\n--- Step 7: New CIKs needing CRSP returns ---")

# Load existing CRSP permnos
pxw = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
existing_ciks = set(pd.to_numeric(pxw["cik"], errors="coerce").dropna().astype(int))

new_ciks = set(all_edges["portfolio_cik"].dropna().astype(int)) - existing_ciks
print(f"  Existing portfolio CIKs with CRSP: {len(existing_ciks):,}")
print(f"  New CIKs needing CRSP: {len(new_ciks):,}")
print(f"  Total CIKs in supplemented network: {len(existing_ciks | new_ciks):,}")

# Save new CIKs for CRSP pull
pd.DataFrame({"cik": sorted(new_ciks)}).to_csv(
    os.path.join(panel_c_dir, "new_ciks_for_crsp.csv"), index=False)


print("\n\nDone.")
