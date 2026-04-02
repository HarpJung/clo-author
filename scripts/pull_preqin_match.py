"""
Match our CIQ VC firms to Preqin, pull fund performance time series,
and test whether fund performance responds to events at observed companies.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import psycopg2
import pandas as pd
import numpy as np
import os

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")

conn = psycopg2.connect(host="wrds-pgdata.wharton.upenn.edu", port=9737,
                         dbname="wrds", user="harperjung", password="Wwjksnm9087yu!")
cur = conn.cursor()

print("=" * 80)
print("PREQIN MATCHING AND FUND PERFORMANCE PULL")
print("=" * 80)

# =====================================================================
# STEP 1: Load our VC firm names
# =====================================================================
print("\n--- Step 1: Load our VC firms ---")
tb = pd.read_csv(os.path.join(data_dir, "table_b_observer_network.csv"))
tb["vc_firm_companyid"] = tb["vc_firm_companyid"].astype(str).str.replace(".0", "", regex=False)

# Get unique VC firms with their CIQ company IDs
our_vcs = tb[["vc_firm_companyid", "vc_firm_name"]].drop_duplicates()
our_vcs = our_vcs.dropna(subset=["vc_firm_name"])
print(f"  Our VC firms: {len(our_vcs):,}")

# For each VC, count how many observed companies they have
vc_obs_count = tb.groupby("vc_firm_companyid")["observed_companyid"].nunique().reset_index()
vc_obs_count.columns = ["vc_firm_companyid", "n_observed_companies"]
our_vcs = our_vcs.merge(vc_obs_count, on="vc_firm_companyid", how="left")

# =====================================================================
# STEP 2: Pull ALL Preqin manager details
# =====================================================================
print("\n--- Step 2: Pull Preqin manager details ---")
cur.execute("""
    SELECT firm_id, firmname, firmtype, status, mainfirmstrategy,
           firmcity, firmstate, firmcountry, established,
           industryfocus, geofocus
    FROM preqin_gp.preqinmanagerdetails
""")
cols = [d[0] for d in cur.description]
preqin_mgrs = pd.DataFrame(cur.fetchall(), columns=cols)
print(f"  Preqin managers: {len(preqin_mgrs):,}")

# =====================================================================
# STEP 3: Name-match CIQ VCs to Preqin managers
# =====================================================================
print("\n--- Step 3: Name matching ---")

def clean_name(name):
    """Normalize firm name for matching."""
    if not isinstance(name, str):
        return ""
    name = name.lower().strip()
    # Remove common suffixes
    for suffix in [", llc", " llc", ", l.p.", " l.p.", ", lp", " lp",
                   ", inc.", " inc.", ", inc", " inc",
                   " management", " partners", " advisors", " capital",
                   " ventures", " fund", " group"]:
        name = name.replace(suffix, "")
    # Remove punctuation
    name = name.replace(",", "").replace(".", "").replace("'", "").strip()
    return name

# Build Preqin lookup
preqin_mgrs["name_clean"] = preqin_mgrs["firmname"].apply(clean_name)
preqin_lookup = {}
for _, r in preqin_mgrs.iterrows():
    nc = r["name_clean"]
    if nc and len(nc) >= 3:
        if nc not in preqin_lookup:
            preqin_lookup[nc] = []
        preqin_lookup[nc].append(r)

# Match our VCs
matches = []
for _, vc in our_vcs.iterrows():
    vc_clean = clean_name(vc["vc_firm_name"])
    if not vc_clean or len(vc_clean) < 3:
        continue

    # Try exact match first
    if vc_clean in preqin_lookup:
        for p in preqin_lookup[vc_clean]:
            matches.append({
                "ciq_vc_companyid": vc["vc_firm_companyid"],
                "ciq_vc_name": vc["vc_firm_name"],
                "n_observed_companies": vc.get("n_observed_companies", 0),
                "preqin_firm_id": p["firm_id"],
                "preqin_firm_name": p["firmname"],
                "preqin_industry": p.get("industryfocus", ""),
                "match_type": "exact",
            })
        continue

    # Try substring match (CIQ name contains Preqin name or vice versa)
    for nc, ps in preqin_lookup.items():
        if len(nc) >= 5 and (nc in vc_clean or vc_clean in nc):
            for p in ps:
                matches.append({
                    "ciq_vc_companyid": vc["vc_firm_companyid"],
                    "ciq_vc_name": vc["vc_firm_name"],
                    "n_observed_companies": vc.get("n_observed_companies", 0),
                    "preqin_firm_id": p["firm_id"],
                    "preqin_firm_name": p["firmname"],
                    "preqin_industry": p.get("industryfocus", ""),
                    "match_type": "substring",
                })
            break

match_df = pd.DataFrame(matches).drop_duplicates(subset=["ciq_vc_companyid", "preqin_firm_id"])
matched_vcs = match_df["ciq_vc_companyid"].nunique()
print(f"  Matched: {matched_vcs:,} CIQ VCs to {match_df['preqin_firm_id'].nunique():,} Preqin firms")
print(f"  Exact: {(match_df['match_type']=='exact').sum():,}")
print(f"  Substring: {(match_df['match_type']=='substring').sum():,}")

# Top matches by observed company count
print(f"\n  Top matched VCs by observer network size:")
top = match_df.sort_values("n_observed_companies", ascending=False).drop_duplicates("ciq_vc_companyid").head(20)
for _, r in top.iterrows():
    print(f"    {r['ciq_vc_name'][:40]:<40} -> {r['preqin_firm_name'][:35]:<35} ({int(r['n_observed_companies'])} obs cos)")

# Save crosswalk
os.makedirs(os.path.join(data_dir, "Preqin"), exist_ok=True)
match_df.to_csv(os.path.join(data_dir, "Preqin/vc_preqin_crosswalk.csv"), index=False)

# =====================================================================
# STEP 4: Pull fund details for matched VCs
# =====================================================================
print("\n\n--- Step 4: Pull fund details ---")

matched_firm_ids = sorted(match_df["preqin_firm_id"].dropna().astype(int).unique())
fid_str = ", ".join(str(f) for f in matched_firm_ids)

cur.execute(f"""
    SELECT fund_id, firm_id, fund_name, firm_name, vintage, fund_type,
           fund_status, final_size_usd, industry, geographic_scope, fund_focus
    FROM preqin_gp.preqinfunddetails
    WHERE firm_id IN ({fid_str})
    ORDER BY firm_id, vintage
""")
cols = [d[0] for d in cur.description]
funds = pd.DataFrame(cur.fetchall(), columns=cols)
print(f"  Funds at matched firms: {len(funds):,}")
print(f"  Unique firms: {funds['firm_id'].nunique():,}")

# Fund type distribution
print(f"\n  By fund type:")
for ft, n in funds["fund_type"].value_counts().head(10).items():
    print(f"    {ft:<40} {n:>5,}")

# VC funds specifically
vc_funds = funds[funds["fund_type"].str.contains("Venture|Seed|Early", case=False, na=False)]
print(f"\n  VC/Seed/Early Stage funds: {len(vc_funds):,}")
print(f"  Unique firms with VC funds: {vc_funds['firm_id'].nunique():,}")

funds.to_csv(os.path.join(data_dir, "Preqin/matched_fund_details.csv"), index=False)

# =====================================================================
# STEP 5: Pull fund performance time series
# =====================================================================
print("\n\n--- Step 5: Pull fund performance ---")

fund_ids = sorted(funds["fund_id"].dropna().astype(int).unique())
fid_str = ", ".join(str(f) for f in fund_ids)

cur.execute(f"""
    SELECT fund_id, date_reported, vintage, called_pcent,
           distr_dpi_pcent, value_rvpi_pcent, multiple, net_irr_pcent
    FROM preqin_gp.preqinfundperformance
    WHERE fund_id IN ({fid_str})
    ORDER BY fund_id, date_reported
""")
cols = [d[0] for d in cur.description]
perf = pd.DataFrame(cur.fetchall(), columns=cols)
print(f"  Performance records: {len(perf):,}")
print(f"  Unique funds with performance: {perf['fund_id'].nunique():,}")
print(f"  Date range: {perf['date_reported'].min()} to {perf['date_reported'].max()}")

# How many have actual IRR data?
perf["irr_numeric"] = pd.to_numeric(perf["net_irr_pcent"], errors="coerce")
perf["multiple_numeric"] = pd.to_numeric(perf["multiple"], errors="coerce")
has_irr = perf["irr_numeric"].notna().sum()
has_mult = perf["multiple_numeric"].notna().sum()
print(f"  Records with numeric IRR: {has_irr:,}")
print(f"  Records with numeric multiple: {has_mult:,}")
print(f"  Funds with any IRR: {perf[perf['irr_numeric'].notna()]['fund_id'].nunique():,}")

perf.to_csv(os.path.join(data_dir, "Preqin/matched_fund_performance.csv"), index=False)

# =====================================================================
# STEP 6: Pull cashflows
# =====================================================================
print("\n\n--- Step 6: Pull cashflows ---")

cur.execute(f"""
    SELECT fund_id, transaction_date, transaction_type,
           transaction_amount, cumulative_contribution, cumulative_distribution,
           net_cashflow
    FROM preqin_cashflow.cashflow
    WHERE fund_id IN ({fid_str})
    ORDER BY fund_id, transaction_date
""")
cols = [d[0] for d in cur.description]
cf = pd.DataFrame(cur.fetchall(), columns=cols)
print(f"  Cashflow records: {len(cf):,}")
print(f"  Unique funds with cashflows: {cf['fund_id'].nunique():,}")
if len(cf) > 0:
    print(f"  Date range: {cf['transaction_date'].min()} to {cf['transaction_date'].max()}")
    print(f"\n  Transaction types:")
    for tt, n in cf["transaction_type"].value_counts().items():
        print(f"    {tt:<30} {n:>5,}")

cf.to_csv(os.path.join(data_dir, "Preqin/matched_fund_cashflows.csv"), index=False)

conn.close()

# =====================================================================
# STEP 7: Summary - what can we test?
# =====================================================================
print(f"\n\n{'=' * 80}")
print("SUMMARY: Data for VC Fund Performance Tests")
print(f"{'=' * 80}")

# Merge fund details with performance to get the full picture
fund_perf = perf.merge(funds[["fund_id", "firm_id", "firm_name", "fund_type", "industry", "vintage"]],
                        on="fund_id", how="left")

# Merge with our VC crosswalk to connect back to observer network
fund_perf = fund_perf.merge(
    match_df[["preqin_firm_id", "ciq_vc_companyid", "ciq_vc_name"]].drop_duplicates("preqin_firm_id"),
    left_on="firm_id", right_on="preqin_firm_id", how="left"
)

# How many funds have both performance data AND are linked to our observer network?
linked = fund_perf.dropna(subset=["ciq_vc_companyid"])
print(f"\n  Funds with performance AND observer network link: {linked['fund_id'].nunique():,}")
print(f"  Firms: {linked['firm_id'].nunique():,}")
print(f"  Performance records: {len(linked):,}")
print(f"  Records with IRR: {linked['irr_numeric'].notna().sum():,}")

# For these firms, how many observed companies do they have?
linked_vcs = linked[["ciq_vc_companyid", "ciq_vc_name"]].drop_duplicates()
obs_counts = tb.groupby("vc_firm_companyid")["observed_companyid"].nunique()
linked_vcs["n_observed"] = linked_vcs["ciq_vc_companyid"].map(obs_counts)

print(f"\n  Linked VCs with observed company counts:")
for _, r in linked_vcs.sort_values("n_observed", ascending=False).head(15).iterrows():
    print(f"    {r['ciq_vc_name'][:45]:<45} {int(r['n_observed']):>3} observed companies")

# Events at observed companies of linked VCs
co_det = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_priv = set(co_det[(co_det["country"] == "United States") & (co_det["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))

linked_vc_ids = set(linked_vcs["ciq_vc_companyid"])
tb_linked = tb[tb["vc_firm_companyid"].isin(linked_vc_ids)]
linked_obs_cos = set(tb_linked["observed_companyid"].astype(str).str.replace(".0", "", regex=False)) & us_priv

events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events[events["companyid"].isin(linked_obs_cos)]
events = events[events["announcedate"] >= "2010-01-01"]
noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)"]
events = events[~events["eventtype"].isin(noise)]

print(f"\n  Events at observed companies of linked VCs: {len(events):,}")
print(f"  Companies: {events['companyid'].nunique():,}")
print(f"  Date range: {events['announcedate'].min().date()} to {events['announcedate'].max().date()}")

print(f"\n  By event type:")
for et, n in events["eventtype"].value_counts().head(8).items():
    print(f"    {et[:50]:<50} {n:>5,}")

print("\n\nDone. Files saved to Data/Preqin/")
