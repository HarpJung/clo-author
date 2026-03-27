"""Test 3 using Form D filing dates as events.
Each Form D = a capital raise by a private company.
Observer learned about the fundraise in board meetings before the filing.
Test whether connected portfolio companies show pre-filing abnormal returns.

Events: Form D filing dates for CIQ observer companies (matched via CIK)
Network: same observer->VC->portfolio edges as before
CARs: [-30,-1] [-20,-1] [-15,-1] [-10,-1] [-5,-1] [-3,-1] [-2,-1] [-1,0] [0,+3] [0,+5]
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv, glob
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")
formd_dir = os.path.join(data_dir, "FormD")

print("=" * 100)
print("TEST 3: FORM D FILING DATES AS EVENTS")
print("=" * 100)

# =====================================================================
# STEP 1: Load Form D filings and match to CIQ observer companies
# =====================================================================
print("\n--- Loading Form D filings ---")

# Load all quarterly FORMDSUBMISSION + ISSUERS
formd_events = []
quarters = sorted(glob.glob(os.path.join(formd_dir, "20*")))
for qdir in quarters:
    # Find the nested data folder
    subdirs = glob.glob(os.path.join(qdir, "*_d"))
    if not subdirs:
        continue
    ddir = subdirs[0]
    sub_file = os.path.join(ddir, "FORMDSUBMISSION.tsv")
    iss_file = os.path.join(ddir, "ISSUERS.tsv")
    if not os.path.exists(sub_file) or not os.path.exists(iss_file):
        continue
    try:
        sub = pd.read_csv(sub_file, sep="\t", dtype=str, low_memory=False)
        iss = pd.read_csv(iss_file, sep="\t", dtype=str, low_memory=False)
        # Merge to get CIK + filing date
        merged = sub[["ACCESSIONNUMBER", "FILING_DATE"]].merge(
            iss[iss["IS_PRIMARYISSUER_FLAG"] == "YES"][["ACCESSIONNUMBER", "CIK", "ENTITYNAME"]],
            on="ACCESSIONNUMBER", how="inner"
        )
        formd_events.append(merged)
    except Exception as e:
        pass

formd_all = pd.concat(formd_events, ignore_index=True)
formd_all["FILING_DATE"] = pd.to_datetime(formd_all["FILING_DATE"], errors="coerce")
formd_all["CIK"] = formd_all["CIK"].astype(str).str.strip().str.lstrip("0")
formd_all = formd_all.dropna(subset=["FILING_DATE"])
formd_all["filing_year"] = formd_all["FILING_DATE"].dt.year
formd_all = formd_all[(formd_all["filing_year"] >= 2015) & (formd_all["filing_year"] <= 2025)]
print(f"  Total Form D filings (2015-2025): {len(formd_all):,}")
print(f"  Unique CIKs: {formd_all['CIK'].nunique():,}")

# Match to CIQ observer companies via CIK
ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["companyid_str"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_str"] = ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0")
cik_to_cid = dict(zip(ciq_xwalk["cik_str"], ciq_xwalk["companyid_str"]))

formd_all["companyid_str"] = formd_all["CIK"].map(cik_to_cid)
formd_matched = formd_all.dropna(subset=["companyid_str"])
print(f"  Matched to CIQ observer companies: {len(formd_matched):,} filings, {formd_matched['companyid_str'].nunique():,} companies")

# =====================================================================
# STEP 2: Load network edges
# =====================================================================
print("\n--- Loading network ---")
edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")
port_xwalk = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
port_xwalk["cik_int"] = pd.to_numeric(port_xwalk["cik"], errors="coerce")
edges = edges.merge(port_xwalk.drop_duplicates("cik_int", keep="first")[["cik_int", "permno"]].rename(
    columns={"cik_int": "portfolio_cik_int"}), on="portfolio_cik_int", how="inner")
edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce")

industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))
ciq_xwalk_num = ciq_xwalk.copy()
ciq_xwalk_num["cik_int"] = pd.to_numeric(ciq_xwalk_num["cik_str"], errors="coerce")
companyid_to_cik = dict(zip(ciq_xwalk_num["companyid_str"], ciq_xwalk_num["cik_int"]))
edges["same_industry"] = (edges["observed_companyid"].map(companyid_to_cik).map(cik_to_sic2) ==
                           edges["portfolio_cik_int"].map(cik_to_sic2)).astype(int)

# Only keep filings from companies that have edges
observed_with_edges = set(edges["observed_companyid"])
formd_matched = formd_matched[formd_matched["companyid_str"].isin(observed_with_edges)]
print(f"  Form D events with network edges: {len(formd_matched):,} from {formd_matched['companyid_str'].nunique():,} companies")

# Merge events with edges
event_edges = formd_matched.merge(edges, left_on="companyid_str", right_on="observed_companyid", how="inner")
print(f"  Event-edge pairs: {len(event_edges):,}")

# =====================================================================
# STEP 3: Load returns and compute CARs
# =====================================================================
print("\n--- Loading daily returns ---")
port_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["permno"] = pd.to_numeric(port_daily["permno"], errors="coerce")
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
port_daily = port_daily.dropna(subset=["permno", "ret", "date"]).sort_values(["permno", "date"])
print(f"  Returns: {len(port_daily):,}")

print("\n--- Computing CARs ---")
np.random.seed(42)
if len(event_edges) > 150000:
    event_edges = event_edges.sample(150000).reset_index(drop=True)
    print(f"  Sampled to {len(event_edges):,}")

car_windows = [
    ("car_30", -30, -1), ("car_20", -20, -1), ("car_15", -15, -1),
    ("car_10", -10, -1), ("car_5", -5, -1), ("car_3", -3, -1),
    ("car_2", -2, -1), ("car_1", -1, 0),
    ("car_post3", 0, 3), ("car_post5", 0, 5),
]

car_results = []
total = len(event_edges)
for idx, row in event_edges.iterrows():
    pdata = port_daily[port_daily["permno"] == row["permno"]]
    if len(pdata) < 30:
        continue
    dates = pdata["date"].values
    rets = pdata["ret"].values
    event_np = np.datetime64(row["FILING_DATE"])
    diffs = (dates - event_np).astype("timedelta64[D]").astype(int)
    cars = {}
    for wn, d0, d1 in car_windows:
        mask = (diffs >= d0) & (diffs <= d1)
        wr = rets[mask]
        if len(wr) >= max(2, abs(d1 - d0) * 0.3):
            cars[wn] = float(np.sum(wr))
    if cars:
        car_results.append({
            "vc_firm_companyid": str(row.get("vc_firm_companyid", "")),
            "same_industry": row["same_industry"],
            "event_year": row["FILING_DATE"].year,
            **cars,
        })
    if (idx + 1) % 5000 == 0:
        print(f"    {idx+1:,}/{total:,}: {len(car_results):,} CARs")

car_df = pd.DataFrame(car_results)
car_df = car_df[car_df["vc_firm_companyid"] != ""].reset_index(drop=True)
print(f"\n  Total CARs: {len(car_df):,}, VC firms: {car_df['vc_firm_companyid'].nunique():,}")
print(f"  Same-ind: {(car_df['same_industry']==1).sum():,}, Diff-ind: {(car_df['same_industry']==0).sum():,}")


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


windows = [
    ("car_30", "CAR[-30,-1]"), ("car_20", "CAR[-20,-1]"), ("car_15", "CAR[-15,-1]"),
    ("car_10", "CAR[-10,-1]"), ("car_5", "CAR[-5,-1]"), ("car_3", "CAR[-3,-1]"),
    ("car_2", "CAR[-2,-1]"), ("car_1", "CAR[-1,0]"),
    ("car_post3", "CAR[0,+3]"), ("car_post5", "CAR[0,+5]"),
]

subsamples = [
    ("Overall", lambda df: df),
    ("Same-ind", lambda df: df[df["same_industry"] == 1]),
    ("Diff-ind", lambda df: df[df["same_industry"] == 0]),
]


# =====================================================================
# TABLE 1: Full sample means (VC-clustered)
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 1: FORM D EVENTS — SUBSAMPLE MEANS (VC-clustered)")
print("=" * 100)
print(f"\n  {'Window':<14} {'Subsample':<12} {'N':>8} {'Mean CAR':>10} {'p (VC-cl)':>12} {'Clusters':>10}")
print(f"  {'-'*66}")

for var, label in windows:
    for sname, sfn in subsamples:
        sub = sfn(car_df).dropna(subset=[var]).copy()
        sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
        n = len(sub)
        if n < 20:
            print(f"  {label:<14} {sname:<12} {n:>8}  too few")
            continue
        nvc = sub["vc_firm_companyid"].nunique()
        mean_val = sub[var].mean()
        try:
            m = smf.ols(f"{var} ~ 1", data=sub).fit(
                cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
            pcl = m.pvalues["Intercept"]
        except:
            pcl = np.nan
        print(f"  {label:<14} {sname:<12} {n:>8,} {mean_val:>+10.5f} {pcl:>9.4f}{sig(pcl)} {nvc:>10,}")
    print()


# =====================================================================
# TABLE 2: Pre-2020 vs Post-2020
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 2: FORM D — PRE-2020 vs POST-2020")
print("=" * 100)
print(f"\n  {'Window':<14} {'Period':<14} {'Subsample':<12} {'N':>8} {'Mean CAR':>10} {'p (VC-cl)':>12}")
print(f"  {'-'*70}")

for var, label in windows:
    for pname, pfn in [("Pre-2020", lambda df: df[df["event_year"] < 2020]),
                        ("Post-2020", lambda df: df[df["event_year"] >= 2020])]:
        for sname, sfn in subsamples:
            sub = sfn(pfn(car_df)).dropna(subset=[var]).copy()
            sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
            n = len(sub)
            if n < 20:
                continue
            mean_val = sub[var].mean()
            try:
                m = smf.ols(f"{var} ~ 1", data=sub).fit(
                    cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
                pcl = m.pvalues["Intercept"]
            except:
                pcl = np.nan
            print(f"  {label:<14} {pname:<14} {sname:<12} {n:>8,} {mean_val:>+10.5f} {pcl:>9.4f}{sig(pcl)}")
        print()


# =====================================================================
# TABLE 3: CIQ vs Form D comparison (same windows, same sample period)
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 3: COMPARISON — CIQ Key Dev vs Form D Events")
print("Using same network, same portfolio returns, same windows")
print("=" * 100)
print(f"\n  Form D sample: {len(car_df):,} CARs from {car_df['vc_firm_companyid'].nunique():,} VC firms")
print(f"  (Compare to CIQ Key Dev: ~24,000 CARs from ~1,155 VC firms)")


print("\n\nDone.")
