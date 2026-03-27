"""Test 3: Year FE only (no VC FE), VC-clustered.
Shows overall, same-industry, diff-industry for every window and period."""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 95)
print("TEST 3: YEAR FE + VC CLUSTERING (no VC FE)")
print("=" * 95)

# --- Load ---
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
ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["companyid_str"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.lstrip("0"), errors="coerce")
companyid_to_cik = dict(zip(ciq_xwalk["companyid_str"], ciq_xwalk["cik_int"]))
edges["same_industry"] = (edges["observed_companyid"].map(companyid_to_cik).map(cik_to_sic2) ==
                           edges["portfolio_cik_int"].map(cik_to_sic2)).astype(int)

events = pd.read_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
events["companyid_str"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
pub = set()
with open(os.path.join(ciq_dir, "04_observer_company_details.csv"), "r", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        cid = str(r.get("companyid", "")).strip().replace(".0", "")
        if "public" in str(r.get("companytypename", "")).lower():
            pub.add(cid)
events = events[~events["companyid_str"].isin(pub)]
events["event_date"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["event_date"])
events["event_year"] = events["event_date"].dt.year
events = events[(events["event_year"] >= 2015) & (events["event_year"] <= 2024)]

event_edges = events.merge(edges, left_on="companyid_str", right_on="observed_companyid", how="inner")

port_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["permno"] = pd.to_numeric(port_daily["permno"], errors="coerce")
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
port_daily = port_daily.dropna(subset=["permno", "ret", "date"]).sort_values(["permno", "date"])

np.random.seed(42)
if len(event_edges) > 120000:
    event_edges = event_edges.sample(120000).reset_index(drop=True)

print("Computing CARs...")
car_results = []
for idx, row in event_edges.iterrows():
    pdata = port_daily[port_daily["permno"] == row["permno"]]
    if len(pdata) < 30:
        continue
    dates = pdata["date"].values
    rets = pdata["ret"].values
    event_np = np.datetime64(row["event_date"])
    diffs = (dates - event_np).astype("timedelta64[D]").astype(int)
    cars = {}
    for wn, d0, d1 in [("car_60", -60, -1), ("car_30", -30, -1),
                        ("car_10", -10, -1), ("car_post", 0, 5)]:
        mask = (diffs >= d0) & (diffs <= d1)
        wr = rets[mask]
        if len(wr) >= max(3, abs(d1 - d0) * 0.3):
            cars[wn] = float(np.sum(wr))
    if cars:
        ey = row["event_date"].year if hasattr(row["event_date"], "year") else pd.Timestamp(row["event_date"]).year
        car_results.append({
            "vc_firm_companyid": str(row.get("vc_firm_companyid", "")),
            "portfolio_id": str(row["permno"]),
            "same_industry": row["same_industry"],
            "event_year": int(ey),
            **cars,
        })
    if len(car_results) % 10000 == 0 and len(car_results) > 0:
        print(f"  {len(car_results):,}...")

car_df = pd.DataFrame(car_results)
car_df = car_df[car_df["vc_firm_companyid"] != ""].reset_index(drop=True)
print(f"Total: {len(car_df):,} CARs, {car_df['vc_firm_companyid'].nunique():,} VC firms\n")


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


windows = [("car_60", "CAR[-60,-1]"), ("car_30", "CAR[-30,-1]"),
           ("car_10", "CAR[-10,-1]"), ("car_post", "CAR[0,+5]")]

periods = [
    ("Pre-2020", lambda df: df[df["event_year"] < 2020]),
    ("Post-2020", lambda df: df[df["event_year"] >= 2020]),
    ("Full", lambda df: df),
]

subsamples = [
    ("Overall", lambda df: df),
    ("Same-ind", lambda df: df[df["same_industry"] == 1]),
    ("Diff-ind", lambda df: df[df["same_industry"] == 0]),
]

# =====================================================================
# TABLE 1: Raw means (no FE), VC-clustered
# =====================================================================
print("=" * 100)
print("TABLE 1: RAW MEANS (no FE), VC-clustered p-values")
print("=" * 100)

for var, label in windows:
    print(f"\n  {label}")
    print(f"  {'Period':<12} {'Subsample':<12} {'N':>8} {'Mean':>10} {'p (VC-cl)':>12} {'Clusters':>10}")
    print(f"  {'-'*64}")
    for pname, pfn in periods:
        for sname, sfn in subsamples:
            sub = sfn(pfn(car_df)).dropna(subset=[var]).copy()
            sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
            n = len(sub)
            if n < 30: continue
            nvc = sub["vc_firm_companyid"].nunique()
            mean_val = sub[var].mean()
            try:
                m = smf.ols(f"{var} ~ 1", data=sub).fit(
                    cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
                pcl = m.pvalues["Intercept"]
            except:
                pcl = np.nan
            print(f"  {pname:<12} {sname:<12} {n:>8,} {mean_val:>+10.5f} {pcl:>9.4f}{sig(pcl)} {nvc:>10,}")
        print()

# =====================================================================
# TABLE 2: Year FE (demean by year), VC-clustered
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 2: YEAR FE (demean by year), VC-clustered p-values")
print("=" * 100)

for var, label in windows:
    print(f"\n  {label}")
    print(f"  {'Period':<12} {'Subsample':<12} {'N':>8} {'Mean(dm)':>10} {'p (Yr FE+cl)':>14} {'Clusters':>10}")
    print(f"  {'-'*66}")
    for pname, pfn in periods:
        for sname, sfn in subsamples:
            sub = sfn(pfn(car_df)).dropna(subset=[var]).copy()
            sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
            n = len(sub)
            if n < 30: continue
            nvc = sub["vc_firm_companyid"].nunique()
            # Demean by year
            yr_mean = sub.groupby("event_year")[var].transform("mean")
            sub["_dm"] = sub[var] - yr_mean
            mean_dm = sub["_dm"].mean()
            try:
                m = smf.ols("_dm ~ 1", data=sub).fit(
                    cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
                pcl = m.pvalues["Intercept"]
            except:
                pcl = np.nan
            print(f"  {pname:<12} {sname:<12} {n:>8,} {mean_dm:>+10.5f} {pcl:>11.4f}{sig(pcl)} {nvc:>10,}")
        print()

# =====================================================================
# TABLE 3: NVCA shock interaction with Year FE
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 3: NVCA SHOCK — same_industry x post_2020")
print("Specs: (A) No FE + VC-cl, (B) Year FE + VC-cl")
print("=" * 100)

car_df["post_2020"] = (car_df["event_year"] >= 2020).astype(int)

shock_samples = [
    ("Full 2015-2024", car_df.copy(), "post_2020"),
    ("Ex-COVID", car_df[car_df["event_year"] != 2020].copy(), "post_2020"),
    ("Placebo @2018", car_df[car_df["event_year"] <= 2019].copy(), "post_2018"),
]
# Fix placebo
shock_samples[2][1]["post_2018"] = (shock_samples[2][1]["event_year"] >= 2018).astype(int)

print(f"\n  {'Window':<14} {'Sample':<18} {'Spec':<18} {'Coef(interact)':>14} {'p':>10} {'N':>8}")
print(f"  {'-'*86}")

for var, label in windows:
    for sample_name, sample_df, post_col in shock_samples:
        sub = sample_df.dropna(subset=[var, "same_industry"]).copy()
        sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
        if len(sub) < 200: continue
        n = len(sub)

        sub["same_x_post"] = sub["same_industry"] * sub[post_col]

        # (A) No FE + VC-cluster
        formula_a = f"{var} ~ same_industry + {post_col} + same_x_post"
        m_a = smf.ols(formula_a, data=sub).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

        # (B) Year FE + VC-cluster (demean by year, post is collinear so drop it)
        xvars = [var, "same_industry", "same_x_post"]
        sub_dm = sub[xvars].copy()
        yr_means = sub_dm.groupby(sub["event_year"]).transform("mean")
        sub_dm = sub_dm - yr_means
        m_b = smf.ols(f"{var} ~ same_industry + same_x_post - 1", data=sub_dm).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

        # Print
        coef_a = m_a.params["same_x_post"]
        p_a = m_a.pvalues["same_x_post"]
        coef_b = m_b.params["same_x_post"]
        p_b = m_b.pvalues["same_x_post"]

        print(f"  {label:<14} {sample_name:<18} {'No FE + VC-cl':<18} {coef_a:>+12.5f}{sig(p_a)} {p_a:>10.4f} {n:>8,}")
        print(f"  {'':<14} {'':<18} {'Yr FE + VC-cl':<18} {coef_b:>+12.5f}{sig(p_b)} {p_b:>10.4f} {n:>8,}")
    print()

print("Done.")
