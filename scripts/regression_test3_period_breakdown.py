"""Test 3: Full breakdown by period (pre-2020 vs post-2020 vs full)
Shows: overall means, same/diff means, VC FE coef, VC+Year FE coef."""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

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
    for wn, d0, d1 in [("car_60", -60, -1), ("car_30", -30, -1), ("car_10", -10, -1), ("car_post", 0, 5)]:
        mask = (diffs >= d0) & (diffs <= d1)
        wr = rets[mask]
        if len(wr) >= max(3, abs(d1 - d0) * 0.3):
            cars[wn] = float(np.sum(wr))
    if cars:
        ey = row["event_date"].year if hasattr(row["event_date"], "year") else pd.Timestamp(row["event_date"]).year
        car_results.append({
            "vc_firm_companyid": str(row.get("vc_firm_companyid", "")),
            "same_industry": row["same_industry"],
            "event_year": int(ey),
            **cars,
        })
    if len(car_results) % 10000 == 0 and len(car_results) > 0:
        print(f"  {len(car_results):,}...")

car_df = pd.DataFrame(car_results)
car_df = car_df[car_df["vc_firm_companyid"] != ""].reset_index(drop=True)
print(f"Total: {len(car_df):,} CARs\n")


def sig(p):
    if p < 0.01:
        return "***"
    if p < 0.05:
        return "**"
    if p < 0.10:
        return "*"
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
# TABLE 1: Subsample means by period, with VC-clustered p-values
# =====================================================================
print("=" * 110)
print("TABLE 1: SUBSAMPLE MEANS (VC-clustered p-values)")
print("=" * 110)

for var, label in windows:
    print(f"\n  {label}")
    print(f"  {'Period':<12} {'Subsample':<12} {'N':>8} {'Mean CAR':>10} {'t-test p':>10} {'VC-cl p':>10} {'VC clusters':>12}")
    print(f"  {'-'*74}")

    for pname, pfn in periods:
        for sname, sfn in subsamples:
            sub = sfn(pfn(car_df)).dropna(subset=[var]).copy()
            sub = sub[sub["vc_firm_companyid"] != ""]
            n = len(sub)
            if n < 30:
                continue
            mean_val = sub[var].mean()
            t, pt = stats.ttest_1samp(sub[var], 0)
            nvc = sub["vc_firm_companyid"].nunique()
            try:
                m = smf.ols(f"{var} ~ 1", data=sub.reset_index(drop=True)).fit(
                    cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"].values})
                pcl = m.pvalues["Intercept"]
            except:
                pcl = np.nan
            print(f"  {pname:<12} {sname:<12} {n:>8,} {mean_val:>+10.5f} {pt:>7.4f}{sig(pt)} {pcl:>7.4f}{sig(pcl)} {nvc:>12,}")
        print()

# =====================================================================
# TABLE 2: VC FE same_industry coefficient by period
# =====================================================================
print("\n" + "=" * 110)
print("TABLE 2: VC FE same_industry COEFFICIENT by Period (within-VC, VC-clustered)")
print("=" * 110)
print(f"  {'Window':<16} {'Period':<12} {'Coef':>10} {'p (VC FE+cl)':>14} {'N':>8} {'VC clusters':>12}")
print(f"  {'-'*72}")

for var, label in windows:
    for pname, pfn in periods:
        sub = pfn(car_df).dropna(subset=[var, "same_industry"]).copy()
        sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
        if len(sub) < 100:
            continue
        n = len(sub)
        nvc = sub["vc_firm_companyid"].nunique()
        dm = sub.groupby("vc_firm_companyid")[[var, "same_industry"]].transform("mean")
        sub_dm = sub[[var, "same_industry"]] - dm
        m = smf.ols(f"{var} ~ same_industry - 1", data=sub_dm).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
        coef = m.params["same_industry"]
        p = m.pvalues["same_industry"]
        print(f"  {label:<16} {pname:<12} {coef:>+10.5f} {p:>11.4f}{sig(p)}  {n:>8,} {nvc:>12,}")
    print()

# =====================================================================
# TABLE 3: VC + Year FE same_industry coefficient by period
# =====================================================================
print("\n" + "=" * 110)
print("TABLE 3: VC + YEAR FE same_industry COEFFICIENT by Period (double-demeaned, VC-clustered)")
print("=" * 110)
print(f"  {'Window':<16} {'Period':<12} {'Coef':>10} {'p (VC+Yr+cl)':>14} {'N':>8}")
print(f"  {'-'*60}")

for var, label in windows:
    for pname, pfn in periods:
        sub = pfn(car_df).dropna(subset=[var, "same_industry"]).copy()
        sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
        if len(sub) < 100:
            continue
        n = len(sub)
        # Double demean
        sub_dm = sub[[var, "same_industry"]].copy()
        for _ in range(10):
            vc_m = sub_dm.groupby(sub["vc_firm_companyid"]).transform("mean")
            sub_dm = sub_dm - vc_m
            yr_m = sub_dm.groupby(sub["event_year"]).transform("mean")
            sub_dm = sub_dm - yr_m
        m = smf.ols(f"{var} ~ same_industry - 1", data=sub_dm).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
        coef = m.params["same_industry"]
        p = m.pvalues["same_industry"]
        print(f"  {label:<16} {pname:<12} {coef:>+10.5f} {p:>11.4f}{sig(p)}  {n:>8,}")
    print()

print("Done.")
