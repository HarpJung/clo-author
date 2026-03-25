"""Test 3 Multi-Window: Pre-event CARs at [-60,-1], [-30,-1], [-10,-1]
Private company events only. Clustered and unclustered side by side."""

import os
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats
import csv

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 80)
print("TEST 3 MULTI-WINDOW: Private Events, Multiple Pre-Event Windows")
print("=" * 80)

# =====================================================================
# STEP 1: Load all data (same as before)
# =====================================================================
print("\n--- Loading data ---")

edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")

port_xwalk = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
port_xwalk["cik_int"] = pd.to_numeric(port_xwalk["cik"], errors="coerce")
port_xwalk_dedup = port_xwalk.drop_duplicates("cik_int", keep="first")
edges = edges.merge(
    port_xwalk_dedup[["cik_int", "permno"]].rename(columns={"cik_int": "portfolio_cik_int"}),
    on="portfolio_cik_int", how="inner"
)
edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce")
edges["is_director_at_portfolio"] = edges["portfolio_title"].str.contains(
    "Director|Chairman|Board Member", case=False, na=False
).astype(int)

# Industry
industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))

ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["companyid_str"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.lstrip("0"), errors="coerce")
companyid_to_cik = dict(zip(ciq_xwalk["companyid_str"], ciq_xwalk["cik_int"]))

edges["observed_cik"] = edges["observed_companyid"].map(companyid_to_cik)
edges["observed_sic2"] = edges["observed_cik"].map(cik_to_sic2)
edges["portfolio_sic2"] = edges["portfolio_cik_int"].map(cik_to_sic2)
edges["same_industry"] = (
    edges["observed_sic2"].notna() & edges["portfolio_sic2"].notna() &
    (edges["observed_sic2"] == edges["portfolio_sic2"])
).astype(int)

# Events - private firms only
events = pd.read_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)

cos_types = {}
with open(os.path.join(ciq_dir, "04_observer_company_details.csv"), "r", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        cid = str(r["companyid"]).strip()
        if cid.endswith(".0"):
            cid = cid[:-2]
        cos_types[cid] = r["companytypename"]

events["company_type"] = events["companyid"].map(cos_types)
private_events = events[events["company_type"] == "Private Company"].copy()
private_selected = private_events[private_events["keydeveventtypename"].isin([
    "Executive/Board Changes - Other",
    "Strategic Alliances",
    "Seeking Financing/Partners",
    "Lawsuits & Legal Issues",
    "Bankruptcy - Other",
    "Discontinued Operations/Downsizings",
    "Announcements of Earnings",
])].copy()
private_selected = private_selected.dropna(subset=["announcedate"])
private_selected = private_selected[["companyid", "announcedate", "keydeveventtypename"]].drop_duplicates()

print(f"  Edges: {len(edges):,}")
print(f"  Private events: {len(private_selected):,}")

# Merge events with edges
event_edges = edges.merge(
    private_selected, left_on="observed_companyid", right_on="companyid", how="inner"
)
print(f"  Event-edge pairs: {len(event_edges):,}")

# Daily returns
port_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
mkt_daily = port_daily.groupby("date")["ret"].mean().reset_index()
mkt_daily.columns = ["date", "mkt_ret"]
port_daily = port_daily.merge(mkt_daily, on="date")
port_daily["abnormal_ret"] = port_daily["ret"] - port_daily["mkt_ret"]

print(f"  Daily returns: {len(port_daily):,} rows")

# =====================================================================
# STEP 2: Compute CARs for three windows simultaneously
# =====================================================================
print("\n--- Computing CARs for [-60,-1], [-30,-1], [-10,-1], [0,+5] ---")

car_results = []
total = len(event_edges)

# Sample if too large
if total > 80000:
    event_edges_sample = event_edges.sample(80000, random_state=42)
    print(f"  Sampled {len(event_edges_sample):,} from {total:,}")
else:
    event_edges_sample = event_edges

chunk_size = 10000
total_chunks = (len(event_edges_sample) + chunk_size - 1) // chunk_size

for chunk_idx in range(total_chunks):
    start = chunk_idx * chunk_size
    end = min(start + chunk_size, len(event_edges_sample))
    chunk = event_edges_sample.iloc[start:end]

    for _, row in chunk.iterrows():
        permno = int(row["permno"])
        event_date = row["announcedate"]

        # Get returns in wide window [-90, +10]
        date_lo = event_date - pd.Timedelta(days=100)
        date_hi = event_date + pd.Timedelta(days=15)

        firm_rets = port_daily[
            (port_daily["permno"] == permno) &
            (port_daily["date"] >= date_lo) &
            (port_daily["date"] <= date_hi)
        ].sort_values("date")

        if len(firm_rets) < 10:
            continue

        # Split into windows
        pre_all = firm_rets[firm_rets["date"] < event_date].copy()
        post = firm_rets[firm_rets["date"] >= event_date].head(6)

        if len(pre_all) < 5:
            continue

        # Pre-event windows (trading days from the end)
        pre_10 = pre_all.tail(10)
        pre_30 = pre_all.tail(30)
        pre_60 = pre_all.tail(60)

        car_10 = pre_10["abnormal_ret"].sum() if len(pre_10) >= 5 else np.nan
        car_30 = pre_30["abnormal_ret"].sum() if len(pre_30) >= 15 else np.nan
        car_60 = pre_60["abnormal_ret"].sum() if len(pre_60) >= 30 else np.nan
        car_post = post["abnormal_ret"].sum() if len(post) >= 3 else np.nan

        car_results.append({
            "observer_personid": row["observer_personid"],
            "observed_companyid": row["observed_companyid"],
            "vc_firm_companyid": row["vc_firm_companyid"],
            "portfolio_permno": permno,
            "event_date": event_date,
            "event_type": row["keydeveventtypename"],
            "car_60": car_60,
            "car_30": car_30,
            "car_10": car_10,
            "car_post": car_post,
            "n_pre_60": len(pre_60),
            "n_pre_30": len(pre_30),
            "n_pre_10": len(pre_10),
            "is_director_at_portfolio": row["is_director_at_portfolio"],
            "same_industry": row["same_industry"],
        })

    if (chunk_idx + 1) % 3 == 0 or chunk_idx == total_chunks - 1:
        print(f"    Chunk {chunk_idx+1}/{total_chunks}: {len(car_results):,} CARs")

car_df = pd.DataFrame(car_results)
print(f"\n  Total CARs: {len(car_df):,}")

# =====================================================================
# STEP 3: Results - side by side for all windows
# =====================================================================

windows = [
    ("car_60", "CAR[-60,-1]", "~3 months"),
    ("car_30", "CAR[-30,-1]", "~6 weeks"),
    ("car_10", "CAR[-10,-1]", "~2 weeks"),
    ("car_post", "CAR[0,+5]", "post-event"),
]

def run_tests(df, var, label, period):
    """Run all tests for one window, return formatted results."""
    sub = df.dropna(subset=[var]).copy()
    n = len(sub)
    if n < 30:
        return

    print(f"\n  {'='*70}")
    print(f"  {label} ({period}) | N = {n:,}")
    print(f"  {'='*70}")

    # Overall
    mean_val = sub[var].mean()
    t_unc, p_unc = stats.ttest_1samp(sub[var], 0)

    # Clustered
    reg = sub.reset_index(drop=True)
    n_vc = reg["vc_firm_companyid"].nunique()
    try:
        m_clust = smf.ols(f"{var} ~ 1", data=reg).fit(
            cov_type="cluster", cov_kwds={"groups": reg["vc_firm_companyid"]})
        t_cl = m_clust.tvalues["Intercept"]
        p_cl = m_clust.pvalues["Intercept"]
    except:
        t_cl, p_cl = np.nan, np.nan

    sig_unc = "***" if p_unc < 0.01 else "**" if p_unc < 0.05 else "*" if p_unc < 0.10 else ""
    sig_cl = "***" if p_cl < 0.01 else "**" if p_cl < 0.05 else "*" if p_cl < 0.10 else ""

    print(f"\n  {'Test':<35} {'N':>8} {'Mean':>10} {'t(unc)':>8} {'p(unc)':>8} {'Sig':>4} {'t(clust)':>9} {'p(clust)':>9} {'Sig':>4} {'Clusters':>9}")
    print(f"  {'-'*105}")
    print(f"  {'Overall':<35} {n:>8,} {mean_val:>10.5f} {t_unc:>8.2f} {p_unc:>8.4f} {sig_unc:>4} {t_cl:>9.2f} {p_cl:>9.4f} {sig_cl:>4} {n_vc:>9,}")

    # Same industry vs different
    for ind, ind_label in [(1, "Same industry"), (0, "Different industry")]:
        ind_sub = sub[sub["same_industry"] == ind][var].dropna()
        if len(ind_sub) > 10:
            t_u, p_u = stats.ttest_1samp(ind_sub, 0)
            sig_u = "***" if p_u < 0.01 else "**" if p_u < 0.05 else "*" if p_u < 0.10 else ""
            print(f"  {ind_label:<35} {len(ind_sub):>8,} {ind_sub.mean():>10.5f} {t_u:>8.2f} {p_u:>8.4f} {sig_u:>4} {'':>9} {'':>9} {'':>4} {'':>9}")

    # Same vs different t-test
    same = sub[sub["same_industry"] == 1][var].dropna()
    diff = sub[sub["same_industry"] == 0][var].dropna()
    if len(same) > 10 and len(diff) > 10:
        t_d, p_d = stats.ttest_ind(same, diff, equal_var=False)
        sig_d = "***" if p_d < 0.01 else "**" if p_d < 0.05 else "*" if p_d < 0.10 else ""
        print(f"  {'Same vs Diff (difference)':<35} {'':>8} {same.mean()-diff.mean():>10.5f} {t_d:>8.2f} {p_d:>8.4f} {sig_d:>4}")

    # Regressions: same_industry + is_director (clustered and unclustered)
    reg_sub = sub.dropna(subset=[var, "same_industry", "is_director_at_portfolio"]).reset_index(drop=True)
    if len(reg_sub) > 50 and reg_sub["same_industry"].sum() > 10:
        print(f"\n  {'Regression':<35} {'N':>8} {'Coef':>10} {'t(unc)':>8} {'p(unc)':>8} {'Sig':>4} {'t(clust)':>9} {'p(clust)':>9} {'Sig':>4} {'Clusters':>9}")
        print(f"  {'-'*105}")

        # Unclustered
        m_unc = smf.ols(f"{var} ~ same_industry + is_director_at_portfolio", data=reg_sub).fit(cov_type="HC1")
        # Clustered
        try:
            m_cl2 = smf.ols(f"{var} ~ same_industry + is_director_at_portfolio", data=reg_sub).fit(
                cov_type="cluster", cov_kwds={"groups": reg_sub["vc_firm_companyid"]})
        except:
            m_cl2 = None

        for param in ["same_industry", "is_director_at_portfolio"]:
            coef = m_unc.params[param]
            t_u = m_unc.tvalues[param]
            p_u = m_unc.pvalues[param]
            sig_u = "***" if p_u < 0.01 else "**" if p_u < 0.05 else "*" if p_u < 0.10 else ""

            if m_cl2:
                t_c = m_cl2.tvalues[param]
                p_c = m_cl2.pvalues[param]
                sig_c = "***" if p_c < 0.01 else "**" if p_c < 0.05 else "*" if p_c < 0.10 else ""
            else:
                t_c, p_c, sig_c = np.nan, np.nan, ""

            n_vc2 = reg_sub["vc_firm_companyid"].nunique()
            print(f"  {param:<35} {len(reg_sub):>8,} {coef:>10.5f} {t_u:>8.2f} {p_u:>8.4f} {sig_u:>4} {t_c:>9.2f} {p_c:>9.4f} {sig_c:>4} {n_vc2:>9,}")

# Run for each window
for var, label, period in windows:
    run_tests(car_df, var, label, period)

# =====================================================================
# STEP 4: Compact summary table
# =====================================================================
print(f"\n\n{'='*80}")
print("COMPACT SUMMARY: All Windows Side by Side")
print(f"{'='*80}")

print(f"\n  {'Test':<30} {'CAR[-60,-1]':>14} {'CAR[-30,-1]':>14} {'CAR[-10,-1]':>14} {'CAR[0,+5]':>14}")
print(f"  {'-'*86}")

for test_name, filter_fn in [
    ("Overall mean", lambda df: df),
    ("Same-industry mean", lambda df: df[df["same_industry"] == 1]),
    ("Diff-industry mean", lambda df: df[df["same_industry"] == 0]),
]:
    row = f"  {test_name:<30}"
    for var, _, _ in windows:
        sub = filter_fn(car_df)[var].dropna()
        if len(sub) > 10:
            row += f" {sub.mean():>13.5f}"
        else:
            row += f" {'N/A':>13}"
    print(row)

print()
for test_name, filter_fn in [
    ("Overall p (unclustered)", lambda df: df),
    ("Same-industry p (unclust)", lambda df: df[df["same_industry"] == 1]),
]:
    row = f"  {test_name:<30}"
    for var, _, _ in windows:
        sub = filter_fn(car_df)[var].dropna()
        if len(sub) > 10:
            t, p = stats.ttest_1samp(sub, 0)
            sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            row += f" {p:>10.4f}{sig:>3}"
        else:
            row += f" {'N/A':>13}"
    print(row)

print()
print(f"  {'N (observations)':<30}", end="")
for var, _, _ in windows:
    n = car_df[var].notna().sum()
    print(f" {n:>13,}", end="")
print()

# =====================================================================
# STEP 5: By event type across windows
# =====================================================================
print(f"\n\n{'='*80}")
print("BY EVENT TYPE: Pre-Event CARs Across Windows (unclustered)")
print(f"{'='*80}")

print(f"\n  {'Event Type':<40} {'N':>6} {'CAR[-60]':>10} {'p':>8} {'CAR[-30]':>10} {'p':>8} {'CAR[-10]':>10} {'p':>8}")
print(f"  {'-'*96}")

for et in sorted(car_df["event_type"].unique()):
    et_sub = car_df[car_df["event_type"] == et]
    n = len(et_sub)
    if n < 30:
        continue
    row = f"  {et[:40]:<40} {n:>6}"
    for var in ["car_60", "car_30", "car_10"]:
        vals = et_sub[var].dropna()
        if len(vals) > 10:
            t, p = stats.ttest_1samp(vals, 0)
            sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            row += f" {vals.mean():>9.5f} {p:>6.4f}{sig:>2}"
        else:
            row += f" {'N/A':>10} {'':>8}"
    print(row)
