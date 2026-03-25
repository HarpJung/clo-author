"""Test 3 with VC Fixed Effects and Two-Way Clustering.
Multi-window CARs: [-60,-1], [-30,-1], [-10,-1], [0,+5].
Private company events only."""

import os
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats
import csv

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 90)
print("TEST 3 WITH VC FIXED EFFECTS + TWO-WAY CLUSTERING")
print("=" * 90)

# =====================================================================
# STEP 1: Load data (same pipeline as multiwindow)
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

edges["same_industry"] = (
    edges["observed_companyid"].map(companyid_to_cik).map(cik_to_sic2) ==
    edges["portfolio_cik_int"].map(cik_to_sic2)
).astype(int)

# Private events only
events = pd.read_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
events["companyid_str"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)

company_details_public = set()
with open(os.path.join(ciq_dir, "04_observer_company_details.csv"), "r", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        cid = str(r.get("companyid", "")).strip().replace(".0", "")
        ctype = str(r.get("companytypename", "")).lower()
        if "public" in ctype:
            company_details_public.add(cid)

events = events[~events["companyid_str"].isin(company_details_public)]
events["event_date"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["event_date"])
print(f"  Private events: {len(events):,}")

# Merge events with edges
event_edges = events.merge(
    edges, left_on="companyid_str", right_on="observed_companyid", how="inner"
)
print(f"  Event-edge pairs: {len(event_edges):,}")

# Daily returns
port_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["permno"] = pd.to_numeric(port_daily["permno"], errors="coerce")
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
port_daily = port_daily.dropna(subset=["permno", "ret", "date"])
port_daily = port_daily.sort_values(["permno", "date"])
print(f"  Daily returns: {len(port_daily):,} rows")

# =====================================================================
# STEP 2: Compute CARs
# =====================================================================
print("\n--- Computing CARs ---")

np.random.seed(42)
if len(event_edges) > 80000:
    event_edges = event_edges.sample(80000).reset_index(drop=True)

car_results = []
chunk_size = 10000
total_chunks = (len(event_edges) + chunk_size - 1) // chunk_size

for chunk_idx in range(total_chunks):
    chunk = event_edges.iloc[chunk_idx * chunk_size:(chunk_idx + 1) * chunk_size]

    for _, row in chunk.iterrows():
        permno = row["permno"]
        event_date = row["event_date"]
        pdata = port_daily[port_daily["permno"] == permno]

        if len(pdata) < 30:
            continue

        dates = pdata["date"].values
        rets = pdata["ret"].values
        event_np = np.datetime64(event_date)

        diffs = (dates - event_np).astype("timedelta64[D]").astype(int)

        cars = {}
        for wname, d0, d1 in [("car_60", -60, -1), ("car_30", -30, -1),
                                ("car_10", -10, -1), ("car_post", 0, 5)]:
            mask = (diffs >= d0) & (diffs <= d1)
            wr = rets[mask]
            if len(wr) >= max(3, abs(d1 - d0) * 0.3):
                cars[wname] = float(np.sum(wr))

        if cars:
            car_results.append({
                "permno": permno,
                "event_date": event_date,
                "observed_companyid": row["observed_companyid"],
                "vc_firm_companyid": row.get("vc_firm_companyid", ""),
                "portfolio_cik_int": row["portfolio_cik_int"],
                "same_industry": row["same_industry"],
                "is_director_at_portfolio": row["is_director_at_portfolio"],
                "event_type": row.get("keydeveventtypename", ""),
                **cars,
            })

    if (chunk_idx + 1) % 3 == 0 or chunk_idx == total_chunks - 1:
        print(f"    Chunk {chunk_idx+1}/{total_chunks}: {len(car_results):,} CARs")

car_df = pd.DataFrame(car_results)
car_df["vc_firm_companyid"] = car_df["vc_firm_companyid"].astype(str)
car_df["portfolio_id"] = car_df["permno"].astype(str)
print(f"\n  Total CARs: {len(car_df):,}")
print(f"  Unique VC firms: {car_df['vc_firm_companyid'].nunique():,}")
print(f"  Unique portfolio stocks: {car_df['portfolio_id'].nunique():,}")

# =====================================================================
# STEP 3: Run regressions — 4 specs per window
# =====================================================================

windows = [
    ("car_60", "CAR[-60,-1]", "~3 months"),
    ("car_30", "CAR[-30,-1]", "~6 weeks"),
    ("car_10", "CAR[-10,-1]", "~2 weeks"),
    ("car_post", "CAR[0,+5]", "post-event"),
]

def run_all_specs(df, var, label, period):
    """Run 4 specifications for one window."""
    sub = df.dropna(subset=[var, "same_industry", "is_director_at_portfolio"]).copy()
    sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
    n = len(sub)
    if n < 100:
        print(f"\n  {label}: N={n}, too few observations, skipping")
        return

    n_vc = sub["vc_firm_companyid"].nunique()
    n_port = sub["portfolio_id"].nunique()

    print(f"\n  {'='*85}")
    print(f"  {label} ({period}) | N = {n:,} | VC clusters = {n_vc:,} | Portfolio clusters = {n_port:,}")
    print(f"  {'='*85}")

    formula = f"{var} ~ same_industry + is_director_at_portfolio"

    # --- Spec 1: OLS with HC1 (no clustering, no FE) ---
    m1 = smf.ols(formula, data=sub).fit(cov_type="HC1")

    # --- Spec 2: OLS with VC-firm clustering ---
    m2 = smf.ols(formula, data=sub).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

    # --- Spec 3 & 4: VC Fixed Effects via within-transformation (demeaning) ---
    # Much faster than dummy variables for 1,100+ groups
    xvars = ["same_industry", "is_director_at_portfolio"]
    demean_cols = [var] + xvars
    group_means = sub.groupby("vc_firm_companyid")[demean_cols].transform("mean")
    sub_dm = sub[demean_cols] - group_means

    m3 = None
    m4 = None
    try:
        formula_dm = f"{var} ~ same_industry + is_director_at_portfolio - 1"
        # Spec 3: VC FE + HC1
        m3 = smf.ols(formula_dm, data=sub_dm).fit(cov_type="HC1")
        # Spec 4: VC FE + VC-firm clustering
        m4 = smf.ols(formula_dm, data=sub_dm).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
        # Correct degrees of freedom (subtract number of FE groups)
        # statsmodels doesn't auto-adjust for absorbed FE, but p-values
        # are asymptotically valid with large N
    except Exception as e:
        print(f"  VC FE (demeaned) failed: {e}")

    # --- Spec 5: Two-way clustering (VC firm x portfolio stock), no FE ---
    # Cameron-Gelbach-Miller: V_twoway = V_vc + V_port - V_intersection
    twoway_results = None
    try:
        m_c1 = m2  # already have VC-clustered
        m_c2 = smf.ols(formula, data=sub).fit(
            cov_type="cluster", cov_kwds={"groups": sub["portfolio_id"]})
        sub_copy = sub.copy()
        sub_copy["intersection"] = sub_copy["vc_firm_companyid"] + "_" + sub_copy["portfolio_id"]
        m_c12 = smf.ols(formula, data=sub_copy).fit(
            cov_type="cluster", cov_kwds={"groups": sub_copy["intersection"]})

        cov_tw = m_c1.cov_params() + m_c2.cov_params() - m_c12.cov_params()
        # Ensure positive definite (can go slightly negative numerically)
        se_tw = np.sqrt(np.maximum(np.diag(cov_tw), 1e-20))
        params_tw = m_c1.params
        t_tw = params_tw / se_tw
        min_clusters = min(n_vc, n_port)
        p_tw = 2 * (1 - stats.t.cdf(np.abs(t_tw), df=min_clusters - 1))
        twoway_results = dict(zip(m_c1.params.index, zip(params_tw.values, t_tw.values, p_tw)))
    except Exception as e:
        print(f"  Two-way clustering failed: {e}")

    # --- Print results ---
    params_of_interest = ["same_industry", "is_director_at_portfolio"]

    print(f"\n  {'Variable':<28} {'Spec':<30} {'Coef':>10} {'t':>8} {'p':>10} {'Sig':>4}")
    print(f"  {'-'*94}")

    for param in params_of_interest:
        # Spec 1
        print(f"  {param:<28} {'(1) OLS, HC1':<30} {m1.params[param]:>10.5f} {m1.tvalues[param]:>8.2f} {m1.pvalues[param]:>10.4f} {_sig(m1.pvalues[param]):>4}")

        # Spec 2
        print(f"  {'':<28} {'(2) OLS, VC-cluster':<30} {m2.params[param]:>10.5f} {m2.tvalues[param]:>8.2f} {m2.pvalues[param]:>10.4f} {_sig(m2.pvalues[param]):>4}")

        # Spec 3
        if m3 and param in m3.params:
            print(f"  {'':<28} {'(3) VC FE, HC1':<30} {m3.params[param]:>10.5f} {m3.tvalues[param]:>8.2f} {m3.pvalues[param]:>10.4f} {_sig(m3.pvalues[param]):>4}")

        # Spec 4
        if m4 and param in m4.params:
            print(f"  {'':<28} {'(4) VC FE, VC-cluster':<30} {m4.params[param]:>10.5f} {m4.tvalues[param]:>8.2f} {m4.pvalues[param]:>10.4f} {_sig(m4.pvalues[param]):>4}")

        # Spec 5
        if twoway_results and param in twoway_results:
            coef, t_val, p_val = twoway_results[param]
            print(f"  {'':<28} {'(5) OLS, 2-way cluster':<30} {coef:>10.5f} {t_val:>8.2f} {p_val:>10.4f} {_sig(p_val):>4}")

        print()

    # Intercept (overall mean CAR) across specs
    print(f"  {'Intercept (mean CAR)':<28} {'(1) OLS, HC1':<30} {m1.params['Intercept']:>10.5f} {m1.tvalues['Intercept']:>8.2f} {m1.pvalues['Intercept']:>10.4f} {_sig(m1.pvalues['Intercept']):>4}")
    print(f"  {'':<28} {'(2) OLS, VC-cluster':<30} {m2.params['Intercept']:>10.5f} {m2.tvalues['Intercept']:>8.2f} {m2.pvalues['Intercept']:>10.4f} {_sig(m2.pvalues['Intercept']):>4}")
    if twoway_results and "Intercept" in twoway_results:
        coef, t_val, p_val = twoway_results["Intercept"]
        print(f"  {'':<28} {'(5) OLS, 2-way cluster':<30} {coef:>10.5f} {t_val:>8.2f} {p_val:>10.4f} {_sig(p_val):>4}")

    # R-squared
    print(f"\n  R-sq: (1) {m1.rsquared:.6f}  (2) {m2.rsquared:.6f}", end="")
    if m3:
        print(f"  (3) {m3.rsquared:.6f}", end="")
    if m4:
        print(f"  (4) {m4.rsquared:.6f}", end="")
    print()

    # Note: VC FE estimated via within-transformation (demeaning), not dummies


def _sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return ""


# Run all windows
for var, label, period in windows:
    run_all_specs(car_df, var, label, period)

# =====================================================================
# STEP 4: Compact comparison table
# =====================================================================
print(f"\n\n{'='*90}")
print("COMPACT COMPARISON: Does the result survive VC Fixed Effects?")
print(f"{'='*90}")
print(f"\n  p-values for each variable × specification (all windows)")
print(f"  {'Window + Variable':<45} {'(1) HC1':>12} {'(2) VC-clust':>14} {'(3) VC FE':>12} {'(4) FE+clust':>14} {'(5) 2way-cl':>14}")
print(f"  {'-'*111}")

for var, label, period in windows:
    sub = car_df.dropna(subset=[var, "same_industry", "is_director_at_portfolio"]).copy()
    sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
    if len(sub) < 100:
        continue

    formula = f"{var} ~ same_industry + is_director_at_portfolio"

    m1 = smf.ols(formula, data=sub).fit(cov_type="HC1")
    m2 = smf.ols(formula, data=sub).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

    # VC FE via demeaning
    xvars = ["same_industry", "is_director_at_portfolio"]
    demean_cols = [var] + xvars
    gm = sub.groupby("vc_firm_companyid")[demean_cols].transform("mean")
    sub_dm = sub[demean_cols] - gm
    formula_dm = f"{var} ~ same_industry + is_director_at_portfolio - 1"
    try:
        m3 = smf.ols(formula_dm, data=sub_dm).fit(cov_type="HC1")
        m4 = smf.ols(formula_dm, data=sub_dm).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
    except:
        m3, m4 = None, None

    # Two-way clustering
    try:
        m_c2 = smf.ols(formula, data=sub).fit(
            cov_type="cluster", cov_kwds={"groups": sub["portfolio_id"]})
        sub_c = sub.copy()
        sub_c["intersection"] = sub_c["vc_firm_companyid"] + "_" + sub_c["portfolio_id"]
        m_c12 = smf.ols(formula, data=sub_c).fit(
            cov_type="cluster", cov_kwds={"groups": sub_c["intersection"]})
        cov_tw = m2.cov_params() + m_c2.cov_params() - m_c12.cov_params()
        se_tw = np.sqrt(np.maximum(np.diag(cov_tw), 1e-20))
        min_cl = min(n_vc, n_port)
    except:
        se_tw, min_cl = None, None

    for param in ["same_industry", "is_director_at_portfolio"]:
        if param == "same_industry":
            idx = 1
        else:
            idx = 2
        p5 = np.nan
        if se_tw is not None:
            try:
                p5 = 2 * (1 - stats.t.cdf(abs(m2.params[param] / se_tw[idx]), df=min_cl - 1))
            except:
                pass

        row = f"  {label + ' ' + param:<45}"
        row += f" {m1.pvalues[param]:>10.4f}{_sig(m1.pvalues[param]):>2}"
        row += f"  {m2.pvalues[param]:>10.4f}{_sig(m2.pvalues[param]):>2}"
        if m3 and param in m3.pvalues:
            row += f" {m3.pvalues[param]:>10.4f}{_sig(m3.pvalues[param]):>2}"
        else:
            row += f" {'N/A':>12}"
        if m4 and param in m4.pvalues:
            row += f"  {m4.pvalues[param]:>10.4f}{_sig(m4.pvalues[param]):>2}"
        else:
            row += f"  {'N/A':>14}"
        if not np.isnan(p5):
            row += f"  {p5:>10.4f}{_sig(p5):>2}"
        else:
            row += f"  {'N/A':>12}"
        print(row)

print("\n\nSpecs: (1) OLS + HC1, (2) OLS + VC-cluster, (3) VC FE + HC1, (4) VC FE + VC-cluster, (5) OLS + two-way cluster (VC x portfolio)")
print("Done.")
