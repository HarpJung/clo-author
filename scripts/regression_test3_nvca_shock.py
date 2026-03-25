"""Test 3 + NVCA 2020 Shock: Does information spillover strengthen
after the NVCA removed fiduciary language from observer provisions?

Hypothesis: Post-2020, observers feel less constrained by fiduciary
obligations → share more freely with VC firm → stronger spillover.
Prediction: β(same_industry × post_2020) > 0.

Specifications:
  (A) Full sample: 2015-2024 with post_2020 = {2020,2021,...,2024}
  (B) Excluding COVID: 2015-2019 vs 2021-2024 (drop 2020)
  (C) Placebo: fake shock at 2018 (2015-2017 vs 2018-2019)

For each: OLS, VC-cluster, VC FE, VC FE + cluster.
"""

import os, numpy as np, pandas as pd, csv
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 95)
print("TEST 3 + NVCA 2020 SHOCK: Fiduciary Language Removal")
print("=" * 95)

# =====================================================================
# STEP 1: Load data
# =====================================================================
print("\n--- Loading data ---")

edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")

port_xwalk = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
port_xwalk["cik_int"] = pd.to_numeric(port_xwalk["cik"], errors="coerce")
edges = edges.merge(
    port_xwalk.drop_duplicates("cik_int", keep="first")[["cik_int", "permno"]].rename(
        columns={"cik_int": "portfolio_cik_int"}),
    on="portfolio_cik_int", how="inner")
edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce")
edges["is_director_at_portfolio"] = edges["portfolio_title"].str.contains(
    "Director|Chairman|Board Member", case=False, na=False).astype(int)

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

# Restrict to 2015-2024 (CRSP coverage)
events = events[(events["event_year"] >= 2015) & (events["event_year"] <= 2024)]
print(f"  Private events (2015-2024): {len(events):,}")
print(f"  Pre-2020: {len(events[events['event_year'] < 2020]):,}")
print(f"  Post-2020: {len(events[events['event_year'] >= 2020]):,}")

event_edges = events.merge(edges, left_on="companyid_str", right_on="observed_companyid", how="inner")
print(f"  Event-edge pairs: {len(event_edges):,}")

# Daily returns
port_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["permno"] = pd.to_numeric(port_daily["permno"], errors="coerce")
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
port_daily = port_daily.dropna(subset=["permno", "ret", "date"]).sort_values(["permno", "date"])
print(f"  Daily returns: {len(port_daily):,} rows")

# =====================================================================
# STEP 2: Compute CARs (use ALL event-edges, no subsampling)
# =====================================================================
print("\n--- Computing CARs (full sample, no subsampling) ---")

# For speed, sample if too large but keep more than before
np.random.seed(42)
if len(event_edges) > 120000:
    event_edges = event_edges.sample(120000).reset_index(drop=True)
    print(f"  Sampled down to {len(event_edges):,}")

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
        for wn, d0, d1 in [("car_60", -60, -1), ("car_30", -30, -1),
                            ("car_10", -10, -1), ("car_post", 0, 5)]:
            mask = (diffs >= d0) & (diffs <= d1)
            wr = rets[mask]
            if len(wr) >= max(3, abs(d1 - d0) * 0.3):
                cars[wn] = float(np.sum(wr))

        if cars:
            car_results.append({
                "vc_firm_companyid": str(row.get("vc_firm_companyid", "")),
                "portfolio_id": str(permno),
                "same_industry": row["same_industry"],
                "is_director_at_portfolio": row["is_director_at_portfolio"],
                "event_year": row["event_date"].year if hasattr(row["event_date"], "year") else pd.Timestamp(row["event_date"]).year,
                **cars,
            })
    if (chunk_idx + 1) % 4 == 0 or chunk_idx == total_chunks - 1:
        print(f"    Chunk {chunk_idx+1}/{total_chunks}: {len(car_results):,} CARs")

car_df = pd.DataFrame(car_results)
car_df = car_df[car_df["vc_firm_companyid"] != ""].reset_index(drop=True)
car_df["event_year"] = car_df["event_year"].astype(int)
print(f"\n  Total CARs: {len(car_df):,}")
print(f"  Unique VC firms: {car_df['vc_firm_companyid'].nunique():,}")
print(f"  Pre-2020 CARs: {len(car_df[car_df['event_year'] < 2020]):,}")
print(f"  Post-2020 CARs: {len(car_df[car_df['event_year'] >= 2020]):,}")

# Year distribution
print("\n  CARs by year:")
for yr, n in car_df.groupby("event_year").size().items():
    print(f"    {yr}: {n:,}")


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


# =====================================================================
# STEP 3: Regression specifications
# =====================================================================

windows = [
    ("car_60", "CAR[-60,-1]"),
    ("car_30", "CAR[-30,-1]"),
    ("car_10", "CAR[-10,-1]"),
    ("car_post", "CAR[0,+5]"),
]

def run_nvca_test(df, var, label, sample_label):
    """Run the NVCA shock interaction test for one window."""
    sub = df.dropna(subset=[var, "same_industry", "post"]).copy()
    sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
    n = len(sub)
    n_pre = (sub["post"] == 0).sum()
    n_post = (sub["post"] == 1).sum()
    n_vc = sub["vc_firm_companyid"].nunique()

    if n < 100:
        return

    sub["same_x_post"] = sub["same_industry"] * sub["post"]

    print(f"\n  {'='*90}")
    print(f"  {label} | {sample_label} | N={n:,} (pre={n_pre:,}, post={n_post:,}) | VC={n_vc:,}")
    print(f"  {'='*90}")

    # --- Spec 1: OLS + HC1 ---
    formula = f"{var} ~ same_industry + post + same_x_post"
    m1 = smf.ols(formula, data=sub).fit(cov_type="HC1")

    # --- Spec 2: OLS + VC-cluster ---
    m2 = smf.ols(formula, data=sub).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

    # --- Spec 3: VC FE + HC1 (demeaning) ---
    xvars = [var, "same_industry", "post", "same_x_post"]
    gm = sub.groupby("vc_firm_companyid")[xvars].transform("mean")
    sub_dm = sub[xvars] - gm
    formula_dm = f"{var} ~ same_industry + post + same_x_post - 1"
    try:
        m3 = smf.ols(formula_dm, data=sub_dm).fit(cov_type="HC1")
    except:
        m3 = None

    # --- Spec 4: VC FE + VC-cluster ---
    try:
        m4 = smf.ols(formula_dm, data=sub_dm).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
    except:
        m4 = None

    # Print results
    params = ["same_industry", "post", "same_x_post"]
    param_labels = ["same_industry (baseline effect)", "post (level shift)", "same_ind x post (KEY)"]

    print(f"\n  {'Variable':<35} {'(1) HC1':>14} {'(2) VC-cl':>14} {'(3) FE+HC1':>14} {'(4) FE+cl':>14}")
    print(f"  {'-'*91}")

    for param, plabel in zip(params, param_labels):
        row = f"  {plabel:<35}"
        # Spec 1
        row += f" {m1.params[param]:>+8.5f}{sig(m1.pvalues[param])}"
        # Spec 2
        row += f" {m2.params[param]:>+8.5f}{sig(m2.pvalues[param])}"
        # Spec 3
        if m3 and param in m3.params:
            row += f" {m3.params[param]:>+8.5f}{sig(m3.pvalues[param])}"
        else:
            row += f" {'N/A':>14}"
        # Spec 4
        if m4 and param in m4.params:
            row += f" {m4.params[param]:>+8.5f}{sig(m4.pvalues[param])}"
        else:
            row += f" {'N/A':>14}"
        print(row)

    # Intercept for OLS specs
    print(f"  {'Intercept':<35} {m1.params['Intercept']:>+8.5f}{sig(m1.pvalues['Intercept'])} {m2.params['Intercept']:>+8.5f}{sig(m2.pvalues['Intercept'])}")

    # Marginal effects: total same_industry effect in each period
    beta_same = m2.params["same_industry"]
    beta_int = m2.params["same_x_post"]
    pre_effect = beta_same
    post_effect = beta_same + beta_int

    print(f"\n  Implied same-industry spillover:")
    print(f"    Pre-2020:  {pre_effect:>+.5f}")
    print(f"    Post-2020: {post_effect:>+.5f}")
    print(f"    Change:    {beta_int:>+.5f} (this is the interaction)")

    # Also show subsample means for intuition
    for period_name, period_val in [("Pre-2020", 0), ("Post-2020", 1)]:
        for ind_name, ind_val in [("Same-ind", 1), ("Diff-ind", 0)]:
            cell = sub[(sub["post"] == period_val) & (sub["same_industry"] == ind_val)][var]
            if len(cell) > 10:
                t, p = stats.ttest_1samp(cell, 0)
                print(f"    {period_name} {ind_name}: mean={cell.mean():>+.5f}, N={len(cell):,}, p={p:.4f}{sig(p)}")

    return {
        "label": label,
        "sample": sample_label,
        "n": n,
        "interaction_coef_vc_cluster": m2.params.get("same_x_post", np.nan),
        "interaction_p_vc_cluster": m2.pvalues.get("same_x_post", np.nan),
        "interaction_coef_fe_cluster": m4.params.get("same_x_post", np.nan) if m4 else np.nan,
        "interaction_p_fe_cluster": m4.pvalues.get("same_x_post", np.nan) if m4 else np.nan,
    }


# =====================================================================
# (A) FULL SAMPLE: 2015-2024, post = year >= 2020
# =====================================================================
print("\n\n" + "=" * 95)
print("(A) FULL SAMPLE: 2015-2024, shock at 2020")
print("=" * 95)

car_df["post"] = (car_df["event_year"] >= 2020).astype(int)
results_a = []
for var, label in windows:
    r = run_nvca_test(car_df, var, label, "Full 2015-2024")
    if r:
        results_a.append(r)

# =====================================================================
# (B) EXCLUDING COVID: 2015-2019 vs 2021-2024 (drop 2020)
# =====================================================================
print("\n\n" + "=" * 95)
print("(B) EXCLUDING COVID YEAR: 2015-2019 vs 2021-2024")
print("=" * 95)

car_no_covid = car_df[car_df["event_year"] != 2020].copy()
car_no_covid["post"] = (car_no_covid["event_year"] >= 2021).astype(int)
print(f"  Dropped 2020: {len(car_df) - len(car_no_covid):,} obs")

results_b = []
for var, label in windows:
    r = run_nvca_test(car_no_covid, var, label, "Ex-COVID 2015-19 vs 2021-24")
    if r:
        results_b.append(r)

# =====================================================================
# (C) PLACEBO: fake shock at 2018 (2015-2017 vs 2018-2019)
# =====================================================================
print("\n\n" + "=" * 95)
print("(C) PLACEBO TEST: fake shock at 2018 (pre-period only)")
print("=" * 95)

car_placebo = car_df[car_df["event_year"] <= 2019].copy()
car_placebo["post"] = (car_placebo["event_year"] >= 2018).astype(int)
print(f"  Using 2015-2019 only, fake break at 2018")
print(f"  'Pre': 2015-2017, 'Post': 2018-2019")

results_c = []
for var, label in windows:
    r = run_nvca_test(car_placebo, var, label, "Placebo 2015-17 vs 2018-19")
    if r:
        results_c.append(r)

# =====================================================================
# STEP 4: Compact summary
# =====================================================================
print("\n\n" + "=" * 95)
print("COMPACT SUMMARY: same_industry x post interaction coefficient")
print("=" * 95)

print(f"\n  {'Window':<16} {'Sample':<30} {'Coef(VC-cl)':>14} {'p':>10} {'Coef(FE+cl)':>14} {'p':>10}")
print(f"  {'-'*94}")

for results, sample_tag in [(results_a, "Full"), (results_b, "Ex-COVID"), (results_c, "Placebo")]:
    for r in results:
        coef1 = r["interaction_coef_vc_cluster"]
        p1 = r["interaction_p_vc_cluster"]
        coef2 = r["interaction_coef_fe_cluster"]
        p2 = r["interaction_p_fe_cluster"]
        print(f"  {r['label']:<16} {sample_tag:<30} {coef1:>+12.5f}{sig(p1)} {p1:>10.4f} {coef2:>+12.5f}{sig(p2) if not np.isnan(p2) else '   '} {p2:>10.4f}")
    print()

print("\nInterpretation:")
print("  Positive interaction → spillover STRONGER after NVCA removed fiduciary language")
print("  Negative interaction → spillover WEAKER after change")
print("  Placebo should show NO significant interaction (no real shock at 2018)")
print("\nDone.")
