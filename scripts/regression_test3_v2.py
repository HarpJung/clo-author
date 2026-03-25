"""Test 3 v2: Information Leakage — Fixed industry matching and clustering."""

import os
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 70)
print("TEST 3 v2: Information Leakage - Cross-Portfolio Return Spillovers")
print("=" * 70)

# =====================================================================
# STEP 1: Load data
# =====================================================================
print("\n--- Step 1: Loading data ---")

edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
port_crsp = pd.read_csv(os.path.join(panel_c_dir, "04_portfolio_crsp_monthly.csv"))
port_crsp["date"] = pd.to_datetime(port_crsp["date"])
port_crsp["ret"] = pd.to_numeric(port_crsp["ret"], errors="coerce")

port_xwalk = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
port_xwalk["cik_int"] = pd.to_numeric(port_xwalk["cik"], errors="coerce")
port_xwalk = port_xwalk.drop_duplicates("cik_int", keep="first")

industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]

# CIQ-CIK crosswalk for observed companies
ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["companyid_str"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.lstrip("0"), errors="coerce")

events = pd.read_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)

print(f"  Edges: {len(edges):,} | Portfolio CRSP: {len(port_crsp):,} rows")
print(f"  CIQ-CIK crosswalk: {len(ciq_xwalk):,} | Industry: {len(industry):,}")

# =====================================================================
# STEP 2: Build industry mapping for BOTH observed and portfolio cos
# =====================================================================
print("\n--- Step 2: Building industry mappings ---")

# Map: CIK -> SIC2
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))

# Map: CIQ companyid -> CIK -> SIC2 (for observed companies)
companyid_to_cik = dict(zip(ciq_xwalk["companyid_str"], ciq_xwalk["cik_int"]))

# Normalize edge IDs
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")

# Add SIC codes
edges["observed_cik"] = edges["observed_companyid"].map(companyid_to_cik)
edges["observed_sic2"] = edges["observed_cik"].map(cik_to_sic2)
edges["portfolio_sic2"] = edges["portfolio_cik_int"].map(cik_to_sic2)
edges["same_industry"] = (
    edges["observed_sic2"].notna() &
    edges["portfolio_sic2"].notna() &
    (edges["observed_sic2"] == edges["portfolio_sic2"])
).astype(int)

obs_with_sic = edges["observed_sic2"].notna().sum()
port_with_sic = edges["portfolio_sic2"].notna().sum()
both_sic = (edges["observed_sic2"].notna() & edges["portfolio_sic2"].notna()).sum()
same_ind = edges["same_industry"].sum()

print(f"  Observed companies with SIC: {obs_with_sic:,} / {len(edges):,}")
print(f"  Portfolio companies with SIC: {port_with_sic:,} / {len(edges):,}")
print(f"  Both have SIC: {both_sic:,}")
print(f"  Same industry (SIC2): {same_ind:,}")

# =====================================================================
# STEP 3: Add PERMNO to edges and classify connection types
# =====================================================================
print("\n--- Step 3: Preparing edges ---")

edges = edges.merge(
    port_xwalk[["cik_int", "permno"]].rename(columns={"cik_int": "portfolio_cik_int"}),
    on="portfolio_cik_int", how="inner"
)
edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce")

# Classify connection type from the observer's title at the portfolio company
edges["is_director_at_portfolio"] = edges["portfolio_title"].str.contains(
    "Director|Chairman|Board Member", case=False, na=False
).astype(int)
edges["is_observer_at_portfolio"] = edges["portfolio_title"].str.contains(
    "Observer", case=False, na=False
).astype(int)
edges["is_other_at_portfolio"] = (
    (edges["is_director_at_portfolio"] == 0) &
    (edges["is_observer_at_portfolio"] == 0)
).astype(int)

print(f"  Edges with PERMNO: {len(edges):,}")
print(f"  Role at portfolio company:")
print(f"    Director:  {edges['is_director_at_portfolio'].sum():,}")
print(f"    Observer:  {edges['is_observer_at_portfolio'].sum():,}")
print(f"    Other:     {edges['is_other_at_portfolio'].sum():,}")
print(f"  Same industry: {edges['same_industry'].sum():,}")

# =====================================================================
# STEP 4: Build event-month spillover sample
# =====================================================================
print("\n--- Step 4: Building spillover sample ---")

earnings = events[events["keydeveventtypename"] == "Announcements of Earnings"].copy()
earnings["ym"] = earnings["announcedate"].dt.to_period("M")
earnings_ym = earnings[["companyid", "ym"]].drop_duplicates()
print(f"  Earnings event-months: {len(earnings_ym):,}")

# Market return for abnormal return
port_crsp["ym"] = port_crsp["date"].dt.to_period("M")
mkt_ret = port_crsp.groupby("ym")["ret"].mean().reset_index()
mkt_ret.columns = ["ym", "mkt_ret"]
port_crsp = port_crsp.merge(mkt_ret, on="ym")
port_crsp["abnormal_ret"] = port_crsp["ret"] - port_crsp["mkt_ret"]

# Merge: edges x events x returns
spillover = edges.merge(earnings_ym, left_on="observed_companyid", right_on="companyid", how="inner")
print(f"  Edge-event pairs: {len(spillover):,}")

spillover = spillover.merge(
    port_crsp[["permno", "ym", "ret", "abnormal_ret"]],
    on=["permno", "ym"], how="inner"
)
print(f"  After matching returns: {len(spillover):,}")

# =====================================================================
# STEP 5: Summary statistics and t-tests
# =====================================================================
print(f"\n--- Step 5: Spillover analysis ---")

print(f"\n  Sample: {len(spillover):,} observations")
print(f"  Unique portfolio companies: {spillover['permno'].nunique():,}")
print(f"  Unique observers: {spillover['observer_personid'].nunique():,}")
print(f"  Unique event-months: {spillover['ym'].nunique():,}")

print(f"\n  Abnormal return: mean={spillover['abnormal_ret'].mean():.6f}, "
      f"median={spillover['abnormal_ret'].median():.6f}, "
      f"std={spillover['abnormal_ret'].std():.6f}")

# Overall: is there a spillover?
t, p = stats.ttest_1samp(spillover["abnormal_ret"].dropna(), 0)
sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
print(f"\n  H0: Abnormal return = 0")
print(f"  t={t:.4f}, p={p:.6f} {sig}")

# By connection type
print(f"\n  --- By connection type at portfolio company ---")
for role, label in [(1, "Director"), (0, "Non-Director")]:
    sub = spillover.loc[spillover["is_director_at_portfolio"] == role, "abnormal_ret"].dropna()
    if len(sub) > 10:
        t, p = stats.ttest_1samp(sub, 0)
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"    {label:20} mean={sub.mean():>9.6f}  N={len(sub):>6,}  t={t:>6.2f}  p={p:.4f} {sig}")

# Director vs non-director difference
dir_ret = spillover.loc[spillover["is_director_at_portfolio"] == 1, "abnormal_ret"].dropna()
nondir_ret = spillover.loc[spillover["is_director_at_portfolio"] == 0, "abnormal_ret"].dropna()
if len(dir_ret) > 10 and len(nondir_ret) > 10:
    t, p = stats.ttest_ind(dir_ret, nondir_ret, equal_var=False)
    print(f"    {'Difference':20} diff={dir_ret.mean() - nondir_ret.mean():>9.6f}  t={t:>6.2f}  p={p:.4f}")

# By same industry
if spillover["same_industry"].sum() > 10:
    print(f"\n  --- By industry overlap ---")
    for ind, label in [(1, "Same Industry"), (0, "Different Industry")]:
        sub = spillover.loc[spillover["same_industry"] == ind, "abnormal_ret"].dropna()
        if len(sub) > 10:
            t, p = stats.ttest_1samp(sub, 0)
            sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            print(f"    {label:20} mean={sub.mean():>9.6f}  N={len(sub):>6,}  t={t:>6.2f}  p={p:.4f} {sig}")

    same = spillover.loc[spillover["same_industry"] == 1, "abnormal_ret"].dropna()
    diff = spillover.loc[spillover["same_industry"] == 0, "abnormal_ret"].dropna()
    if len(same) > 10 and len(diff) > 10:
        t, p = stats.ttest_ind(same, diff, equal_var=False)
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"    {'Same vs Diff':20} diff={same.mean() - diff.mean():>9.6f}  t={t:>6.2f}  p={p:.4f} {sig}")

# =====================================================================
# STEP 6: Regressions with proper clustering
# =====================================================================
print(f"\n\n--- Step 6: Regressions ---")

# Clean up for regression
reg = spillover.dropna(subset=["abnormal_ret", "same_industry", "is_director_at_portfolio"]).copy()
reg = reg.reset_index(drop=True)  # critical for clustering alignment

print(f"  Regression sample: {len(reg):,}")

# Model 1: Baseline - just intercept (is there a spillover at all?)
m1 = smf.ols("abnormal_ret ~ 1", data=reg).fit(
    cov_type="cluster", cov_kwds={"groups": reg["observer_personid"]})
print(f"\n  Model 1: Intercept only (clustered by observer)")
print(f"    Intercept: {m1.params['Intercept']:.6f} (t={m1.tvalues['Intercept']:.2f}, p={m1.pvalues['Intercept']:.4f})")

# Model 2: Director connection
m2 = smf.ols("abnormal_ret ~ is_director_at_portfolio", data=reg).fit(
    cov_type="cluster", cov_kwds={"groups": reg["observer_personid"]})
print(f"\n  Model 2: Director connection")
print(f"    is_director: {m2.params['is_director_at_portfolio']:.6f} "
      f"(t={m2.tvalues['is_director_at_portfolio']:.2f}, p={m2.pvalues['is_director_at_portfolio']:.4f})")

# Model 3: Same industry
if reg["same_industry"].sum() > 0:
    m3 = smf.ols("abnormal_ret ~ same_industry", data=reg).fit(
        cov_type="cluster", cov_kwds={"groups": reg["observer_personid"]})
    print(f"\n  Model 3: Same industry")
    print(f"    same_industry: {m3.params['same_industry']:.6f} "
          f"(t={m3.tvalues['same_industry']:.2f}, p={m3.pvalues['same_industry']:.4f})")

    # Model 4: Both
    m4 = smf.ols("abnormal_ret ~ is_director_at_portfolio + same_industry", data=reg).fit(
        cov_type="cluster", cov_kwds={"groups": reg["observer_personid"]})
    print(f"\n  Model 4: Director + Same industry")
    for param in ["is_director_at_portfolio", "same_industry"]:
        print(f"    {param}: {m4.params[param]:.6f} "
              f"(t={m4.tvalues[param]:.2f}, p={m4.pvalues[param]:.4f})")

    # Model 5: Interaction
    m5 = smf.ols("abnormal_ret ~ is_director_at_portfolio * same_industry", data=reg).fit(
        cov_type="cluster", cov_kwds={"groups": reg["observer_personid"]})
    print(f"\n  Model 5: Interaction")
    for param in m5.params.index:
        if param != "Intercept":
            sig = "***" if m5.pvalues[param] < 0.01 else "**" if m5.pvalues[param] < 0.05 else "*" if m5.pvalues[param] < 0.10 else ""
            print(f"    {param:45} {m5.params[param]:>10.6f} (t={m5.tvalues[param]:>6.2f}, p={m5.pvalues[param]:.4f}) {sig}")

    print(f"\n  R-squared: M1={m1.rsquared:.6f}, M2={m2.rsquared:.6f}, "
          f"M3={m3.rsquared:.6f}, M4={m4.rsquared:.6f}, M5={m5.rsquared:.6f}")
else:
    print("\n  No same-industry pairs — skipping industry models")
    print(f"\n  R-squared: M1={m1.rsquared:.6f}, M2={m2.rsquared:.6f}")

print(f"  N: {int(m1.nobs):,}")
print(f"  Clusters (observers): {reg['observer_personid'].nunique():,}")

# =====================================================================
# STEP 7: Placebo test — random event dates
# =====================================================================
print(f"\n\n--- Step 7: Placebo test (random event dates) ---")

np.random.seed(42)
n_placebo = 1000
placebo_t_stats = []

for _ in range(n_placebo):
    # Shuffle the event dates
    shuffled = reg.copy()
    shuffled["abnormal_ret"] = np.random.permutation(shuffled["abnormal_ret"].values)
    m_placebo = smf.ols("abnormal_ret ~ 1", data=shuffled).fit()
    placebo_t_stats.append(m_placebo.tvalues["Intercept"])

actual_t = m1.tvalues["Intercept"]
p_perm = np.mean(np.abs(placebo_t_stats) >= np.abs(actual_t))
print(f"  Actual t-stat: {actual_t:.4f}")
print(f"  Permutation p-value (1000 draws): {p_perm:.4f}")
print(f"  Placebo t-stat distribution: mean={np.mean(placebo_t_stats):.4f}, std={np.std(placebo_t_stats):.4f}")

# =====================================================================
# SUMMARY
# =====================================================================
print(f"\n\n{'='*70}")
print("TEST 3 v2 SUMMARY")
print(f"{'='*70}")
print(f"  Spillover observations: {len(reg):,}")
print(f"  Same-industry pairs: {reg['same_industry'].sum():,}")
print(f"  Overall spillover: mean={reg['abnormal_ret'].mean():.6f} (p={m1.pvalues['Intercept']:.4f})")
print(f"  Permutation test: p={p_perm:.4f}")
