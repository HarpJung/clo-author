"""Test 3: Information Leakage — Cross-Portfolio Return Spillovers
Compare: observer-connected vs director-connected portfolio company pairs
Events at observed companies -> abnormal returns at VC's other portfolio companies
"""

import os
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 70)
print("TEST 3: Information Leakage — Cross-Portfolio Return Spillovers")
print("=" * 70)

# =====================================================================
# STEP 1: Load network and returns data
# =====================================================================
print("\n--- Step 1: Loading data ---")

# Network edges
edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
print(f"  Network edges: {len(edges):,}")
print(f"  Unique observers: {edges['observer_personid'].nunique():,}")
print(f"  Unique observed companies: {edges['observed_companyid'].nunique():,}")
print(f"  Unique portfolio companies: {edges['portfolio_companyid'].nunique():,}")

# Portfolio company CRSP returns
port_crsp = pd.read_csv(os.path.join(panel_c_dir, "04_portfolio_crsp_monthly.csv"))
port_crsp["date"] = pd.to_datetime(port_crsp["date"])
port_crsp["ret"] = pd.to_numeric(port_crsp["ret"], errors="coerce")
print(f"  Portfolio CRSP returns: {len(port_crsp):,} rows | {port_crsp['permno'].nunique():,} securities")

# PERMNO crosswalk for portfolio companies
port_xwalk = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
port_xwalk["cik_int"] = pd.to_numeric(port_xwalk["cik"], errors="coerce")
port_xwalk = port_xwalk.drop_duplicates("cik_int", keep="first")

# Industry codes
industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
industry_map = dict(zip(industry["cik_int"], industry["sic2"]))

# CIQ Key Dev events for observed companies
events = pd.read_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
print(f"  Events at observed companies: {len(events):,}")

# =====================================================================
# STEP 2: Build event study sample
# =====================================================================
print("\n--- Step 2: Building event study sample ---")

# Focus on earnings announcements (most frequent, well-defined event)
earnings = events[events["keydeveventtypename"] == "Announcements of Earnings"].copy()
exec_changes = events[events["keydeveventtypename"] == "Executive/Board Changes - Other"].copy()

print(f"  Earnings announcements: {len(earnings):,}")
print(f"  Executive changes: {len(exec_changes):,}")

# Map portfolio companies to PERMNOs
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")
edges = edges.merge(
    port_xwalk[["cik_int", "permno"]].rename(columns={"cik_int": "portfolio_cik_int"}),
    on="portfolio_cik_int", how="inner"
)
edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce")
print(f"  Edges with PERMNO: {len(edges):,}")

# Classify connection type from title
edges["is_observer_connection"] = edges["portfolio_title"].str.contains("Observer", case=False, na=False).astype(int)
edges["is_director_connection"] = edges["portfolio_title"].str.contains("Director|Chairman|Board", case=False, na=False).astype(int)

# For each edge, mark whether the OBSERVER role (at observed company) is the connection
# Since all edges are from observer -> VC -> portfolio company,
# the observer connection is through the observer role at the observed company
# The portfolio connection is through whatever role the observer holds at the portfolio company
edges["observer_at_observed"] = 1  # all edges have this by construction
edges["role_at_portfolio"] = edges["portfolio_title"]

print(f"  Observer connections (observer title at portfolio co): {edges['is_observer_connection'].sum():,}")
print(f"  Director connections (director title at portfolio co): {edges['is_director_connection'].sum():,}")

# Add industry codes
edges["observed_sic2"] = edges["observed_companyid"].map(
    dict(zip(
        pd.read_csv(os.path.join(data_dir, "table_a_company_master.csv"))["companyid"].astype(str),
        pd.read_csv(os.path.join(data_dir, "table_a_company_master.csv")).apply(
            lambda r: industry_map.get(pd.to_numeric(str(r["cik"]).lstrip("0"), errors="coerce"), ""), axis=1
        )
    ))
)
edges["portfolio_sic2"] = edges["portfolio_cik_int"].map(industry_map)
edges["same_industry"] = (edges["observed_sic2"] == edges["portfolio_sic2"]).astype(int)

# =====================================================================
# STEP 3: Compute monthly abnormal returns around events
# =====================================================================
print("\n--- Step 3: Computing event-month returns ---")

# Compute market returns for abnormal return calculation
mkt_ret = port_crsp.groupby("date")["ret"].mean().reset_index()
mkt_ret.columns = ["date", "mkt_ret"]

port_crsp = port_crsp.merge(mkt_ret, on="date")
port_crsp["abnormal_ret"] = port_crsp["ret"] - port_crsp["mkt_ret"]

# Create year-month key for matching
port_crsp["ym"] = port_crsp["date"].dt.to_period("M")

# For each event at an observed company, find the month and look up
# portfolio company returns in that month
earnings["ym"] = earnings["announcedate"].dt.to_period("M")
earnings_ym = earnings[["companyid", "ym", "announcedate"]].drop_duplicates()

print(f"  Unique event year-months: {earnings_ym['ym'].nunique():,}")
print(f"  Unique observed companies with events: {earnings_ym['companyid'].nunique():,}")

# Build the spillover sample:
# For each (observed company, event month) x (portfolio company connected via observer)
# -> get portfolio company's abnormal return in that month

# Ensure matching types
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
earnings_ym["companyid"] = earnings_ym["companyid"].astype(str).str.replace(".0", "", regex=False)

# Merge events with edges
spillover_sample = edges.merge(
    earnings_ym,
    left_on="observed_companyid",
    right_on="companyid",
    how="inner"
)
print(f"  Event-edge pairs: {len(spillover_sample):,}")

# Merge with portfolio company returns
spillover_sample = spillover_sample.merge(
    port_crsp[["permno", "ym", "ret", "abnormal_ret"]],
    on=["permno", "ym"],
    how="inner"
)
print(f"  After matching returns: {len(spillover_sample):,}")

if len(spillover_sample) == 0:
    print("\n  WARNING: No spillover observations matched. Check data alignment.")
else:
    # =====================================================================
    # STEP 4: Run spillover regressions
    # =====================================================================
    print(f"\n--- Step 4: Spillover regressions ---")

    # Classify each observation by connection type
    # All observations are observer-connected (by construction of our network)
    # But some observers also hold director positions at the portfolio company
    print(f"\n  Sample composition:")
    print(f"    Total spillover observations: {len(spillover_sample):,}")
    print(f"    Observer role at portfolio co: {spillover_sample['is_observer_connection'].sum():,}")
    print(f"    Director role at portfolio co: {spillover_sample['is_director_connection'].sum():,}")
    print(f"    Same industry: {spillover_sample['same_industry'].sum():,}")

    # Summary stats
    print(f"\n  Abnormal return stats:")
    print(f"    Mean:   {spillover_sample['abnormal_ret'].mean():.6f}")
    print(f"    Median: {spillover_sample['abnormal_ret'].median():.6f}")
    print(f"    Std:    {spillover_sample['abnormal_ret'].std():.6f}")

    # Test 1: Is average abnormal return significantly different from zero?
    t_stat, p_val = stats.ttest_1samp(spillover_sample["abnormal_ret"].dropna(), 0)
    print(f"\n  One-sample t-test (H0: abnormal return = 0):")
    print(f"    t={t_stat:.4f}, p={p_val:.4f}")

    # Test 2: Same-industry vs different-industry spillover
    same = spillover_sample[spillover_sample["same_industry"] == 1]["abnormal_ret"].dropna()
    diff = spillover_sample[spillover_sample["same_industry"] == 0]["abnormal_ret"].dropna()
    if len(same) > 10 and len(diff) > 10:
        t_stat, p_val = stats.ttest_ind(same, diff, equal_var=False)
        print(f"\n  Same-industry vs different-industry spillover:")
        print(f"    Same industry mean:  {same.mean():.6f} (N={len(same):,})")
        print(f"    Diff industry mean:  {diff.mean():.6f} (N={len(diff):,})")
        print(f"    Difference: {same.mean() - diff.mean():.6f}, t={t_stat:.4f}, p={p_val:.4f}")

    # Test 3: Director connection vs non-director connection
    dir_conn = spillover_sample[spillover_sample["is_director_connection"] == 1]["abnormal_ret"].dropna()
    non_dir = spillover_sample[spillover_sample["is_director_connection"] == 0]["abnormal_ret"].dropna()
    if len(dir_conn) > 10 and len(non_dir) > 10:
        t_stat, p_val = stats.ttest_ind(dir_conn, non_dir, equal_var=False)
        print(f"\n  Director-connected vs non-director-connected spillover:")
        print(f"    Director mean:     {dir_conn.mean():.6f} (N={len(dir_conn):,})")
        print(f"    Non-director mean: {non_dir.mean():.6f} (N={len(non_dir):,})")
        print(f"    Difference: {dir_conn.mean() - non_dir.mean():.6f}, t={t_stat:.4f}, p={p_val:.4f}")

    # Regression: abnormal return ~ same_industry + director_connection
    print(f"\n  === Regression: Abnormal Return ~ Network Characteristics ===")

    try:
        # Model 1: Same industry only
        m1 = smf.ols("abnormal_ret ~ same_industry", data=spillover_sample).fit(
            cov_type="cluster", cov_kwds={"groups": spillover_sample["observer_personid"]})
        print(f"\n  Model 1: same_industry coef={m1.params['same_industry']:.6f} "
              f"(t={m1.tvalues['same_industry']:.2f}, p={m1.pvalues['same_industry']:.4f})")

        # Model 2: Same industry + director connection
        m2 = smf.ols("abnormal_ret ~ same_industry + is_director_connection",
                      data=spillover_sample).fit(
            cov_type="cluster", cov_kwds={"groups": spillover_sample["observer_personid"]})
        print(f"\n  Model 2:")
        print(f"    same_industry:          coef={m2.params['same_industry']:.6f} "
              f"(t={m2.tvalues['same_industry']:.2f}, p={m2.pvalues['same_industry']:.4f})")
        print(f"    is_director_connection: coef={m2.params['is_director_connection']:.6f} "
              f"(t={m2.tvalues['is_director_connection']:.2f}, p={m2.pvalues['is_director_connection']:.4f})")

        # Model 3: Interaction
        m3 = smf.ols("abnormal_ret ~ same_industry * is_director_connection",
                      data=spillover_sample).fit(
            cov_type="cluster", cov_kwds={"groups": spillover_sample["observer_personid"]})
        print(f"\n  Model 3 (interaction):")
        for param in m3.params.index:
            if param != "Intercept":
                print(f"    {param:40} coef={m3.params[param]:.6f} "
                      f"(t={m3.tvalues[param]:.2f}, p={m3.pvalues[param]:.4f})")

        print(f"\n  R-squared: M1={m1.rsquared:.6f}, M2={m2.rsquared:.6f}, M3={m3.rsquared:.6f}")
        print(f"  N: {int(m1.nobs):,}")

    except Exception as e:
        print(f"  Regression error: {e}")

# =====================================================================
# SUMMARY
# =====================================================================
print(f"\n\n{'='*70}")
print("TEST 3 SUMMARY")
print(f"{'='*70}")
print(f"  Network edges used: {len(edges):,}")
if len(spillover_sample) > 0:
    print(f"  Spillover observations: {len(spillover_sample):,}")
    print(f"  Unique event months: {spillover_sample['ym'].nunique():,}")
    print(f"  Unique portfolio companies: {spillover_sample['permno'].nunique():,}")
