"""Regression 1: Post-IPO Return Volatility ~ Fiduciary Language
Panel A direct test of NVCA 2020 hypothesis (H1).

Outcome: Standard deviation of daily returns in year 1 post-IPO
Treatment: NoFiduciary indicator (IRA disclaims fiduciary duty for observers)
Controls: Log assets, leverage, board size, observer count, IPO year FE, industry FE
"""

import csv, os, sys
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
edgar_dir = os.path.join(data_dir, "EDGAR_Extract")
panel_a_dir = os.path.join(data_dir, "Panel_A_Outcomes")
ciq_dir = os.path.join(data_dir, "CIQ_Extract")

print("=" * 70)
print("REGRESSION 1: Post-IPO Return Volatility ~ Fiduciary Language")
print("=" * 70)

# =====================================================================
# STEP 1: Build the treatment variable
# =====================================================================
print("\n--- Step 1: Building treatment variable from exhibit data ---")

exhibits = pd.read_csv(os.path.join(edgar_dir, "exhibit_analysis_results.csv"))
exhibits = exhibits[exhibits["fetch_status"].isin(["ok", "cached"])].copy()

# Create treatment categories
exhibits["fiduciary_category"] = "neither"
exhibits.loc[exhibits["has_fiduciary_manner"] == True, "fiduciary_category"] = "fiduciary"
exhibits.loc[exhibits["has_no_fiduciary_duty"] == True, "fiduciary_category"] = "no_fiduciary"

# Keep one record per CIK (most observer mentions = richest exhibit)
exhibits["observer_mentions"] = pd.to_numeric(exhibits["observer_mentions"], errors="coerce").fillna(0)
exhibits = exhibits.sort_values("observer_mentions", ascending=False).drop_duplicates("cik", keep="first")

# Treatment indicator
exhibits["no_fiduciary"] = (exhibits["fiduciary_category"] == "no_fiduciary").astype(int)
exhibits["has_fiduciary"] = (exhibits["fiduciary_category"] == "fiduciary").astype(int)
exhibits["filing_year"] = pd.to_numeric(exhibits["file_date"].str[:4], errors="coerce")

print(f"  Exhibit companies: {len(exhibits)}")
print(f"  Treatment distribution:")
print(f"    Fiduciary manner (control):  {exhibits['has_fiduciary'].sum()}")
print(f"    No fiduciary duty (treated): {exhibits['no_fiduciary'].sum()}")
print(f"    Neither:                     {(exhibits['fiduciary_category'] == 'neither').sum()}")

# =====================================================================
# STEP 2: Load identifier crosswalk
# =====================================================================
print("\n--- Step 2: Loading identifier crosswalk ---")

xwalk = pd.read_csv(os.path.join(panel_a_dir, "01_identifier_crosswalk.csv"))
# Keep primary links only
xwalk = xwalk[xwalk["linkprim"].isin(["P", "C"])].copy()
# One permno per CIK
xwalk["cik_int"] = pd.to_numeric(xwalk["cik"], errors="coerce")
xwalk = xwalk.drop_duplicates("cik_int", keep="first")

print(f"  Crosswalk entries: {len(xwalk)}")

# =====================================================================
# STEP 3: Load CRSP daily returns and compute volatility
# =====================================================================
print("\n--- Step 3: Computing post-IPO return volatility from CRSP daily ---")

crsp = pd.read_csv(os.path.join(panel_a_dir, "03_crsp_daily_returns.csv"))
crsp["date"] = pd.to_datetime(crsp["date"])
crsp["ret"] = pd.to_numeric(crsp["ret"], errors="coerce")

# Get first trading date per permno (proxy for IPO date)
first_dates = crsp.groupby("permno")["date"].min().reset_index()
first_dates.columns = ["permno", "ipo_date"]

# Merge IPO date back
crsp = crsp.merge(first_dates, on="permno")

# Filter to first 252 trading days post-IPO (approximately 1 year)
crsp["days_since_ipo"] = (crsp["date"] - crsp["ipo_date"]).dt.days
crsp_year1 = crsp[(crsp["days_since_ipo"] >= 0) & (crsp["days_since_ipo"] <= 365)].copy()

# Compute return volatility (std dev of daily returns in year 1)
vol = crsp_year1.groupby("permno").agg(
    ret_vol=("ret", "std"),
    ret_mean=("ret", "mean"),
    n_trading_days=("ret", "count"),
    ipo_date=("ipo_date", "first")
).reset_index()

# Annualize volatility
vol["ret_vol_annual"] = vol["ret_vol"] * np.sqrt(252)

# Require at least 100 trading days
vol = vol[vol["n_trading_days"] >= 100].copy()

print(f"  Securities with volatility computed: {len(vol)}")
print(f"  Mean annualized volatility: {vol['ret_vol_annual'].mean():.4f}")
print(f"  Median annualized volatility: {vol['ret_vol_annual'].median():.4f}")

# =====================================================================
# STEP 4: Load Compustat for controls
# =====================================================================
print("\n--- Step 4: Loading Compustat controls ---")

comp = pd.read_csv(os.path.join(panel_a_dir, "04_compustat_annual.csv"))
comp["datadate"] = pd.to_datetime(comp["datadate"])

# Use first available year (closest to IPO)
comp = comp.sort_values("datadate").drop_duplicates("gvkey", keep="first")

# Compute control variables
comp["log_assets"] = np.log(pd.to_numeric(comp["at"], errors="coerce").clip(lower=0.01))
comp["leverage"] = pd.to_numeric(comp["lt"], errors="coerce") / pd.to_numeric(comp["at"], errors="coerce")
comp["roa"] = pd.to_numeric(comp["ni"], errors="coerce") / pd.to_numeric(comp["at"], errors="coerce")
comp["log_sales"] = np.log(pd.to_numeric(comp["sale"], errors="coerce").clip(lower=0.01))

# SIC code for industry FE (from Compustat company table or gvkey)
# We'll use 2-digit SIC from the industry codes file
industry = pd.read_csv(os.path.join(data_dir, "Panel_C_Network", "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
industry = industry.drop_duplicates("cik_int", keep="first")[["cik_int", "sic", "sic2"]]

print(f"  Compustat firms: {len(comp)}")

# =====================================================================
# STEP 5: Load CIQ board composition
# =====================================================================
print("\n--- Step 5: Loading board composition from Table A ---")

master = pd.read_csv(os.path.join(data_dir, "table_a_company_master.csv"))
# Need to match CIQ companies to CIK
master_with_cik = master[master["cik"].notna() & (master["cik"] != "")].copy()
master_with_cik["cik_int"] = pd.to_numeric(master_with_cik["cik"].astype(str).str.lstrip("0"), errors="coerce")

print(f"  Companies with board data + CIK: {len(master_with_cik)}")

# =====================================================================
# STEP 6: Merge everything together
# =====================================================================
print("\n--- Step 6: Merging datasets ---")

# Start with exhibits (treatment)
exhibits["cik_int"] = pd.to_numeric(exhibits["cik"].astype(str).str.lstrip("0"), errors="coerce")

# Merge treatment -> crosswalk (get permno and gvkey)
df = exhibits[["cik", "cik_int", "no_fiduciary", "has_fiduciary", "fiduciary_category",
               "filing_year", "observer_mentions"]].copy()
df = df.merge(xwalk[["cik_int", "permno", "gvkey"]], on="cik_int", how="inner")
print(f"  After crosswalk merge: {len(df)}")

# Merge -> volatility
df["permno"] = pd.to_numeric(df["permno"], errors="coerce")
df = df.merge(vol[["permno", "ret_vol", "ret_vol_annual", "n_trading_days", "ipo_date"]],
              on="permno", how="inner")
print(f"  After volatility merge: {len(df)}")

# Merge -> Compustat controls
df = df.merge(comp[["gvkey", "log_assets", "leverage", "roa", "log_sales", "at"]],
              on="gvkey", how="left")
print(f"  After Compustat merge: {len(df)}")

# Merge -> industry codes
df = df.merge(industry[["cik_int", "sic2"]], on="cik_int", how="left")

# Merge -> board composition
df = df.merge(
    master_with_cik[["cik_int", "n_directors", "n_observers", "n_advisory",
                     "observer_ratio", "total_board"]],
    on="cik_int", how="left"
)

# IPO year
df["ipo_year"] = pd.to_datetime(df["ipo_date"]).dt.year

print(f"  Final merged sample: {len(df)}")

# =====================================================================
# STEP 7: Summary statistics
# =====================================================================
print("\n--- Step 7: Summary statistics ---")

# By treatment group
for cat in ["fiduciary", "no_fiduciary", "neither"]:
    sub = df[df["fiduciary_category"] == cat]
    if len(sub) > 0:
        print(f"\n  {cat.upper()} group (N={len(sub)}):")
        print(f"    Ret vol (annual): {sub['ret_vol_annual'].mean():.4f} (mean), {sub['ret_vol_annual'].median():.4f} (median)")
        print(f"    Log assets:       {sub['log_assets'].mean():.2f}")
        print(f"    Leverage:         {sub['leverage'].mean():.4f}")
        print(f"    N directors:      {sub['n_directors'].mean():.1f}")
        print(f"    N observers:      {sub['n_observers'].mean():.1f}")
        print(f"    IPO year range:   {sub['ipo_year'].min():.0f}-{sub['ipo_year'].max():.0f}")

# =====================================================================
# STEP 8: Run regressions
# =====================================================================
print("\n\n--- Step 8: Running regressions ---")

try:
    import statsmodels.formula.api as smf
    from statsmodels.iolib.summary2 import summary_col

    # Drop missing values
    reg_vars = ["ret_vol_annual", "no_fiduciary", "has_fiduciary", "fiduciary_category",
                "log_assets", "leverage", "ipo_year", "sic2",
                "n_directors", "n_observers"]
    reg_df = df.dropna(subset=["ret_vol_annual", "no_fiduciary", "log_assets", "leverage"]).copy()
    reg_df["ipo_year_str"] = reg_df["ipo_year"].astype(int).astype(str)

    print(f"\n  Regression sample: {len(reg_df)} firms")
    print(f"    No fiduciary: {reg_df['no_fiduciary'].sum()}")
    print(f"    Has fiduciary: {reg_df['has_fiduciary'].sum()}")
    print(f"    Neither: {(reg_df['fiduciary_category'] == 'neither').sum()}")

    # ---- Model 1: Simple comparison (no controls) ----
    print("\n  === Model 1: No controls ===")
    m1 = smf.ols("ret_vol_annual ~ no_fiduciary", data=reg_df).fit(cov_type="HC1")
    print(f"  NoFiduciary coef: {m1.params['no_fiduciary']:.4f} (t={m1.tvalues['no_fiduciary']:.2f}, p={m1.pvalues['no_fiduciary']:.4f})")
    print(f"  R-squared: {m1.rsquared:.4f}")
    print(f"  N: {int(m1.nobs)}")

    # ---- Model 2: With firm controls ----
    print("\n  === Model 2: With firm controls ===")
    m2 = smf.ols("ret_vol_annual ~ no_fiduciary + log_assets + leverage",
                  data=reg_df).fit(cov_type="HC1")
    print(f"  NoFiduciary coef: {m2.params['no_fiduciary']:.4f} (t={m2.tvalues['no_fiduciary']:.2f}, p={m2.pvalues['no_fiduciary']:.4f})")
    print(f"  Log assets coef:  {m2.params['log_assets']:.4f} (t={m2.tvalues['log_assets']:.2f})")
    print(f"  Leverage coef:    {m2.params['leverage']:.4f} (t={m2.tvalues['leverage']:.2f})")
    print(f"  R-squared: {m2.rsquared:.4f}")
    print(f"  N: {int(m2.nobs)}")

    # ---- Model 3: With IPO year FE ----
    print("\n  === Model 3: With firm controls + IPO year FE ===")
    m3 = smf.ols("ret_vol_annual ~ no_fiduciary + log_assets + leverage + C(ipo_year_str)",
                  data=reg_df).fit(cov_type="HC1")
    print(f"  NoFiduciary coef: {m3.params['no_fiduciary']:.4f} (t={m3.tvalues['no_fiduciary']:.2f}, p={m3.pvalues['no_fiduciary']:.4f})")
    print(f"  R-squared: {m3.rsquared:.4f}")
    print(f"  N: {int(m3.nobs)}")

    # ---- Model 4: With IPO year FE + Industry FE ----
    reg_df_ind = reg_df.dropna(subset=["sic2"]).copy()
    if len(reg_df_ind) > 30:
        print("\n  === Model 4: With firm controls + IPO year FE + Industry FE ===")
        m4 = smf.ols("ret_vol_annual ~ no_fiduciary + log_assets + leverage + C(ipo_year_str) + C(sic2)",
                      data=reg_df_ind).fit(cov_type="HC1")
        print(f"  NoFiduciary coef: {m4.params['no_fiduciary']:.4f} (t={m4.tvalues['no_fiduciary']:.2f}, p={m4.pvalues['no_fiduciary']:.4f})")
        print(f"  R-squared: {m4.rsquared:.4f}")
        print(f"  N: {int(m4.nobs)}")

    # ---- Model 5: Fiduciary vs Neither vs No-Fiduciary (3 groups) ----
    print("\n  === Model 5: Three-group comparison ===")
    m5 = smf.ols("ret_vol_annual ~ C(fiduciary_category, Treatment(reference='fiduciary')) + log_assets + leverage",
                  data=reg_df).fit(cov_type="HC1")
    for param in m5.params.index:
        if "fiduciary_category" in param:
            print(f"  {param}: {m5.params[param]:.4f} (t={m5.tvalues[param]:.2f}, p={m5.pvalues[param]:.4f})")
    print(f"  R-squared: {m5.rsquared:.4f}")
    print(f"  N: {int(m5.nobs)}")

    # ---- Summary table ----
    print("\n\n" + "=" * 70)
    print("REGRESSION SUMMARY TABLE")
    print("=" * 70)
    print(f"\nDependent variable: Annualized return volatility (year 1 post-IPO)")
    print(f"\n{'':30} {'Model 1':>10} {'Model 2':>10} {'Model 3':>10} {'Model 4':>10}")
    print(f"{'-'*70}")
    print(f"{'NoFiduciary':30} {m1.params['no_fiduciary']:>10.4f} {m2.params['no_fiduciary']:>10.4f} {m3.params['no_fiduciary']:>10.4f} {m4.params['no_fiduciary'] if len(reg_df_ind) > 30 else 'N/A':>10}")
    print(f"{'':30} ({m1.tvalues['no_fiduciary']:>8.2f}) ({m2.tvalues['no_fiduciary']:>8.2f}) ({m3.tvalues['no_fiduciary']:>8.2f}) ({m4.tvalues['no_fiduciary'] if len(reg_df_ind) > 30 else 'N/A':>8})")
    print(f"{'':30} [{m1.pvalues['no_fiduciary']:>8.4f}] [{m2.pvalues['no_fiduciary']:>8.4f}] [{m3.pvalues['no_fiduciary']:>8.4f}] [{m4.pvalues['no_fiduciary'] if len(reg_df_ind) > 30 else 'N/A':>8}]")
    print(f"{'Firm controls':30} {'No':>10} {'Yes':>10} {'Yes':>10} {'Yes':>10}")
    print(f"{'IPO year FE':30} {'No':>10} {'No':>10} {'Yes':>10} {'Yes':>10}")
    print(f"{'Industry FE':30} {'No':>10} {'No':>10} {'No':>10} {'Yes':>10}")
    print(f"{'R-squared':30} {m1.rsquared:>10.4f} {m2.rsquared:>10.4f} {m3.rsquared:>10.4f} {m4.rsquared if len(reg_df_ind) > 30 else 'N/A':>10}")
    print(f"{'N':30} {int(m1.nobs):>10} {int(m2.nobs):>10} {int(m3.nobs):>10} {int(m4.nobs) if len(reg_df_ind) > 30 else 'N/A':>10}")

    # Save regression data for further analysis
    reg_df.to_csv(os.path.join(data_dir, "regression_01_sample.csv"), index=False)
    print(f"\nRegression sample saved to regression_01_sample.csv")

except ImportError as e:
    print(f"\n  ERROR: Missing package - {e}")
    print(f"  Install with: pip install statsmodels")
except Exception as e:
    print(f"\n  ERROR: {e}")
    import traceback
    traceback.print_exc()
