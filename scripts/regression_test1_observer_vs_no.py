"""Test 1 Regression: Post-IPO Outcomes ~ Observer Presence
Treatment: S-1 filer with "board observer" mention
Control: S-1 filer without "board observer" mention
Outcomes: Return volatility, IPO underpricing, analyst coverage, analyst dispersion
"""

import os
import numpy as np
import pandas as pd

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
test1_dir = os.path.join(data_dir, "Test1_Observer_vs_NoObserver")

print("=" * 70)
print("TEST 1: Post-IPO Outcomes ~ Board Observer Presence")
print("=" * 70)

# =====================================================================
# STEP 1: Load and merge data
# =====================================================================
print("\n--- Step 1: Loading data ---")

# Treatment/control assignment
tc = pd.read_csv(os.path.join(test1_dir, "00_treatment_control_assignment.csv"),
                 dtype={"cik": str})
tc["cik_int"] = pd.to_numeric(tc["cik"].str.lstrip("0"), errors="coerce")
print(f"  Treatment/control: {len(tc):,} ({tc['has_observer_mention'].sum():,} treatment, {(tc['has_observer_mention']==0).sum():,} control)")

# Crosswalk
xwalk = pd.read_csv(os.path.join(test1_dir, "01_identifier_crosswalk.csv"))
xwalk["cik_int"] = pd.to_numeric(xwalk["cik"], errors="coerce")
xwalk = xwalk.drop_duplicates("cik_int", keep="first")
print(f"  Crosswalk: {len(xwalk):,} CIK-PERMNO links")

# CRSP daily returns
print("  Loading CRSP daily... ", end="", flush=True)
crsp_daily = pd.read_csv(os.path.join(test1_dir, "03_crsp_daily_returns.csv"))
crsp_daily["date"] = pd.to_datetime(crsp_daily["date"])
crsp_daily["ret"] = pd.to_numeric(crsp_daily["ret"], errors="coerce")
print(f"{len(crsp_daily):,} rows")

# Compustat
comp = pd.read_csv(os.path.join(test1_dir, "04_compustat_annual.csv"))
comp["cik_int"] = pd.to_numeric(comp["cik"], errors="coerce")
# Use first year available (closest to IPO)
comp = comp.sort_values("datadate").drop_duplicates("gvkey", keep="first")
comp["log_assets"] = np.log(pd.to_numeric(comp["at"], errors="coerce").clip(lower=0.01))
comp["leverage"] = pd.to_numeric(comp["lt"], errors="coerce") / pd.to_numeric(comp["at"], errors="coerce")
print(f"  Compustat: {len(comp):,} companies")

# Industry codes
industry = pd.read_csv(os.path.join(test1_dir, "05_industry_codes.csv"))
industry["sic2"] = industry["sic"].astype(str).str[:2]
industry = industry.drop_duplicates("gvkey", keep="first")
print(f"  Industry: {len(industry):,} companies")

# IBES
ibes = pd.read_csv(os.path.join(test1_dir, "06_ibes_consensus.csv"))
ibes["statpers"] = pd.to_datetime(ibes["statpers"])
ibes["numest"] = pd.to_numeric(ibes["numest"], errors="coerce")
ibes["stdev"] = pd.to_numeric(ibes["stdev"], errors="coerce")

# Link tickers to gvkeys for merging
ibes_links_raw = []
for i in range(0, len(pd.unique(industry["gvkey"])), 1000):
    pass  # We'll merge via gvkey->ticker differently

# Aggregate IBES: average coverage and dispersion per ticker
ibes_agg = ibes.groupby("ticker").agg(
    avg_analysts=("numest", "mean"),
    avg_dispersion=("stdev", "mean"),
    n_months=("statpers", "count")
).reset_index()
print(f"  IBES aggregated: {len(ibes_agg):,} tickers")

# =====================================================================
# STEP 2: Compute outcome variables
# =====================================================================
print("\n--- Step 2: Computing outcome variables ---")

# 2a: First trading date per security (proxy for IPO date)
first_dates = crsp_daily.groupby("permno")["date"].min().reset_index()
first_dates.columns = ["permno", "first_trade_date"]

# 2b: Return volatility (year 1 post-listing)
crsp_daily = crsp_daily.merge(first_dates, on="permno")
crsp_daily["days_since_listing"] = (crsp_daily["date"] - crsp_daily["first_trade_date"]).dt.days
crsp_year1 = crsp_daily[(crsp_daily["days_since_listing"] >= 1) & (crsp_daily["days_since_listing"] <= 365)]

vol = crsp_year1.groupby("permno").agg(
    ret_vol=("ret", "std"),
    ret_mean=("ret", "mean"),
    n_days=("ret", "count")
).reset_index()
vol["ret_vol_annual"] = vol["ret_vol"] * np.sqrt(252)
vol = vol[vol["n_days"] >= 100]  # require 100+ trading days
print(f"  Return volatility computed: {len(vol):,} securities")

# 2c: IPO underpricing (first-day return)
first_day = crsp_daily[crsp_daily["days_since_listing"] == 0].copy()
if len(first_day) == 0:
    # Try first available day
    first_day = crsp_daily.sort_values(["permno", "date"]).drop_duplicates("permno", keep="first")
first_day = first_day[["permno", "ret"]].rename(columns={"ret": "ipo_first_day_ret"})
print(f"  IPO first-day return: {len(first_day):,} securities")

# 2d: Post-IPO 6-month buy-and-hold return
crsp_6m = crsp_daily[(crsp_daily["days_since_listing"] >= 1) & (crsp_daily["days_since_listing"] <= 180)]
bhar_6m = crsp_6m.groupby("permno").agg(
    bhar_6m=("ret", lambda x: (1 + x).prod() - 1),
    n_days_6m=("ret", "count")
).reset_index()
bhar_6m = bhar_6m[bhar_6m["n_days_6m"] >= 50]
print(f"  6-month BHAR: {len(bhar_6m):,} securities")

# =====================================================================
# STEP 3: Merge into analysis dataset
# =====================================================================
print("\n--- Step 3: Merging into analysis dataset ---")

# Start with treatment/control
df = tc[["cik", "cik_int", "has_observer_mention"]].copy()

# -> crosswalk
df = df.merge(xwalk[["cik_int", "permno", "gvkey"]], on="cik_int", how="inner")
print(f"  After crosswalk: {len(df):,}")

# -> volatility
df["permno"] = pd.to_numeric(df["permno"], errors="coerce")
df = df.merge(vol[["permno", "ret_vol_annual", "n_days"]], on="permno", how="inner")
print(f"  After volatility: {len(df):,}")

# -> IPO underpricing
df = df.merge(first_day, on="permno", how="left")

# -> 6-month BHAR
df = df.merge(bhar_6m[["permno", "bhar_6m"]], on="permno", how="left")

# -> Compustat
df = df.merge(comp[["gvkey", "log_assets", "leverage", "at", "sale", "ni"]], on="gvkey", how="left")

# -> Industry
df = df.merge(industry[["gvkey", "sic2", "sic"]], on="gvkey", how="left")

# -> IBES (need gvkey->ticker link)
# Get ticker from Compustat
comp_ticker = pd.read_csv(os.path.join(test1_dir, "04_compustat_annual.csv"))
comp_ticker = comp_ticker[["gvkey", "tic"]].drop_duplicates("gvkey", keep="first")
comp_ticker = comp_ticker.rename(columns={"tic": "ticker"})
df = df.merge(comp_ticker, on="gvkey", how="left")
df = df.merge(ibes_agg, on="ticker", how="left")

# IPO year from first trade date
df = df.merge(first_dates.rename(columns={"permno": "permno_"}),
              left_on="permno", right_on="permno_", how="left")
df["ipo_year"] = df["first_trade_date"].dt.year
df["ipo_year_str"] = df["ipo_year"].astype("Int64").astype(str)

print(f"  Final sample: {len(df):,}")
print(f"    Treatment (observer): {df['has_observer_mention'].sum():,}")
print(f"    Control (no observer): {(df['has_observer_mention']==0).sum():,}")

# =====================================================================
# STEP 4: Summary statistics by group
# =====================================================================
print("\n--- Step 4: Summary statistics ---")

for group, label in [(1, "TREATMENT (Observer)"), (0, "CONTROL (No Observer)")]:
    sub = df[df["has_observer_mention"] == group]
    print(f"\n  {label} (N={len(sub):,}):")
    print(f"    Ret vol (annual):     {sub['ret_vol_annual'].mean():.4f} mean, {sub['ret_vol_annual'].median():.4f} median")
    print(f"    IPO 1st-day return:   {sub['ipo_first_day_ret'].mean():.4f} mean")
    print(f"    6-month BHAR:         {sub['bhar_6m'].mean():.4f} mean")
    print(f"    Log assets:           {sub['log_assets'].mean():.2f}")
    print(f"    Leverage:             {sub['leverage'].median():.4f}")
    print(f"    Analyst coverage:     {sub['avg_analysts'].mean():.1f}")
    print(f"    Forecast dispersion:  {sub['avg_dispersion'].mean():.4f}")
    print(f"    IPO year range:       {sub['ipo_year'].min():.0f}-{sub['ipo_year'].max():.0f}")

# T-test for key variables
from scipy import stats

print("\n  --- Univariate t-tests ---")
test_vars = [
    ("ret_vol_annual", "Return Volatility"),
    ("ipo_first_day_ret", "IPO Underpricing"),
    ("bhar_6m", "6-Month BHAR"),
    ("avg_analysts", "Analyst Coverage"),
    ("avg_dispersion", "Forecast Dispersion"),
    ("log_assets", "Log Assets"),
]

for var, label in test_vars:
    treat = df.loc[df["has_observer_mention"] == 1, var].dropna()
    ctrl = df.loc[df["has_observer_mention"] == 0, var].dropna()
    if len(treat) > 5 and len(ctrl) > 5:
        t_stat, p_val = stats.ttest_ind(treat, ctrl, equal_var=False)
        diff = treat.mean() - ctrl.mean()
        print(f"    {label:25} diff={diff:>8.4f}  t={t_stat:>6.2f}  p={p_val:.4f}  (N={len(treat)}+{len(ctrl)})")

# =====================================================================
# STEP 5: Regressions
# =====================================================================
print("\n\n--- Step 5: Regressions ---")

import statsmodels.formula.api as smf

# Drop missing for regression
reg_df = df.dropna(subset=["ret_vol_annual", "has_observer_mention", "log_assets", "leverage"]).copy()
print(f"  Regression sample: {len(reg_df):,} firms")
print(f"    Treatment: {reg_df['has_observer_mention'].sum():,}")
print(f"    Control: {(reg_df['has_observer_mention']==0).sum():,}")

outcomes = [
    ("ret_vol_annual", "Return Volatility (1yr)"),
    ("ipo_first_day_ret", "IPO Underpricing"),
    ("bhar_6m", "6-Month BHAR"),
    ("avg_analysts", "Analyst Coverage"),
    ("avg_dispersion", "Forecast Dispersion"),
]

print(f"\n{'='*70}")
print(f"REGRESSION RESULTS")
print(f"{'='*70}")

for outcome_var, outcome_label in outcomes:
    sub = reg_df.dropna(subset=[outcome_var]).copy()
    if len(sub) < 30:
        print(f"\n  {outcome_label}: Skipped (N={len(sub)} < 30)")
        continue

    print(f"\n  === {outcome_label} (N={len(sub):,}) ===")

    # Run all 4 models, handling missing data gracefully
    results = {}

    # Model 1: No controls
    try:
        sub1 = sub.dropna(subset=[outcome_var, "has_observer_mention"])
        m = smf.ols(f"{outcome_var} ~ has_observer_mention", data=sub1).fit(cov_type="HC1")
        results["M1"] = {"coef": m.params["has_observer_mention"], "t": m.tvalues["has_observer_mention"],
                         "p": m.pvalues["has_observer_mention"], "r2": m.rsquared, "n": int(m.nobs)}
    except Exception as e:
        results["M1"] = {"coef": np.nan, "t": np.nan, "p": np.nan, "r2": np.nan, "n": 0}

    # Model 2: With firm controls
    try:
        sub2 = sub.dropna(subset=[outcome_var, "has_observer_mention", "log_assets", "leverage"])
        m = smf.ols(f"{outcome_var} ~ has_observer_mention + log_assets + leverage", data=sub2).fit(cov_type="HC1")
        results["M2"] = {"coef": m.params["has_observer_mention"], "t": m.tvalues["has_observer_mention"],
                         "p": m.pvalues["has_observer_mention"], "r2": m.rsquared, "n": int(m.nobs)}
    except Exception as e:
        results["M2"] = {"coef": np.nan, "t": np.nan, "p": np.nan, "r2": np.nan, "n": 0}

    # Model 3: With firm controls + IPO year FE
    try:
        sub3 = sub.dropna(subset=[outcome_var, "has_observer_mention", "log_assets", "leverage", "ipo_year_str"])
        m = smf.ols(f"{outcome_var} ~ has_observer_mention + log_assets + leverage + C(ipo_year_str)", data=sub3).fit(cov_type="HC1")
        results["M3"] = {"coef": m.params["has_observer_mention"], "t": m.tvalues["has_observer_mention"],
                         "p": m.pvalues["has_observer_mention"], "r2": m.rsquared, "n": int(m.nobs)}
    except Exception as e:
        results["M3"] = {"coef": np.nan, "t": np.nan, "p": np.nan, "r2": np.nan, "n": 0}

    # Model 4: With firm controls + IPO year FE + Industry FE
    try:
        sub4 = sub.dropna(subset=[outcome_var, "has_observer_mention", "log_assets", "leverage", "ipo_year_str", "sic2"])
        if len(sub4) > 50:
            m = smf.ols(f"{outcome_var} ~ has_observer_mention + log_assets + leverage + C(ipo_year_str) + C(sic2)", data=sub4).fit(cov_type="HC1")
            results["M4"] = {"coef": m.params["has_observer_mention"], "t": m.tvalues["has_observer_mention"],
                             "p": m.pvalues["has_observer_mention"], "r2": m.rsquared, "n": int(m.nobs)}
        else:
            results["M4"] = {"coef": np.nan, "t": np.nan, "p": np.nan, "r2": np.nan, "n": 0}
    except Exception as e:
        results["M4"] = {"coef": np.nan, "t": np.nan, "p": np.nan, "r2": np.nan, "n": 0}

    print(f"  {'':30} {'M1':>10} {'M2':>10} {'M3':>10} {'M4':>10}")
    print(f"  {'-'*70}")
    for label, key in [("HasObserver", "coef"), ("", "t"), ("", "p")]:
        vals = []
        for m in ["M1", "M2", "M3", "M4"]:
            v = results[m][key]
            if key == "coef":
                vals.append(f"{v:>10.4f}" if not np.isnan(v) else f"{'N/A':>10}")
            elif key == "t":
                vals.append(f"({v:>8.2f})" if not np.isnan(v) else f"{'(N/A)':>10}")
            elif key == "p":
                vals.append(f"[{v:>8.4f}]" if not np.isnan(v) else f"{'[N/A]':>10}")
        print(f"  {label:30} {''.join(vals)}")
    print(f"  {'Controls':30} {'No':>10} {'Yes':>10} {'Yes':>10} {'Yes':>10}")
    print(f"  {'IPO Year FE':30} {'No':>10} {'No':>10} {'Yes':>10} {'Yes':>10}")
    print(f"  {'Industry FE':30} {'No':>10} {'No':>10} {'No':>10} {'Yes':>10}")
    r2_line = "".join(f"{results[m]['r2']:>10.4f}" if not np.isnan(results[m]['r2']) else f"{'N/A':>10}" for m in ["M1","M2","M3","M4"])
    n_line = "".join(f"{results[m]['n']:>10}" for m in ["M1","M2","M3","M4"])
    print(f"  {'R-squared':30} {r2_line}")
    print(f"  {'N':30} {n_line}")

    # Flag significance using best available model
    best_p = results["M3"]["p"] if not np.isnan(results["M3"]["p"]) else results["M1"]["p"]
    stars = ""
    if best_p < 0.01: stars = "***"
    elif best_p < 0.05: stars = "**"
    elif best_p < 0.10: stars = "*"
    if stars:
        print(f"  >>> SIGNIFICANT: p={best_p:.4f} {stars}")

# Save regression sample
reg_df.to_csv(os.path.join(data_dir, "regression_test1_sample.csv"), index=False)
print(f"\nRegression sample saved to regression_test1_sample.csv")
