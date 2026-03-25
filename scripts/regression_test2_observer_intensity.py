"""Test 2: Post-IPO Outcomes ~ Observer Intensity (continuous)
Within CIQ observer firms matched to CRSP/Compustat.
Treatment: observer_ratio (observers / total board) and observer_count (continuous)
"""

import os
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"

print("=" * 70)
print("TEST 2: Post-IPO Outcomes ~ Observer Intensity (Continuous)")
print("=" * 70)

# Load company master (has observer counts)
master = pd.read_csv(os.path.join(data_dir, "table_a_company_master.csv"))
master = master[master["cik"].notna() & (master["cik"] != "")].copy()
master["cik_int"] = pd.to_numeric(master["cik"].astype(str).str.lstrip("0"), errors="coerce")
master["n_observers"] = pd.to_numeric(master["n_observers"], errors="coerce")
master["n_directors"] = pd.to_numeric(master["n_directors"], errors="coerce")
master["total_board"] = pd.to_numeric(master["total_board"], errors="coerce")
master["observer_ratio"] = pd.to_numeric(master["observer_ratio"], errors="coerce")

print(f"CIQ companies with CIK: {len(master):,}")

# Load Panel B outcome data
xwalk_b = pd.read_csv(os.path.join(data_dir, "Panel_B_Outcomes", "01_identifier_crosswalk.csv"))
xwalk_b["cik_int"] = pd.to_numeric(xwalk_b["cik"], errors="coerce")
xwalk_b = xwalk_b.drop_duplicates("cik_int", keep="first")

comp_b = pd.read_csv(os.path.join(data_dir, "Panel_B_Outcomes", "02_compustat_annual.csv"))
comp_b = comp_b.sort_values("datadate").drop_duplicates("gvkey", keep="first")
comp_b["log_assets"] = np.log(pd.to_numeric(comp_b["at"], errors="coerce").clip(lower=0.01))
comp_b["leverage"] = pd.to_numeric(comp_b["lt"], errors="coerce") / pd.to_numeric(comp_b["at"], errors="coerce")
comp_b.loc[np.isinf(comp_b["leverage"]), "leverage"] = np.nan
lev_cap = comp_b["leverage"].quantile(0.99)
comp_b.loc[comp_b["leverage"] > lev_cap, "leverage"] = lev_cap

crsp_b = pd.read_csv(os.path.join(data_dir, "Panel_B_Outcomes", "03_crsp_monthly_returns.csv"))
crsp_b["date"] = pd.to_datetime(crsp_b["date"])
crsp_b["ret"] = pd.to_numeric(crsp_b["ret"], errors="coerce")

# Compute annual volatility from monthly returns
vol_monthly = crsp_b.groupby("permno").agg(
    ret_vol_monthly=("ret", "std"),
    n_months=("ret", "count"),
    first_date=("date", "min")
).reset_index()
vol_monthly["ret_vol_annual"] = vol_monthly["ret_vol_monthly"] * np.sqrt(12)
vol_monthly["ipo_year"] = vol_monthly["first_date"].dt.year
vol_monthly = vol_monthly[vol_monthly["n_months"] >= 12]

# IBES
ibes_b = pd.read_csv(os.path.join(data_dir, "Panel_B_Outcomes", "04_ibes_consensus.csv"))
ibes_b["numest"] = pd.to_numeric(ibes_b["numest"], errors="coerce")
ibes_b["stdev"] = pd.to_numeric(ibes_b["stdev"], errors="coerce")
ibes_agg = ibes_b.groupby("ticker").agg(
    avg_analysts=("numest", "mean"),
    avg_dispersion=("stdev", "mean")
).reset_index()

# Ticker from Compustat
comp_ticker = pd.read_csv(os.path.join(data_dir, "Panel_B_Outcomes", "02_compustat_annual.csv"))
comp_ticker = comp_ticker[["gvkey", "tic"]].drop_duplicates("gvkey", keep="first").rename(columns={"tic": "ticker"})

# Industry codes
try:
    ind = pd.read_csv(os.path.join(data_dir, "Panel_C_Network", "05_industry_codes.csv"))
    ind["sic2"] = ind["sic"].astype(str).str[:2]
    ind = ind.drop_duplicates("gvkey", keep="first")[["gvkey", "sic2"]]
except:
    ind = pd.DataFrame(columns=["gvkey", "sic2"])

# Merge everything
df = master[["cik_int", "n_observers", "n_directors", "n_advisory",
             "total_board", "observer_ratio", "companyname"]].copy()
df = df.merge(xwalk_b[["cik_int", "permno", "gvkey"]], on="cik_int", how="inner")
print(f"After crosswalk merge: {len(df):,}")

df["permno"] = pd.to_numeric(df["permno"], errors="coerce")
df = df.merge(vol_monthly[["permno", "ret_vol_annual", "ipo_year"]], on="permno", how="inner")
print(f"After volatility merge: {len(df):,}")

df = df.merge(comp_b[["gvkey", "log_assets", "leverage"]], on="gvkey", how="left")
df = df.merge(comp_ticker, on="gvkey", how="left")
df = df.merge(ibes_agg, on="ticker", how="left")
df = df.merge(ind, on="gvkey", how="left")
df["ipo_year_str"] = df["ipo_year"].astype("Int64").astype(str)

# Create high/low observer groups for comparison
median_ratio = df["observer_ratio"].median()
df["high_observer"] = (df["observer_ratio"] > median_ratio).astype(int)

print(f"Final sample: {len(df):,}")
print(f"  Observer ratio: mean={df['observer_ratio'].mean():.3f}, median={df['observer_ratio'].median():.3f}")
print(f"  Observer count: mean={df['n_observers'].mean():.1f}, median={df['n_observers'].median():.1f}")

# =====================================================================
# REGRESSIONS
# =====================================================================
print(f"\n{'='*70}")
print("REGRESSION RESULTS: Observer Intensity")
print(f"{'='*70}")

outcomes = [
    ("ret_vol_annual", "Return Volatility"),
    ("avg_analysts", "Analyst Coverage"),
    ("avg_dispersion", "Forecast Dispersion"),
]

treatments = [
    ("observer_ratio", "Observer Ratio (obs/total board)"),
    ("n_observers", "Observer Count"),
    ("high_observer", "High Observer (above median ratio)"),
]

for treat_var, treat_label in treatments:
    print(f"\n\n  ### Treatment: {treat_label} ###")

    for outcome_var, outcome_label in outcomes:
        required = [outcome_var, treat_var, "log_assets", "leverage", "ipo_year_str"]
        sub = df.dropna(subset=required).copy()

        if len(sub) < 30:
            continue

        print(f"\n  === {outcome_label} (N={len(sub):,}) ===")

        # Model 1: No controls
        m1 = smf.ols(f"{outcome_var} ~ {treat_var}", data=sub).fit(cov_type="HC1")

        # Model 2: With controls
        m2 = smf.ols(f"{outcome_var} ~ {treat_var} + log_assets + leverage",
                      data=sub).fit(cov_type="HC1")

        # Model 3: With controls + year FE
        m3 = smf.ols(f"{outcome_var} ~ {treat_var} + log_assets + leverage + C(ipo_year_str)",
                      data=sub).fit(cov_type="HC1")

        models = [("M1", m1), ("M2", m2), ("M3", m3)]

        print(f"  {'':30} {'M1':>10} {'M2':>10} {'M3':>10}")
        print(f"  {'-'*60}")
        coefs = "".join(f"{m.params[treat_var]:>10.4f}" for _, m in models)
        tstats = "".join(f"({m.tvalues[treat_var]:>8.2f})" for _, m in models)
        pvals = "".join(f"[{m.pvalues[treat_var]:>8.4f}]" for _, m in models)
        print(f"  {treat_var:30} {coefs}")
        print(f"  {'':30} {tstats}")
        print(f"  {'':30} {pvals}")
        r2s = "".join(f"{m.rsquared:>10.4f}" for _, m in models)
        ns = "".join(f"{int(m.nobs):>10}" for _, m in models)
        print(f"  {'R-squared':30} {r2s}")
        print(f"  {'N':30} {ns}")

        p3 = m3.pvalues[treat_var]
        if p3 < 0.10:
            stars = "***" if p3 < 0.01 else "**" if p3 < 0.05 else "*"
            print(f"  >>> SIGNIFICANT: coef={m3.params[treat_var]:.4f}, p={p3:.4f} {stars}")

# =====================================================================
# CIQ KEY DEV OUTCOMES (private firm specific)
# =====================================================================
print(f"\n\n{'='*70}")
print("PRIVATE FIRM OUTCOMES: Observer Intensity ~ Company Events")
print(f"{'='*70}")

# Use the full CIQ master (not just CRSP-matched)
master_full = pd.read_csv(os.path.join(data_dir, "table_a_company_master.csv"))
master_full["n_observers"] = pd.to_numeric(master_full["n_observers"], errors="coerce")
master_full["observer_ratio"] = pd.to_numeric(master_full["observer_ratio"], errors="coerce")
master_full["n_exec_board_changes"] = pd.to_numeric(master_full["n_exec_board_changes"], errors="coerce")
master_full["n_lawsuits"] = pd.to_numeric(master_full["n_lawsuits"], errors="coerce")
master_full["n_restatements"] = pd.to_numeric(master_full["n_restatements"], errors="coerce")
master_full["n_bankruptcy"] = pd.to_numeric(master_full["n_bankruptcy"], errors="coerce")

# Binary outcomes
master_full["has_lawsuit"] = (master_full["n_lawsuits"] > 0).astype(int)
master_full["has_restatement"] = (master_full["n_restatements"] > 0).astype(int)
master_full["has_bankruptcy"] = (master_full["n_bankruptcy"] > 0).astype(int)
master_full["is_failed"] = master_full["companystatustypename"].isin(["Out of Business", "Liquidating"]).astype(int)

# High observer indicator
median_ratio_full = master_full["observer_ratio"].median()
master_full["high_observer"] = (master_full["observer_ratio"] > median_ratio_full).astype(int)

private = master_full[master_full["companytypename"] == "Private Company"].copy()
print(f"\nPrivate firm sample: {len(private):,}")
print(f"  Has lawsuit:     {private['has_lawsuit'].sum():,} ({100*private['has_lawsuit'].mean():.1f}%)")
print(f"  Has restatement: {private['has_restatement'].sum():,} ({100*private['has_restatement'].mean():.1f}%)")
print(f"  Has bankruptcy:  {private['has_bankruptcy'].sum():,} ({100*private['has_bankruptcy'].mean():.1f}%)")
print(f"  Failed:          {private['is_failed'].sum():,} ({100*private['is_failed'].mean():.1f}%)")

# Correlations between observer intensity and outcomes
print(f"\n  Correlations (observer_ratio vs outcomes):")
for var, label in [("n_exec_board_changes", "Exec/Board Changes"),
                   ("has_lawsuit", "Has Lawsuit"),
                   ("has_restatement", "Has Restatement"),
                   ("is_failed", "Company Failed")]:
    sub = private[[var, "observer_ratio"]].dropna()
    if len(sub) > 30:
        corr, p = stats.pearsonr(sub["observer_ratio"], sub[var])
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"    {label:25} r={corr:>7.4f}  p={p:.4f} {sig}")

# Logistic regressions for binary outcomes
print(f"\n  Logistic regressions (high_observer -> outcome):")
import statsmodels.api as sm

for var, label in [("has_lawsuit", "Has Lawsuit"),
                   ("has_restatement", "Has Restatement"),
                   ("is_failed", "Company Failed")]:
    sub = private[[var, "high_observer"]].dropna()
    if len(sub) > 30 and sub[var].sum() > 5:
        try:
            X = sm.add_constant(sub[["high_observer"]])
            m = sm.Logit(sub[var], X).fit(disp=0)
            coef = m.params["high_observer"]
            p = m.pvalues["high_observer"]
            odds = np.exp(coef)
            sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            print(f"    {label:25} coef={coef:>7.4f}  OR={odds:>6.3f}  p={p:.4f} {sig}")
        except:
            print(f"    {label:25} model failed")
