"""Test 1 Regression v2: Observer vs No-Observer — fixed data cleaning."""

import os
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
test1_dir = os.path.join(data_dir, "Test1_Observer_vs_NoObserver")

print("=" * 70)
print("TEST 1 v2: Post-IPO Outcomes ~ Board Observer Presence")
print("=" * 70)

# Load the pre-built regression sample
df = pd.read_csv(os.path.join(data_dir, "regression_test1_sample.csv"))

# Fix data issues
df["leverage"] = pd.to_numeric(df["leverage"], errors="coerce")
df["log_assets"] = pd.to_numeric(df["log_assets"], errors="coerce")
df["ret_vol_annual"] = pd.to_numeric(df["ret_vol_annual"], errors="coerce")
df["ipo_first_day_ret"] = pd.to_numeric(df["ipo_first_day_ret"], errors="coerce")
df["bhar_6m"] = pd.to_numeric(df["bhar_6m"], errors="coerce")
df["avg_analysts"] = pd.to_numeric(df["avg_analysts"], errors="coerce")
df["avg_dispersion"] = pd.to_numeric(df["avg_dispersion"], errors="coerce")

# Cap leverage at 99th percentile and remove inf
df.loc[np.isinf(df["leverage"]), "leverage"] = np.nan
lev_cap = df["leverage"].quantile(0.99)
df.loc[df["leverage"] > lev_cap, "leverage"] = lev_cap
print(f"Leverage capped at {lev_cap:.2f} (99th percentile), inf removed")

# Winsorize extreme return values
for col in ["ret_vol_annual", "ipo_first_day_ret", "bhar_6m"]:
    if df[col].notna().sum() > 100:
        lo, hi = df[col].quantile(0.01), df[col].quantile(0.99)
        df[col] = df[col].clip(lo, hi)

# Ensure ipo_year_str is clean
df["ipo_year_str"] = df["ipo_year"].astype("Int64").astype(str)
df.loc[df["ipo_year_str"] == "<NA>", "ipo_year_str"] = np.nan

print(f"Sample: {len(df):,} firms | Treatment: {df['has_observer_mention'].sum():,} | Control: {(df['has_observer_mention']==0).sum():,}")

# =====================================================================
# SUMMARY STATISTICS
# =====================================================================
print(f"\n{'='*70}")
print("PANEL A: SUMMARY STATISTICS")
print(f"{'='*70}")

summary_vars = [
    ("ret_vol_annual", "Return Volatility (annualized)"),
    ("ipo_first_day_ret", "IPO First-Day Return"),
    ("bhar_6m", "6-Month BHAR"),
    ("avg_analysts", "Analyst Coverage (#)"),
    ("avg_dispersion", "Forecast Dispersion"),
    ("log_assets", "Log(Assets)"),
    ("leverage", "Leverage (L/A)"),
]

print(f"\n  {'Variable':35} {'Obs Mean':>10} {'Obs Med':>10} {'Ctrl Mean':>10} {'Ctrl Med':>10} {'Diff':>8} {'t':>7} {'p':>8}")
print(f"  {'-'*110}")

for var, label in summary_vars:
    t = df.loc[df["has_observer_mention"] == 1, var].dropna()
    c = df.loc[df["has_observer_mention"] == 0, var].dropna()
    if len(t) > 5 and len(c) > 5:
        t_stat, p_val = stats.ttest_ind(t, c, equal_var=False)
        diff = t.mean() - c.mean()
        sig = "***" if p_val < 0.01 else "**" if p_val < 0.05 else "*" if p_val < 0.10 else ""
        print(f"  {label:35} {t.mean():>10.4f} {t.median():>10.4f} {c.mean():>10.4f} {c.median():>10.4f} {diff:>8.4f} {t_stat:>7.2f} {p_val:>7.4f} {sig}")
    else:
        print(f"  {label:35} {'N/A':>10} {'':>10} {'N/A':>10} {'':>10}")

# =====================================================================
# REGRESSIONS
# =====================================================================
print(f"\n\n{'='*70}")
print("PANEL B: REGRESSION RESULTS")
print(f"{'='*70}")

outcomes = [
    ("ret_vol_annual", "Return Volatility"),
    ("ipo_first_day_ret", "IPO Underpricing"),
    ("bhar_6m", "6-Month BHAR"),
    ("avg_analysts", "Analyst Coverage"),
    ("avg_dispersion", "Forecast Dispersion"),
]

all_results = {}

for outcome_var, outcome_label in outcomes:
    # Build clean subsample for this outcome
    required = [outcome_var, "has_observer_mention", "log_assets", "leverage", "ipo_year_str", "sic2"]
    sub = df.dropna(subset=required).copy()

    if len(sub) < 30:
        print(f"\n  {outcome_label}: SKIPPED (N={len(sub)} < 30)")
        continue

    n_treat = sub["has_observer_mention"].sum()
    n_ctrl = len(sub) - n_treat
    print(f"\n  === {outcome_label} (N={len(sub):,}, Treatment={n_treat:,}, Control={n_ctrl:,}) ===")

    results = {}

    # Model 1: No controls
    m = smf.ols(f"{outcome_var} ~ has_observer_mention", data=sub).fit(cov_type="HC1")
    results["M1"] = {
        "coef": m.params["has_observer_mention"],
        "t": m.tvalues["has_observer_mention"],
        "p": m.pvalues["has_observer_mention"],
        "r2": m.rsquared, "n": int(m.nobs)
    }

    # Model 2: Firm controls
    m = smf.ols(f"{outcome_var} ~ has_observer_mention + log_assets + leverage", data=sub).fit(cov_type="HC1")
    results["M2"] = {
        "coef": m.params["has_observer_mention"],
        "t": m.tvalues["has_observer_mention"],
        "p": m.pvalues["has_observer_mention"],
        "r2": m.rsquared, "n": int(m.nobs)
    }

    # Model 3: Firm controls + IPO year FE
    m = smf.ols(f"{outcome_var} ~ has_observer_mention + log_assets + leverage + C(ipo_year_str)",
                data=sub).fit(cov_type="HC1")
    results["M3"] = {
        "coef": m.params["has_observer_mention"],
        "t": m.tvalues["has_observer_mention"],
        "p": m.pvalues["has_observer_mention"],
        "r2": m.rsquared, "n": int(m.nobs)
    }

    # Model 4: Firm controls + IPO year FE + Industry FE
    m = smf.ols(f"{outcome_var} ~ has_observer_mention + log_assets + leverage + C(ipo_year_str) + C(sic2)",
                data=sub).fit(cov_type="HC1")
    results["M4"] = {
        "coef": m.params["has_observer_mention"],
        "t": m.tvalues["has_observer_mention"],
        "p": m.pvalues["has_observer_mention"],
        "r2": m.rsquared, "n": int(m.nobs)
    }

    all_results[outcome_label] = results

    # Print table
    print(f"  {'':30} {'M1':>10} {'M2':>10} {'M3':>10} {'M4':>10}")
    print(f"  {'-'*70}")
    coefs = "".join(f"{results[m]['coef']:>10.4f}" for m in ["M1","M2","M3","M4"])
    tstats = "".join(f"({results[m]['t']:>8.2f})" for m in ["M1","M2","M3","M4"])
    pvals = "".join(f"[{results[m]['p']:>8.4f}]" for m in ["M1","M2","M3","M4"])
    print(f"  {'HasObserver':30} {coefs}")
    print(f"  {'':30} {tstats}")
    print(f"  {'':30} {pvals}")
    print(f"  {'Controls':30} {'No':>10} {'Yes':>10} {'Yes':>10} {'Yes':>10}")
    print(f"  {'IPO Year FE':30} {'No':>10} {'No':>10} {'Yes':>10} {'Yes':>10}")
    print(f"  {'Industry FE':30} {'No':>10} {'No':>10} {'No':>10} {'Yes':>10}")
    r2s = "".join(f"{results[m]['r2']:>10.4f}" for m in ["M1","M2","M3","M4"])
    ns = "".join(f"{results[m]['n']:>10}" for m in ["M1","M2","M3","M4"])
    print(f"  {'R-squared':30} {r2s}")
    print(f"  {'N':30} {ns}")

    # Significance flags
    for m_name in ["M3", "M4"]:
        p = results[m_name]["p"]
        if p < 0.10:
            stars = "***" if p < 0.01 else "**" if p < 0.05 else "*"
            print(f"  >>> {m_name} SIGNIFICANT: coef={results[m_name]['coef']:.4f}, p={p:.4f} {stars}")

# =====================================================================
# SUMMARY: COEFFICIENT HEATMAP
# =====================================================================
print(f"\n\n{'='*70}")
print("PANEL C: COEFFICIENT SUMMARY (Model 3 — Controls + Year FE)")
print(f"{'='*70}")

print(f"\n  {'Outcome':30} {'Coef':>8} {'t-stat':>8} {'p-value':>8} {'Sig':>5} {'N':>6}")
print(f"  {'-'*65}")
for outcome_label, results in all_results.items():
    r = results["M3"]
    stars = "***" if r["p"] < 0.01 else "**" if r["p"] < 0.05 else "*" if r["p"] < 0.10 else ""
    print(f"  {outcome_label:30} {r['coef']:>8.4f} {r['t']:>8.2f} {r['p']:>8.4f} {stars:>5} {r['n']:>6}")

print(f"\n  Interpretation:")
print(f"  Positive coef = observer firms have HIGHER values")
print(f"  Negative coef = observer firms have LOWER values")
