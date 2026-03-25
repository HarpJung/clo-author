"""Test 5 Rerun: Private firm outcomes with capital raised as additional control."""

import os
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")

# Load and prepare data
master = pd.read_csv(os.path.join(data_dir, "table_a_company_master.csv"))
for col in ["n_directors", "n_observers", "n_advisory", "total_board", "observer_ratio",
            "n_exec_board_changes", "n_lawsuits", "n_restatements", "n_bankruptcy"]:
    master[col] = pd.to_numeric(master[col], errors="coerce").fillna(0)
master["yearfounded"] = pd.to_numeric(master["yearfounded"], errors="coerce")

# Load deal amounts
deals = pd.read_csv(os.path.join(ciq_dir, "08_company_deal_amounts.csv"))
deals["companyid"] = deals["companyid"].astype(str).str.replace(".0", "", regex=False)
deals["log_capital_raised"] = np.log1p(pd.to_numeric(deals["total_size_usd"], errors="coerce").fillna(0))
deals["n_deals"] = pd.to_numeric(deals["n_deals"], errors="coerce")

# Merge
master["companyid"] = master["companyid"].astype(str)
master = master.merge(deals[["companyid", "log_capital_raised", "n_deals"]], on="companyid", how="left")
master["log_capital_raised"] = master["log_capital_raised"].fillna(0)
master["has_deal_data"] = (master["log_capital_raised"] > 0).astype(int)

# Build sample
sample = master[master["companytypename"] == "Private Company"].copy()
sample["has_lawsuit"] = (sample["n_lawsuits"] > 0).astype(int)
sample["has_restatement"] = (sample["n_restatements"] > 0).astype(int)
sample["has_bankruptcy"] = (sample["n_bankruptcy"] > 0).astype(int)
sample["is_failed"] = sample["companystatustypename"].isin(["Out of Business", "Liquidating"]).astype(int)
sample["is_acquired"] = (sample["companystatustypename"] == "Acquired").astype(int)
median_ratio = sample["observer_ratio"].median()
sample["high_observer"] = (sample["observer_ratio"] > median_ratio).astype(int)
sample["log_board_size"] = np.log1p(sample["total_board"])
sample["has_advisory"] = (sample["n_advisory"] > 0).astype(int)
sample["is_us"] = (sample["country"] == "United States").astype(int)
sample["firm_age"] = 2026 - sample["yearfounded"]

print("=" * 70)
print("TEST 5 RERUN: With Total Capital Raised as Control")
print("=" * 70)
print(f"\nFull private sample: {len(sample):,}")
print(f"With deal data: {sample['has_deal_data'].sum():,} ({100*sample['has_deal_data'].mean():.1f}%)")

outcomes = [
    ("has_lawsuit", "Has Lawsuit"),
    ("has_restatement", "Has Restatement"),
    ("has_bankruptcy", "Has Bankruptcy"),
    ("is_acquired", "Was Acquired"),
    ("is_failed", "Company Failed"),
]

# =====================================================================
# PANEL A: Original controls (baseline)
# =====================================================================
print(f"\n{'=' * 70}")
print("PANEL A: Original Controls Only (N=2,602)")
print(f"{'=' * 70}")
print(f"  {'Outcome':25} {'Coef':>8} {'OR':>8} {'p':>8} {'Sig':>5}")
print(f"  {'-' * 55}")

for var, label in outcomes:
    sub = sample.dropna(subset=[var, "high_observer", "log_board_size", "is_us"]).copy()
    if sub[var].sum() < 5:
        continue
    try:
        X = sm.add_constant(sub[["high_observer", "log_board_size", "has_advisory", "is_us"]])
        m = sm.Logit(sub[var], X).fit(disp=0, maxiter=100)
        coef = m.params["high_observer"]
        p = m.pvalues["high_observer"]
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"  {label:25} {coef:>8.4f} {np.exp(coef):>8.3f} {p:>7.4f} {sig:>5}")
    except:
        print(f"  {label:25} model failed")

# =====================================================================
# PANEL B: + Capital Raised
# =====================================================================
print(f"\n{'=' * 70}")
print("PANEL B: + Log Capital Raised")
print(f"{'=' * 70}")
print(f"  {'Outcome':25} {'Coef':>8} {'OR':>8} {'p':>8} {'Sig':>5}  {'CapRaised':>10} {'cap_p':>8}")
print(f"  {'-' * 80}")

for var, label in outcomes:
    sub = sample.dropna(subset=[var, "high_observer", "log_board_size", "is_us", "log_capital_raised"]).copy()
    if sub[var].sum() < 5:
        continue
    try:
        X = sm.add_constant(sub[["high_observer", "log_board_size", "has_advisory", "is_us", "log_capital_raised"]])
        m = sm.Logit(sub[var], X).fit(disp=0, maxiter=100)
        coef = m.params["high_observer"]
        p = m.pvalues["high_observer"]
        cap_coef = m.params["log_capital_raised"]
        cap_p = m.pvalues["log_capital_raised"]
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        cap_sig = "***" if cap_p < 0.01 else "**" if cap_p < 0.05 else "*" if cap_p < 0.10 else ""
        print(f"  {label:25} {coef:>8.4f} {np.exp(coef):>8.3f} {p:>7.4f} {sig:>5}  {cap_coef:>10.4f} {cap_p:>7.4f} {cap_sig}")
    except:
        print(f"  {label:25} model failed")

# =====================================================================
# PANEL C: All available controls
# =====================================================================
print(f"\n{'=' * 70}")
print("PANEL C: All Controls (board size + advisory + US + capital raised + firm age)")
print(f"{'=' * 70}")
print(f"  {'Outcome':25} {'Coef':>8} {'OR':>8} {'p':>8} {'Sig':>5} {'N':>6}")
print(f"  {'-' * 60}")

for var, label in outcomes:
    sub = sample.dropna(subset=[var, "high_observer", "log_board_size", "is_us",
                                "log_capital_raised", "firm_age"]).copy()
    if sub[var].sum() < 5:
        continue
    try:
        X = sm.add_constant(sub[["high_observer", "log_board_size", "has_advisory", "is_us",
                                 "log_capital_raised", "firm_age"]])
        m = sm.Logit(sub[var], X).fit(disp=0, maxiter=100)
        coef = m.params["high_observer"]
        p = m.pvalues["high_observer"]
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"  {label:25} {coef:>8.4f} {np.exp(coef):>8.3f} {p:>7.4f} {sig:>5} {int(m.nobs):>6}")
    except Exception as e:
        print(f"  {label:25} failed: {str(e)[:40]}")

# =====================================================================
# PANEL D: Continuous observer_ratio + all controls
# =====================================================================
print(f"\n{'=' * 70}")
print("PANEL D: Observer Ratio (continuous) + All Controls")
print(f"{'=' * 70}")
print(f"  {'Outcome':25} {'Coef':>8} {'OR':>8} {'p':>8} {'Sig':>5} {'N':>6}")
print(f"  {'-' * 60}")

for var, label in outcomes:
    sub = sample.dropna(subset=[var, "observer_ratio", "log_board_size", "is_us",
                                "log_capital_raised", "firm_age"]).copy()
    if sub[var].sum() < 5:
        continue
    try:
        X = sm.add_constant(sub[["observer_ratio", "log_board_size", "has_advisory", "is_us",
                                 "log_capital_raised", "firm_age"]])
        m = sm.Logit(sub[var], X).fit(disp=0, maxiter=100)
        coef = m.params["observer_ratio"]
        p = m.pvalues["observer_ratio"]
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"  {label:25} {coef:>8.4f} {np.exp(coef):>8.3f} {p:>7.4f} {sig:>5} {int(m.nobs):>6}")
    except Exception as e:
        print(f"  {label:25} failed: {str(e)[:40]}")

# =====================================================================
# PANEL E: OLS count outcomes + all controls
# =====================================================================
print(f"\n{'=' * 70}")
print("PANEL E: OLS Count Outcomes + All Controls")
print(f"{'=' * 70}")

for treat in ["high_observer", "observer_ratio"]:
    print(f"\n  Treatment: {treat}")
    print(f"  {'Outcome':25} {'Coef':>8} {'t':>7} {'p':>8} {'Sig':>5} {'N':>6}")
    print(f"  {'-' * 60}")
    for var, label in [("n_exec_board_changes", "N Exec Changes"), ("n_lawsuits", "N Lawsuits")]:
        sub = sample.dropna(subset=[var, treat, "log_board_size", "is_us",
                                    "log_capital_raised", "firm_age"]).copy()
        if len(sub) < 30:
            continue
        formula = f"{var} ~ {treat} + log_board_size + has_advisory + is_us + log_capital_raised + firm_age"
        m = smf.ols(formula, data=sub).fit(cov_type="HC1")
        coef = m.params[treat]
        t = m.tvalues[treat]
        p = m.pvalues[treat]
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"  {label:25} {coef:>8.4f} {t:>7.2f} {p:>7.4f} {sig:>5} {int(m.nobs):>6}")
