"""Test 5 Enhanced: Private firm outcomes with Form D capital + EM board controls.
Compares results across progressively richer control sets."""

import os
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"

print("=" * 70)
print("TEST 5 ENHANCED: Private Firm Outcomes with New Controls")
print("=" * 70)

# Load enhanced master
df = pd.read_csv(os.path.join(data_dir, "table_a_company_master_enhanced.csv"))

# Convert numeric columns
for col in ["n_directors", "n_observers", "n_advisory", "total_board", "observer_ratio",
            "n_exec_board_changes", "n_lawsuits", "n_restatements", "n_bankruptcy",
            "yearfounded", "formd_total_sold", "formd_log_capital", "formd_n_filings",
            "em_numOut", "em_numExecs", "em_numVCs"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Build variables
sample = df[df["companytypename"] == "Private Company"].copy()
sample["has_lawsuit"] = (sample["n_lawsuits"].fillna(0) > 0).astype(int)
sample["has_restatement"] = (sample["n_restatements"].fillna(0) > 0).astype(int)
sample["has_bankruptcy"] = (sample["n_bankruptcy"].fillna(0) > 0).astype(int)
sample["is_failed"] = sample["companystatustypename"].isin(["Out of Business", "Liquidating"]).astype(int)
sample["is_acquired"] = (sample["companystatustypename"] == "Acquired").astype(int)
median_ratio = sample["observer_ratio"].median()
sample["high_observer"] = (sample["observer_ratio"] > median_ratio).astype(int)
sample["log_board_size"] = np.log1p(sample["total_board"].fillna(0))
sample["has_advisory"] = (sample["n_advisory"].fillna(0) > 0).astype(int)
sample["is_us"] = (sample["country"] == "United States").astype(int)
sample["firm_age"] = 2026 - sample["yearfounded"]

# Capital raised: use best available
sample["best_log_capital"] = pd.to_numeric(sample.get("best_log_capital", 0), errors="coerce").fillna(0)
if "best_log_capital" not in sample.columns or sample["best_log_capital"].sum() == 0:
    sample["best_log_capital"] = sample["formd_log_capital"].fillna(0)

sample["has_capital_data"] = (sample["best_log_capital"] > 0).astype(int)
sample["has_em_data"] = sample["em_numVCs"].notna().astype(int)

print(f"\nPrivate firm sample: {len(sample):,}")
print(f"  With Form D capital data: {sample['has_capital_data'].sum():,} ({100*sample['has_capital_data'].mean():.1f}%)")
print(f"  With EM board data: {sample['has_em_data'].sum():,} ({100*sample['has_em_data'].mean():.1f}%)")

# =====================================================================
# Define control sets
# =====================================================================
outcomes = [
    ("has_lawsuit", "Has Lawsuit"),
    ("has_restatement", "Has Restatement"),
    ("has_bankruptcy", "Has Bankruptcy"),
    ("is_acquired", "Was Acquired"),
    ("is_failed", "Company Failed"),
]

control_sets = [
    ("Spec 1: Basic", ["log_board_size", "has_advisory", "is_us"]),
    ("Spec 2: + Firm Age", ["log_board_size", "has_advisory", "is_us", "firm_age"]),
    ("Spec 3: + Capital Raised", ["log_board_size", "has_advisory", "is_us", "firm_age", "best_log_capital"]),
    ("Spec 4: + N Filings", ["log_board_size", "has_advisory", "is_us", "firm_age", "best_log_capital", "formd_n_filings"]),
]

# =====================================================================
# PANEL A: Logistic regressions across control sets
# =====================================================================
print(f"\n{'='*70}")
print("PANEL A: High Observer -> Outcomes (Logistic, progressive controls)")
print(f"{'='*70}")

for outcome_var, outcome_label in outcomes:
    print(f"\n  === {outcome_label} ===")
    print(f"  {'Specification':35} {'Coef':>8} {'OR':>8} {'p':>8} {'Sig':>5} {'N':>6}")
    print(f"  {'-'*70}")

    for spec_name, controls in control_sets:
        required = [outcome_var, "high_observer"] + controls
        sub = sample.dropna(subset=required).copy()

        if sub[outcome_var].sum() < 5 or len(sub) < 30:
            print(f"  {spec_name:35} {'too few events or obs':>30}")
            continue

        try:
            X = sm.add_constant(sub[["high_observer"] + controls])
            model = sm.Logit(sub[outcome_var], X).fit(disp=0, maxiter=100)
            coef = model.params["high_observer"]
            p = model.pvalues["high_observer"]
            odds = np.exp(coef)
            sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            print(f"  {spec_name:35} {coef:>8.4f} {odds:>8.3f} {p:>7.4f} {sig:>5} {len(sub):>6}")
        except Exception as e:
            print(f"  {spec_name:35} ERROR: {str(e)[:40]}")

# =====================================================================
# PANEL B: With Ewens-Malenko controls (subsample)
# =====================================================================
print(f"\n\n{'='*70}")
print("PANEL B: With Ewens-Malenko Board Composition Controls (N~288)")
print(f"{'='*70}")

em_sample = sample[sample["has_em_data"] == 1].copy()
em_sample["em_numVCs"] = em_sample["em_numVCs"].fillna(0)
em_sample["em_numOut"] = em_sample["em_numOut"].fillna(0)

print(f"\n  EM subsample: {len(em_sample):,} companies")

em_controls = ["log_board_size", "has_advisory", "is_us", "firm_age",
               "best_log_capital", "em_numVCs", "em_numOut"]

for outcome_var, outcome_label in outcomes:
    sub = em_sample.dropna(subset=[outcome_var, "high_observer"] + em_controls).copy()
    n_events = sub[outcome_var].sum()

    if n_events < 5 or len(sub) < 30:
        print(f"\n  {outcome_label}: {n_events} events in {len(sub)} obs — skipped")
        continue

    try:
        X = sm.add_constant(sub[["high_observer"] + em_controls])
        model = sm.Logit(sub[outcome_var], X).fit(disp=0, maxiter=100)
        coef = model.params["high_observer"]
        p = model.pvalues["high_observer"]
        odds = np.exp(coef)
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""

        print(f"\n  {outcome_label}: coef={coef:.4f}, OR={odds:.3f}, p={p:.4f} {sig} (N={len(sub)}, events={n_events})")

        # Show EM control coefficients
        for ctrl in ["em_numVCs", "em_numOut"]:
            if ctrl in model.params:
                c_coef = model.params[ctrl]
                c_p = model.pvalues[ctrl]
                c_sig = "***" if c_p < 0.01 else "**" if c_p < 0.05 else "*" if c_p < 0.10 else ""
                print(f"    {ctrl}: coef={c_coef:.4f}, OR={np.exp(c_coef):.3f}, p={c_p:.4f} {c_sig}")
    except Exception as e:
        print(f"\n  {outcome_label}: ERROR - {str(e)[:50]}")

# =====================================================================
# PANEL C: OLS count outcomes with all controls
# =====================================================================
print(f"\n\n{'='*70}")
print("PANEL C: OLS Count Outcomes (progressive controls)")
print(f"{'='*70}")

ols_outcomes = [
    ("n_exec_board_changes", "N Exec/Board Changes"),
    ("n_lawsuits", "N Lawsuits"),
]

for outcome_var, outcome_label in ols_outcomes:
    print(f"\n  === {outcome_label} ===")
    print(f"  {'Specification':35} {'Coef':>8} {'t':>7} {'p':>8} {'Sig':>5} {'N':>6}")
    print(f"  {'-'*70}")

    for spec_name, controls in control_sets:
        required = [outcome_var, "high_observer"] + controls
        sub = sample.dropna(subset=required).copy()

        if len(sub) < 30:
            continue

        formula = f"{outcome_var} ~ high_observer + " + " + ".join(controls)
        model = smf.ols(formula, data=sub).fit(cov_type="HC1")
        coef = model.params["high_observer"]
        t = model.tvalues["high_observer"]
        p = model.pvalues["high_observer"]
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"  {spec_name:35} {coef:>8.4f} {t:>7.2f} {p:>7.4f} {sig:>5} {len(sub):>6}")

# =====================================================================
# PANEL D: Continuous observer_ratio with all controls
# =====================================================================
print(f"\n\n{'='*70}")
print("PANEL D: Continuous Observer Ratio + All Controls")
print(f"{'='*70}")

spec3_controls = ["log_board_size", "has_advisory", "is_us", "firm_age", "best_log_capital"]

print(f"\n  {'Outcome':25} {'Coef':>8} {'OR':>8} {'p':>8} {'Sig':>5} {'N':>6}")
print(f"  {'-'*60}")

for outcome_var, outcome_label in outcomes:
    sub = sample.dropna(subset=[outcome_var, "observer_ratio"] + spec3_controls).copy()
    if sub[outcome_var].sum() < 5:
        continue

    try:
        X = sm.add_constant(sub[["observer_ratio"] + spec3_controls])
        model = sm.Logit(sub[outcome_var], X).fit(disp=0, maxiter=100)
        coef = model.params["observer_ratio"]
        p = model.pvalues["observer_ratio"]
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"  {outcome_label:25} {coef:>8.4f} {np.exp(coef):>8.3f} {p:>7.4f} {sig:>5} {len(sub):>6}")
    except Exception as e:
        print(f"  {outcome_label:25} ERROR: {str(e)[:40]}")

# =====================================================================
# SUMMARY
# =====================================================================
print(f"\n\n{'='*70}")
print("SUMMARY: Which Results Survive Enhanced Controls?")
print(f"{'='*70}")
print("""
  Compare Spec 1 (basic) vs Spec 3 (+ age + capital) for each outcome.
  If the coefficient stays significant through Spec 3, the result is
  robust to the best available private firm size controls.

  The EM subsample (Panel B, N~288) tests whether results hold after
  controlling for VC director count and independent director count --
  addressing the concern that observer effects are really VC effects.
""")
