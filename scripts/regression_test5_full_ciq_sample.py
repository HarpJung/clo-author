"""Test 5: Full CIQ Sample — All Private Firm Outcomes (No CRSP Required)
Uses the FULL 3,058 observer companies with CIQ-only outcome variables.
This is where our statistical power lives.
"""

import os
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
import statsmodels.api as sm
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")

print("=" * 70)
print("TEST 5: Full CIQ Sample — Private Firm Outcomes")
print("=" * 70)

# =====================================================================
# STEP 1: Build the full analysis dataset from CIQ
# =====================================================================
print("\n--- Step 1: Building full CIQ analysis dataset ---")

# Company master
master = pd.read_csv(os.path.join(data_dir, "table_a_company_master.csv"))

# Convert numeric columns
for col in ["n_directors", "n_observers", "n_advisory", "total_board", "observer_ratio",
            "n_exec_board_changes", "n_lawsuits", "n_restatements",
            "n_earnings_announcements", "n_financing_events", "n_bankruptcy"]:
    master[col] = pd.to_numeric(master[col], errors="coerce").fillna(0)

master["yearfounded"] = pd.to_numeric(master["yearfounded"], errors="coerce")

# =====================================================================
# STEP 2: Construct outcome variables
# =====================================================================
print("\n--- Step 2: Constructing outcome variables ---")

# Binary outcomes
master["has_lawsuit"] = (master["n_lawsuits"] > 0).astype(int)
master["has_restatement"] = (master["n_restatements"] > 0).astype(int)
master["has_bankruptcy"] = (master["n_bankruptcy"] > 0).astype(int)
master["is_failed"] = master["companystatustypename"].isin(
    ["Out of Business", "Liquidating"]
).astype(int)
master["is_acquired"] = (master["companystatustypename"] == "Acquired").astype(int)
# For regulatory inquiries, pull from events file
events = pd.read_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)

reg_inquiries = events[events["keydeveventtypename"] == "Regulatory Agency Inquiries"]
reg_inquiry_cos = set(reg_inquiries["companyid"].unique())
master["has_regulatory_inquiry"] = master["companyid"].astype(str).isin(reg_inquiry_cos).astype(int)

# Count-based outcomes
master["log_exec_changes"] = np.log1p(master["n_exec_board_changes"])
master["log_lawsuits"] = np.log1p(master["n_lawsuits"])

# Treatment variables
median_ratio = master["observer_ratio"].median()
master["high_observer"] = (master["observer_ratio"] > median_ratio).astype(int)
master["log_observers"] = np.log1p(master["n_observers"])

# Control variables
master["is_private"] = (master["companytypename"] == "Private Company").astype(int)
master["is_us"] = (master["country"] == "United States").astype(int)
master["firm_age"] = 2026 - master["yearfounded"]
master["log_board_size"] = np.log1p(master["total_board"])
master["has_advisory"] = (master["n_advisory"] > 0).astype(int)

# =====================================================================
# STEP 3: Sample overview
# =====================================================================
print("\n--- Step 3: Sample overview ---")

subsamples = {
    "All observer companies": master,
    "Private only": master[master["is_private"] == 1],
    "US Private only": master[(master["is_private"] == 1) & (master["is_us"] == 1)],
    "Operating only": master[master["companystatustypename"] == "Operating"],
}

for name, sub in subsamples.items():
    print(f"\n  {name} (N={len(sub):,}):")
    print(f"    Observer ratio:  mean={sub['observer_ratio'].mean():.3f}, median={sub['observer_ratio'].median():.3f}")
    print(f"    N observers:     mean={sub['n_observers'].mean():.1f}, median={sub['n_observers'].median():.1f}")
    print(f"    N directors:     mean={sub['n_directors'].mean():.1f}")
    print(f"    Board size:      mean={sub['total_board'].mean():.1f}")
    print(f"    Firm age:        mean={sub['firm_age'].mean():.1f} yrs")
    print(f"    Outcomes:")
    print(f"      Lawsuits:      {sub['has_lawsuit'].sum():>5} ({100*sub['has_lawsuit'].mean():.1f}%)")
    print(f"      Restatements:  {sub['has_restatement'].sum():>5} ({100*sub['has_restatement'].mean():.1f}%)")
    print(f"      Bankruptcy:    {sub['has_bankruptcy'].sum():>5} ({100*sub['has_bankruptcy'].mean():.1f}%)")
    print(f"      Failed:        {sub['is_failed'].sum():>5} ({100*sub['is_failed'].mean():.1f}%)")
    print(f"      Acquired:      {sub['is_acquired'].sum():>5} ({100*sub['is_acquired'].mean():.1f}%)")
    print(f"      Reg inquiry:   {sub['has_regulatory_inquiry'].sum():>5} ({100*sub['has_regulatory_inquiry'].mean():.1f}%)")
    print(f"      Exec changes:  mean={sub['n_exec_board_changes'].mean():.1f}")

# =====================================================================
# STEP 4: Univariate tests (high vs low observer)
# =====================================================================
print(f"\n\n{'='*70}")
print("PANEL A: UNIVARIATE TESTS (High vs Low Observer Ratio)")
print(f"{'='*70}")

sample = master[master["is_private"] == 1].copy()  # private firms only
print(f"\nSample: {len(sample):,} private firms")
print(f"High observer (above median ratio): {sample['high_observer'].sum():,}")
print(f"Low observer (at or below median):  {(sample['high_observer']==0).sum():,}")

test_vars = [
    ("has_lawsuit", "Has Lawsuit (binary)"),
    ("has_restatement", "Has Restatement (binary)"),
    ("has_bankruptcy", "Has Bankruptcy (binary)"),
    ("is_failed", "Company Failed (binary)"),
    ("is_acquired", "Was Acquired (binary)"),
    ("has_regulatory_inquiry", "Regulatory Inquiry (binary)"),
    ("n_exec_board_changes", "N Exec/Board Changes (count)"),
    ("n_lawsuits", "N Lawsuits (count)"),
]

print(f"\n  {'Variable':35} {'High Mean':>10} {'Low Mean':>10} {'Diff':>8} {'t':>7} {'p':>8}")
print(f"  {'-'*80}")

for var, label in test_vars:
    high = sample.loc[sample["high_observer"] == 1, var].dropna()
    low = sample.loc[sample["high_observer"] == 0, var].dropna()
    if len(high) > 10 and len(low) > 10:
        t, p = stats.ttest_ind(high, low, equal_var=False)
        diff = high.mean() - low.mean()
        sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"  {label:35} {high.mean():>10.4f} {low.mean():>10.4f} {diff:>8.4f} {t:>7.2f} {p:>7.4f} {sig}")

# =====================================================================
# STEP 5: Logistic regressions (binary outcomes)
# =====================================================================
print(f"\n\n{'='*70}")
print("PANEL B: LOGISTIC REGRESSIONS (Binary Outcomes)")
print(f"{'='*70}")

binary_outcomes = [
    ("has_lawsuit", "Has Lawsuit"),
    ("has_restatement", "Has Restatement"),
    ("has_bankruptcy", "Has Bankruptcy"),
    ("is_failed", "Company Failed"),
    ("is_acquired", "Was Acquired"),
    ("has_regulatory_inquiry", "Regulatory Inquiry"),
]

treatments = [
    ("observer_ratio", "Observer Ratio (continuous)"),
    ("high_observer", "High Observer (binary)"),
    ("n_observers", "N Observers (count)"),
]

for treat_var, treat_label in treatments:
    print(f"\n  ### Treatment: {treat_label} ###")
    print(f"  {'Outcome':25} {'Coef':>8} {'OR':>8} {'z':>7} {'p':>8} {'Sig':>5} {'N':>6} {'Events':>7}")
    print(f"  {'-'*75}")

    for outcome_var, outcome_label in binary_outcomes:
        sub = sample.dropna(subset=[outcome_var, treat_var]).copy()
        n_events = sub[outcome_var].sum()

        if n_events < 5:
            print(f"  {outcome_label:25} {'too few events':>30} {int(n_events):>7}")
            continue

        try:
            # Model with controls
            X_vars = [treat_var, "log_board_size", "has_advisory", "is_us"]
            sub_clean = sub.dropna(subset=X_vars + [outcome_var])

            if len(sub_clean) < 30 or sub_clean[outcome_var].sum() < 5:
                continue

            X = sm.add_constant(sub_clean[X_vars])
            model = sm.Logit(sub_clean[outcome_var], X).fit(disp=0, maxiter=100)

            coef = model.params[treat_var]
            odds = np.exp(coef)
            z = model.tvalues[treat_var]
            p = model.pvalues[treat_var]
            sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""

            print(f"  {outcome_label:25} {coef:>8.4f} {odds:>8.3f} {z:>7.2f} {p:>7.4f} {sig:>5} {len(sub_clean):>6} {int(sub_clean[outcome_var].sum()):>7}")
        except Exception as e:
            print(f"  {outcome_label:25} ERROR: {str(e)[:40]}")

# =====================================================================
# STEP 6: OLS regressions (count/continuous outcomes)
# =====================================================================
print(f"\n\n{'='*70}")
print("PANEL C: OLS REGRESSIONS (Count/Continuous Outcomes)")
print(f"{'='*70}")

count_outcomes = [
    ("n_exec_board_changes", "N Exec/Board Changes"),
    ("log_exec_changes", "Log(1+Exec Changes)"),
    ("n_lawsuits", "N Lawsuits"),
    ("log_lawsuits", "Log(1+Lawsuits)"),
]

for treat_var, treat_label in treatments:
    print(f"\n  ### Treatment: {treat_label} ###")
    print(f"  {'Outcome':30} {'Coef':>8} {'t':>7} {'p':>8} {'Sig':>5} {'N':>6}")
    print(f"  {'-'*65}")

    for outcome_var, outcome_label in count_outcomes:
        sub = sample.dropna(subset=[outcome_var, treat_var, "log_board_size", "is_us"]).copy()

        if len(sub) < 30:
            continue

        try:
            m = smf.ols(
                f"{outcome_var} ~ {treat_var} + log_board_size + has_advisory + is_us",
                data=sub
            ).fit(cov_type="HC1")

            coef = m.params[treat_var]
            t = m.tvalues[treat_var]
            p = m.pvalues[treat_var]
            sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""

            print(f"  {outcome_label:30} {coef:>8.4f} {t:>7.2f} {p:>7.4f} {sig:>5} {int(m.nobs):>6}")
        except Exception as e:
            print(f"  {outcome_label:30} ERROR: {str(e)[:40]}")

# =====================================================================
# STEP 7: Observer-to-Director Transitions
# =====================================================================
print(f"\n\n{'='*70}")
print("PANEL D: OBSERVER-TO-DIRECTOR TRANSITIONS")
print(f"{'='*70}")

# Load observer records
observers = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
observers["personid"] = observers["personid"].astype(str).str.replace(".0", "", regex=False)

# Load all board members at observer companies
directors = pd.read_csv(os.path.join(ciq_dir, "03_directors_at_observer_companies.csv"))
directors["personid"] = directors["personid"].astype(str).str.replace(".0", "", regex=False)
directors["companyid"] = directors["companyid"].astype(str).str.replace(".0", "", regex=False)

# Find people who are observers AND directors at the SAME company
observer_persons = set(zip(observers["personid"], observers["companyid"].astype(str).str.replace(".0", "", regex=False)))
director_records = directors[directors["title"].str.contains("Director|Chairman", case=False, na=False)]
director_persons = set(zip(director_records["personid"], director_records["companyid"]))

transitions = observer_persons & director_persons
obs_only = observer_persons - director_persons

print(f"  Observer-company pairs: {len(observer_persons):,}")
print(f"  Observer who ALSO became director (same company): {len(transitions):,}")
print(f"  Observer-only (never became director): {len(obs_only):,}")
print(f"  Transition rate: {100*len(transitions)/len(observer_persons):.1f}%")

# Look at title patterns for transitioned persons
transition_pids = set(t[0] for t in transitions)
transition_titles = directors[
    directors["personid"].isin(transition_pids)
]["title"].value_counts().head(15)
print(f"\n  Most common titles for observers who became directors:")
for title, count in transition_titles.items():
    print(f"    {title:50} {count:>5}")

# =====================================================================
# STEP 8: Board composition analysis
# =====================================================================
print(f"\n\n{'='*70}")
print("PANEL E: BOARD COMPOSITION ANALYSIS")
print(f"{'='*70}")

# Board composition by company type
for comp_type in ["Private Company", "Public Company"]:
    sub = master[master["companytypename"] == comp_type]
    print(f"\n  {comp_type} (N={len(sub):,}):")
    print(f"    Directors:      mean={sub['n_directors'].mean():>6.1f}  median={sub['n_directors'].median():>6.1f}")
    print(f"    Observers:      mean={sub['n_observers'].mean():>6.1f}  median={sub['n_observers'].median():>6.1f}")
    print(f"    Advisory:       mean={sub['n_advisory'].mean():>6.1f}  median={sub['n_advisory'].median():>6.1f}")
    print(f"    Total board:    mean={sub['total_board'].mean():>6.1f}  median={sub['total_board'].median():>6.1f}")
    print(f"    Observer ratio: mean={sub['observer_ratio'].mean():>6.3f}  median={sub['observer_ratio'].median():>6.3f}")

# By country
print(f"\n  By country (top 5):")
for country in master["country"].value_counts().head(5).index:
    sub = master[master["country"] == country]
    print(f"    {country:20} N={len(sub):>5}  obs_ratio={sub['observer_ratio'].mean():.3f}  directors={sub['n_directors'].mean():.1f}")

# By founding decade
master["decade"] = (master["yearfounded"] // 10 * 10).astype("Int64")
print(f"\n  By founding decade:")
for decade in sorted(master["decade"].dropna().unique()):
    if decade >= 1990:
        sub = master[master["decade"] == decade]
        print(f"    {int(decade)}s: N={len(sub):>5}  obs_ratio={sub['observer_ratio'].mean():.3f}  observers={sub['n_observers'].mean():.1f}")

# =====================================================================
# SUMMARY
# =====================================================================
print(f"\n\n{'='*70}")
print("TEST 5 SUMMARY: Full CIQ Private Firm Results")
print(f"{'='*70}")
print(f"\n  Sample: {len(sample):,} private firms (NO CRSP/Compustat matching required)")
print(f"  This is {len(sample)/184*100:.0f}x larger than the CRSP-matched sample (184)")
