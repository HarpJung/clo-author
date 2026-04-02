"""
Match CIQ observed companies to Preqin VC deals and test whether
companies with board observers raise more capital.

Cross-sectional regressions:
  ln_total_funding = b1*n_observers + b2*year_founded + industry_FE
  n_rounds          = b1*n_observers + b2*year_founded + industry_FE
  realized(logistic) = b1*n_observers + controls

Compares companies with 0, 1, and 2+ observers.
"""

import sys, os, re, warnings
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
from scipy import stats

# ── paths ────────────────────────────────────────────────────────────
DATA_CIQ = r"C:\Users\hjung\Documents\Claude\CorpAcct\Data\CIQ_Extract"
DATA_PRQ = r"C:\Users\hjung\Documents\Claude\CorpAcct\Data\Preqin"
OUT_DIR  = r"C:\Users\hjung\Documents\Claude\CorpAcct\clo-author\quality_reports"
os.makedirs(OUT_DIR, exist_ok=True)

# ====================================================================
# 1. Load CIQ observed companies — US, private only
# ====================================================================
print("=" * 72)
print("STEP 1: Load CIQ observed companies (US, private)")
print("=" * 72)

ciq_co = pd.read_csv(os.path.join(DATA_CIQ, "04_observer_company_details.csv"))
print(f"  Raw CIQ companies: {len(ciq_co):,}")

# Filter US only
ciq_co = ciq_co[ciq_co["country"] == "United States"].copy()
print(f"  After US filter:   {len(ciq_co):,}")

# Filter private only (keep Private Company, exclude Public Company)
private_types = ciq_co["companytypename"].str.contains("Private", case=False, na=False)
ciq_co = ciq_co[private_types].copy()
print(f"  After private filter: {len(ciq_co):,}")
print(f"  Company types: {ciq_co['companytypename'].value_counts().to_dict()}")

# ====================================================================
# 2. Load CIQ observer records — count observers per company
# ====================================================================
print("\n" + "=" * 72)
print("STEP 2: Count observers per company from CIQ records")
print("=" * 72)

obs_rec = pd.read_csv(os.path.join(DATA_CIQ, "01_observer_records.csv"))
print(f"  Raw observer records: {len(obs_rec):,}")

# Count distinct persons per company
obs_count = (obs_rec.groupby("companyid")["personid"]
             .nunique()
             .reset_index()
             .rename(columns={"personid": "n_observers"}))
print(f"  Companies with observer counts: {len(obs_count):,}")
print(f"  Observer count distribution:")
print(obs_count["n_observers"].describe().to_string())

# Merge onto CIQ companies
ciq_co = ciq_co.merge(obs_count, on="companyid", how="left")
ciq_co["n_observers"] = ciq_co["n_observers"].fillna(0).astype(int)
print(f"\n  CIQ companies with observer counts merged: {len(ciq_co):,}")
print(f"  Observer distribution among US private companies:")
print(ciq_co["n_observers"].value_counts().sort_index().to_string())

# ====================================================================
# 3. Load Preqin VC deals — US only
# ====================================================================
print("\n" + "=" * 72)
print("STEP 3: Load Preqin VC deals (US only)")
print("=" * 72)

prq = pd.read_csv(os.path.join(DATA_PRQ, "vc_deals_full.csv"), low_memory=False)
print(f"  Raw Preqin deals: {len(prq):,}")

# US filter: country contains "US" or state is not null
us_country = prq["portfolio_company_country"].fillna("").str.contains("US", case=False)
us_state   = prq["portfolio_company_state"].notna() & (prq["portfolio_company_state"] != "")
prq = prq[us_country | us_state].copy()
print(f"  After US filter: {len(prq):,}")
print(f"  Unique portfolio companies: {prq['portfolio_company_name'].nunique():,}")

# Parse deal date
prq["deal_date"] = pd.to_datetime(prq["deal_date"], errors="coerce")
prq["deal_year"] = prq["deal_date"].dt.year

# Clean deal_financing_size_usd
prq["deal_financing_size_usd"] = pd.to_numeric(
    prq["deal_financing_size_usd"], errors="coerce"
)

# Parse year_established — column is empty in this extract, so extract from firm_about
prq["year_established"] = pd.to_numeric(prq["year_established"], errors="coerce")

# Extract founding year from firm_about text (patterns: "Founded in YYYY", "Established in YYYY",
# "founded in YYYY", "was founded in YYYY")
def extract_year_from_about(text):
    if pd.isna(text):
        return np.nan
    m = re.search(r"(?:founded|established)\s+in\s+(\d{4})", str(text), re.IGNORECASE)
    if m:
        yr = int(m.group(1))
        if 1900 <= yr <= 2025:
            return yr
    return np.nan

prq["year_from_about"] = prq["firm_about"].apply(extract_year_from_about)
prq["year_established"] = prq["year_established"].fillna(prq["year_from_about"])
n_extracted = prq["year_from_about"].notna().sum()
print(f"  Years extracted from firm_about: {n_extracted:,} deals")
n_unique_co_with_yr = prq.loc[prq["year_established"].notna(), "portfolio_company_name"].nunique()
print(f"  Unique companies with year_established: {n_unique_co_with_yr:,}")

print(f"  Deals with non-null financing size: {prq['deal_financing_size_usd'].notna().sum():,}")
print(f"  Stage distribution (top 10):")
print(prq["stage"].value_counts().head(10).to_string())

# ====================================================================
# 4. Name-matching: CIQ companies → Preqin deals
# ====================================================================
print("\n" + "=" * 72)
print("STEP 4: Name-match CIQ companies to Preqin deals")
print("=" * 72)

def clean_name(name):
    """Standardize company name for matching."""
    if pd.isna(name):
        return ""
    s = str(name).lower().strip()
    # Remove common suffixes
    for suffix in [", inc.", ", inc", " inc.", " inc", ", llc", " llc",
                   ", corp.", " corp.", ", corp", " corp",
                   ", ltd.", " ltd.", ", ltd", " ltd",
                   ", co.", " co.", " company", ", company",
                   ", l.p.", " l.p.", ", lp", " lp",
                   " holdings", " group", " technologies",
                   " technology", " solutions", " services",
                   " international", " global", " systems",
                   ", s.a.", " s.a."]:
        if s.endswith(suffix):
            s = s[:-len(suffix)]
    # Remove punctuation
    s = re.sub(r"[^a-z0-9\s]", "", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s

# Clean CIQ names
ciq_co["name_clean"] = ciq_co["companyname"].apply(clean_name)

# Build Preqin company-level lookup (unique companies)
prq_companies = (prq.groupby("portfolio_company_name")
                 .agg(year_established=("year_established", "first"),
                      primary_industry=("primary_industry", "first"),
                      industry_classification=("industry_classification", "first"))
                 .reset_index())
prq_companies["name_clean"] = prq_companies["portfolio_company_name"].apply(clean_name)

# ── Exact match ──────────────────────────────────────────────────────
print("  Attempting exact name matches...")
exact = ciq_co.merge(
    prq_companies[["name_clean", "portfolio_company_name", "year_established",
                    "primary_industry", "industry_classification"]],
    on="name_clean",
    how="inner"
)
exact["match_type"] = "exact"
print(f"    Exact matches: {len(exact):,}")

# ── Fuzzy match (containment) for unmatched ──────────────────────────
matched_ciq_ids = set(exact["companyid"])
unmatched_ciq = ciq_co[~ciq_co["companyid"].isin(matched_ciq_ids)].copy()
print(f"  Attempting fuzzy matches for {len(unmatched_ciq):,} unmatched companies...")

# Build lookup dict for Preqin names
prq_name_dict = {}
for _, row in prq_companies.iterrows():
    cn = row["name_clean"]
    if cn and len(cn) >= 4:  # skip very short names
        prq_name_dict[cn] = row

fuzzy_matches = []
for _, ciq_row in unmatched_ciq.iterrows():
    ciq_name = ciq_row["name_clean"]
    if not ciq_name or len(ciq_name) < 4:
        continue
    best_match = None
    for prq_name, prq_row in prq_name_dict.items():
        # Check containment (one contains the other)
        if ciq_name in prq_name or prq_name in ciq_name:
            # Tighten: shorter name must be >= 70% of longer name length
            shorter = min(len(ciq_name), len(prq_name))
            longer  = max(len(ciq_name), len(prq_name))
            if shorter / longer < 0.70:
                continue
            # Validate by founding year if both available
            ciq_yr = ciq_row.get("yearfounded")
            prq_yr = prq_row.get("year_established")
            if pd.notna(ciq_yr) and pd.notna(prq_yr):
                if abs(float(ciq_yr) - float(prq_yr)) > 2:
                    continue
            # Accept match — prefer longer overlap
            overlap = shorter
            if best_match is None or overlap > best_match[1]:
                best_match = (prq_row, overlap)

    if best_match is not None:
        prq_row = best_match[0]
        row_dict = ciq_row.to_dict()
        row_dict["portfolio_company_name"] = prq_row["portfolio_company_name"]
        row_dict["year_established"] = prq_row["year_established"]
        row_dict["primary_industry"] = prq_row["primary_industry"]
        row_dict["industry_classification"] = prq_row["industry_classification"]
        row_dict["match_type"] = "fuzzy"
        fuzzy_matches.append(row_dict)

fuzzy_df = pd.DataFrame(fuzzy_matches)
print(f"    Fuzzy matches: {len(fuzzy_df):,}")

# Combine matches
if len(fuzzy_df) > 0:
    matched = pd.concat([exact, fuzzy_df], ignore_index=True)
else:
    matched = exact.copy()

# Deduplicate: keep one match per CIQ company (prefer exact)
matched = matched.sort_values("match_type").drop_duplicates(subset="companyid", keep="first")
print(f"  Total unique matched companies: {len(matched):,}")
print(f"  Match type breakdown: {matched['match_type'].value_counts().to_dict()}")

match_rate = len(matched) / len(ciq_co) * 100
print(f"  Overall match rate: {match_rate:.1f}%")

# ====================================================================
# 5. Compute deal-level outcomes for matched companies
# ====================================================================
print("\n" + "=" * 72)
print("STEP 5: Compute funding outcomes for matched companies")
print("=" * 72)

# Merge matched company names back to Preqin deals
matched_deals = prq.merge(
    matched[["companyid", "n_observers", "portfolio_company_name",
             "yearfounded", "match_type"]],
    on="portfolio_company_name",
    how="inner"
)
print(f"  Deals for matched companies: {len(matched_deals):,}")

# Define stage ordering
stage_order = {
    "Angel": 0, "Pre-Seed": 1, "Seed": 2,
    "Series A": 3, "Series A / Round 1": 3,
    "Series B": 4, "Series B / Round 2": 4,
    "Series C": 5, "Series C / Round 3": 5,
    "Series D": 6, "Series D / Round 4": 6,
    "Series E": 7, "Series E / Round 5": 7,
    "Series F": 8, "Series F+": 8,
    "Series G": 9, "Series H": 10, "Series I": 11,
    "Venture Round": 3,
    "Growth Round": 7, "Growth Round / Series A": 7,
    "Unspecified Round": -1,
    "Grant": -2, "Add-on": -1,
}

def highest_stage(stages):
    """Return highest stage reached."""
    vals = [stage_order.get(s, -1) for s in stages if pd.notna(s)]
    if not vals:
        return -1
    return max(vals)

# Company-level aggregation from deals
company_outcomes = (matched_deals
    .groupby(["companyid", "portfolio_company_name", "n_observers",
              "yearfounded", "match_type"])
    .agg(
        total_funding=("deal_financing_size_usd", "sum"),
        n_rounds=("deal_date", "count"),
        n_rounds_with_funding=("deal_financing_size_usd", lambda x: x.notna().sum()),
        highest_stage=("stage", highest_stage),
        first_deal_date=("deal_date", "min"),
        last_deal_date=("deal_date", "max"),
        investment_status=("investment_status", "first"),
        primary_industry=("primary_industry", "first"),
        industry_classification=("industry_classification", "first"),
    )
    .reset_index()
)

# Time between rounds (days from first to last / (n_rounds - 1))
company_outcomes["funding_span_days"] = (
    (company_outcomes["last_deal_date"] - company_outcomes["first_deal_date"]).dt.days
)
company_outcomes["avg_days_between_rounds"] = np.where(
    company_outcomes["n_rounds"] > 1,
    company_outcomes["funding_span_days"] / (company_outcomes["n_rounds"] - 1),
    np.nan
)

# Log total funding
company_outcomes["ln_total_funding"] = np.log1p(company_outcomes["total_funding"])

# Realized indicator
company_outcomes["is_realized"] = (
    company_outcomes["investment_status"] == "Realized"
).astype(int)

# Observer categories
company_outcomes["obs_group"] = pd.cut(
    company_outcomes["n_observers"],
    bins=[-0.5, 0.5, 1.5, 100],
    labels=["0 observers", "1 observer", "2+ observers"]
)

print(f"  Companies with outcomes: {len(company_outcomes):,}")
print(f"  Companies with positive total funding: "
      f"{(company_outcomes['total_funding'] > 0).sum():,}")

# ====================================================================
# 6. Build control group: Preqin companies NOT in CIQ (0 observers)
# ====================================================================
print("\n" + "=" * 72)
print("STEP 6: Build control group (Preqin companies not in CIQ)")
print("=" * 72)

matched_prq_names = set(matched["portfolio_company_name"].unique())
control_deals = prq[~prq["portfolio_company_name"].isin(matched_prq_names)].copy()
print(f"  Control deals (not matched to CIQ): {len(control_deals):,}")

control_outcomes = (control_deals
    .groupby("portfolio_company_name")
    .agg(
        total_funding=("deal_financing_size_usd", "sum"),
        n_rounds=("deal_date", "count"),
        n_rounds_with_funding=("deal_financing_size_usd", lambda x: x.notna().sum()),
        highest_stage=("stage", highest_stage),
        first_deal_date=("deal_date", "min"),
        last_deal_date=("deal_date", "max"),
        investment_status=("investment_status", "first"),
        primary_industry=("primary_industry", "first"),
        industry_classification=("industry_classification", "first"),
        yearfounded=("year_established", "first"),
    )
    .reset_index()
)

control_outcomes["n_observers"] = 0
control_outcomes["companyid"] = np.nan
control_outcomes["match_type"] = "control"
control_outcomes["obs_group"] = "0 observers"
# yearfounded already comes from year_established via the agg above

control_outcomes["funding_span_days"] = (
    (control_outcomes["last_deal_date"] - control_outcomes["first_deal_date"]).dt.days
)
control_outcomes["avg_days_between_rounds"] = np.where(
    control_outcomes["n_rounds"] > 1,
    control_outcomes["funding_span_days"] / (control_outcomes["n_rounds"] - 1),
    np.nan
)
control_outcomes["ln_total_funding"] = np.log1p(control_outcomes["total_funding"])
control_outcomes["is_realized"] = (
    control_outcomes["investment_status"] == "Realized"
).astype(int)

print(f"  Control companies: {len(control_outcomes):,}")

# ====================================================================
# 7. Combine and create analysis sample
# ====================================================================
print("\n" + "=" * 72)
print("STEP 7: Combine treatment and control for analysis")
print("=" * 72)

# Align columns
keep_cols = ["companyid", "portfolio_company_name", "n_observers", "obs_group",
             "yearfounded", "total_funding", "ln_total_funding",
             "n_rounds", "n_rounds_with_funding", "highest_stage",
             "avg_days_between_rounds", "funding_span_days",
             "investment_status", "is_realized",
             "primary_industry", "industry_classification", "match_type"]

for col in keep_cols:
    if col not in company_outcomes.columns:
        company_outcomes[col] = np.nan
    if col not in control_outcomes.columns:
        control_outcomes[col] = np.nan

full = pd.concat([company_outcomes[keep_cols], control_outcomes[keep_cols]],
                 ignore_index=True)

# Has observer indicator (any CIQ match = has observer)
full["has_observer"] = (full["n_observers"] > 0).astype(int)
full["n_observers_winsor"] = full["n_observers"].clip(upper=full["n_observers"].quantile(0.99))

# Clean yearfounded
full["yearfounded"] = pd.to_numeric(full["yearfounded"], errors="coerce")

# Drop companies with zero / missing funding for log regressions
full_funded = full[full["total_funding"] > 0].copy()

print(f"  Full sample: {len(full):,} companies")
print(f"  Funded sample (total_funding > 0): {len(full_funded):,}")
print(f"  By observer group:")
print(full["obs_group"].value_counts().to_string())

# ====================================================================
# 8. Summary statistics
# ====================================================================
print("\n" + "=" * 72)
print("STEP 8: Summary statistics by observer group")
print("=" * 72)

summary_vars = ["total_funding", "n_rounds", "highest_stage",
                "avg_days_between_rounds", "is_realized", "yearfounded"]

for grp_name, grp_df in full.groupby("obs_group", observed=True):
    print(f"\n  --- {grp_name} (N = {len(grp_df):,}) ---")
    for var in summary_vars:
        col = grp_df[var].dropna()
        if len(col) > 0:
            print(f"    {var:30s}  mean={col.mean():12.2f}  "
                  f"median={col.median():12.2f}  sd={col.std():12.2f}  "
                  f"N={len(col):,}")

# T-tests: 0 vs 1 observer, 0 vs 2+
print("\n  T-tests (Welch):")
for var in ["total_funding", "n_rounds", "is_realized"]:
    g0 = full.loc[full["obs_group"] == "0 observers", var].dropna()
    g1 = full.loc[full["obs_group"] == "1 observer", var].dropna()
    g2 = full.loc[full["obs_group"] == "2+ observers", var].dropna()
    if len(g0) > 1 and len(g1) > 1:
        t1, p1 = stats.ttest_ind(g1, g0, equal_var=False)
        print(f"    {var:30s}  1 vs 0: t={t1:7.3f}, p={p1:.4f}")
    if len(g0) > 1 and len(g2) > 1:
        t2, p2 = stats.ttest_ind(g2, g0, equal_var=False)
        print(f"    {var:30s}  2+ vs 0: t={t2:7.3f}, p={p2:.4f}")

# ====================================================================
# 9. Regressions
# ====================================================================
print("\n" + "=" * 72)
print("STEP 9: Cross-sectional regressions")
print("=" * 72)

# Prepare industry FE — use industry_classification (broad)
full_funded["ind_fe"] = full_funded["industry_classification"].fillna("Unknown")
full["ind_fe"] = full["industry_classification"].fillna("Unknown")

# Drop singletons
ind_counts = full_funded["ind_fe"].value_counts()
valid_inds = ind_counts[ind_counts >= 5].index
full_funded_reg = full_funded[full_funded["ind_fe"].isin(valid_inds)].copy()
full_reg = full[full["ind_fe"].isin(valid_inds)].copy()

print(f"  Regression sample (funded, non-singleton FE): {len(full_funded_reg):,}")
print(f"  Full regression sample (non-singleton FE):    {len(full_reg):,}")
print(f"  Industries in FE: {full_funded_reg['ind_fe'].nunique()}")

# ── Helper for pretty printing ───────────────────────────────────────
def print_reg(model, title):
    """Print compact regression output."""
    print(f"\n  {'─' * 68}")
    print(f"  {title}")
    print(f"  {'─' * 68}")
    print(f"  N = {int(model.nobs):,}   R² = {model.rsquared:.4f}   "
          f"Adj-R² = {model.rsquared_adj:.4f}")
    # Print key coefficients (not FE dummies)
    for name in model.params.index:
        if "ind_fe" in name or name == "Intercept":
            continue
        coef = model.params[name]
        se   = model.bse[name]
        t    = model.tvalues[name]
        p    = model.pvalues[name]
        stars = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.1 else ""
        print(f"    {name:30s}  coef={coef:10.4f}  se={se:10.4f}  "
              f"t={t:7.3f}  p={p:.4f} {stars}")

def print_logit(model, title):
    """Print compact logistic regression output."""
    print(f"\n  {'─' * 68}")
    print(f"  {title}")
    print(f"  {'─' * 68}")
    print(f"  N = {int(model.nobs):,}   Pseudo-R² = {model.prsquared:.4f}")
    for name in model.params.index:
        if "ind_fe" in name or name == "Intercept":
            continue
        coef = model.params[name]
        se   = model.bse[name]
        z    = model.tvalues[name]
        p    = model.pvalues[name]
        odds = np.exp(coef)
        stars = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.1 else ""
        print(f"    {name:30s}  coef={coef:10.4f}  se={se:10.4f}  "
              f"z={z:7.3f}  p={p:.4f}  OR={odds:.4f} {stars}")

# ═══════════════════════════════════════════════════════════════════════
# Panel A: FULL SAMPLE, no yearfounded (maximize N)
# ═══════════════════════════════════════════════════════════════════════
print("\n  === PANEL A: FULL SAMPLE — industry FE only (all obs) ===")

try:
    m1a = smf.ols(
        "ln_total_funding ~ n_observers + C(ind_fe)",
        data=full_funded_reg
    ).fit(cov_type="HC1")
    print_reg(m1a, "A1: ln(total_funding) ~ n_observers + industry_FE  [OLS, HC1]")
except Exception as e:
    print(f"  A1 FAILED: {e}")

try:
    m2a = smf.ols(
        "n_rounds ~ n_observers + C(ind_fe)",
        data=full_reg
    ).fit(cov_type="HC1")
    print_reg(m2a, "A2: n_rounds ~ n_observers + industry_FE  [OLS, HC1]")
except Exception as e:
    print(f"  A2 FAILED: {e}")

try:
    m3a = smf.logit(
        "is_realized ~ n_observers + C(ind_fe)",
        data=full_reg
    ).fit(disp=0, maxiter=200)
    print_logit(m3a, "A3: Pr(Realized) ~ n_observers + industry_FE  [Logit]")
except Exception as e:
    print(f"  A3 FAILED: {e}")

# Binary treatment
try:
    m4a = smf.ols(
        "ln_total_funding ~ has_observer + C(ind_fe)",
        data=full_funded_reg
    ).fit(cov_type="HC1")
    print_reg(m4a, "A4: ln(total_funding) ~ has_observer + industry_FE  [OLS, HC1]")
except Exception as e:
    print(f"  A4 FAILED: {e}")

try:
    m5a = smf.ols(
        "n_rounds ~ has_observer + C(ind_fe)",
        data=full_reg
    ).fit(cov_type="HC1")
    print_reg(m5a, "A5: n_rounds ~ has_observer + industry_FE  [OLS, HC1]")
except Exception as e:
    print(f"  A5 FAILED: {e}")

try:
    m6a = smf.logit(
        "is_realized ~ has_observer + C(ind_fe)",
        data=full_reg
    ).fit(disp=0, maxiter=200)
    print_logit(m6a, "A6: Pr(Realized) ~ has_observer + industry_FE  [Logit]")
except Exception as e:
    print(f"  A6 FAILED: {e}")

# ═══════════════════════════════════════════════════════════════════════
# Panel B: FULL SAMPLE with yearfounded control (subset with non-null year)
# ═══════════════════════════════════════════════════════════════════════
print("\n\n  === PANEL B: FULL SAMPLE — with yearfounded control ===")

reg_yr_funded = full_funded_reg.dropna(subset=["ln_total_funding", "yearfounded"]).copy()
reg_yr_all    = full_reg.dropna(subset=["yearfounded"]).copy()
print(f"  Funded sample with yearfounded: {len(reg_yr_funded):,}")
print(f"  Full sample with yearfounded:   {len(reg_yr_all):,}")

try:
    m1b = smf.ols(
        "ln_total_funding ~ n_observers + yearfounded + C(ind_fe)",
        data=reg_yr_funded
    ).fit(cov_type="HC1")
    print_reg(m1b, "B1: ln(total_funding) ~ n_observers + yearfounded + industry_FE  [OLS, HC1]")
except Exception as e:
    print(f"  B1 FAILED: {e}")

try:
    m2b = smf.ols(
        "n_rounds ~ n_observers + yearfounded + C(ind_fe)",
        data=reg_yr_all
    ).fit(cov_type="HC1")
    print_reg(m2b, "B2: n_rounds ~ n_observers + yearfounded + industry_FE  [OLS, HC1]")
except Exception as e:
    print(f"  B2 FAILED: {e}")

if reg_yr_all["is_realized"].nunique() > 1:
    try:
        m3b = smf.logit(
            "is_realized ~ n_observers + yearfounded + C(ind_fe)",
            data=reg_yr_all
        ).fit(disp=0, maxiter=200)
        print_logit(m3b, "B3: Pr(Realized) ~ n_observers + yearfounded + industry_FE  [Logit]")
    except Exception as e:
        print(f"  B3 FAILED: {e}")

# Binary treatment with yearfounded
try:
    m4b = smf.ols(
        "ln_total_funding ~ has_observer + yearfounded + C(ind_fe)",
        data=reg_yr_funded
    ).fit(cov_type="HC1")
    print_reg(m4b, "B4: ln(total_funding) ~ has_observer + yearfounded + industry_FE  [OLS, HC1]")
except Exception as e:
    print(f"  B4 FAILED: {e}")

try:
    m5b = smf.ols(
        "n_rounds ~ has_observer + yearfounded + C(ind_fe)",
        data=reg_yr_all
    ).fit(cov_type="HC1")
    print_reg(m5b, "B5: n_rounds ~ has_observer + yearfounded + industry_FE  [OLS, HC1]")
except Exception as e:
    print(f"  B5 FAILED: {e}")

if reg_yr_all["is_realized"].nunique() > 1:
    try:
        m6b = smf.logit(
            "is_realized ~ has_observer + yearfounded + C(ind_fe)",
            data=reg_yr_all
        ).fit(disp=0, maxiter=200)
        print_logit(m6b, "B6: Pr(Realized) ~ has_observer + yearfounded + industry_FE  [Logit]")
    except Exception as e:
        print(f"  B6 FAILED: {e}")

# ── 9G-H. Industry subsamples (full sample within industry) ──────────
for ind_label, ind_pattern in [("Information Technology", "Information Technology"),
                                ("Healthcare", "Healthcare")]:
    tag = "9G" if ind_label == "Information Technology" else "9H"
    print(f"\n\n  === {ind_label.upper()} SUBSAMPLE ===")
    ind_mask = full_funded_reg["industry_classification"].str.contains(
        ind_pattern, case=False, na=False
    )
    ind_df = full_funded_reg[ind_mask].dropna(
        subset=["ln_total_funding", "n_observers", "yearfounded"]
    )
    print(f"  {ind_label} subsample: {len(ind_df):,}")
    if len(ind_df) > 30:
        try:
            m_ind = smf.ols(
                "ln_total_funding ~ n_observers + yearfounded",
                data=ind_df
            ).fit(cov_type="HC1")
            print_reg(m_ind, f"{tag}: ln(total_funding) ~ n_observers + yearfounded  "
                      f"[{ind_label}, OLS, HC1]")
        except Exception as e:
            print(f"  {tag} FAILED: {e}")

        # Also binary treatment within industry
        try:
            m_ind_bin = smf.ols(
                "ln_total_funding ~ has_observer + yearfounded",
                data=ind_df
            ).fit(cov_type="HC1")
            print_reg(m_ind_bin, f"{tag}b: ln(total_funding) ~ has_observer + yearfounded  "
                      f"[{ind_label}, OLS, HC1]")
        except Exception as e:
            print(f"  {tag}b FAILED: {e}")

# ====================================================================
# 10. Match quality diagnostics
# ====================================================================
print("\n" + "=" * 72)
print("STEP 10: Match quality diagnostics")
print("=" * 72)

# Year validation for matches
yr_compare = matched.dropna(subset=["yearfounded", "year_established"]).copy()
yr_compare["yr_diff"] = abs(yr_compare["yearfounded"] - yr_compare["year_established"])
print(f"\n  Matched companies with both founding years: {len(yr_compare):,}")
if len(yr_compare) > 0:
    print(f"  Year difference (|CIQ - Preqin|):")
    print(f"    Mean:   {yr_compare['yr_diff'].mean():.2f}")
    print(f"    Median: {yr_compare['yr_diff'].median():.1f}")
    print(f"    ≤ 0 years: {(yr_compare['yr_diff'] <= 0).sum():,} "
          f"({(yr_compare['yr_diff'] <= 0).mean()*100:.1f}%)")
    print(f"    ≤ 1 year:  {(yr_compare['yr_diff'] <= 1).sum():,} "
          f"({(yr_compare['yr_diff'] <= 1).mean()*100:.1f}%)")
    print(f"    ≤ 2 years: {(yr_compare['yr_diff'] <= 2).sum():,} "
          f"({(yr_compare['yr_diff'] <= 2).mean()*100:.1f}%)")

# Industry distribution of matched vs unmatched
print(f"\n  Industry distribution of matched CIQ companies:")
ind_matched = matched["industry_classification"].value_counts().head(10)
print(ind_matched.to_string())

# Sample matches for inspection
print(f"\n  Sample exact matches:")
sample_exact = matched[matched["match_type"] == "exact"].head(5)
for _, r in sample_exact.iterrows():
    print(f"    CIQ: {r['companyname']:40s} → Preqin: {r['portfolio_company_name']}")

if len(fuzzy_df) > 0:
    print(f"\n  Sample fuzzy matches:")
    sample_fuzzy = matched[matched["match_type"] == "fuzzy"].head(5)
    for _, r in sample_fuzzy.iterrows():
        print(f"    CIQ: {r['companyname']:40s} → Preqin: {r['portfolio_company_name']}")

# ====================================================================
# 11. Summary table
# ====================================================================
print("\n" + "=" * 72)
print("STEP 11: Summary table for paper")
print("=" * 72)

summary_rows = []
for grp in ["0 observers", "1 observer", "2+ observers"]:
    g = full[full["obs_group"] == grp]
    g_funded = g[g["total_funding"] > 0]
    # Note: deal_financing_size_usd in Preqin is in millions
    summary_rows.append({
        "Group": grp,
        "N_companies": len(g),
        "Pct_with_funding": f"{(g['total_funding'] > 0).mean()*100:.1f}%",
        "Mean_total_funding_M": f"${g_funded['total_funding'].mean():.1f}M" if len(g_funded) > 0 else "N/A",
        "Median_total_funding_M": f"${g_funded['total_funding'].median():.1f}M" if len(g_funded) > 0 else "N/A",
        "Mean_n_rounds": f"{g['n_rounds'].mean():.1f}",
        "Mean_highest_stage": f"{g['highest_stage'].mean():.1f}",
        "Pct_realized": f"{g['is_realized'].mean()*100:.1f}%",
        "Mean_year_founded": f"{g['yearfounded'].mean():.0f}" if g['yearfounded'].notna().any() else "N/A",
    })

summary_table = pd.DataFrame(summary_rows)
print(summary_table.to_string(index=False))

# Save summary
summary_table.to_csv(
    os.path.join(OUT_DIR, "observed_company_outcomes_summary.csv"),
    index=False
)
print(f"\n  Saved summary to: {OUT_DIR}/observed_company_outcomes_summary.csv")

print("\n" + "=" * 72)
print("DONE")
print("=" * 72)
