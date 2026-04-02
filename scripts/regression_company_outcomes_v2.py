"""
Matched-sample comparison of observed vs non-observed companies using
Preqin VC deal data.

Tests:
  1. Propensity score / CEM matching on industry, year founded, first deal
     year, first deal size quartile
  2. Time to next round (days between consecutive deals)
  3. Stage progression (ordinal highest stage reached)
  4. Exit outcomes with better controls
  5. Dose-response (1 vs 2 vs 3+ observers)
  6. Full specs: HC1, industry-clustered SE, industry FE + HC1

Data linkage: CIQ observed companies -> Preqin deals via name matching.
"""

import sys, os, re, warnings
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
from scipy import stats
from scipy.spatial import KDTree

# ── paths ────────────────────────────────────────────────────────────
DATA_CIQ = r"C:\Users\hjung\Documents\Claude\CorpAcct\Data\CIQ_Extract"
DATA_PRQ = r"C:\Users\hjung\Documents\Claude\CorpAcct\Data\Preqin"
DATA_IND = r"C:\Users\hjung\Documents\Claude\CorpAcct\Data\Panel_C_Network"
OUT_DIR  = r"C:\Users\hjung\Documents\Claude\CorpAcct\clo-author\quality_reports"
os.makedirs(OUT_DIR, exist_ok=True)

SEP = "=" * 76

# =====================================================================
# UTILITIES
# =====================================================================

def clean_name(name):
    """Standardize company name for matching."""
    if pd.isna(name):
        return ""
    s = str(name).lower().strip()
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
    s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_year_from_about(text):
    if pd.isna(text):
        return np.nan
    m = re.search(r"(?:founded|established)\s+in\s+(\d{4})", str(text),
                  re.IGNORECASE)
    if m:
        yr = int(m.group(1))
        if 1900 <= yr <= 2025:
            return yr
    return np.nan


# Stage ordinal: 1=Seed, 2=Series A, 3=Series B, 4=Series C+, 5=Late/Growth
STAGE_ORDINAL = {
    "Angel": 1, "Pre-Seed": 1, "Seed": 1, "Grant": 0,
    "Series A": 2, "Series A / Round 1": 2, "Venture Round": 2,
    "Series B": 3, "Series B / Round 2": 3,
    "Series C": 4, "Series C / Round 3": 4,
    "Series D": 4, "Series D / Round 4": 4,
    "Series E": 4, "Series E / Round 5": 4,
    "Series F": 4, "Series F+": 4, "Series G": 4,
    "Series H": 4, "Series I": 4, "Series J": 4,
    "Series K": 4, "Series L": 4,
    "Growth": 5, "Growth Round": 5, "Growth Round / Series A": 5,
    "Pre-IPO": 5, "PIPE": 5,
    "Unspecified Round": 0, "Add-on": 0, "Venture Debt": 0,
    "Merger": 0, "Secondary Stock Purchase": 0, "Venture Capital": 0,
}


def highest_stage_ordinal(stages):
    vals = [STAGE_ORDINAL.get(s, 0) for s in stages if pd.notna(s)]
    return max(vals) if vals else 0


def print_sep(title):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


def print_reg(model, title, key_vars=None):
    """Print compact OLS regression output."""
    print(f"\n  {'─' * 70}")
    print(f"  {title}")
    print(f"  {'─' * 70}")
    print(f"  N = {int(model.nobs):,}   R2 = {model.rsquared:.4f}   "
          f"Adj-R2 = {model.rsquared_adj:.4f}")
    for name in model.params.index:
        if "C(ind_fe)" in name or name == "Intercept":
            continue
        if key_vars and not any(k in name for k in key_vars):
            continue
        coef = model.params[name]
        se   = model.bse[name]
        t    = model.tvalues[name]
        p    = model.pvalues[name]
        stars = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.1 else ""
        print(f"    {name:35s}  b={coef:10.4f}  se={se:10.4f}  "
              f"t={t:7.3f}  p={p:.4f} {stars}")


def print_logit(model, title, key_vars=None):
    """Print compact logistic regression output."""
    print(f"\n  {'─' * 70}")
    print(f"  {title}")
    print(f"  {'─' * 70}")
    print(f"  N = {int(model.nobs):,}   Pseudo-R2 = {model.prsquared:.4f}")
    for name in model.params.index:
        if "C(ind_fe)" in name or name == "Intercept":
            continue
        if key_vars and not any(k in name for k in key_vars):
            continue
        coef = model.params[name]
        se   = model.bse[name]
        z    = model.tvalues[name]
        p    = model.pvalues[name]
        odds = np.exp(coef)
        stars = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.1 else ""
        print(f"    {name:35s}  b={coef:10.4f}  se={se:10.4f}  "
              f"z={z:7.3f}  p={p:.4f}  OR={odds:.4f} {stars}")


def run_ols_triple(formula, data, title_base, key_vars=None, cluster_var="ind_fe"):
    """Run OLS with (1) HC1, (2) industry-clustered SE, (3) industry FE + HC1.
    Returns list of fitted models."""
    results = []
    # ---------- (1) HC1 ----------
    try:
        m1 = smf.ols(formula, data=data).fit(cov_type="HC1")
        print_reg(m1, f"{title_base}  [HC1]", key_vars)
        results.append(m1)
    except Exception as e:
        print(f"    FAILED (HC1): {e}")
        results.append(None)

    # ---------- (2) Industry-clustered SE ----------
    try:
        groups = data[cluster_var].astype("category").cat.codes.values
        m2 = smf.ols(formula, data=data).fit(
            cov_type="cluster", cov_kwds={"groups": groups})
        print_reg(m2, f"{title_base}  [Cluster-{cluster_var}]", key_vars)
        results.append(m2)
    except Exception as e:
        print(f"    FAILED (Cluster): {e}")
        results.append(None)

    # ---------- (3) Industry FE + HC1 ----------
    fe_formula = formula + " + C(ind_fe)" if "C(ind_fe)" not in formula else formula
    try:
        m3 = smf.ols(fe_formula, data=data).fit(cov_type="HC1")
        print_reg(m3, f"{title_base}  [IndFE + HC1]", key_vars)
        results.append(m3)
    except Exception as e:
        print(f"    FAILED (IndFE+HC1): {e}")
        results.append(None)

    return results


def run_logit_triple(formula, data, title_base, key_vars=None, cluster_var="ind_fe"):
    """Run logit with three SE specs."""
    results = []
    # ---------- (1) HC1 ----------
    try:
        m1 = smf.logit(formula, data=data).fit(disp=0, maxiter=300,
                                                 cov_type="HC1")
        print_logit(m1, f"{title_base}  [HC1]", key_vars)
        results.append(m1)
    except Exception as e:
        print(f"    FAILED (HC1): {e}")
        results.append(None)

    # ---------- (2) Industry-clustered SE ----------
    try:
        groups = data[cluster_var].astype("category").cat.codes.values
        m2 = smf.logit(formula, data=data).fit(
            disp=0, maxiter=300,
            cov_type="cluster", cov_kwds={"groups": groups})
        print_logit(m2, f"{title_base}  [Cluster-{cluster_var}]", key_vars)
        results.append(m2)
    except Exception as e:
        print(f"    FAILED (Cluster): {e}")
        results.append(None)

    # ---------- (3) Industry FE + HC1 ----------
    fe_formula = formula + " + C(ind_fe)" if "C(ind_fe)" not in formula else formula
    try:
        m3 = smf.logit(fe_formula, data=data).fit(disp=0, maxiter=300,
                                                    cov_type="HC1")
        print_logit(m3, f"{title_base}  [IndFE + HC1]", key_vars)
        results.append(m3)
    except Exception as e:
        print(f"    FAILED (IndFE+HC1): {e}")
        results.append(None)

    return results


# =====================================================================
# 1. LOAD DATA
# =====================================================================
print_sep("STEP 1: Load CIQ observed companies (US, private)")

ciq_co = pd.read_csv(os.path.join(DATA_CIQ, "04_observer_company_details.csv"))
print(f"  Raw CIQ companies: {len(ciq_co):,}")
ciq_co = ciq_co[ciq_co["country"] == "United States"].copy()
print(f"  After US filter:   {len(ciq_co):,}")
ciq_co = ciq_co[ciq_co["companytypename"].str.contains("Private", case=False, na=False)].copy()
print(f"  After private filter: {len(ciq_co):,}")

# Observer counts
print_sep("STEP 2: Count observers per company")
obs_rec = pd.read_csv(os.path.join(DATA_CIQ, "01_observer_records.csv"))
obs_count = (obs_rec.groupby("companyid")["personid"]
             .nunique().reset_index()
             .rename(columns={"personid": "n_observers"}))
ciq_co = ciq_co.merge(obs_count, on="companyid", how="left")
ciq_co["n_observers"] = ciq_co["n_observers"].fillna(0).astype(int)
print(f"  Observer distribution:\n{ciq_co['n_observers'].value_counts().sort_index().to_string()}")

# Preqin deals
print_sep("STEP 3: Load Preqin VC deals (US only)")
prq = pd.read_csv(os.path.join(DATA_PRQ, "vc_deals_full.csv"), low_memory=False)
print(f"  Raw Preqin deals: {len(prq):,}")
us_mask = (prq["portfolio_company_country"].fillna("").str.contains("US", case=False)
           | prq["portfolio_company_state"].notna())
prq = prq[us_mask].copy()
print(f"  After US filter: {len(prq):,}")

prq["deal_date"] = pd.to_datetime(prq["deal_date"], errors="coerce")
prq["deal_year"] = prq["deal_date"].dt.year
prq["deal_financing_size_usd"] = pd.to_numeric(
    prq["deal_financing_size_usd"], errors="coerce")
prq["total_known_funding_usd"] = pd.to_numeric(
    prq["total_known_funding_usd"], errors="coerce")
prq["year_established"] = pd.to_numeric(prq["year_established"], errors="coerce")
prq["year_from_about"] = prq["firm_about"].apply(extract_year_from_about)
prq["year_established"] = prq["year_established"].fillna(prq["year_from_about"])

print(f"  Unique US portfolio companies: {prq['portfolio_company_name'].nunique():,}")

# =====================================================================
# 2. NAME MATCHING: CIQ companies -> Preqin
# =====================================================================
print_sep("STEP 4: Name-match CIQ companies to Preqin deals")

ciq_co["name_clean"] = ciq_co["companyname"].apply(clean_name)

prq_companies = (prq.groupby("portfolio_company_name")
                 .agg(year_established=("year_established", "first"),
                      primary_industry=("primary_industry", "first"),
                      industry_classification=("industry_classification", "first"),
                      total_known_funding_usd=("total_known_funding_usd", "first"))
                 .reset_index())
prq_companies["name_clean"] = prq_companies["portfolio_company_name"].apply(clean_name)

# Exact match
exact = ciq_co.merge(
    prq_companies[["name_clean", "portfolio_company_name", "year_established",
                    "primary_industry", "industry_classification",
                    "total_known_funding_usd"]],
    on="name_clean", how="inner")
exact["match_type"] = "exact"
print(f"  Exact matches: {len(exact):,}")

# Fuzzy match (containment)
matched_ciq_ids = set(exact["companyid"])
unmatched_ciq = ciq_co[~ciq_co["companyid"].isin(matched_ciq_ids)].copy()

prq_name_dict = {}
for _, row in prq_companies.iterrows():
    cn = row["name_clean"]
    if cn and len(cn) >= 4:
        prq_name_dict[cn] = row

fuzzy_matches = []
for _, ciq_row in unmatched_ciq.iterrows():
    ciq_name = ciq_row["name_clean"]
    if not ciq_name or len(ciq_name) < 4:
        continue
    best_match = None
    for prq_name, prq_row in prq_name_dict.items():
        if ciq_name in prq_name or prq_name in ciq_name:
            shorter = min(len(ciq_name), len(prq_name))
            longer  = max(len(ciq_name), len(prq_name))
            if shorter / longer < 0.70:
                continue
            ciq_yr = ciq_row.get("yearfounded")
            prq_yr = prq_row.get("year_established")
            if pd.notna(ciq_yr) and pd.notna(prq_yr):
                if abs(float(ciq_yr) - float(prq_yr)) > 2:
                    continue
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
        row_dict["total_known_funding_usd"] = prq_row["total_known_funding_usd"]
        row_dict["match_type"] = "fuzzy"
        fuzzy_matches.append(row_dict)

fuzzy_df = pd.DataFrame(fuzzy_matches)
print(f"  Fuzzy matches: {len(fuzzy_df):,}")

if len(fuzzy_df) > 0:
    matched = pd.concat([exact, fuzzy_df], ignore_index=True)
else:
    matched = exact.copy()
matched = matched.sort_values("match_type").drop_duplicates(
    subset="companyid", keep="first")
print(f"  Total unique matched companies: {len(matched):,}")

# =====================================================================
# 3. BUILD COMPANY-LEVEL OUTCOMES FROM PREQIN DEALS
# =====================================================================
print_sep("STEP 5: Compute deal outcomes for matched companies")

matched_deals = prq.merge(
    matched[["companyid", "n_observers", "portfolio_company_name",
             "yearfounded", "match_type"]],
    on="portfolio_company_name", how="inner")
print(f"  Deals for matched companies: {len(matched_deals):,}")

company_outcomes = (matched_deals
    .groupby(["companyid", "portfolio_company_name", "n_observers",
              "yearfounded", "match_type"])
    .agg(
        total_funding=("deal_financing_size_usd", "sum"),
        total_known_funding=("total_known_funding_usd", "first"),
        n_rounds=("deal_date", "count"),
        highest_stage=("stage", highest_stage_ordinal),
        first_deal_date=("deal_date", "min"),
        last_deal_date=("deal_date", "max"),
        first_deal_size=("deal_financing_size_usd", "first"),
        investment_status=("investment_status", "first"),
        primary_industry=("primary_industry", "first"),
        industry_classification=("industry_classification", "first"),
    )
    .reset_index())

company_outcomes["first_deal_year"] = company_outcomes["first_deal_date"].dt.year
company_outcomes["funding_span_days"] = (
    (company_outcomes["last_deal_date"] - company_outcomes["first_deal_date"]).dt.days)
company_outcomes["avg_days_between_rounds"] = np.where(
    company_outcomes["n_rounds"] > 1,
    company_outcomes["funding_span_days"] / (company_outcomes["n_rounds"] - 1),
    np.nan)
company_outcomes["ln_total_funding"] = np.log1p(
    company_outcomes["total_funding"].fillna(0))
company_outcomes["is_realized"] = (
    company_outcomes["investment_status"] == "Realized").astype(int)
company_outcomes["has_observer"] = 1

print(f"  Treatment companies (CIQ matched): {len(company_outcomes):,}")

# =====================================================================
# 4. BUILD CONTROL GROUP: all Preqin US companies NOT in CIQ
# =====================================================================
print_sep("STEP 6: Build control group (Preqin companies not in CIQ)")

matched_prq_names = set(matched["portfolio_company_name"].unique())
control_deals = prq[~prq["portfolio_company_name"].isin(matched_prq_names)].copy()

control_outcomes = (control_deals
    .groupby("portfolio_company_name")
    .agg(
        total_funding=("deal_financing_size_usd", "sum"),
        total_known_funding=("total_known_funding_usd", "first"),
        n_rounds=("deal_date", "count"),
        highest_stage=("stage", highest_stage_ordinal),
        first_deal_date=("deal_date", "min"),
        last_deal_date=("deal_date", "max"),
        first_deal_size=("deal_financing_size_usd", "first"),
        investment_status=("investment_status", "first"),
        primary_industry=("primary_industry", "first"),
        industry_classification=("industry_classification", "first"),
        yearfounded=("year_established", "first"),
    )
    .reset_index())

control_outcomes["companyid"] = np.nan
control_outcomes["n_observers"] = 0
control_outcomes["match_type"] = "control"
control_outcomes["has_observer"] = 0
control_outcomes["first_deal_year"] = control_outcomes["first_deal_date"].dt.year
control_outcomes["funding_span_days"] = (
    (control_outcomes["last_deal_date"] - control_outcomes["first_deal_date"]).dt.days)
control_outcomes["avg_days_between_rounds"] = np.where(
    control_outcomes["n_rounds"] > 1,
    control_outcomes["funding_span_days"] / (control_outcomes["n_rounds"] - 1),
    np.nan)
control_outcomes["ln_total_funding"] = np.log1p(
    control_outcomes["total_funding"].fillna(0))
control_outcomes["is_realized"] = (
    control_outcomes["investment_status"] == "Realized").astype(int)

print(f"  Control companies (Preqin non-CIQ): {len(control_outcomes):,}")

# Combine
keep_cols = [
    "companyid", "portfolio_company_name", "n_observers", "has_observer",
    "yearfounded", "total_funding", "total_known_funding", "ln_total_funding",
    "n_rounds", "highest_stage", "first_deal_year", "first_deal_size",
    "avg_days_between_rounds", "funding_span_days",
    "investment_status", "is_realized",
    "primary_industry", "industry_classification", "match_type",
]
for col in keep_cols:
    if col not in company_outcomes.columns:
        company_outcomes[col] = np.nan
    if col not in control_outcomes.columns:
        control_outcomes[col] = np.nan

full = pd.concat([company_outcomes[keep_cols], control_outcomes[keep_cols]],
                 ignore_index=True)
full["yearfounded"] = pd.to_numeric(full["yearfounded"], errors="coerce")
full["first_deal_size"] = pd.to_numeric(full["first_deal_size"], errors="coerce")
full["ind_fe"] = full["industry_classification"].fillna("Unknown")

# Drop singleton industries for FE regressions
ind_counts = full["ind_fe"].value_counts()
valid_inds = ind_counts[ind_counts >= 5].index
full = full[full["ind_fe"].isin(valid_inds)].copy()

print(f"\n  Combined sample: {len(full):,}")
print(f"    Treatment (has_observer=1): {full['has_observer'].sum():,}")
print(f"    Control   (has_observer=0): {(full['has_observer']==0).sum():,}")

# =====================================================================
# 5. CEM / PROPENSITY-SCORE MATCHING
# =====================================================================
print_sep("STEP 7: Coarsened Exact Matching (CEM)")

# Matching variables:
#   - Industry (exact)
#   - Year founded (bin: 2-year windows)
#   - First deal year (bin: 2-year windows)
#   - First deal size quartile

match_df = full.dropna(subset=["yearfounded", "first_deal_year",
                                "first_deal_size", "industry_classification"]).copy()
print(f"  Companies with all matching vars: {len(match_df):,}")
print(f"    Treatment: {match_df['has_observer'].sum():,}")
print(f"    Control:   {(match_df['has_observer']==0).sum():,}")

# Create CEM bins
match_df["yr_founded_bin"] = (match_df["yearfounded"] // 2) * 2
match_df["first_deal_yr_bin"] = (match_df["first_deal_year"] // 2) * 2
match_df["deal_size_quartile"] = pd.qcut(
    match_df["first_deal_size"], q=4, labels=False, duplicates="drop")

# CEM: exact match on (industry, yr_founded_bin, first_deal_yr_bin, deal_size_quartile)
cem_key = ["industry_classification", "yr_founded_bin",
           "first_deal_yr_bin", "deal_size_quartile"]

treated = match_df[match_df["has_observer"] == 1].copy()
control = match_df[match_df["has_observer"] == 0].copy()

# Build strata
treated["cem_strata"] = (treated[cem_key].astype(str)
                         .apply(lambda r: "|".join(r), axis=1))
control["cem_strata"] = (control[cem_key].astype(str)
                         .apply(lambda r: "|".join(r), axis=1))

# Find common strata
common_strata = set(treated["cem_strata"]) & set(control["cem_strata"])
print(f"\n  CEM strata: {len(common_strata):,} common strata "
      f"(treatment has {treated['cem_strata'].nunique():,}, "
      f"control has {control['cem_strata'].nunique():,})")

cem_treated = treated[treated["cem_strata"].isin(common_strata)].copy()
cem_control = control[control["cem_strata"].isin(common_strata)].copy()
print(f"  CEM matched treatment: {len(cem_treated):,}")
print(f"  CEM matched control:   {len(cem_control):,}")

# Within each stratum, keep up to 3 controls per treated
np.random.seed(42)
matched_controls = []
for strata_val in common_strata:
    t_in_strata = cem_treated[cem_treated["cem_strata"] == strata_val]
    c_in_strata = cem_control[cem_control["cem_strata"] == strata_val]
    n_treat = len(t_in_strata)
    n_want = min(n_treat * 3, len(c_in_strata))
    if n_want > 0:
        matched_controls.append(
            c_in_strata.sample(n=n_want, replace=False, random_state=42))

if matched_controls:
    cem_control_matched = pd.concat(matched_controls, ignore_index=True)
else:
    cem_control_matched = pd.DataFrame(columns=cem_control.columns)

cem_sample = pd.concat([cem_treated, cem_control_matched], ignore_index=True)
print(f"\n  Final CEM sample: {len(cem_sample):,}")
print(f"    Treatment: {cem_sample['has_observer'].sum():,}")
print(f"    Control:   {(cem_sample['has_observer']==0).sum():,}")

# ── Also try nearest-neighbor matching using scipy KDTree ────────────
print("\n  --- Nearest-Neighbor (NN) Matching (scipy KDTree) ---")

nn_df = match_df.copy()
# Standardize continuous matching vars for NN
for col in ["yearfounded", "first_deal_year", "first_deal_size"]:
    mu = nn_df[col].mean()
    sd = nn_df[col].std()
    nn_df[col + "_z"] = (nn_df[col] - mu) / (sd if sd > 0 else 1)

# Encode industry as numeric
nn_df["ind_code"] = nn_df["industry_classification"].astype("category").cat.codes

treated_nn = nn_df[nn_df["has_observer"] == 1].copy()
control_nn = nn_df[nn_df["has_observer"] == 0].copy()

if len(treated_nn) > 0 and len(control_nn) > 0:
    match_features = ["yearfounded_z", "first_deal_year_z",
                      "first_deal_size_z", "ind_code"]
    T_mat = treated_nn[match_features].values
    C_mat = control_nn[match_features].values

    tree = KDTree(C_mat)
    # For each treated unit, find 3 nearest controls
    dists, idxs = tree.query(T_mat, k=min(3, len(control_nn)))

    nn_control_idx = set()
    for row_idxs in idxs:
        if hasattr(row_idxs, '__iter__'):
            for idx in row_idxs:
                nn_control_idx.add(idx)
        else:
            nn_control_idx.add(row_idxs)

    nn_matched_control = control_nn.iloc[list(nn_control_idx)].copy()
    nn_sample = pd.concat([treated_nn, nn_matched_control], ignore_index=True)
    print(f"  NN sample: {len(nn_sample):,} "
          f"(T={nn_sample['has_observer'].sum():,}, "
          f"C={(nn_sample['has_observer']==0).sum():,})")
    print(f"  Mean distance: {dists.mean():.4f}")
else:
    nn_sample = cem_sample.copy()
    print("  NN matching skipped (insufficient data); using CEM sample")

# =====================================================================
# 6. COVARIATE BALANCE CHECK
# =====================================================================
print_sep("STEP 8: Covariate balance (CEM sample)")

balance_vars = ["yearfounded", "first_deal_year", "first_deal_size"]
for var in balance_vars:
    t_vals = cem_sample.loc[cem_sample["has_observer"] == 1, var].dropna()
    c_vals = cem_sample.loc[cem_sample["has_observer"] == 0, var].dropna()
    if len(t_vals) > 1 and len(c_vals) > 1:
        tstat, pval = stats.ttest_ind(t_vals, c_vals, equal_var=False)
        smd = (t_vals.mean() - c_vals.mean()) / np.sqrt(
            (t_vals.std()**2 + c_vals.std()**2) / 2)
        print(f"  {var:25s}  T_mean={t_vals.mean():10.1f}  C_mean={c_vals.mean():10.1f}  "
              f"SMD={smd:7.3f}  t={tstat:7.3f}  p={pval:.4f}")
    else:
        print(f"  {var:25s}  insufficient data")

# Industry overlap
t_ind = cem_sample.loc[cem_sample["has_observer"] == 1,
                       "industry_classification"].value_counts(normalize=True)
c_ind = cem_sample.loc[cem_sample["has_observer"] == 0,
                       "industry_classification"].value_counts(normalize=True)
all_inds = set(t_ind.index) | set(c_ind.index)
ind_tvd = sum(abs(t_ind.get(i, 0) - c_ind.get(i, 0)) for i in all_inds) / 2
print(f"  Industry TVD (total variation distance): {ind_tvd:.3f} "
      f"(0=perfect, 1=disjoint)")

# Also for NN sample
print("\n  Covariate balance (NN sample):")
for var in balance_vars:
    t_vals = nn_sample.loc[nn_sample["has_observer"] == 1, var].dropna()
    c_vals = nn_sample.loc[nn_sample["has_observer"] == 0, var].dropna()
    if len(t_vals) > 1 and len(c_vals) > 1:
        tstat, pval = stats.ttest_ind(t_vals, c_vals, equal_var=False)
        smd = (t_vals.mean() - c_vals.mean()) / np.sqrt(
            (t_vals.std()**2 + c_vals.std()**2) / 2)
        print(f"  {var:25s}  T_mean={t_vals.mean():10.1f}  C_mean={c_vals.mean():10.1f}  "
              f"SMD={smd:7.3f}  t={tstat:7.3f}  p={pval:.4f}")

# =====================================================================
# TEST 1: MATCHED SAMPLE OUTCOME COMPARISONS (CEM + NN)
# =====================================================================
print_sep("TEST 1: Matched outcome comparisons")

for sample_label, sdf in [("CEM", cem_sample), ("NN", nn_sample)]:
    print(f"\n  ===== {sample_label} SAMPLE =====")

    # Summary by group
    for grp, glabel in [(1, "Treatment (observed)"), (0, "Control")]:
        g = sdf[sdf["has_observer"] == grp]
        print(f"\n  --- {glabel} (N={len(g):,}) ---")
        for var in ["total_funding", "n_rounds", "highest_stage",
                     "avg_days_between_rounds", "is_realized"]:
            col = g[var].dropna()
            if len(col) > 0:
                print(f"    {var:30s}  mean={col.mean():12.2f}  "
                      f"median={col.median():12.2f}  N={len(col):,}")

    # T-tests
    print(f"\n  Welch t-tests ({sample_label}):")
    for var in ["total_funding", "ln_total_funding", "n_rounds",
                "highest_stage", "is_realized", "avg_days_between_rounds"]:
        t_vals = sdf.loc[sdf["has_observer"] == 1, var].dropna()
        c_vals = sdf.loc[sdf["has_observer"] == 0, var].dropna()
        if len(t_vals) > 1 and len(c_vals) > 1:
            tstat, pval = stats.ttest_ind(t_vals, c_vals, equal_var=False)
            diff = t_vals.mean() - c_vals.mean()
            stars = "***" if pval < 0.01 else "**" if pval < 0.05 else "*" if pval < 0.1 else ""
            print(f"    {var:30s}  diff={diff:12.2f}  t={tstat:7.3f}  "
                  f"p={pval:.4f} {stars}")

    # OLS regressions on matched sample (triple spec)
    sdf_reg = sdf.copy()
    sdf_funded = sdf_reg[sdf_reg["total_funding"] > 0].copy()

    if len(sdf_funded) > 30:
        print(f"\n  --- {sample_label}: ln(total_funding) regressions ---")
        run_ols_triple("ln_total_funding ~ has_observer + yearfounded",
                       sdf_funded.dropna(subset=["ln_total_funding", "yearfounded"]),
                       f"{sample_label}: ln(total_funding) ~ has_observer + yearfounded",
                       key_vars=["has_observer", "yearfounded"])

    if len(sdf_reg) > 30:
        print(f"\n  --- {sample_label}: n_rounds regressions ---")
        run_ols_triple("n_rounds ~ has_observer + yearfounded",
                       sdf_reg.dropna(subset=["n_rounds", "yearfounded"]),
                       f"{sample_label}: n_rounds ~ has_observer + yearfounded",
                       key_vars=["has_observer", "yearfounded"])

# =====================================================================
# TEST 2: TIME TO NEXT ROUND
# =====================================================================
print_sep("TEST 2: Time to next round (consecutive deal gaps)")

# Build deal-level panel for companies with 2+ deals
def compute_inter_deal_days(deals_df, company_col, date_col):
    """Compute days between consecutive deals per company."""
    d = deals_df[[company_col, date_col]].dropna().copy()
    d = d.sort_values([company_col, date_col])
    d["prev_date"] = d.groupby(company_col)[date_col].shift(1)
    d["inter_deal_days"] = (d[date_col] - d["prev_date"]).dt.days
    return d.dropna(subset=["inter_deal_days"])

# Treatment: matched companies
treat_deals = prq[prq["portfolio_company_name"].isin(
    matched_prq_names)].copy()
treat_deals["has_observer"] = 1

# Control: non-matched companies (use CEM-matched names if available)
cem_control_names = set(cem_control_matched["portfolio_company_name"].unique()) \
    if len(cem_control_matched) > 0 else set()

# Use all controls for the full sample, CEM controls for matched
ctrl_deals_full = prq[~prq["portfolio_company_name"].isin(
    matched_prq_names)].copy()
ctrl_deals_full["has_observer"] = 0

# Full sample inter-deal gaps
all_deals_for_gap = pd.concat([treat_deals, ctrl_deals_full], ignore_index=True)
gaps = compute_inter_deal_days(all_deals_for_gap,
                               "portfolio_company_name", "deal_date")
gaps = gaps.merge(
    all_deals_for_gap[["portfolio_company_name", "has_observer"]].drop_duplicates(),
    on="portfolio_company_name", how="left",
    suffixes=("_x", ""))

# Use the correct has_observer column
if "has_observer" not in gaps.columns:
    if "has_observer_x" in gaps.columns:
        gaps["has_observer"] = gaps["has_observer_x"]

# Drop outlier gaps (> 5 years = 1825 days)
gaps = gaps[gaps["inter_deal_days"].between(1, 1825)].copy()
print(f"  Inter-deal observations: {len(gaps):,}")
print(f"    Treatment: {(gaps['has_observer']==1).sum():,}")
print(f"    Control:   {(gaps['has_observer']==0).sum():,}")

t_gaps = gaps.loc[gaps["has_observer"] == 1, "inter_deal_days"]
c_gaps = gaps.loc[gaps["has_observer"] == 0, "inter_deal_days"]
if len(t_gaps) > 1 and len(c_gaps) > 1:
    tstat, pval = stats.ttest_ind(t_gaps, c_gaps, equal_var=False)
    print(f"\n  Mean inter-deal days:  Treatment={t_gaps.mean():.1f}  "
          f"Control={c_gaps.mean():.1f}")
    print(f"  Median inter-deal days: Treatment={t_gaps.median():.1f}  "
          f"Control={c_gaps.median():.1f}")
    print(f"  Welch t-test: t={tstat:.3f}, p={pval:.4f}")
    # Mann-Whitney U
    u_stat, u_pval = stats.mannwhitneyu(t_gaps, c_gaps, alternative="two-sided")
    print(f"  Mann-Whitney U: U={u_stat:.0f}, p={u_pval:.4f}")

# Company-level average gap on matched (CEM) sample
print("\n  Company-level avg gap (CEM sample):")
cem_names = set(cem_sample["portfolio_company_name"])
gaps_cem = gaps[gaps["portfolio_company_name"].isin(cem_names)].copy()
co_gaps = (gaps_cem.groupby(["portfolio_company_name", "has_observer"])
           ["inter_deal_days"].mean().reset_index())
co_gaps["ind_fe"] = co_gaps["portfolio_company_name"].map(
    cem_sample.drop_duplicates("portfolio_company_name")
    .set_index("portfolio_company_name")["ind_fe"])

if len(co_gaps.dropna(subset=["inter_deal_days", "has_observer", "ind_fe"])) > 20:
    run_ols_triple("inter_deal_days ~ has_observer",
                   co_gaps.dropna(subset=["inter_deal_days", "has_observer", "ind_fe"]),
                   "Time-to-next-round ~ has_observer (CEM matched)",
                   key_vars=["has_observer"])

# =====================================================================
# TEST 3: STAGE PROGRESSION
# =====================================================================
print_sep("TEST 3: Stage progression (ordinal highest stage)")

# Full sample
print("  Full sample stage distribution by treatment:")
for grp, glabel in [(1, "Treatment"), (0, "Control")]:
    g = full[full["has_observer"] == grp]
    print(f"\n  {glabel} (N={len(g):,}):")
    print(f"    Highest stage mean: {g['highest_stage'].mean():.2f}")
    for s in range(6):
        n = (g["highest_stage"] == s).sum()
        pct = n / len(g) * 100 if len(g) > 0 else 0
        print(f"      Stage {s}: {n:6,} ({pct:5.1f}%)")

# T-test on stage
t_stage = full.loc[full["has_observer"] == 1, "highest_stage"]
c_stage = full.loc[full["has_observer"] == 0, "highest_stage"]
if len(t_stage) > 1 and len(c_stage) > 1:
    tstat, pval = stats.ttest_ind(t_stage, c_stage, equal_var=False)
    print(f"\n  Welch t-test (highest_stage): t={tstat:.3f}, p={pval:.4f}")
    u_stat, u_pval = stats.mannwhitneyu(t_stage, c_stage, alternative="two-sided")
    print(f"  Mann-Whitney U: U={u_stat:.0f}, p={u_pval:.4f}")

# On CEM sample
print(f"\n  CEM sample:")
t_stage_c = cem_sample.loc[cem_sample["has_observer"] == 1, "highest_stage"]
c_stage_c = cem_sample.loc[cem_sample["has_observer"] == 0, "highest_stage"]
if len(t_stage_c) > 1 and len(c_stage_c) > 1:
    print(f"  T mean={t_stage_c.mean():.2f}, C mean={c_stage_c.mean():.2f}")
    tstat, pval = stats.ttest_ind(t_stage_c, c_stage_c, equal_var=False)
    print(f"  t={tstat:.3f}, p={pval:.4f}")

# Ordered logit proxy: OLS on ordinal stage with triple specs
print("\n  Stage progression regressions (full sample):")
stage_reg = full.dropna(subset=["highest_stage", "yearfounded"]).copy()
if len(stage_reg) > 30:
    run_ols_triple("highest_stage ~ has_observer + yearfounded",
                   stage_reg,
                   "highest_stage ~ has_observer + yearfounded",
                   key_vars=["has_observer", "yearfounded"])

print("\n  Stage progression regressions (CEM sample):")
stage_cem = cem_sample.dropna(subset=["highest_stage", "yearfounded"]).copy()
if len(stage_cem) > 20:
    run_ols_triple("highest_stage ~ has_observer + yearfounded",
                   stage_cem,
                   "CEM: highest_stage ~ has_observer + yearfounded",
                   key_vars=["has_observer", "yearfounded"])

# =====================================================================
# TEST 4: EXIT OUTCOMES WITH BETTER CONTROLS
# =====================================================================
print_sep("TEST 4: Exit outcomes (is_realized) with better controls")

# Full sample with founding year + total funding controls
exit_df = full.dropna(subset=["is_realized", "yearfounded"]).copy()
exit_df["ln_total_funding"] = np.log1p(exit_df["total_funding"].fillna(0))
print(f"  Exit sample: {len(exit_df):,}")
print(f"  Realized rate: T={exit_df.loc[exit_df['has_observer']==1, 'is_realized'].mean():.3f}, "
      f"C={exit_df.loc[exit_df['has_observer']==0, 'is_realized'].mean():.3f}")

if exit_df["is_realized"].nunique() > 1:
    print("\n  --- Logit: is_realized ~ has_observer + yearfounded ---")
    run_logit_triple("is_realized ~ has_observer + yearfounded",
                     exit_df,
                     "is_realized ~ has_observer + yearfounded",
                     key_vars=["has_observer", "yearfounded"])

    # With total funding control
    exit_funded = exit_df[exit_df["total_funding"] > 0].copy()
    if len(exit_funded) > 30 and exit_funded["is_realized"].nunique() > 1:
        print("\n  --- Logit: is_realized ~ has_observer + yearfounded + ln_total_funding ---")
        run_logit_triple(
            "is_realized ~ has_observer + yearfounded + ln_total_funding",
            exit_funded,
            "is_realized ~ has_observer + yearfounded + ln_total_funding",
            key_vars=["has_observer", "yearfounded", "ln_total_funding"])

# CEM sample exit
cem_exit = cem_sample.dropna(subset=["is_realized", "yearfounded"]).copy()
cem_exit["ln_total_funding"] = np.log1p(cem_exit["total_funding"].fillna(0))
if len(cem_exit) > 20 and cem_exit["is_realized"].nunique() > 1:
    print("\n  --- CEM: is_realized ~ has_observer + yearfounded ---")
    run_logit_triple("is_realized ~ has_observer + yearfounded",
                     cem_exit,
                     "CEM: is_realized ~ has_observer + yearfounded",
                     key_vars=["has_observer", "yearfounded"])

# =====================================================================
# TEST 5: DOSE-RESPONSE
# =====================================================================
print_sep("TEST 5: Dose-response (1 vs 2 vs 3+ observers)")

# Create dose categories from treatment companies only
dose_df = company_outcomes.copy()
dose_df["dose"] = pd.cut(dose_df["n_observers"],
                         bins=[0, 1, 2, 100],
                         labels=["1_observer", "2_observers", "3plus_observers"],
                         right=True)
dose_df["ln_total_funding"] = np.log1p(dose_df["total_funding"].fillna(0))
dose_df["ind_fe"] = dose_df["industry_classification"].fillna("Unknown")

# Filter to valid industries
dose_df = dose_df[dose_df["ind_fe"].isin(valid_inds)].copy()

print(f"  Dose distribution (treatment companies only):")
print(dose_df["dose"].value_counts().sort_index().to_string())

# Summary by dose
for dose_val in ["1_observer", "2_observers", "3plus_observers"]:
    g = dose_df[dose_df["dose"] == dose_val]
    if len(g) > 0:
        print(f"\n  --- {dose_val} (N={len(g):,}) ---")
        for var in ["total_funding", "n_rounds", "highest_stage",
                     "avg_days_between_rounds", "is_realized"]:
            col = g[var].dropna()
            if len(col) > 0:
                print(f"    {var:30s}  mean={col.mean():12.2f}  "
                      f"median={col.median():12.2f}")

# ANOVA / Kruskal-Wallis across dose groups
print("\n  Kruskal-Wallis tests (across dose groups):")
for var in ["total_funding", "n_rounds", "highest_stage", "is_realized"]:
    groups = []
    for dose_val in ["1_observer", "2_observers", "3plus_observers"]:
        vals = dose_df.loc[dose_df["dose"] == dose_val, var].dropna()
        if len(vals) > 1:
            groups.append(vals)
    if len(groups) >= 2:
        h_stat, h_pval = stats.kruskal(*groups)
        stars = "***" if h_pval < 0.01 else "**" if h_pval < 0.05 else "*" if h_pval < 0.1 else ""
        print(f"    {var:30s}  H={h_stat:8.3f}  p={h_pval:.4f} {stars}")

# Regression with n_observers as continuous (dose-response)
print("\n  Dose-response regressions (n_observers continuous, treatment only):")
dose_funded = dose_df[dose_df["total_funding"] > 0].dropna(
    subset=["ln_total_funding", "yearfounded"]).copy()
if len(dose_funded) > 20:
    run_ols_triple("ln_total_funding ~ n_observers + yearfounded",
                   dose_funded,
                   "Dose: ln(total_funding) ~ n_observers + yearfounded",
                   key_vars=["n_observers", "yearfounded"])

dose_rounds = dose_df.dropna(subset=["n_rounds", "yearfounded"]).copy()
if len(dose_rounds) > 20:
    run_ols_triple("n_rounds ~ n_observers + yearfounded",
                   dose_rounds,
                   "Dose: n_rounds ~ n_observers + yearfounded",
                   key_vars=["n_observers", "yearfounded"])

# Also with full sample (treated + matched controls)
print("\n  Dose-response with full sample (0, 1, 2, 3+ observers):")
full["n_obs_cat"] = pd.cut(full["n_observers"],
                           bins=[-0.5, 0.5, 1.5, 2.5, 100],
                           labels=["0", "1", "2", "3+"])
full_dose = full.dropna(subset=["ln_total_funding", "yearfounded"]).copy()
full_dose_funded = full_dose[full_dose["total_funding"] > 0].copy()

if len(full_dose_funded) > 50:
    run_ols_triple(
        "ln_total_funding ~ C(n_obs_cat, Treatment('0')) + yearfounded",
        full_dose_funded,
        "Full: ln(total_funding) ~ C(n_obs_cat) + yearfounded",
        key_vars=["n_obs_cat", "yearfounded"])

# =====================================================================
# TEST 6: COMPREHENSIVE REGRESSIONS (FULL SPECS)
# =====================================================================
print_sep("TEST 6: Full specification regressions")

# Prepare
full_reg = full.dropna(subset=["yearfounded"]).copy()
full_funded_reg = full_reg[full_reg["total_funding"] > 0].copy()
full_funded_reg["ln_total_funding"] = np.log1p(full_funded_reg["total_funding"])

print(f"  Full regression sample: {len(full_reg):,}")
print(f"  Funded regression sample: {len(full_funded_reg):,}")

# --- 6A: ln(total_funding) with full triple ---
print("\n  --- 6A: ln(total_funding) ---")
if len(full_funded_reg) > 50:
    # Binary treatment
    run_ols_triple("ln_total_funding ~ has_observer + yearfounded",
                   full_funded_reg,
                   "6A.1: ln(total_funding) ~ has_observer + yearfounded",
                   key_vars=["has_observer", "yearfounded"])

    # Continuous treatment
    run_ols_triple("ln_total_funding ~ n_observers + yearfounded",
                   full_funded_reg,
                   "6A.2: ln(total_funding) ~ n_observers + yearfounded",
                   key_vars=["n_observers", "yearfounded"])

# --- 6B: n_rounds ---
print("\n  --- 6B: n_rounds ---")
if len(full_reg) > 50:
    run_ols_triple("n_rounds ~ has_observer + yearfounded",
                   full_reg,
                   "6B.1: n_rounds ~ has_observer + yearfounded",
                   key_vars=["has_observer", "yearfounded"])

    run_ols_triple("n_rounds ~ n_observers + yearfounded",
                   full_reg,
                   "6B.2: n_rounds ~ n_observers + yearfounded",
                   key_vars=["n_observers", "yearfounded"])

# --- 6C: is_realized (logit) ---
print("\n  --- 6C: is_realized (logit) ---")
if len(full_reg) > 50 and full_reg["is_realized"].nunique() > 1:
    run_logit_triple("is_realized ~ has_observer + yearfounded",
                     full_reg,
                     "6C.1: is_realized ~ has_observer + yearfounded",
                     key_vars=["has_observer", "yearfounded"])

    run_logit_triple("is_realized ~ n_observers + yearfounded",
                     full_reg,
                     "6C.2: is_realized ~ n_observers + yearfounded",
                     key_vars=["n_observers", "yearfounded"])

# --- 6D: highest_stage ---
print("\n  --- 6D: highest_stage ---")
if len(full_reg) > 50:
    run_ols_triple("highest_stage ~ has_observer + yearfounded",
                   full_reg,
                   "6D.1: highest_stage ~ has_observer + yearfounded",
                   key_vars=["has_observer", "yearfounded"])

# =====================================================================
# SUMMARY TABLE
# =====================================================================
print_sep("SUMMARY TABLE")

# Build compact summary
rows = []
for sample_name, sdf in [("Full", full), ("CEM", cem_sample), ("NN", nn_sample)]:
    for grp, glabel in [(1, "Treatment"), (0, "Control")]:
        g = sdf[sdf["has_observer"] == grp]
        g_funded = g[g["total_funding"] > 0]
        rows.append({
            "Sample": sample_name,
            "Group": glabel,
            "N": len(g),
            "Mean_funding": g_funded["total_funding"].mean() if len(g_funded) > 0 else np.nan,
            "Median_funding": g_funded["total_funding"].median() if len(g_funded) > 0 else np.nan,
            "Mean_rounds": g["n_rounds"].mean(),
            "Mean_stage": g["highest_stage"].mean(),
            "Pct_realized": g["is_realized"].mean() * 100,
            "Mean_yr_founded": g["yearfounded"].mean(),
        })

summary = pd.DataFrame(rows)
print(summary.to_string(index=False, float_format="%.2f"))

summary.to_csv(os.path.join(OUT_DIR, "company_outcomes_v2_summary.csv"),
               index=False)
print(f"\n  Saved: {OUT_DIR}/company_outcomes_v2_summary.csv")

print(f"\n{SEP}")
print("  DONE")
print(SEP)
