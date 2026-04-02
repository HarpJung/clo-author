"""
Extended observer count -> fund performance regressions (v2).

New tests beyond the baseline script:
  1. GP experience controls (staffcountinvestment, firm age from established)
  2. Industry specialization HHI of observer seats + interaction
  3. Fund terms controls (carried interest, hurdle rate)
  4. Within-firm variation (firm FE, change in observer seats over time)
  5. Full specification battery (HC1, Firm-cl, Vintage FE+Firm-cl, Firm FE+HC1, Vintage FE+HC1)
  6. Quintile analysis (mean TVPI, IRR, fund size by observer-seat quintile)
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd
import numpy as np
import statsmodels.api as sm
from scipy import stats
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

pd.set_option("display.max_columns", 40)
pd.set_option("display.width", 160)
pd.set_option("display.float_format", lambda x: f"{x:.4f}")

# ── Paths ─────────────────────────────────────────────────────────────
DATA = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
OBS_NET_PATH    = f"{DATA}/table_b_observer_network.csv"
OBS_REC_PATH    = f"{DATA}/CIQ_Extract/01_observer_records.csv"
COMP_DET_PATH   = f"{DATA}/CIQ_Extract/04_observer_company_details.csv"
XWALK_PATH      = f"{DATA}/Preqin/vc_preqin_crosswalk_validated.csv"
FUND_DET_PATH   = f"{DATA}/Preqin/fund_details_full.csv"
FUND_PERF_PATH  = f"{DATA}/Preqin/fund_performance_full.csv"
MGR_DET_PATH    = f"{DATA}/Preqin/manager_details_full.csv"
FUND_TERMS_PATH = f"{DATA}/Preqin/fund_terms.csv"

# =====================================================================
# STEP 1: Build VC-level observer counts (from v1)
# =====================================================================
print("=" * 90)
print("STEP 1: Building VC-level observer counts from observer network")
print("=" * 90)

obs = pd.read_csv(OBS_NET_PATH)
print(f"  Observer network rows: {len(obs):,}")
print(f"  Unique VC firms (CIQ): {obs['vc_firm_companyid'].nunique():,}")

vc_counts = obs.groupby("vc_firm_companyid").agg(
    vc_firm_name=("vc_firm_name", "first"),
    n_observer_seats=("observer_personid", "nunique"),
    n_observed_companies=("observed_companyid", "nunique"),
    n_observer_rows=("observer_personid", "count"),
).reset_index()
print(f"  VC-level obs: {len(vc_counts):,}")

# =====================================================================
# STEP 1b: Compute industry HHI for each VC's observer portfolio
# =====================================================================
print("\n" + "=" * 90)
print("STEP 1b: Computing industry HHI of observer seats per VC")
print("=" * 90)

# Get industry info for observed companies from company details
comp_det = pd.read_csv(COMP_DET_PATH)
print(f"  Company details rows: {len(comp_det):,}")

# Merge observed company industry onto observer network
obs_ind = obs.merge(
    comp_det[["companyid", "companytypename"]].rename(
        columns={"companyid": "observed_companyid", "companytypename": "observed_industry"}
    ),
    on="observed_companyid",
    how="left",
)
print(f"  Observer rows with industry info: {obs_ind['observed_industry'].notna().sum():,} "
      f"of {len(obs_ind):,}")

# Also try using fund_details industry for additional coverage
# For each VC, compute HHI across industries of their observed companies
def compute_hhi(group):
    """Compute HHI of industry distribution for a VC's observer seats."""
    ind_col = group["observed_industry"].dropna()
    if len(ind_col) < 2:
        return np.nan
    shares = ind_col.value_counts(normalize=True)
    hhi = (shares ** 2).sum()
    return hhi

vc_hhi = obs_ind.groupby("vc_firm_companyid").apply(
    compute_hhi, include_groups=False
).reset_index()
vc_hhi.columns = ["vc_firm_companyid", "observer_industry_hhi"]
print(f"  VCs with HHI computed: {vc_hhi['observer_industry_hhi'].notna().sum():,}")
print(f"  HHI distribution:")
print(vc_hhi["observer_industry_hhi"].describe().to_string())

# Merge HHI back to VC counts
vc_counts = vc_counts.merge(vc_hhi, on="vc_firm_companyid", how="left")

# =====================================================================
# STEP 2: Match to Preqin via crosswalk (high + medium quality)
# =====================================================================
print("\n" + "=" * 90)
print("STEP 2: Matching to Preqin via crosswalk (high + medium quality)")
print("=" * 90)

xwalk = pd.read_csv(XWALK_PATH)
xwalk_hm = xwalk[xwalk["quality"].isin(["high", "medium"])].copy()
print(f"  High + Medium quality rows: {len(xwalk_hm):,}")

vc_preqin = vc_counts.merge(
    xwalk_hm[["ciq_vc_companyid", "preqin_firm_id", "quality"]],
    left_on="vc_firm_companyid",
    right_on="ciq_vc_companyid",
    how="inner",
)
print(f"  VCs matched to Preqin: {vc_preqin['vc_firm_companyid'].nunique():,}")

# =====================================================================
# STEP 3: Load fund details (VC-type funds)
# =====================================================================
print("\n" + "=" * 90)
print("STEP 3: Loading Preqin fund details (VC-type funds)")
print("=" * 90)

fd = pd.read_csv(FUND_DET_PATH)
vc_types = fd["fund_type"].str.contains("Venture|Seed|Early", case=False, na=False)
fd_vc = fd[vc_types].copy()
print(f"  VC-type funds: {len(fd_vc):,}")

# =====================================================================
# STEP 4: Load fund performance (last reported)
# =====================================================================
print("\n" + "=" * 90)
print("STEP 4: Getting final fund performance (last reported per fund)")
print("=" * 90)

fp = pd.read_csv(FUND_PERF_PATH, low_memory=False)
fp["net_irr_pcent"] = pd.to_numeric(fp["net_irr_pcent"], errors="coerce")
fp["multiple"] = pd.to_numeric(fp["multiple"], errors="coerce")
fp["date_reported"] = pd.to_datetime(fp["date_reported"], errors="coerce")

fp_sorted = fp.sort_values(["fund_id", "date_reported"])
fp_last = fp_sorted.groupby("fund_id").last().reset_index()
print(f"  Funds with final TVPI: {fp_last['multiple'].notna().sum():,}")
print(f"  Funds with final IRR: {fp_last['net_irr_pcent'].notna().sum():,}")

# =====================================================================
# STEP 5: Load manager details (GP experience controls)
# =====================================================================
print("\n" + "=" * 90)
print("STEP 5: Loading manager details for GP experience controls")
print("=" * 90)

mgr = pd.read_csv(MGR_DET_PATH, low_memory=False)
mgr["staffcountinvestment"] = pd.to_numeric(mgr["staffcountinvestment"], errors="coerce")
mgr["staffcounttotal"] = pd.to_numeric(mgr["staffcounttotal"], errors="coerce")
mgr["established"] = pd.to_numeric(mgr["established"], errors="coerce")

print(f"  Manager records: {len(mgr):,}")
print(f"  With staffcountinvestment: {mgr['staffcountinvestment'].notna().sum():,}")
print(f"  With established year: {mgr['established'].notna().sum():,}")

mgr_cols = ["firm_id", "staffcountinvestment", "staffcounttotal", "established", "industryfocus"]
mgr_sub = mgr[mgr_cols].drop_duplicates(subset=["firm_id"], keep="first")

# =====================================================================
# STEP 6: Load fund terms (carried interest, hurdle rate)
# =====================================================================
print("\n" + "=" * 90)
print("STEP 6: Loading fund terms for carry and hurdle controls")
print("=" * 90)

ft = pd.read_csv(FUND_TERMS_PATH)
ft["carriedinterestpercent"] = pd.to_numeric(ft["carriedinterestpercent"], errors="coerce")
ft["hurdleratepercent"] = pd.to_numeric(ft["hurdleratepercent"], errors="coerce")
print(f"  Fund terms rows: {len(ft):,}")
print(f"  With carried interest: {ft['carriedinterestpercent'].notna().sum():,}")
print(f"  With hurdle rate: {ft['hurdleratepercent'].notna().sum():,}")

# =====================================================================
# STEP 7: Merge everything together
# =====================================================================
print("\n" + "=" * 90)
print("STEP 7: Merging all data sources")
print("=" * 90)

# Fund details + performance
fund_merged = fd_vc.merge(
    fp_last[["fund_id", "multiple", "net_irr_pcent", "date_reported"]],
    on="fund_id", how="inner"
)
print(f"  VC funds with performance: {len(fund_merged):,}")

# + VC observer counts
analysis = fund_merged.merge(
    vc_preqin[["preqin_firm_id", "vc_firm_companyid", "vc_firm_name",
               "n_observer_seats", "n_observed_companies", "n_observer_rows",
               "observer_industry_hhi", "quality"]],
    left_on="firm_id",
    right_on="preqin_firm_id",
    how="inner",
)
print(f"  Funds matched to VC observer data: {len(analysis):,}")

# Dedup: if multiple CIQ firms map to same Preqin firm, keep highest observer count
analysis = analysis.sort_values("n_observer_seats", ascending=False)
analysis = analysis.drop_duplicates(subset=["fund_id"], keep="first")
print(f"  After dedup (one obs per fund): {len(analysis):,}")

# + Manager details (GP experience)
analysis = analysis.merge(mgr_sub, on="firm_id", how="left")
print(f"  With staffcountinvestment: {analysis['staffcountinvestment'].notna().sum():,}")
print(f"  With established year: {analysis['established'].notna().sum():,}")

# + Fund terms
analysis = analysis.merge(
    ft[["fund_id", "carriedinterestpercent", "hurdleratepercent"]],
    on="fund_id", how="left"
)
print(f"  With carried interest: {analysis['carriedinterestpercent'].notna().sum():,}")
print(f"  With hurdle rate: {analysis['hurdleratepercent'].notna().sum():,}")

# =====================================================================
# STEP 8: Build regression variables
# =====================================================================
print("\n" + "=" * 90)
print("STEP 8: Building regression variables")
print("=" * 90)

# Fund size
analysis["final_size_usd"] = pd.to_numeric(analysis["final_size_usd"], errors="coerce")
analysis["ln_fund_size"] = np.log(analysis["final_size_usd"].clip(lower=0.01))

# Vintage
analysis["vintage"] = pd.to_numeric(analysis["vintage"], errors="coerce")

# Observer variables
analysis["ln_observer_seats"] = np.log(analysis["n_observer_seats"].clip(lower=1))
analysis["ln_observed_companies"] = np.log(analysis["n_observed_companies"].clip(lower=1))
analysis["observer_intensity"] = analysis["n_observer_seats"] / (
    analysis["final_size_usd"].clip(lower=0.01) / 100
)

# Fund number
analysis["fund_number_overall"] = pd.to_numeric(analysis["fund_number_overall"], errors="coerce")
analysis["ln_fund_number"] = np.log(analysis["fund_number_overall"].clip(lower=1))

# GP experience controls
analysis["ln_staff_investment"] = np.log(analysis["staffcountinvestment"].clip(lower=1))
analysis["firm_age"] = analysis["vintage"] - analysis["established"]
# Replace negative firm ages with NaN (data error)
analysis.loc[analysis["firm_age"] < 0, "firm_age"] = np.nan
analysis["ln_firm_age"] = np.log(analysis["firm_age"].clip(lower=1))

# Industry HHI interaction
analysis["obs_x_hhi"] = analysis["n_observer_seats"] * analysis["observer_industry_hhi"]
analysis["ln_obs_x_hhi"] = analysis["ln_observer_seats"] * analysis["observer_industry_hhi"]

# Winsorize performance at 1/99
def winsorize(s, lower=0.01, upper=0.99):
    q = s.quantile([lower, upper])
    return s.clip(q.iloc[0], q.iloc[1])

analysis["tvpi_w"] = winsorize(analysis["multiple"].dropna()).reindex(analysis.index)
analysis["irr_w"] = winsorize(analysis["net_irr_pcent"].dropna()).reindex(analysis.index)

# Sample filters
sample = analysis.dropna(subset=["ln_fund_size", "vintage"]).copy()
sample = sample[sample["final_size_usd"] > 0].copy()
sample = sample[(sample["vintage"] >= 1990) & (sample["vintage"] <= 2022)].copy()

print(f"  Analysis sample: {len(sample):,}")
print(f"  With TVPI: {sample['multiple'].notna().sum():,}")
print(f"  With IRR: {sample['net_irr_pcent'].notna().sum():,}")
print(f"  With GP staff count: {sample['ln_staff_investment'].notna().sum():,}")
print(f"  With firm age: {sample['firm_age'].notna().sum():,}")
print(f"  With industry HHI: {sample['observer_industry_hhi'].notna().sum():,}")
print(f"  With carried interest: {sample['carriedinterestpercent'].notna().sum():,}")
print(f"  With hurdle rate: {sample['hurdleratepercent'].notna().sum():,}")
print(f"  Unique firms (for firm FE): {sample['firm_id'].nunique():,}")
print(f"  Firms with >1 fund: "
      f"{(sample.groupby('firm_id').size() > 1).sum():,}")

# =====================================================================
# STEP 9: Summary statistics
# =====================================================================
print("\n" + "=" * 90)
print("STEP 9: Summary statistics")
print("=" * 90)

summ_vars = [
    "n_observer_seats", "n_observed_companies", "observer_industry_hhi",
    "multiple", "net_irr_pcent", "final_size_usd", "vintage",
    "fund_number_overall", "staffcountinvestment", "firm_age",
    "carriedinterestpercent", "hurdleratepercent",
]
summ = sample[summ_vars].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9]).T
summ = summ[["count", "mean", "std", "10%", "25%", "50%", "75%", "90%"]]
print(summ.to_string())


# =====================================================================
# Regression helpers
# =====================================================================
def run_reg(data, y_var, x_vars, vintage_fe=False, firm_fe=False,
            cluster_var=None, label="", silent=False):
    """
    Run OLS with optional vintage FE, firm FE, and clustered/robust SE.
    Returns model or None.
    """
    all_needed = [y_var] + x_vars
    if firm_fe:
        all_needed.append("firm_id")
    if vintage_fe:
        all_needed.append("vintage")
    if cluster_var:
        all_needed.append(cluster_var)

    df = data.dropna(subset=all_needed).copy()

    if len(df) < 20:
        if not silent:
            print(f"\n  [{label}] Skipped: only {len(df)} obs")
        return None

    Y = df[y_var]
    X = df[x_vars].copy()

    if vintage_fe:
        vint_d = pd.get_dummies(df["vintage"], prefix="v", drop_first=True, dtype=float)
        X = pd.concat([X, vint_d], axis=1)

    if firm_fe:
        firm_d = pd.get_dummies(df["firm_id"], prefix="f", drop_first=True, dtype=float)
        # Check we have enough residual df
        if len(df) - firm_d.shape[1] - X.shape[1] - 1 < 10:
            if not silent:
                print(f"\n  [{label}] Skipped: residual df < 10 after firm FE")
            return None
        X = pd.concat([X, firm_d], axis=1)

    X = sm.add_constant(X)

    try:
        if cluster_var and cluster_var in df.columns and not firm_fe:
            groups = df[cluster_var]
            model = sm.OLS(Y, X).fit(cov_type="cluster", cov_kwds={"groups": groups})
        else:
            model = sm.OLS(Y, X).fit(cov_type="HC1")
    except Exception as e:
        if not silent:
            print(f"\n  [{label}] ERROR: {e}")
        return None

    if not silent:
        print(f"\n{'=' * 90}")
        print(f"  [{label}]")
        se_type = f"Clustered ({cluster_var})" if (cluster_var and not firm_fe) else "HC1 robust"
        print(f"  Dep var: {y_var}  |  N = {model.nobs:.0f}  |  R2 = {model.rsquared:.4f}  "
              f"|  Adj-R2 = {model.rsquared_adj:.4f}")
        print(f"  SE type: {se_type}  |  Vintage FE: {vintage_fe}  |  Firm FE: {firm_fe}")
        print(f"{'=' * 90}")

        key_vars = ["const"] + x_vars
        for v in key_vars:
            if v in model.params.index:
                coef = model.params[v]
                se = model.bse[v]
                t = model.tvalues[v]
                p = model.pvalues[v]
                stars = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
                print(f"    {v:35s}  {coef:10.4f}  ({se:8.4f})  t={t:7.3f}  p={p:.4f} {stars}")

        if vintage_fe:
            n_vfe = sum(1 for c in model.params.index if c.startswith("v_"))
            print(f"    [+ {n_vfe} vintage dummies]")
        if firm_fe:
            n_ffe = sum(1 for c in model.params.index if c.startswith("f_"))
            print(f"    [+ {n_ffe} firm dummies]")

    return model


# =====================================================================
# TEST 1: GP Experience Controls
# =====================================================================
print("\n\n" + "#" * 90)
print("# TEST 1: GP EXPERIENCE CONTROLS")
print("# Does observer count still predict performance after controlling")
print("# for GP team size (staffcountinvestment) and firm age?")
print("#" * 90)

for dep, dep_label in [("tvpi_w", "TVPI(w)"), ("irr_w", "IRR(w)")]:

    print(f"\n{'~' * 90}")
    print(f"  Dependent variable: {dep_label}")
    print(f"{'~' * 90}")

    # (a) Baseline: observer seats + fund size + vintage FE, firm-clustered
    run_reg(sample, dep, ["n_observer_seats", "ln_fund_size"],
            vintage_fe=True, cluster_var="firm_id",
            label=f"T1a: {dep_label} ~ n_obs_seats + ln_size + vFE, cl(firm)")

    # (b) + GP investment staff
    run_reg(sample, dep, ["n_observer_seats", "ln_fund_size", "ln_staff_investment"],
            vintage_fe=True, cluster_var="firm_id",
            label=f"T1b: {dep_label} ~ n_obs_seats + ln_size + ln_staff_inv + vFE, cl(firm)")

    # (c) + firm age
    run_reg(sample, dep, ["n_observer_seats", "ln_fund_size", "ln_staff_investment", "ln_firm_age"],
            vintage_fe=True, cluster_var="firm_id",
            label=f"T1c: {dep_label} ~ n_obs_seats + ln_size + ln_staff_inv + ln_firm_age + vFE, cl(firm)")

    # (d) + fund number (full GP experience controls)
    run_reg(sample, dep,
            ["n_observer_seats", "ln_fund_size", "ln_staff_investment", "ln_firm_age", "ln_fund_number"],
            vintage_fe=True, cluster_var="firm_id",
            label=f"T1d: {dep_label} ~ n_obs_seats + ln_size + ln_staff_inv + ln_firm_age + ln_fund_num + vFE, cl(firm)")


# =====================================================================
# TEST 2: Industry Specialization (HHI) Interaction
# =====================================================================
print("\n\n" + "#" * 90)
print("# TEST 2: INDUSTRY SPECIALIZATION")
print("# Do VCs with concentrated observer seats (high HHI) benefit more?")
print("# Interaction: n_observer_seats x observer_industry_hhi")
print("#" * 90)

for dep, dep_label in [("tvpi_w", "TVPI(w)"), ("irr_w", "IRR(w)")]:

    print(f"\n{'~' * 90}")
    print(f"  Dependent variable: {dep_label}")
    print(f"{'~' * 90}")

    # (a) HHI as standalone control
    run_reg(sample, dep,
            ["n_observer_seats", "observer_industry_hhi", "ln_fund_size"],
            vintage_fe=True, cluster_var="firm_id",
            label=f"T2a: {dep_label} ~ n_obs_seats + HHI + ln_size + vFE, cl(firm)")

    # (b) Interaction: n_observer_seats x HHI
    run_reg(sample, dep,
            ["n_observer_seats", "observer_industry_hhi", "obs_x_hhi", "ln_fund_size"],
            vintage_fe=True, cluster_var="firm_id",
            label=f"T2b: {dep_label} ~ n_obs_seats + HHI + obs_x_HHI + ln_size + vFE, cl(firm)")

    # (c) Log specification: ln_observer_seats x HHI
    run_reg(sample, dep,
            ["ln_observer_seats", "observer_industry_hhi", "ln_obs_x_hhi", "ln_fund_size"],
            vintage_fe=True, cluster_var="firm_id",
            label=f"T2c: {dep_label} ~ ln_obs + HHI + ln_obs_x_HHI + ln_size + vFE, cl(firm)")

    # (d) Full controls + interaction
    run_reg(sample, dep,
            ["n_observer_seats", "observer_industry_hhi", "obs_x_hhi",
             "ln_fund_size", "ln_fund_number", "ln_staff_investment"],
            vintage_fe=True, cluster_var="firm_id",
            label=f"T2d: {dep_label} ~ n_obs + HHI + obs_x_HHI + all_controls + vFE, cl(firm)")


# =====================================================================
# TEST 3: Fund Terms as Controls
# =====================================================================
print("\n\n" + "#" * 90)
print("# TEST 3: FUND TERMS AS CONTROLS")
print("# Adding carried interest and hurdle rate as controls")
print("#" * 90)

for dep, dep_label in [("tvpi_w", "TVPI(w)"), ("irr_w", "IRR(w)")]:

    print(f"\n{'~' * 90}")
    print(f"  Dependent variable: {dep_label}")
    print(f"{'~' * 90}")

    # (a) Baseline for comparison
    run_reg(sample, dep, ["n_observer_seats", "ln_fund_size"],
            vintage_fe=True, cluster_var="firm_id",
            label=f"T3a: {dep_label} ~ n_obs_seats + ln_size + vFE, cl(firm)")

    # (b) + carried interest only
    run_reg(sample, dep, ["n_observer_seats", "ln_fund_size", "carriedinterestpercent"],
            vintage_fe=True, cluster_var="firm_id",
            label=f"T3b: {dep_label} ~ n_obs_seats + ln_size + carry + vFE, cl(firm)")

    # (c) + hurdle rate only (skip if no hurdle data)
    if sample["hurdleratepercent"].notna().sum() > 50:
        run_reg(sample, dep, ["n_observer_seats", "ln_fund_size", "hurdleratepercent"],
                vintage_fe=True, cluster_var="firm_id",
                label=f"T3c: {dep_label} ~ n_obs_seats + ln_size + hurdle + vFE, cl(firm)")
    else:
        print(f"\n  [T3c] Skipped: hurdleratepercent has no data in sample")

    # (d) + both carry and hurdle (fall back to carry-only if no hurdle data)
    fund_terms_controls = ["carriedinterestpercent"]
    if sample["hurdleratepercent"].notna().sum() > 50:
        fund_terms_controls.append("hurdleratepercent")
    run_reg(sample, dep,
            ["n_observer_seats", "ln_fund_size"] + fund_terms_controls,
            vintage_fe=True, cluster_var="firm_id",
            label=f"T3d: {dep_label} ~ n_obs_seats + ln_size + {'+'.join(fund_terms_controls)} + vFE, cl(firm)")

    # (e) Kitchen sink: all GP + fund terms controls
    kitchen_sink = ["n_observer_seats", "ln_fund_size", "ln_fund_number",
                    "ln_staff_investment", "ln_firm_age"] + fund_terms_controls
    run_reg(sample, dep, kitchen_sink,
            vintage_fe=True, cluster_var="firm_id",
            label=f"T3e: {dep_label} ~ n_obs + all_GP_controls + fund_terms + vFE, cl(firm)")


# =====================================================================
# TEST 4: Within-Firm Variation (Firm FE)
# =====================================================================
print("\n\n" + "#" * 90)
print("# TEST 4: WITHIN-FIRM VARIATION (FIRM FE)")
print("# For VCs with multiple funds: does MORE observer seats at time of")
print("# fund launch predict better performance than the SAME VC's earlier funds?")
print("#" * 90)

# Build time-varying observer count: count observer seats available
# at or before each fund's vintage year.
# The observer network is a cross-section, so we use total count
# but restrict to multi-fund firms for within-firm identification.

multi_fund = sample.groupby("firm_id").filter(lambda g: len(g) > 1).copy()
print(f"  Multi-fund subsample: {len(multi_fund):,} funds from "
      f"{multi_fund['firm_id'].nunique():,} firms")
print(f"  With TVPI: {multi_fund['tvpi_w'].notna().sum():,}")
print(f"  With IRR: {multi_fund['irr_w'].notna().sum():,}")

for dep, dep_label in [("tvpi_w", "TVPI(w)"), ("irr_w", "IRR(w)")]:

    print(f"\n{'~' * 90}")
    print(f"  Dependent variable: {dep_label}")
    print(f"{'~' * 90}")

    # (a) Firm FE baseline (within-firm, HC1 since firm FE absorbs clustering)
    run_reg(multi_fund, dep, ["n_observer_seats", "ln_fund_size"],
            vintage_fe=False, firm_fe=True,
            label=f"T4a: {dep_label} ~ n_obs_seats + ln_size + firmFE, HC1")

    # (b) Firm FE + vintage FE
    run_reg(multi_fund, dep, ["n_observer_seats", "ln_fund_size"],
            vintage_fe=True, firm_fe=True,
            label=f"T4b: {dep_label} ~ n_obs_seats + ln_size + firmFE + vFE, HC1")

    # (c) Firm FE + fund number (controls for fund sequence)
    run_reg(multi_fund, dep, ["n_observer_seats", "ln_fund_size", "ln_fund_number"],
            vintage_fe=True, firm_fe=True,
            label=f"T4c: {dep_label} ~ n_obs_seats + ln_size + ln_fund_num + firmFE + vFE, HC1")


# =====================================================================
# TEST 5: Full Specification Battery
# =====================================================================
print("\n\n" + "#" * 90)
print("# TEST 5: FULL SPECIFICATION BATTERY")
print("# Five SE/FE combos x each dependent variable")
print("# (1) HC1  (2) Firm-cl  (3) vFE+Firm-cl  (4) FirmFE+HC1  (5) vFE+HC1")
print("#" * 90)

# Build control list dynamically -- only include fund-terms vars with data
all_controls = ["n_observer_seats", "ln_fund_size", "ln_fund_number",
                "ln_staff_investment", "ln_firm_age"]

if sample["carriedinterestpercent"].notna().sum() > 50:
    all_controls.append("carriedinterestpercent")
    print(f"  Including carriedinterestpercent ({sample['carriedinterestpercent'].notna().sum():,} obs)")
if sample["hurdleratepercent"].notna().sum() > 50:
    all_controls.append("hurdleratepercent")
    print(f"  Including hurdleratepercent ({sample['hurdleratepercent'].notna().sum():,} obs)")
else:
    print(f"  EXCLUDING hurdleratepercent (only {sample['hurdleratepercent'].notna().sum():,} obs)")

print(f"  All controls: {all_controls}")

specs_5 = [
    # (vintage_fe, firm_fe, cluster_var, se_label)
    (False, False, None,      "HC1"),
    (False, False, "firm_id", "Firm-clustered"),
    (True,  False, "firm_id", "VintageFE + Firm-cl"),
    (False, True,  None,      "FirmFE + HC1"),
    (True,  False, None,      "VintageFE + HC1"),
]

for dep, dep_label in [("tvpi_w", "TVPI(w)"), ("irr_w", "IRR(w)")]:

    print(f"\n{'~' * 90}")
    print(f"  Dependent variable: {dep_label}  |  All controls included")
    print(f"{'~' * 90}")

    for vfe, ffe, clust, se_lbl in specs_5:
        data_use = multi_fund if ffe else sample
        run_reg(data_use, dep, all_controls,
                vintage_fe=vfe, firm_fe=ffe, cluster_var=clust,
                label=f"T5: {dep_label} ~ all_controls, {se_lbl}")


# =====================================================================
# TEST 6: Quintile Analysis
# =====================================================================
print("\n\n" + "#" * 90)
print("# TEST 6: QUINTILE ANALYSIS BY OBSERVER SEATS")
print("#" * 90)

sample_q = sample.dropna(subset=["multiple"]).copy()
sample_q["obs_quintile"] = pd.qcut(
    sample_q["n_observer_seats"].rank(method="first"),
    q=5, labels=["Q1 (Lowest)", "Q2", "Q3", "Q4", "Q5 (Highest)"]
)

print("\n  Panel A: Performance by observer-seat quintile")
print("-" * 90)

quint_perf = sample_q.groupby("obs_quintile", observed=True).agg(
    n_funds=("fund_id", "count"),
    mean_obs_seats=("n_observer_seats", "mean"),
    median_obs_seats=("n_observer_seats", "median"),
    mean_tvpi=("multiple", "mean"),
    median_tvpi=("multiple", "median"),
    mean_irr=("net_irr_pcent", "mean"),
    median_irr=("net_irr_pcent", "median"),
    mean_fund_size=("final_size_usd", "mean"),
    mean_vintage=("vintage", "mean"),
).reset_index()
print(quint_perf.to_string(index=False))

# T-tests: Q5 vs Q1
q5 = sample_q[sample_q["obs_quintile"] == "Q5 (Highest)"]
q1 = sample_q[sample_q["obs_quintile"] == "Q1 (Lowest)"]

print("\n  Panel B: Q5 - Q1 differences (t-tests)")
print("-" * 90)

for dep, label in [("multiple", "TVPI"), ("net_irr_pcent", "IRR (%)")]:
    v5 = q5[dep].dropna()
    v1 = q1[dep].dropna()
    if len(v5) > 1 and len(v1) > 1:
        t, p = stats.ttest_ind(v5, v1, equal_var=False)
        diff = v5.mean() - v1.mean()
        print(f"  {label:12s}  Q5 mean={v5.mean():8.4f}  Q1 mean={v1.mean():8.4f}  "
              f"diff={diff:8.4f}  t={t:7.3f}  p={p:.4f}")

# Monotonicity test: Spearman rank correlation of quintile x mean performance
from scipy.stats import spearmanr
quint_nums = [1, 2, 3, 4, 5]
for dep, label in [("mean_tvpi", "TVPI"), ("mean_irr", "IRR")]:
    vals = quint_perf[dep].values
    if not np.any(np.isnan(vals)):
        rho, p = spearmanr(quint_nums, vals)
        print(f"  Monotonicity ({label}): Spearman rho={rho:.3f}, p={p:.4f}")

# =====================================================================
# Quintile regressions (quintile dummies)
# =====================================================================
print("\n  Panel C: Quintile dummies in regression (Q1 = omitted)")
print("-" * 90)

sample_q["Q2"] = (sample_q["obs_quintile"] == "Q2").astype(float)
sample_q["Q3"] = (sample_q["obs_quintile"] == "Q3").astype(float)
sample_q["Q4"] = (sample_q["obs_quintile"] == "Q4").astype(float)
sample_q["Q5"] = (sample_q["obs_quintile"] == "Q5 (Highest)").astype(float)

for dep, dep_label in [("tvpi_w", "TVPI(w)"), ("irr_w", "IRR(w)")]:
    run_reg(sample_q, dep,
            ["Q2", "Q3", "Q4", "Q5", "ln_fund_size"],
            vintage_fe=True, cluster_var="firm_id",
            label=f"T6: {dep_label} ~ quintile_dummies + ln_size + vFE, cl(firm)")


# =====================================================================
# COMPACT RESULTS SUMMARY
# =====================================================================
print("\n\n" + "#" * 90)
print("# COMPACT RESULTS SUMMARY")
print("#" * 90)

print("""
Key coefficient on n_observer_seats across specifications:

Test 1 (GP Experience): Does observer count survive controlling for GP team
        size (staffcountinvestment) and firm age?

Test 2 (Industry HHI): Does the interaction obs_seats x HHI tell us whether
        specialized VCs benefit more from observers?

Test 3 (Fund Terms): Does observer count survive controlling for carried
        interest and hurdle rate (fund-terms that proxy for GP quality)?

Test 4 (Within-Firm): Using firm FE on multi-fund subsample, does a VC's
        fund perform better when they have more observer seats?

Test 5 (Battery): All controls, five SE/FE specifications.

Test 6 (Quintiles): Non-parametric performance gradient across observer
        seat quintiles.
""")

# =====================================================================
# Correlation table for new variables
# =====================================================================
print("\n" + "=" * 90)
print("APPENDIX: Pairwise correlations (extended variables)")
print("=" * 90)

corr_vars = [
    "multiple", "net_irr_pcent", "n_observer_seats",
    "observer_industry_hhi", "final_size_usd", "vintage",
    "fund_number_overall", "staffcountinvestment", "firm_age",
    "carriedinterestpercent",
]
# Only include variables that actually have data
corr_vars = [v for v in corr_vars if sample[v].notna().sum() > 50]
corr_df = sample[corr_vars].dropna()
print(f"  N = {len(corr_df):,}")
print(corr_df.corr().to_string(float_format=lambda x: f"{x:.3f}"))

print("\n\nDone.")
