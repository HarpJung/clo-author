"""
Cross-sectional test: Do VCs with more observer seats have better fund performance?
Following Hochberg, Ljungqvist & Lu (2007) who show VC network centrality predicts fund returns.

Pipeline:
1. Count observer seats per CIQ VC firm from observer network
2. Match to Preqin via validated crosswalk (high + medium quality)
3. Get final fund performance (last reported TVPI and IRR)
4. Filter to VC-type funds
5. Run cross-sectional regressions of performance on observer counts
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd
import numpy as np
import statsmodels.api as sm
from scipy import stats

pd.set_option("display.max_columns", 40)
pd.set_option("display.width", 160)
pd.set_option("display.float_format", lambda x: f"{x:.4f}")

# ── Paths ──────────────────────────────────────────────────────────────
DATA = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
OBS_PATH = f"{DATA}/table_b_observer_network.csv"
XWALK_PATH = f"{DATA}/Preqin/vc_preqin_crosswalk_validated.csv"
FUND_DET_PATH = f"{DATA}/Preqin/fund_details_full.csv"
FUND_PERF_PATH = f"{DATA}/Preqin/fund_performance_full.csv"

# =====================================================================
# STEP 1: Build VC-level observer counts
# =====================================================================
print("=" * 80)
print("STEP 1: Building VC-level observer counts from observer network")
print("=" * 80)

obs = pd.read_csv(OBS_PATH)
print(f"  Observer network rows: {len(obs):,}")
print(f"  Unique VC firms (CIQ): {obs['vc_firm_companyid'].nunique():,}")
print(f"  Unique observers: {obs['observer_personid'].nunique():,}")
print(f"  Unique observed companies: {obs['observed_companyid'].nunique():,}")

# Aggregate to VC-firm level
vc_counts = obs.groupby("vc_firm_companyid").agg(
    vc_firm_name=("vc_firm_name", "first"),
    n_observer_seats=("observer_personid", "nunique"),  # unique observer persons
    n_observed_companies=("observed_companyid", "nunique"),  # unique portfolio cos observed
    n_observer_rows=("observer_personid", "count"),  # total observer-company pairs
).reset_index()

print(f"\n  VC-level counts (N={len(vc_counts):,}):")
print(vc_counts[["n_observer_seats", "n_observed_companies"]].describe())

# =====================================================================
# STEP 2: Match to Preqin via crosswalk (high + medium quality)
# =====================================================================
print("\n" + "=" * 80)
print("STEP 2: Matching to Preqin via crosswalk (high + medium quality)")
print("=" * 80)

xwalk = pd.read_csv(XWALK_PATH)
print(f"  Crosswalk total rows: {len(xwalk):,}")
print(f"  Quality distribution:\n{xwalk['quality'].value_counts().to_string()}")

xwalk_hm = xwalk[xwalk["quality"].isin(["high", "medium"])].copy()
print(f"\n  High + Medium quality rows: {len(xwalk_hm):,}")
print(f"  Unique CIQ firms: {xwalk_hm['ciq_vc_companyid'].nunique():,}")
print(f"  Unique Preqin firms: {xwalk_hm['preqin_firm_id'].nunique():,}")

# Merge VC counts with crosswalk
vc_preqin = vc_counts.merge(
    xwalk_hm[["ciq_vc_companyid", "preqin_firm_id", "quality"]],
    left_on="vc_firm_companyid",
    right_on="ciq_vc_companyid",
    how="inner",
)
print(f"\n  After merge with observer counts:")
print(f"    Matched rows: {len(vc_preqin):,}")
print(f"    Unique CIQ VC firms matched: {vc_preqin['vc_firm_companyid'].nunique():,}")
print(f"    Unique Preqin firms matched: {vc_preqin['preqin_firm_id'].nunique():,}")

# =====================================================================
# STEP 3: Get Preqin fund details (VC-type funds only)
# =====================================================================
print("\n" + "=" * 80)
print("STEP 3: Loading Preqin fund details (VC-type funds)")
print("=" * 80)

fd = pd.read_csv(FUND_DET_PATH)
print(f"  Total funds in Preqin: {len(fd):,}")

# Filter to VC-related fund types
vc_types = fd["fund_type"].str.contains(
    "Venture|Seed|Early", case=False, na=False
)
fd_vc = fd[vc_types].copy()
print(f"  VC-type funds (Venture/Seed/Early): {len(fd_vc):,}")
print(f"  Fund type breakdown:\n{fd_vc['fund_type'].value_counts().to_string()}")

# =====================================================================
# STEP 4: Get final fund performance (last reported TVPI and IRR)
# =====================================================================
print("\n" + "=" * 80)
print("STEP 4: Getting final fund performance (last reported per fund)")
print("=" * 80)

fp = pd.read_csv(FUND_PERF_PATH, low_memory=False)
print(f"  Total performance rows: {len(fp):,}")
print(f"  Unique funds with performance: {fp['fund_id'].nunique():,}")

# Convert IRR to numeric
fp["net_irr_pcent"] = pd.to_numeric(fp["net_irr_pcent"], errors="coerce")
fp["multiple"] = pd.to_numeric(fp["multiple"], errors="coerce")
fp["date_reported"] = pd.to_datetime(fp["date_reported"], errors="coerce")

# Get last reported observation per fund
fp_sorted = fp.sort_values(["fund_id", "date_reported"])
fp_last = fp_sorted.groupby("fund_id").last().reset_index()
print(f"  Funds with final TVPI: {fp_last['multiple'].notna().sum():,}")
print(f"  Funds with final IRR: {fp_last['net_irr_pcent'].notna().sum():,}")

# =====================================================================
# STEP 5: Merge everything together
# =====================================================================
print("\n" + "=" * 80)
print("STEP 5: Merging VC observer counts with fund performance")
print("=" * 80)

# Merge fund details with performance
fund_merged = fd_vc.merge(fp_last[["fund_id", "multiple", "net_irr_pcent", "date_reported"]],
                          on="fund_id", how="inner")
print(f"  VC funds with performance data: {len(fund_merged):,}")

# Merge with VC observer counts via preqin_firm_id
# vc_preqin links CIQ VC -> Preqin firm_id; fd_vc has firm_id
analysis = fund_merged.merge(
    vc_preqin[["preqin_firm_id", "vc_firm_companyid", "vc_firm_name",
               "n_observer_seats", "n_observed_companies", "n_observer_rows",
               "quality"]],
    left_on="firm_id",
    right_on="preqin_firm_id",
    how="inner",
)
print(f"  Funds matched to VC observer data: {len(analysis):,}")
print(f"  Unique Preqin firms: {analysis['firm_id'].nunique():,}")
print(f"  Unique CIQ VC firms: {analysis['vc_firm_companyid'].nunique():,}")

# If multiple CIQ firms map to same Preqin firm, keep highest observer count
analysis = analysis.sort_values("n_observer_seats", ascending=False)
analysis = analysis.drop_duplicates(subset=["fund_id"], keep="first")
print(f"  After dedup (one obs per fund): {len(analysis):,}")

# =====================================================================
# STEP 6: Build regression variables
# =====================================================================
print("\n" + "=" * 80)
print("STEP 6: Building regression variables")
print("=" * 80)

# Fund size in logs
analysis["final_size_usd"] = pd.to_numeric(analysis["final_size_usd"], errors="coerce")
analysis["ln_fund_size"] = np.log(analysis["final_size_usd"].clip(lower=0.01))

# Vintage
analysis["vintage"] = pd.to_numeric(analysis["vintage"], errors="coerce")

# Observer intensity = n_observer_seats / fund_size (per $100M)
analysis["observer_intensity"] = analysis["n_observer_seats"] / (
    analysis["final_size_usd"].clip(lower=0.01) / 100
)

# Log of observer counts
analysis["ln_observer_seats"] = np.log(analysis["n_observer_seats"].clip(lower=1))
analysis["ln_observed_companies"] = np.log(analysis["n_observed_companies"].clip(lower=1))

# Winsorize extreme performance values at 1/99
def winsorize(s, lower=0.01, upper=0.99):
    q = s.quantile([lower, upper])
    return s.clip(q.iloc[0], q.iloc[1])

analysis["tvpi_w"] = winsorize(analysis["multiple"].dropna()).reindex(analysis.index)
analysis["irr_w"] = winsorize(analysis["net_irr_pcent"].dropna()).reindex(analysis.index)

# Fund number as proxy for GP experience
analysis["fund_number_overall"] = pd.to_numeric(
    analysis["fund_number_overall"], errors="coerce"
)
analysis["ln_fund_number"] = np.log(analysis["fund_number_overall"].clip(lower=1))

# Sample filters
sample = analysis.dropna(subset=["ln_fund_size", "vintage"]).copy()
sample = sample[sample["final_size_usd"] > 0].copy()
sample = sample[sample["vintage"] >= 1990].copy()
sample = sample[sample["vintage"] <= 2022].copy()

print(f"  Analysis sample (with fund size, vintage 1990-2022): {len(sample):,}")
print(f"  With TVPI: {sample['multiple'].notna().sum():,}")
print(f"  With IRR: {sample['net_irr_pcent'].notna().sum():,}")

# =====================================================================
# STEP 7: Summary statistics
# =====================================================================
print("\n" + "=" * 80)
print("STEP 7: Summary statistics")
print("=" * 80)

summ_vars = ["n_observer_seats", "n_observed_companies", "observer_intensity",
             "multiple", "net_irr_pcent", "final_size_usd", "vintage",
             "fund_number_overall"]

print("\nPanel A: Full sample summary statistics")
print("-" * 80)
summ = sample[summ_vars].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9]).T
summ = summ[["count", "mean", "std", "10%", "25%", "50%", "75%", "90%"]]
print(summ.to_string())

print("\n\nPanel B: Distribution of observer seat counts")
print("-" * 80)
obs_dist = sample["n_observer_seats"].value_counts().sort_index()
print(f"  Min: {sample['n_observer_seats'].min()}")
print(f"  Max: {sample['n_observer_seats'].max()}")
print(f"  Mean: {sample['n_observer_seats'].mean():.2f}")
print(f"  Median: {sample['n_observer_seats'].median():.1f}")
print(f"\n  Frequency table (top 20):")
print(obs_dist.head(20).to_string())

print("\n\nPanel C: Vintage distribution")
print("-" * 80)
print(sample["vintage"].value_counts().sort_index().to_string())

# =====================================================================
# STEP 8: Quartile analysis by observer seats
# =====================================================================
print("\n" + "=" * 80)
print("STEP 8: Performance by observer-seat quartile")
print("=" * 80)

# Create quartiles
sample["obs_quartile"] = pd.qcut(
    sample["n_observer_seats"].rank(method="first"),
    q=4, labels=["Q1 (Low)", "Q2", "Q3", "Q4 (High)"]
)

quartile_perf = sample.groupby("obs_quartile", observed=True).agg(
    n_funds=("fund_id", "count"),
    mean_observer_seats=("n_observer_seats", "mean"),
    mean_tvpi=("multiple", "mean"),
    median_tvpi=("multiple", "median"),
    mean_irr=("net_irr_pcent", "mean"),
    median_irr=("net_irr_pcent", "median"),
    mean_fund_size=("final_size_usd", "mean"),
    mean_vintage=("vintage", "mean"),
).reset_index()
print(quartile_perf.to_string(index=False))

# T-test: Q4 vs Q1
q4 = sample[sample["obs_quartile"] == "Q4 (High)"]
q1 = sample[sample["obs_quartile"] == "Q1 (Low)"]

for dep, label in [("multiple", "TVPI"), ("net_irr_pcent", "IRR")]:
    v4 = q4[dep].dropna()
    v1 = q1[dep].dropna()
    if len(v4) > 1 and len(v1) > 1:
        t, p = stats.ttest_ind(v4, v1, equal_var=False)
        print(f"\n  Q4-Q1 difference in {label}: {v4.mean() - v1.mean():.4f}  "
              f"t={t:.3f}  p={p:.4f}")

# =====================================================================
# STEP 9: Cross-sectional regressions
# =====================================================================
print("\n" + "=" * 80)
print("STEP 9: Cross-sectional regressions")
print("=" * 80)


def run_reg(data, y_var, x_vars, vintage_fe=False, cluster_var=None, label=""):
    """Run OLS with optional vintage FE and clustered/robust SE."""
    df = data.dropna(subset=[y_var] + x_vars).copy()

    if len(df) < 20:
        print(f"\n  [{label}] Skipped: only {len(df)} obs")
        return None

    Y = df[y_var]
    X = df[x_vars].copy()

    if vintage_fe:
        vint_dummies = pd.get_dummies(df["vintage"], prefix="v", drop_first=True, dtype=float)
        X = pd.concat([X, vint_dummies], axis=1)

    X = sm.add_constant(X)

    if cluster_var and cluster_var in df.columns:
        # Firm-clustered SE
        groups = df[cluster_var]
        model = sm.OLS(Y, X).fit(
            cov_type="cluster", cov_kwds={"groups": groups}
        )
    else:
        # HC1 robust SE
        model = sm.OLS(Y, X).fit(cov_type="HC1")

    # Print results
    print(f"\n{'─' * 80}")
    print(f"  [{label}]")
    print(f"  Dep var: {y_var}  |  N = {model.nobs:.0f}  |  R2 = {model.rsquared:.4f}  "
          f"|  Adj-R2 = {model.rsquared_adj:.4f}")
    se_type = f"Clustered ({cluster_var})" if cluster_var else "HC1 robust"
    print(f"  SE type: {se_type}  |  Vintage FE: {vintage_fe}")
    print(f"{'─' * 80}")

    # Print only key coefficients (skip vintage dummies)
    key_vars = ["const"] + x_vars
    for v in key_vars:
        if v in model.params.index:
            coef = model.params[v]
            se = model.bse[v]
            t = model.tvalues[v]
            p = model.pvalues[v]
            stars = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            print(f"    {v:30s}  {coef:10.4f}  ({se:8.4f})  t={t:7.3f}  p={p:.4f} {stars}")

    if vintage_fe:
        n_vfe = sum(1 for c in model.params.index if c.startswith("v_"))
        print(f"    [+ {n_vfe} vintage dummies]")

    return model


# ── Define regression specifications ──────────────────────────────────
print("\n\n" + "=" * 80)
print("A. TVPI Regressions")
print("=" * 80)

base_controls = ["ln_fund_size"]
extended_controls = ["ln_fund_size", "ln_fund_number"]

specs = [
    # (dep_var, key_x, controls, vintage_fe, cluster, label)
    ("multiple", "n_observer_seats", base_controls, False, None,
     "TVPI ~ n_observer_seats + ln_fund_size, HC1"),
    ("multiple", "n_observer_seats", base_controls, True, None,
     "TVPI ~ n_observer_seats + ln_fund_size + vintage_FE, HC1"),
    ("multiple", "n_observer_seats", base_controls, True, "firm_id",
     "TVPI ~ n_observer_seats + ln_fund_size + vintage_FE, clustered(firm)"),
    ("multiple", "n_observer_seats", extended_controls, True, "firm_id",
     "TVPI ~ n_observer_seats + ln_fund_size + ln_fund_number + vintage_FE, clustered(firm)"),
    ("multiple", "ln_observer_seats", base_controls, True, "firm_id",
     "TVPI ~ ln_observer_seats + ln_fund_size + vintage_FE, clustered(firm)"),
    ("multiple", "n_observed_companies", base_controls, True, "firm_id",
     "TVPI ~ n_observed_companies + ln_fund_size + vintage_FE, clustered(firm)"),
    ("multiple", "observer_intensity", base_controls, True, "firm_id",
     "TVPI ~ observer_intensity + ln_fund_size + vintage_FE, clustered(firm)"),
]

for dep, key_x, ctrls, vfe, clust, lbl in specs:
    run_reg(sample, dep, [key_x] + ctrls, vintage_fe=vfe, cluster_var=clust, label=lbl)

print("\n\n" + "=" * 80)
print("B. IRR Regressions")
print("=" * 80)

specs_irr = [
    ("net_irr_pcent", "n_observer_seats", base_controls, False, None,
     "IRR ~ n_observer_seats + ln_fund_size, HC1"),
    ("net_irr_pcent", "n_observer_seats", base_controls, True, None,
     "IRR ~ n_observer_seats + ln_fund_size + vintage_FE, HC1"),
    ("net_irr_pcent", "n_observer_seats", base_controls, True, "firm_id",
     "IRR ~ n_observer_seats + ln_fund_size + vintage_FE, clustered(firm)"),
    ("net_irr_pcent", "n_observer_seats", extended_controls, True, "firm_id",
     "IRR ~ n_observer_seats + ln_fund_size + ln_fund_number + vintage_FE, clustered(firm)"),
    ("net_irr_pcent", "ln_observer_seats", base_controls, True, "firm_id",
     "IRR ~ ln_observer_seats + ln_fund_size + vintage_FE, clustered(firm)"),
    ("net_irr_pcent", "n_observed_companies", base_controls, True, "firm_id",
     "IRR ~ n_observed_companies + ln_fund_size + vintage_FE, clustered(firm)"),
    ("net_irr_pcent", "observer_intensity", base_controls, True, "firm_id",
     "IRR ~ observer_intensity + ln_fund_size + vintage_FE, clustered(firm)"),
]

for dep, key_x, ctrls, vfe, clust, lbl in specs_irr:
    run_reg(sample, dep, [key_x] + ctrls, vintage_fe=vfe, cluster_var=clust, label=lbl)


# ── Winsorized versions ──────────────────────────────────────────────
print("\n\n" + "=" * 80)
print("C. Winsorized performance (1/99 pctile)")
print("=" * 80)

for dep, lbl_short in [("tvpi_w", "TVPI_w"), ("irr_w", "IRR_w")]:
    for key_x, xlab in [("n_observer_seats", "n_obs_seats"),
                         ("ln_observer_seats", "ln_obs_seats")]:
        run_reg(sample, dep, [key_x] + base_controls,
                vintage_fe=True, cluster_var="firm_id",
                label=f"{lbl_short} ~ {xlab} + ln_fund_size + vintage_FE, clustered(firm)")


# =====================================================================
# STEP 10: Correlation table
# =====================================================================
print("\n\n" + "=" * 80)
print("STEP 10: Pairwise correlations")
print("=" * 80)

corr_vars = ["multiple", "net_irr_pcent", "n_observer_seats", "n_observed_companies",
             "observer_intensity", "final_size_usd", "vintage", "fund_number_overall"]
corr_df = sample[corr_vars].dropna()
print(f"\n  N = {len(corr_df):,}")
print(corr_df.corr().to_string(float_format=lambda x: f"{x:.3f}"))


# =====================================================================
# FINAL: Compact results table
# =====================================================================
print("\n\n" + "=" * 80)
print("COMPACT RESULTS TABLE")
print("=" * 80)

print("""
The table below collects the key coefficient on observer-seat measures
across specifications (winsorized, vintage FE, firm-clustered SE).

Key finding: whether VCs with larger observer networks have better fund
returns, consistent with Hochberg, Ljungqvist & Lu (2007)'s finding that
VC network centrality predicts fund returns.
""")

print("\nDone.")
