"""
Network Structure Tests — Observer Overlap, VC Concentration, Centrality, Contagion.

Tests whether observer network structure (overlap, concentration) predicts outcomes.

Test 1: Observer overlap — same person observes at 2+ companies
Test 2: VC industry concentration (HHI) vs fund performance
Test 3: Network centrality measures vs fund performance
Test 4: Information flow / event contagion within VC portfolios

All regressions with: (1) HC1, (2) Firm-clustered, (3) Vintage FE where applicable.
Always reports: Overall, Same-Industry, Different-Industry subsamples.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd
import numpy as np
import os
import warnings
from itertools import combinations
from collections import defaultdict
import statsmodels.api as sm
from statsmodels.regression.linear_model import OLS

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ═══════════════════════════════════════════════════════════════════
# PATHS
# ═══════════════════════════════════════════════════════════════════
data_dir   = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir    = os.path.join(data_dir, "CIQ_Extract")
preqin_dir = os.path.join(data_dir, "Preqin")
net_dir    = os.path.join(data_dir, "Panel_C_Network")

# ═══════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════
print("=" * 90)
print("NETWORK STRUCTURE TESTS")
print("Observer Overlap, VC Concentration, Centrality, Event Contagion")
print("=" * 90)

print("\n--- Loading data ---")

observers   = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
companies   = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
events      = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"))
network     = pd.read_csv(os.path.join(data_dir, "table_b_observer_network.csv"))
positions   = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
xwalk       = pd.read_csv(os.path.join(preqin_dir, "vc_preqin_crosswalk_validated.csv"))
fund_det    = pd.read_csv(os.path.join(preqin_dir, "fund_details_full.csv"))
fund_perf   = pd.read_csv(os.path.join(preqin_dir, "fund_performance_full.csv"), low_memory=False)
ciq_cik     = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ind_codes   = pd.read_csv(os.path.join(net_dir, "05_industry_codes.csv"))

print(f"  Observer records:    {len(observers):>8,}")
print(f"  Companies:           {len(companies):>8,}")
print(f"  Events:              {len(events):>8,}")
print(f"  Network links:       {len(network):>8,}")
print(f"  All positions:       {len(positions):>8,}")
print(f"  Preqin crosswalk:    {len(xwalk):>8,}")
print(f"  Fund details:        {len(fund_det):>8,}")
print(f"  Fund performance:    {len(fund_perf):>8,}")
print(f"  CIQ-CIK crosswalk:  {len(ciq_cik):>8,}")
print(f"  Industry codes:      {len(ind_codes):>8,}")

# ═══════════════════════════════════════════════════════════════════
# PREPROCESSING
# ═══════════════════════════════════════════════════════════════════
print("\n--- Preprocessing ---")

# Quality-filter crosswalk
xwalk = xwalk[xwalk["quality"].isin(["high", "medium"])]
print(f"  Crosswalk after quality filter: {len(xwalk):,}")

# Build mappings
firm_to_ciq = xwalk.drop_duplicates("preqin_firm_id").set_index("preqin_firm_id")["ciq_vc_companyid"].to_dict()
ciq_to_firm = {v: k for k, v in firm_to_ciq.items()}

matched_firm_ids = set(xwalk["preqin_firm_id"].dropna().astype(int))

# Fund details: VC funds only
vc_funds = fund_det[fund_det["firm_id"].isin(matched_firm_ids)].copy()
vc_funds = vc_funds[vc_funds["fund_type"].str.contains("Venture|Seed|Early", case=False, na=False)].copy()
vc_fund_ids = set(vc_funds["fund_id"].dropna().astype(int))
print(f"  VC funds matched: {len(vc_fund_ids):,}")

# Fund performance: latest observation per fund
fund_perf["date_reported"] = pd.to_datetime(fund_perf["date_reported"], errors="coerce")
fund_perf = fund_perf[fund_perf["fund_id"].isin(vc_fund_ids)].copy()
for col in ["multiple", "net_irr_pcent"]:
    fund_perf[f"{col}_num"] = pd.to_numeric(fund_perf[col], errors="coerce")
fund_perf = fund_perf.dropna(subset=["date_reported"]).sort_values(["fund_id", "date_reported"])
latest_perf = fund_perf.groupby("fund_id").last().reset_index()
latest_perf = latest_perf.merge(
    vc_funds[["fund_id", "firm_id", "vintage", "final_size_usd"]].drop_duplicates("fund_id"),
    on="fund_id", how="left", suffixes=("", "_fund")
)
latest_perf["vintage_num"] = pd.to_numeric(latest_perf["vintage"], errors="coerce")
latest_perf["log_size"] = np.log1p(pd.to_numeric(latest_perf["final_size_usd"], errors="coerce"))
print(f"  Funds with latest performance: {len(latest_perf):,}")

# Events: parse date, add quarter
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"]).copy()
events["quarter"] = events["announcedate"].dt.to_period("Q")
events["year"] = events["announcedate"].dt.year
print(f"  Events with valid dates: {len(events):,}")

# Industry codes: build companyid -> SIC2 mapping via CIK crosswalk
# CIQ CIK has companyid -> cik; industry codes has cik -> sic
ciq_cik["cik_str"] = ciq_cik["cik"].astype(str).str.zfill(10)
ind_codes["cik_str"] = ind_codes["cik"].astype(str).str.zfill(10)

cik_to_sic = ind_codes.drop_duplicates("cik_str").set_index("cik_str")["sic"].to_dict()
ciq_cik["sic"] = ciq_cik["cik_str"].map(cik_to_sic)
companyid_to_sic = ciq_cik.dropna(subset=["sic"]).drop_duplicates("companyid").set_index("companyid")["sic"].to_dict()

# SIC2 = first 2 digits
def sic_to_sic2(sic_val):
    try:
        s = str(int(float(sic_val)))
        return s[:2] if len(s) >= 2 else s.zfill(2)[:2]
    except:
        return None

companyid_to_sic2 = {k: sic_to_sic2(v) for k, v in companyid_to_sic.items()}
companyid_to_sic2 = {k: v for k, v in companyid_to_sic2.items() if v is not None}

# Deduplicate network to observer-company level
net_dedup = network.drop_duplicates(subset=["observer_personid", "observed_companyid"])
print(f"  Unique observer-company links: {len(net_dedup):,}")

# Build VC firm -> observed companies mapping (deduplicate at firm-company level)
net_vc = network.drop_duplicates(subset=["vc_firm_companyid", "observed_companyid"])
vc_to_companies = net_vc.groupby("vc_firm_companyid")["observed_companyid"].apply(set).to_dict()

# Build person -> observed companies
person_to_companies = net_dedup.groupby("observer_personid")["observed_companyid"].apply(set).to_dict()

print(f"  Unique VC firms in network: {len(vc_to_companies):,}")
print(f"  Unique persons in network:  {len(person_to_companies):,}")
print(f"  Companies with SIC2:        {len(companyid_to_sic2):,}")


# ═══════════════════════════════════════════════════════════════════
# UTILITY: regression runner with 3 SE types
# ═══════════════════════════════════════════════════════════════════
def run_regression(y, X, cluster_var=None, label="", vintage_fe=False, vintage_col=None, df=None):
    """
    Run OLS with (1) HC1, (2) Firm-clustered, (3) optionally Vintage FE.
    Prints a clean table.
    """
    results = {}

    if df is not None and vintage_fe and vintage_col is not None:
        # Add vintage dummies
        vint = pd.to_numeric(df[vintage_col], errors="coerce")
        vint_dummies = pd.get_dummies(vint, prefix="vfe", drop_first=True, dtype=float)
        X_vfe = pd.concat([X.reset_index(drop=True), vint_dummies.reset_index(drop=True)], axis=1)
    else:
        X_vfe = None

    for spec_name, X_use, use_cluster in [
        ("HC1", X, False),
        ("Cluster", X, True),
        ("Vintage FE + HC1", X_vfe, False) if X_vfe is not None else (None, None, None),
    ]:
        if spec_name is None:
            continue

        try:
            X_c = sm.add_constant(X_use, has_constant="skip")
            mask = y.notna() & X_c.notna().all(axis=1)
            y_c, X_c2 = y[mask].reset_index(drop=True), X_c[mask].reset_index(drop=True)

            if len(y_c) < 10:
                continue

            model = OLS(y_c.astype(float), X_c2.astype(float)).fit(
                cov_type="HC1" if not use_cluster else "cluster",
                cov_kwds={"groups": cluster_var[mask].reset_index(drop=True).astype(int)} if use_cluster and cluster_var is not None else {},
            )
            results[spec_name] = model
        except Exception as e:
            results[spec_name] = str(e)

    if not results:
        print(f"  [{label}] No valid regressions could be run.")
        return results

    # Get the treatment variables (not const, not vfe_*)
    treat_vars = [c for c in X.columns if c != "const"]

    print(f"\n  {'=' * 80}")
    print(f"  {label}")
    print(f"  {'=' * 80}")
    hdr = f"  {'Variable':<25}"
    for spec_name in results:
        if isinstance(results[spec_name], str):
            continue
        hdr += f"  {spec_name:>22}"
    print(hdr)
    print(f"  {'-' * 80}")

    for var in treat_vars:
        row = f"  {var:<25}"
        for spec_name, res in results.items():
            if isinstance(res, str):
                row += f"  {'ERR':>22}"
                continue
            if var in res.params.index:
                coef = res.params[var]
                se   = res.bse[var]
                pval = res.pvalues[var]
                stars = "***" if pval < 0.01 else "**" if pval < 0.05 else "*" if pval < 0.10 else ""
                row += f"  {coef:>10.4f}{stars:<3} ({se:.4f})"
            else:
                row += f"  {'--':>22}"
        print(row)

    # N and R-squared
    row_n  = f"  {'N':<25}"
    row_r2 = f"  {'R-squared':<25}"
    for spec_name, res in results.items():
        if isinstance(res, str):
            row_n  += f"  {'ERR':>22}"
            row_r2 += f"  {'ERR':>22}"
        else:
            row_n  += f"  {res.nobs:>22,.0f}"
            row_r2 += f"  {res.rsquared:>22.4f}"
    print(f"  {'-' * 80}")
    print(row_n)
    print(row_r2)

    return results


# ═══════════════════════════════════════════════════════════════════
# SECTION 0: NETWORK STRUCTURE SUMMARY STATISTICS
# ═══════════════════════════════════════════════════════════════════
print("\n")
print("=" * 90)
print("SECTION 0: NETWORK STRUCTURE SUMMARY STATISTICS")
print("=" * 90)

# Multi-company observers
multi_observers = {pid: comps for pid, comps in person_to_companies.items() if len(comps) >= 2}
print(f"\n  Persons observing 1 company:   {sum(1 for v in person_to_companies.values() if len(v) == 1):,}")
print(f"  Persons observing 2+ companies: {len(multi_observers):,}")
print(f"  Persons observing 3+ companies: {sum(1 for v in person_to_companies.values() if len(v) >= 3):,}")
print(f"  Persons observing 5+ companies: {sum(1 for v in person_to_companies.values() if len(v) >= 5):,}")

# Distribution of observer seats per VC
seats_per_vc = {k: len(v) for k, v in vc_to_companies.items()}
seats_arr = np.array(list(seats_per_vc.values()))
print(f"\n  VC firms in network:           {len(seats_per_vc):,}")
print(f"  Observer seats per VC:")
print(f"    Mean:   {seats_arr.mean():.1f}")
print(f"    Median: {np.median(seats_arr):.1f}")
print(f"    P25:    {np.percentile(seats_arr, 25):.0f}")
print(f"    P75:    {np.percentile(seats_arr, 75):.0f}")
print(f"    Max:    {seats_arr.max():.0f}")

# Pairs from multi-company observers
total_pairs = 0
for pid, comps in multi_observers.items():
    total_pairs += len(comps) * (len(comps) - 1) // 2
print(f"\n  Total company pairs from multi-observers: {total_pairs:,}")

# Industry coverage
vc_industries = {}
for vc_id, comp_set in vc_to_companies.items():
    sics = [companyid_to_sic2[c] for c in comp_set if c in companyid_to_sic2]
    vc_industries[vc_id] = sics

vc_with_ind = {k: v for k, v in vc_industries.items() if len(v) >= 2}
print(f"  VC firms with 2+ companies having SIC2: {len(vc_with_ind):,}")


# ═══════════════════════════════════════════════════════════════════
# TEST 1: OBSERVER OVERLAP — EVENT TIMING CORRELATION
# ═══════════════════════════════════════════════════════════════════
print("\n")
print("=" * 90)
print("TEST 1: OBSERVER OVERLAP — CORRELATED EVENT TIMING")
print("Do companies sharing an observer have correlated event timing?")
print("=" * 90)

# Build company -> set of event quarters
comp_event_quarters = events.groupby("companyid")["quarter"].apply(set).to_dict()

# Build pairs from multi-company observers
overlap_pairs = set()
for pid, comps in multi_observers.items():
    sorted_comps = sorted(comps)
    for i in range(len(sorted_comps)):
        for j in range(i+1, len(sorted_comps)):
            overlap_pairs.add((sorted_comps[i], sorted_comps[j]))

print(f"\n  Unique company pairs sharing an observer: {len(overlap_pairs):,}")

# For each pair, check: company A event in quarter t => company B event in t or t+1
# Metric: fraction of A's event quarters where B also has an event in t or t+1
def compute_overlap_score(pair, comp_event_quarters):
    """Fraction of co-event quarters for a pair."""
    c1, c2 = pair
    q1 = comp_event_quarters.get(c1, set())
    q2 = comp_event_quarters.get(c2, set())
    if not q1 or not q2:
        return np.nan, 0, 0

    # For each quarter in q1, check if q2 has event in same quarter or next
    hits = 0
    checks = 0
    for q in q1:
        checks += 1
        next_q = q + 1  # PeriodIndex arithmetic
        if q in q2 or next_q in q2:
            hits += 1
    return hits / checks if checks > 0 else np.nan, hits, checks

# Compute for overlap pairs
print("  Computing event overlap for observer-linked pairs...")
overlap_scores = []
for pair in overlap_pairs:
    score, hits, checks = compute_overlap_score(pair, comp_event_quarters)
    if not np.isnan(score) and checks >= 1:
        c1_sic2 = companyid_to_sic2.get(pair[0])
        c2_sic2 = companyid_to_sic2.get(pair[1])
        same_ind = 1 if (c1_sic2 is not None and c2_sic2 is not None and c1_sic2 == c2_sic2) else 0
        overlap_scores.append({
            "c1": pair[0], "c2": pair[1],
            "overlap_score": score, "hits": hits, "checks": checks,
            "linked": 1,
            "same_industry": same_ind,
            "sic2_c1": c1_sic2, "sic2_c2": c2_sic2,
        })

print(f"  Overlap pairs with event data: {len(overlap_scores):,}")

# Build random control pairs (matched on industry where possible)
print("  Building random control pairs...")
all_companies_with_events = list(comp_event_quarters.keys())
np.random.seed(42)

control_scores = []
n_controls_target = min(len(overlap_scores) * 3, 50000)
attempts = 0
max_attempts = n_controls_target * 10
while len(control_scores) < n_controls_target and attempts < max_attempts:
    attempts += 1
    idx = np.random.choice(len(all_companies_with_events), size=2, replace=False)
    c1, c2 = all_companies_with_events[idx[0]], all_companies_with_events[idx[1]]
    if (c1, c2) in overlap_pairs or (c2, c1) in overlap_pairs:
        continue
    score, hits, checks = compute_overlap_score((c1, c2), comp_event_quarters)
    if not np.isnan(score) and checks >= 1:
        c1_sic2 = companyid_to_sic2.get(c1)
        c2_sic2 = companyid_to_sic2.get(c2)
        same_ind = 1 if (c1_sic2 is not None and c2_sic2 is not None and c1_sic2 == c2_sic2) else 0
        control_scores.append({
            "c1": c1, "c2": c2,
            "overlap_score": score, "hits": hits, "checks": checks,
            "linked": 0,
            "same_industry": same_ind,
            "sic2_c1": c1_sic2, "sic2_c2": c2_sic2,
        })

print(f"  Control pairs: {len(control_scores):,}")

# Combine
df_pairs = pd.DataFrame(overlap_scores + control_scores)
df_pairs["has_industry"] = df_pairs["sic2_c1"].notna() & df_pairs["sic2_c2"].notna()

# Summary stats
print(f"\n  --- Event Overlap Summary ---")
for subset_name, mask in [
    ("All pairs", pd.Series(True, index=df_pairs.index)),
    ("Same-industry pairs", df_pairs["same_industry"] == 1),
    ("Diff-industry pairs", (df_pairs["same_industry"] == 0) & df_pairs["has_industry"]),
]:
    sub = df_pairs[mask]
    linked = sub[sub["linked"] == 1]["overlap_score"]
    control = sub[sub["linked"] == 0]["overlap_score"]
    print(f"\n  {subset_name}:")
    print(f"    Linked pairs:  N={len(linked):>6,}  mean={linked.mean():.4f}  median={linked.median():.4f}")
    print(f"    Control pairs: N={len(control):>6,}  mean={control.mean():.4f}  median={control.median():.4f}")
    if len(linked) > 0 and len(control) > 0:
        diff = linked.mean() - control.mean()
        print(f"    Difference:    {diff:+.4f}")

# Regression: overlap_score ~ linked + same_industry + linked*same_industry
df_pairs["linked_x_same"] = df_pairs["linked"] * df_pairs["same_industry"]

for subset_name, mask in [
    ("Overall", pd.Series(True, index=df_pairs.index)),
    ("Same-industry", df_pairs["same_industry"] == 1),
    ("Diff-industry", (df_pairs["same_industry"] == 0) & df_pairs["has_industry"]),
]:
    sub = df_pairs[mask].reset_index(drop=True)
    if len(sub) < 20:
        print(f"\n  [{subset_name}] Too few observations ({len(sub)}). Skipping.")
        continue

    y = sub["overlap_score"]
    if subset_name == "Overall":
        X = sub[["linked", "same_industry", "linked_x_same"]]
    else:
        X = sub[["linked"]]

    # Use c1 as the cluster variable
    cluster = sub["c1"]

    run_regression(y, X, cluster_var=cluster,
                   label=f"Test 1: Event Overlap — {subset_name}")


# ═══════════════════════════════════════════════════════════════════
# TEST 2: VC INDUSTRY CONCENTRATION (HHI) VS FUND PERFORMANCE
# ═══════════════════════════════════════════════════════════════════
print("\n")
print("=" * 90)
print("TEST 2: VC INDUSTRY CONCENTRATION (HHI) vs FUND PERFORMANCE")
print("Do specialized (high-HHI) VCs have better performance?")
print("=" * 90)

# Compute HHI for each VC firm based on SIC2 distribution
vc_hhi = {}
for vc_id, sics in vc_industries.items():
    if len(sics) < 2:
        continue
    total = len(sics)
    from collections import Counter
    counts = Counter(sics)
    hhi = sum((c / total) ** 2 for c in counts.values())
    vc_hhi[vc_id] = {
        "hhi": hhi,
        "n_companies": total,
        "n_industries": len(counts),
    }

df_hhi = pd.DataFrame.from_dict(vc_hhi, orient="index")
df_hhi.index.name = "vc_firm_companyid"
df_hhi = df_hhi.reset_index()

print(f"\n  VCs with HHI computed: {len(df_hhi):,}")
print(f"  HHI distribution:")
print(f"    Mean:   {df_hhi['hhi'].mean():.3f}")
print(f"    Median: {df_hhi['hhi'].median():.3f}")
print(f"    P25:    {df_hhi['hhi'].quantile(0.25):.3f}")
print(f"    P75:    {df_hhi['hhi'].quantile(0.75):.3f}")
print(f"    Min:    {df_hhi['hhi'].min():.3f}")
print(f"    Max:    {df_hhi['hhi'].max():.3f}")

# Merge to fund performance via CIQ->Preqin crosswalk
df_hhi["preqin_firm_id"] = df_hhi["vc_firm_companyid"].map(ciq_to_firm)
df_hhi_matched = df_hhi.dropna(subset=["preqin_firm_id"]).copy()
df_hhi_matched["preqin_firm_id"] = df_hhi_matched["preqin_firm_id"].astype(int)
print(f"  VCs matched to Preqin: {len(df_hhi_matched):,}")

# Merge to fund-level performance
perf_hhi = latest_perf.merge(
    df_hhi_matched[["preqin_firm_id", "vc_firm_companyid", "hhi", "n_companies", "n_industries"]],
    left_on="firm_id", right_on="preqin_firm_id", how="inner"
)
perf_hhi["high_hhi"] = (perf_hhi["hhi"] > perf_hhi["hhi"].median()).astype(int)

print(f"  Funds with HHI + performance: {len(perf_hhi):,}")
print(f"  Funds with valid TVPI:        {perf_hhi['multiple_num'].notna().sum():,}")
print(f"  Funds with valid IRR:         {perf_hhi['net_irr_pcent_num'].notna().sum():,}")

# Summary: performance by HHI tercile
perf_hhi["hhi_tercile"] = pd.qcut(perf_hhi["hhi"], 3, labels=["Low", "Mid", "High"], duplicates="drop")
print(f"\n  --- Performance by HHI Tercile ---")
for t in ["Low", "Mid", "High"]:
    sub = perf_hhi[perf_hhi["hhi_tercile"] == t]
    tvpi = sub["multiple_num"].dropna()
    irr  = sub["net_irr_pcent_num"].dropna()
    print(f"  {t:>5}: N={len(sub):>4}  TVPI mean={tvpi.mean():.2f} med={tvpi.median():.2f}"
          f"  IRR mean={irr.mean():.1f}% med={irr.median():.1f}%")

# Assign industry subsample based on whether the VC is same-industry concentrated
# We define "same-industry" VC as one where the modal SIC2 accounts for >50% of seats
vc_modal_share = {}
for vc_id, sics in vc_industries.items():
    if len(sics) < 2:
        continue
    counts = Counter(sics)
    modal_share = max(counts.values()) / len(sics)
    vc_modal_share[vc_id] = modal_share

df_hhi["modal_share"] = df_hhi["vc_firm_companyid"].map(vc_modal_share)
df_hhi["concentrated_single"] = (df_hhi["modal_share"] > 0.5).astype(int)

# Merge subsample flag
perf_hhi = perf_hhi.merge(
    df_hhi[["vc_firm_companyid", "modal_share", "concentrated_single"]],
    on="vc_firm_companyid", how="left"
)

# Regressions: TVPI ~ HHI, log_size, vintage controls
for dv_name, dv_col in [("TVPI", "multiple_num"), ("Net IRR (%)", "net_irr_pcent_num")]:
    for subset_name, mask in [
        ("Overall", pd.Series(True, index=perf_hhi.index)),
        ("Same-ind concentrated (modal >50%)", perf_hhi["concentrated_single"] == 1),
        ("Diversified (modal <=50%)", perf_hhi["concentrated_single"] == 0),
    ]:
        sub = perf_hhi[mask].reset_index(drop=True)
        y = sub[dv_col]
        X = sub[["hhi", "log_size"]].copy()
        X = X.apply(pd.to_numeric, errors="coerce")
        cluster = sub["firm_id"]

        run_regression(
            y, X, cluster_var=cluster,
            label=f"Test 2: {dv_name} ~ HHI — {subset_name}",
            vintage_fe=True, vintage_col="vintage_num", df=sub
        )


# ═══════════════════════════════════════════════════════════════════
# TEST 3: NETWORK CENTRALITY vs FUND PERFORMANCE
# ═══════════════════════════════════════════════════════════════════
print("\n")
print("=" * 90)
print("TEST 3: NETWORK CENTRALITY vs FUND PERFORMANCE")
print("Degree, Breadth, Depth measures")
print("=" * 90)

# Compute centrality for each VC firm
centrality = []
for vc_id, comp_set in vc_to_companies.items():
    n_seats = len(comp_set)
    sics = [companyid_to_sic2[c] for c in comp_set if c in companyid_to_sic2]
    n_unique_ind = len(set(sics)) if sics else 0

    # Depth: max observers at any single company for this VC
    vc_rows = network[network["vc_firm_companyid"] == vc_id]
    obs_per_co = vc_rows.groupby("observed_companyid")["observer_personid"].nunique()
    max_depth = obs_per_co.max() if len(obs_per_co) > 0 else 0

    centrality.append({
        "vc_firm_companyid": vc_id,
        "degree": n_seats,
        "breadth": n_unique_ind,
        "depth": max_depth,
    })

df_cent = pd.DataFrame(centrality)
print(f"\n  VCs with centrality: {len(df_cent):,}")
print(f"\n  --- Centrality Summary Statistics ---")
for col in ["degree", "breadth", "depth"]:
    vals = df_cent[col]
    print(f"  {col:>10}: mean={vals.mean():.1f}  med={vals.median():.0f}"
          f"  P25={vals.quantile(0.25):.0f}  P75={vals.quantile(0.75):.0f}  max={vals.max():.0f}")

# Merge to fund performance
df_cent["preqin_firm_id"] = df_cent["vc_firm_companyid"].map(ciq_to_firm)
df_cent_matched = df_cent.dropna(subset=["preqin_firm_id"])
df_cent_matched["preqin_firm_id"] = df_cent_matched["preqin_firm_id"].astype(int)

perf_cent = latest_perf.merge(
    df_cent_matched[["preqin_firm_id", "degree", "breadth", "depth"]],
    left_on="firm_id", right_on="preqin_firm_id", how="inner"
)
print(f"  Funds with centrality + performance: {len(perf_cent):,}")

# Log-transform degree
perf_cent["log_degree"] = np.log1p(perf_cent["degree"])

# Subsample: high-degree vs low-degree
perf_cent["high_degree"] = (perf_cent["degree"] > perf_cent["degree"].median()).astype(int)

# Also add same-ind vs diff-ind subsample using breadth
# "Same-ind" = breadth == 1 or very narrow
perf_cent["narrow_breadth"] = (perf_cent["breadth"] <= 2).astype(int)

# Regressions for each DV x centrality measure
for dv_name, dv_col in [("TVPI", "multiple_num"), ("Net IRR (%)", "net_irr_pcent_num")]:
    for cent_name, cent_cols in [
        ("Degree (log)", ["log_degree"]),
        ("Breadth", ["breadth"]),
        ("Depth", ["depth"]),
        ("All three", ["log_degree", "breadth", "depth"]),
    ]:
        for subset_name, mask in [
            ("Overall", pd.Series(True, index=perf_cent.index)),
            ("Narrow breadth (<=2 ind)", perf_cent["narrow_breadth"] == 1),
            ("Broad breadth (>2 ind)", perf_cent["narrow_breadth"] == 0),
        ]:
            sub = perf_cent[mask].reset_index(drop=True)
            if len(sub) < 20:
                continue
            y = sub[dv_col]
            X = sub[cent_cols + ["log_size"]].copy()
            X = X.apply(pd.to_numeric, errors="coerce")
            cluster = sub["firm_id"]

            run_regression(
                y, X, cluster_var=cluster,
                label=f"Test 3: {dv_name} ~ {cent_name} — {subset_name}",
                vintage_fe=True, vintage_col="vintage_num", df=sub
            )


# ═══════════════════════════════════════════════════════════════════
# TEST 4: EVENT CONTAGION WITHIN VC PORTFOLIOS
# ═══════════════════════════════════════════════════════════════════
print("\n")
print("=" * 90)
print("TEST 4: INFORMATION FLOW — EVENT CONTAGION WITHIN VC PORTFOLIOS")
print("When Company A has a material event, is Company B (same VC portfolio)")
print("more likely to have an event within 90 days?")
print("=" * 90)

# Build event dates per company as sorted list
comp_event_dates = events.groupby("companyid")["announcedate"].apply(
    lambda x: sorted(x.unique())
).to_dict()

# For each VC portfolio, create all within-portfolio pairs
print("\n  Building within-portfolio company pairs...")
portfolio_pairs = set()
vc_for_pair = {}  # map pair -> list of vc_ids
for vc_id, comp_set in vc_to_companies.items():
    sorted_comps = sorted(comp_set)
    for i in range(len(sorted_comps)):
        for j in range(i+1, len(sorted_comps)):
            pair = (sorted_comps[i], sorted_comps[j])
            portfolio_pairs.add(pair)
            if pair not in vc_for_pair:
                vc_for_pair[pair] = []
            vc_for_pair[pair].append(vc_id)

print(f"  Within-portfolio pairs: {len(portfolio_pairs):,}")

def compute_contagion_score(pair, comp_event_dates, window_days=90):
    """
    For a pair (c1, c2): for each event at c1, check if c2 has an event
    within `window_days` days after. Returns fraction, hits, checks.
    Also does the reverse (c2->c1) and averages.
    """
    c1, c2 = pair
    dates1 = comp_event_dates.get(c1, [])
    dates2 = comp_event_dates.get(c2, [])
    if not dates1 or not dates2:
        return np.nan, 0, 0

    dates2_arr = np.array(dates2, dtype="datetime64[ns]")

    hits = 0
    checks = 0
    for d1 in dates1:
        checks += 1
        diffs = (dates2_arr - np.datetime64(d1)) / np.timedelta64(1, "D")
        # Check if any date in dates2 is within (0, window_days]
        if np.any((diffs > 0) & (diffs <= window_days)):
            hits += 1

    # Reverse direction
    dates1_arr = np.array(dates1, dtype="datetime64[ns]")
    for d2 in dates2:
        checks += 1
        diffs = (dates1_arr - np.datetime64(d2)) / np.timedelta64(1, "D")
        if np.any((diffs > 0) & (diffs <= window_days)):
            hits += 1

    return hits / checks if checks > 0 else np.nan, hits, checks


# Compute for portfolio pairs (sample if too many)
print("  Computing contagion scores for portfolio pairs...")
portfolio_list = list(portfolio_pairs)
if len(portfolio_list) > 30000:
    np.random.seed(123)
    sample_idx = np.random.choice(len(portfolio_list), size=30000, replace=False)
    portfolio_sample = [portfolio_list[i] for i in sample_idx]
else:
    portfolio_sample = portfolio_list

contagion_results = []
for i, pair in enumerate(portfolio_sample):
    if i % 5000 == 0 and i > 0:
        print(f"    Processed {i:,}/{len(portfolio_sample):,} pairs...")
    score, hits, checks = compute_contagion_score(pair, comp_event_dates, window_days=90)
    if not np.isnan(score) and checks >= 2:
        c1_sic2 = companyid_to_sic2.get(pair[0])
        c2_sic2 = companyid_to_sic2.get(pair[1])
        same_ind = 1 if (c1_sic2 and c2_sic2 and c1_sic2 == c2_sic2) else 0
        contagion_results.append({
            "c1": pair[0], "c2": pair[1],
            "contagion_score": score, "hits": hits, "checks": checks,
            "portfolio_linked": 1,
            "same_industry": same_ind,
            "sic2_c1": c1_sic2, "sic2_c2": c2_sic2,
        })

print(f"  Portfolio pairs with contagion scores: {len(contagion_results):,}")

# Build matched random control pairs
print("  Building random control pairs for contagion test...")
control_contagion = []
n_control_target = min(len(contagion_results) * 3, 30000)
attempts = 0
max_attempts = n_control_target * 10
while len(control_contagion) < n_control_target and attempts < max_attempts:
    attempts += 1
    idx = np.random.choice(len(all_companies_with_events), size=2, replace=False)
    c1, c2 = all_companies_with_events[idx[0]], all_companies_with_events[idx[1]]
    if (c1, c2) in portfolio_pairs or (c2, c1) in portfolio_pairs:
        continue
    score, hits, checks = compute_contagion_score((c1, c2), comp_event_dates, window_days=90)
    if not np.isnan(score) and checks >= 2:
        c1_sic2 = companyid_to_sic2.get(c1)
        c2_sic2 = companyid_to_sic2.get(c2)
        same_ind = 1 if (c1_sic2 and c2_sic2 and c1_sic2 == c2_sic2) else 0
        control_contagion.append({
            "c1": c1, "c2": c2,
            "contagion_score": score, "hits": hits, "checks": checks,
            "portfolio_linked": 0,
            "same_industry": same_ind,
            "sic2_c1": c1_sic2, "sic2_c2": c2_sic2,
        })

print(f"  Control pairs for contagion: {len(control_contagion):,}")

# Combine
df_contagion = pd.DataFrame(contagion_results + control_contagion)
df_contagion["has_industry"] = df_contagion["sic2_c1"].notna() & df_contagion["sic2_c2"].notna()
df_contagion["linked_x_same"] = df_contagion["portfolio_linked"] * df_contagion["same_industry"]

# Summary
print(f"\n  --- 90-Day Contagion Summary ---")
for subset_name, mask in [
    ("All pairs", pd.Series(True, index=df_contagion.index)),
    ("Same-industry pairs", df_contagion["same_industry"] == 1),
    ("Diff-industry pairs", (df_contagion["same_industry"] == 0) & df_contagion["has_industry"]),
]:
    sub = df_contagion[mask]
    linked = sub[sub["portfolio_linked"] == 1]["contagion_score"]
    control = sub[sub["portfolio_linked"] == 0]["contagion_score"]
    print(f"\n  {subset_name}:")
    print(f"    Portfolio pairs: N={len(linked):>6,}  mean={linked.mean():.4f}  median={linked.median():.4f}")
    print(f"    Control pairs:   N={len(control):>6,}  mean={control.mean():.4f}  median={control.median():.4f}")
    if len(linked) > 0 and len(control) > 0:
        diff = linked.mean() - control.mean()
        print(f"    Difference:      {diff:+.4f}")

# Regressions
for subset_name, mask in [
    ("Overall", pd.Series(True, index=df_contagion.index)),
    ("Same-industry", df_contagion["same_industry"] == 1),
    ("Diff-industry", (df_contagion["same_industry"] == 0) & df_contagion["has_industry"]),
]:
    sub = df_contagion[mask].reset_index(drop=True)
    if len(sub) < 20:
        print(f"\n  [{subset_name}] Too few observations ({len(sub)}). Skipping.")
        continue

    y = sub["contagion_score"]
    if subset_name == "Overall":
        X = sub[["portfolio_linked", "same_industry", "linked_x_same"]]
    else:
        X = sub[["portfolio_linked"]]

    cluster = sub["c1"]

    run_regression(y, X, cluster_var=cluster,
                   label=f"Test 4: 90-Day Contagion — {subset_name}")


# ═══════════════════════════════════════════════════════════════════
# GRAND SUMMARY
# ═══════════════════════════════════════════════════════════════════
print("\n")
print("=" * 90)
print("GRAND SUMMARY")
print("=" * 90)

print("""
Test 1 (Observer Overlap): Companies sharing a board observer are tested for
        correlated event timing (same quarter or t+1) vs random control pairs.

Test 2 (VC Industry Concentration): HHI of 2-digit SIC across observed companies.
        Split into concentrated vs dispersed VCs. Tested against TVPI and IRR.

Test 3 (Network Centrality): Degree (# seats), breadth (# industries),
        depth (max observers per company). Each tested against fund performance.

Test 4 (Event Contagion): Within a VC portfolio, do material events at one
        company predict events at another within 90 days? Compared to random pairs.

All regressions use: HC1, Firm-clustered SE, and Vintage FE where applicable.
All tests report: Overall, Same-industry, Different-industry subsamples.
""")
print("=" * 90)
print("DONE")
print("=" * 90)
