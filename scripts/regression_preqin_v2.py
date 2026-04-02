"""
VC Fund Performance Test v2 — Benchmark-adjusted + Cashflow-based approaches.

Improvements over v1:
  1. Benchmark-adjusted TVPI (fund multiple minus vintage-year median)
  2. DPI changes (realized distributions, less noisy than total TVPI)
  3. Cashflow-based: actual capital calls and distributions per quarter
  4. RVPI changes (unrealized NAV, the "information-sensitive" component)

Following Bradley, Jame & Williams (JF 2022) logic:
  If observers provide information advantage, we should see VCs
  deploying capital (calls) or receiving exits (distributions) in
  quarters where events happen at observed companies.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd
import numpy as np
import os
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
preqin_dir = os.path.join(data_dir, "Preqin")

print("=" * 80)
print("VC FUND PERFORMANCE TEST v2")
print("Benchmark-Adjusted + Cashflow-Based")
print("=" * 80)

# =====================================================================
# STEP 1: Load crosswalk (high + medium only)
# =====================================================================
print("\n--- Step 1: Load crosswalk ---")
xwalk = pd.read_csv(os.path.join(preqin_dir, "vc_preqin_crosswalk_validated.csv"))
xwalk = xwalk[xwalk["quality"].isin(["high", "medium"])]
print(f"  Matched VCs (high+medium): {len(xwalk):,}")

firm_to_ciq = xwalk.drop_duplicates("preqin_firm_id").set_index("preqin_firm_id")["ciq_vc_companyid"].to_dict()
matched_firm_ids = set(xwalk["preqin_firm_id"].dropna().astype(int))

# =====================================================================
# STEP 2: Load fund performance + benchmarks
# =====================================================================
print("\n--- Step 2: Load performance + benchmarks ---")

funds = pd.read_csv(os.path.join(preqin_dir, "fund_details_full.csv"))
funds = funds[funds["firm_id"].isin(matched_firm_ids)]
vc_funds = funds[funds["fund_type"].str.contains("Venture|Seed|Early", case=False, na=False)].copy()
vc_fund_ids = set(vc_funds["fund_id"].dropna().astype(int))
print(f"  VC funds at matched firms: {len(vc_funds):,}")

# Performance
perf = pd.read_csv(os.path.join(preqin_dir, "fund_performance_full.csv"), low_memory=False)
perf = perf[perf["fund_id"].isin(vc_fund_ids)].copy()
perf["date_reported"] = pd.to_datetime(perf["date_reported"], errors="coerce")
perf = perf.dropna(subset=["date_reported"])
perf = perf.sort_values(["fund_id", "date_reported"])

# Parse numeric columns
for col in ["multiple", "net_irr_pcent", "called_pcent", "distr_dpi_pcent", "value_rvpi_pcent"]:
    perf[f"{col}_num"] = pd.to_numeric(perf[col], errors="coerce")

# Merge fund details
perf = perf.merge(vc_funds[["fund_id", "firm_id", "fund_name", "firm_name",
                             "vintage", "fund_type", "final_size_usd", "industry"]],
                   on="fund_id", how="left", suffixes=("", "_fund"))

perf["ciq_vc_companyid"] = perf["firm_id"].map(firm_to_ciq).astype(str)
perf["quarter"] = perf["date_reported"].dt.to_period("Q")
perf["year"] = perf["date_reported"].dt.year

# Compute quarter-over-quarter changes
for col in ["multiple_num", "net_irr_pcent_num", "distr_dpi_pcent_num", "value_rvpi_pcent_num", "called_pcent_num"]:
    perf[f"d_{col}"] = perf.groupby("fund_id")[col].diff()

print(f"  Performance records: {len(perf):,}")
print(f"  Funds: {perf['fund_id'].nunique():,}")

# Benchmarks — vintage-year medians for VC funds
bench = pd.read_csv(os.path.join(preqin_dir, "benchmarks.csv"))
vc_bench = bench[bench["benchmark_fundtype_name"].isin(["Venture", "Early Stage"])].copy()
vc_bench["qdate"] = pd.to_datetime(vc_bench["qdate"], errors="coerce")

# For each (vintage, quarter), get the median multiple and IRR
bench_medians = vc_bench.groupby(["fund_vintage", "qdate"]).agg(
    bench_multiple_med=("multiple_med", "first"),
    bench_irr_med=("irr_med", "first"),
    bench_dpi_med=("distr_med", "first"),
).reset_index()
bench_medians["fund_vintage"] = bench_medians["fund_vintage"].astype(float)

# Merge benchmark to fund performance
perf["vintage_float"] = perf["vintage"].astype(float)
perf = perf.merge(bench_medians,
                   left_on=["vintage_float", "date_reported"],
                   right_on=["fund_vintage", "qdate"],
                   how="left")

# Benchmark-adjusted measures
perf["adj_multiple"] = perf["multiple_num"] - perf["bench_multiple_med"]
perf["adj_irr"] = perf["net_irr_pcent_num"] - perf["bench_irr_med"]
perf["d_adj_multiple"] = perf.groupby("fund_id")["adj_multiple"].diff()

has_bench = perf["bench_multiple_med"].notna().sum()
print(f"  Records with benchmark match: {has_bench:,} ({has_bench/len(perf)*100:.1f}%)")

# =====================================================================
# STEP 3: Load cashflows
# =====================================================================
print("\n--- Step 3: Load cashflows ---")
cf = pd.read_csv(os.path.join(preqin_dir, "cashflows_full.csv"))
cf = cf[cf["fund_id"].isin(vc_fund_ids)].copy()
cf["transaction_date"] = pd.to_datetime(cf["transaction_date"], errors="coerce")
cf = cf.dropna(subset=["transaction_date"])
cf["quarter"] = cf["transaction_date"].dt.to_period("Q")
cf["year"] = cf["transaction_date"].dt.year

print(f"  Cashflow records at matched VC funds: {len(cf):,}")
print(f"  Funds with cashflows: {cf['fund_id'].nunique():,}")

# Aggregate to fund-quarter level
cf_quarterly = cf.groupby(["fund_id", "quarter"]).agg(
    total_calls=("transaction_amount", lambda x: x[cf.loc[x.index, "transaction_type"] == "Capital Call"].sum()),
    total_distributions=("transaction_amount", lambda x: x[cf.loc[x.index, "transaction_type"] == "Distribution"].sum()),
    n_transactions=("transaction_amount", "count"),
).reset_index()

# Simpler approach: separate calls and distributions
calls = cf[cf["transaction_type"] == "Capital Call"].groupby(["fund_id", "quarter"])["transaction_amount"].sum().reset_index(name="q_calls")
dists = cf[cf["transaction_type"] == "Distribution"].groupby(["fund_id", "quarter"])["transaction_amount"].sum().reset_index(name="q_distributions")

cf_q = calls.merge(dists, on=["fund_id", "quarter"], how="outer")
cf_q["q_calls"] = cf_q["q_calls"].fillna(0)
cf_q["q_distributions"] = cf_q["q_distributions"].fillna(0)
cf_q["q_net"] = cf_q["q_distributions"] - cf_q["q_calls"]
cf_q["has_call"] = (cf_q["q_calls"] > 0).astype(int)
cf_q["has_distribution"] = (cf_q["q_distributions"] > 0).astype(int)

# Merge fund info
cf_q = cf_q.merge(vc_funds[["fund_id", "firm_id", "final_size_usd"]], on="fund_id", how="left")
cf_q["ciq_vc_companyid"] = cf_q["firm_id"].map(firm_to_ciq).astype(str)
cf_q["year"] = cf_q["quarter"].dt.year

# Normalize by fund size
cf_q["calls_pct"] = cf_q["q_calls"] / cf_q["final_size_usd"].clip(lower=1) * 100
cf_q["dist_pct"] = cf_q["q_distributions"] / cf_q["final_size_usd"].clip(lower=1) * 100

print(f"  Fund-quarters with cashflows: {len(cf_q):,}")
print(f"  With calls: {cf_q['has_call'].sum():,}")
print(f"  With distributions: {cf_q['has_distribution'].sum():,}")

# =====================================================================
# STEP 4: Event counts per VC-quarter
# =====================================================================
print("\n--- Step 4: Event counts ---")

tb = pd.read_csv(os.path.join(data_dir, "table_b_observer_network.csv"))
tb["vc_firm_companyid"] = tb["vc_firm_companyid"].astype(str).str.replace(".0", "", regex=False)
tb["observed_companyid"] = tb["observed_companyid"].astype(str).str.replace(".0", "", regex=False)

vc_to_obs = {}
for _, r in tb.iterrows():
    vc = r["vc_firm_companyid"]
    if vc not in vc_to_obs:
        vc_to_obs[vc] = set()
    vc_to_obs[vc].add(r["observed_companyid"])

events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])

co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_priv = set(co[(co["country"] == "United States") & (co["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
events = events[events["companyid"].isin(us_priv)]
noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)"]
events = events[~events["eventtype"].isin(noise)]
events["quarter"] = events["announcedate"].dt.to_period("Q")

def is_material(et):
    et = str(et)
    return "M&A" in et or "Bankruptcy" in et or "Executive/Board" in et or "Restructuring" in et or "Downsizing" in et

events["material"] = events["eventtype"].apply(is_material)
events["is_ma"] = events["eventtype"].str.contains("M&A", na=False)
events["is_exit"] = events["eventtype"].str.contains("M&A|Bankruptcy|IPO|Acquisition", na=False)

matched_ciq_vcs = set(xwalk["ciq_vc_companyid"].astype(str))

vc_q_events = []
for vc_cid in matched_ciq_vcs:
    obs_cos = vc_to_obs.get(vc_cid, set())
    if not obs_cos:
        continue
    vc_evt = events[events["companyid"].isin(obs_cos)]
    if len(vc_evt) == 0:
        continue
    qc = vc_evt.groupby("quarter").agg(
        n_events=("companyid", "count"),
        n_material=("material", "sum"),
        n_ma=("is_ma", "sum"),
        n_exit=("is_exit", "sum"),
    ).reset_index()
    qc["ciq_vc_companyid"] = vc_cid
    vc_q_events.append(qc)

evt_counts = pd.concat(vc_q_events, ignore_index=True)
print(f"  VC-quarter event records: {len(evt_counts):,}")

# =====================================================================
# STEP 5: Build panels and run regressions
# =====================================================================
print("\n\n" + "=" * 80)
print("REGRESSION RESULTS")
print("=" * 80)

def run_panel_regression(panel, dv_col, treat_col, dv_name, treat_name, cluster_col="fund_id"):
    """Run a single regression and print results."""
    panel = panel.copy()
    panel["post_2020"] = (panel["year"] >= 2020).astype(int)
    panel["treat_x_post"] = panel[treat_col] * panel["post_2020"]

    y = panel[dv_col].dropna()
    if len(y) < 100:
        print(f"    Too few obs ({len(y)})")
        return

    # Winsorize
    lo, hi = y.quantile([0.01, 0.99])
    y = y.clip(lo, hi)

    X_cols = [treat_col, "post_2020", "treat_x_post"]
    X = panel.loc[y.index, X_cols].copy()

    # Year dummies
    yr = pd.get_dummies(panel.loc[y.index, "year"], prefix="yr", drop_first=True).astype(float)
    X = pd.concat([X, yr], axis=1)
    X = sm.add_constant(X)

    for cov_label, cov_type, cov_kwds in [
        ("HC1", "HC1", {}),
        ("Fund-cl", "cluster", {"groups": panel.loc[y.index, cluster_col]}),
    ]:
        try:
            m = sm.OLS(y, X).fit(cov_type=cov_type, cov_kwds=cov_kwds if cov_kwds else {})
            bt = m.params.get(treat_col, np.nan)
            pt = m.pvalues.get(treat_col, np.nan)
            bi = m.params.get("treat_x_post", np.nan)
            pi = m.pvalues.get("treat_x_post", np.nan)
            st = "***" if pt < 0.01 else "**" if pt < 0.05 else "*" if pt < 0.10 else ""
            si = "***" if pi < 0.01 else "**" if pi < 0.05 else "*" if pi < 0.10 else ""
            n_cl = panel.loc[y.index, cluster_col].nunique() if cov_type == "cluster" else ""
            print(f"    {cov_label:<8} N={len(y):>6,}  b({treat_col})={bt:>8.4f}{st:<3} p={pt:.3f}  b(treat x post20)={bi:>8.4f}{si:<3} p={pi:.3f}")
        except Exception as e:
            print(f"    {cov_label} Error: {str(e)[:50]}")


# === PANEL A: Performance-based (TVPI, DPI, RVPI changes) ===
print(f"\n{'─' * 80}")
print(f"  PANEL A: Fund Performance Measures")
print(f"{'─' * 80}")

perf_panel = perf.merge(evt_counts, on=["ciq_vc_companyid", "quarter"], how="left")
perf_panel["n_events"] = perf_panel["n_events"].fillna(0).astype(int)
perf_panel["n_material"] = perf_panel["n_material"].fillna(0).astype(int)
perf_panel["n_ma"] = perf_panel["n_ma"].fillna(0).astype(int)
perf_panel["has_event"] = (perf_panel["n_events"] > 0).astype(int)
perf_panel["has_material"] = (perf_panel["n_material"] > 0).astype(int)
perf_panel["has_ma"] = (perf_panel["n_ma"] > 0).astype(int)

perf_dvs = [
    ("d_multiple_num", "Delta TVPI (raw)"),
    ("d_adj_multiple", "Delta TVPI (benchmark-adjusted)"),
    ("d_distr_dpi_pcent_num", "Delta DPI (realized distributions)"),
    ("d_value_rvpi_pcent_num", "Delta RVPI (unrealized NAV)"),
    ("d_called_pcent_num", "Delta Called %"),
]

for dv_col, dv_name in perf_dvs:
    valid = perf_panel[dv_col].notna().sum()
    if valid < 100:
        print(f"\n  {dv_name}: only {valid} obs, skipping")
        continue
    print(f"\n  DV: {dv_name} (N={valid:,})")
    for treat_col, treat_name in [("has_event", "Any Event"), ("has_material", "Material Event"), ("has_ma", "M&A Event")]:
        print(f"    Treatment: {treat_name}")
        run_panel_regression(perf_panel, dv_col, treat_col, dv_name, treat_name)

# === PANEL B: Cashflow-based ===
print(f"\n\n{'─' * 80}")
print(f"  PANEL B: Cashflow Measures")
print(f"{'─' * 80}")

cf_panel = cf_q.merge(evt_counts, on=["ciq_vc_companyid", "quarter"], how="left")
cf_panel["n_events"] = cf_panel["n_events"].fillna(0).astype(int)
cf_panel["n_material"] = cf_panel["n_material"].fillna(0).astype(int)
cf_panel["n_ma"] = cf_panel["n_ma"].fillna(0).astype(int)
cf_panel["has_event"] = (cf_panel["n_events"] > 0).astype(int)
cf_panel["has_material"] = (cf_panel["n_material"] > 0).astype(int)
cf_panel["has_ma"] = (cf_panel["n_ma"] > 0).astype(int)

cf_dvs = [
    ("calls_pct", "Capital Calls (% of fund size)"),
    ("dist_pct", "Distributions (% of fund size)"),
    ("has_call", "Has Capital Call (0/1)"),
    ("has_distribution", "Has Distribution (0/1)"),
]

for dv_col, dv_name in cf_dvs:
    valid = cf_panel[dv_col].notna().sum()
    if valid < 100:
        print(f"\n  {dv_name}: only {valid} obs, skipping")
        continue
    print(f"\n  DV: {dv_name} (N={valid:,})")
    for treat_col, treat_name in [("has_event", "Any Event"), ("has_material", "Material Event"), ("has_ma", "M&A Event")]:
        print(f"    Treatment: {treat_name}")
        run_panel_regression(cf_panel, dv_col, treat_col, dv_name, treat_name)

# === PANEL C: Subsample means ===
print(f"\n\n{'─' * 80}")
print(f"  PANEL C: Subsample Means")
print(f"{'─' * 80}")

print(f"\n  Performance panel:")
print(f"  {'DV':<35} {'Event Q':>10} {'N':>6}  {'No-Event':>10} {'N':>6}  {'Diff':>10}")
print(f"  {'-' * 80}")
for dv_col, dv_name in perf_dvs:
    evt_mean = perf_panel[perf_panel["has_event"] == 1][dv_col].mean()
    noevt_mean = perf_panel[perf_panel["has_event"] == 0][dv_col].mean()
    evt_n = perf_panel[perf_panel["has_event"] == 1][dv_col].notna().sum()
    noevt_n = perf_panel[perf_panel["has_event"] == 0][dv_col].notna().sum()
    diff = evt_mean - noevt_mean if pd.notna(evt_mean) and pd.notna(noevt_mean) else np.nan
    print(f"  {dv_name:<35} {evt_mean:>10.4f} {evt_n:>5}  {noevt_mean:>10.4f} {noevt_n:>5}  {diff:>10.4f}")

print(f"\n  Cashflow panel:")
print(f"  {'DV':<35} {'Event Q':>10} {'N':>6}  {'No-Event':>10} {'N':>6}  {'Diff':>10}")
print(f"  {'-' * 80}")
for dv_col, dv_name in cf_dvs:
    evt_mean = cf_panel[cf_panel["has_event"] == 1][dv_col].mean()
    noevt_mean = cf_panel[cf_panel["has_event"] == 0][dv_col].mean()
    evt_n = cf_panel[cf_panel["has_event"] == 1][dv_col].notna().sum()
    noevt_n = cf_panel[cf_panel["has_event"] == 0][dv_col].notna().sum()
    diff = evt_mean - noevt_mean if pd.notna(evt_mean) and pd.notna(noevt_mean) else np.nan
    print(f"  {dv_name:<35} {evt_mean:>10.4f} {evt_n:>5}  {noevt_mean:>10.4f} {noevt_n:>5}  {diff:>10.4f}")

# Pre vs post 2020
print(f"\n  Pre-2020 vs Post-2020 (event quarters only):")
for dv_col, dv_name in perf_dvs[:3]:
    pre = perf_panel[(perf_panel["has_event"] == 1) & (perf_panel["year"] < 2020)][dv_col]
    post = perf_panel[(perf_panel["has_event"] == 1) & (perf_panel["year"] >= 2020)][dv_col]
    if len(pre) > 0 and len(post) > 0:
        print(f"  {dv_name:<35} pre={pre.mean():.4f} (N={len(pre.dropna()):,})  post={post.mean():.4f} (N={len(post.dropna()):,})")

print("\n\nDone.")
