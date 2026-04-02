"""
Fund Performance v4 — Bradley et al. (JF 2022) inspired approach.

Key idea from Bradley: look at CHANGES in trading behavior and PROFITABILITY
of those changes around the information event.

For VC funds, this translates to:
  1. Between-quarter changes in profitability measures
     (acceleration in TVPI, IRR, DPI growth around events)
  2. Between-quarter changes in portfolio activity
     (change in capital call rate and distribution rate around events)
  3. Window analysis: event quarter, quarter before, quarter after

The DV is not the level of performance, but the CHANGE IN THE CHANGE:
  d2_multiple = delta_multiple(t) - delta_multiple(t-1)
  This captures whether performance ACCELERATED in event quarters.
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

print("=" * 90)
print("FUND PERFORMANCE v4: Bradley et al. (JF 2022) Approach")
print("=" * 90)

# === Load data (same setup, condensed) ===
xwalk = pd.read_csv(os.path.join(preqin_dir, "vc_preqin_crosswalk_validated.csv"))
xwalk = xwalk[xwalk["quality"].isin(["high", "medium"])]
firm_to_ciq = xwalk.drop_duplicates("preqin_firm_id").set_index("preqin_firm_id")["ciq_vc_companyid"].to_dict()
matched_firm_ids = set(xwalk["preqin_firm_id"].dropna().astype(int))

funds = pd.read_csv(os.path.join(preqin_dir, "fund_details_full.csv"))
funds = funds[funds["firm_id"].isin(matched_firm_ids)]
vc_funds = funds[funds["fund_type"].str.contains("Venture|Seed|Early", case=False, na=False)].copy()
vc_fund_ids = set(vc_funds["fund_id"].dropna().astype(int))

perf = pd.read_csv(os.path.join(preqin_dir, "fund_performance_full.csv"), low_memory=False)
perf = perf[perf["fund_id"].isin(vc_fund_ids)].copy()
perf["date_reported"] = pd.to_datetime(perf["date_reported"], errors="coerce")
perf = perf.dropna(subset=["date_reported"]).sort_values(["fund_id", "date_reported"])

for col in ["multiple", "net_irr_pcent", "called_pcent", "distr_dpi_pcent", "value_rvpi_pcent"]:
    perf[f"{col}_num"] = pd.to_numeric(perf[col], errors="coerce")

perf = perf.merge(vc_funds[["fund_id", "firm_id", "vintage", "final_size_usd"]],
                   on="fund_id", how="left", suffixes=("", "_fund"))
perf["ciq_vc_companyid"] = perf["firm_id"].map(firm_to_ciq).astype(str)
perf["quarter"] = perf["date_reported"].dt.to_period("Q")
perf["year"] = perf["date_reported"].dt.year

# === Compute first differences (delta) and second differences (acceleration) ===
print("\n--- Computing between-quarter changes ---")

for col in ["multiple_num", "net_irr_pcent_num", "distr_dpi_pcent_num",
            "value_rvpi_pcent_num", "called_pcent_num"]:
    # First difference: quarter-over-quarter change
    perf[f"d1_{col}"] = perf.groupby("fund_id")[col].diff()
    # Second difference: acceleration (change in the change)
    perf[f"d2_{col}"] = perf.groupby("fund_id")[f"d1_{col}"].diff()

print(f"  Performance records: {len(perf):,}")
print(f"  With d1_multiple: {perf['d1_multiple_num'].notna().sum():,}")
print(f"  With d2_multiple: {perf['d2_multiple_num'].notna().sum():,}")

# === Cashflows: compute between-quarter changes ===
cf = pd.read_csv(os.path.join(preqin_dir, "cashflows_full.csv"))
cf = cf[cf["fund_id"].isin(vc_fund_ids)].copy()
cf["transaction_date"] = pd.to_datetime(cf["transaction_date"], errors="coerce")
cf = cf.dropna(subset=["transaction_date"])
cf["quarter"] = cf["transaction_date"].dt.to_period("Q")

# Aggregate to fund-quarter
calls_q = cf[cf["transaction_type"] == "Capital Call"].groupby(
    ["fund_id", "quarter"])["transaction_amount"].agg(["sum", "count"]).reset_index()
calls_q.columns = ["fund_id", "quarter", "call_amount", "n_calls"]

dists_q = cf[cf["transaction_type"] == "Distribution"].groupby(
    ["fund_id", "quarter"])["transaction_amount"].agg(["sum", "count"]).reset_index()
dists_q.columns = ["fund_id", "quarter", "dist_amount", "n_dists"]

# Build full fund-quarter panel from cashflows
cf_panel = calls_q.merge(dists_q, on=["fund_id", "quarter"], how="outer")
cf_panel = cf_panel.fillna(0)
cf_panel = cf_panel.sort_values(["fund_id", "quarter"])

# Merge fund info
cf_panel = cf_panel.merge(vc_funds[["fund_id", "firm_id", "final_size_usd"]], on="fund_id", how="left")
cf_panel["ciq_vc_companyid"] = cf_panel["firm_id"].map(firm_to_ciq).astype(str)
cf_panel["year"] = cf_panel["quarter"].dt.year

# Normalize by fund size
cf_panel["call_pct"] = cf_panel["call_amount"] / cf_panel["final_size_usd"].clip(lower=0.1) * 100
cf_panel["dist_pct"] = cf_panel["dist_amount"] / cf_panel["final_size_usd"].clip(lower=0.1) * 100
cf_panel["net_pct"] = cf_panel["dist_pct"] - cf_panel["call_pct"]

# Between-quarter changes in cashflow activity
for col in ["call_pct", "dist_pct", "net_pct", "n_calls", "n_dists"]:
    cf_panel[f"d_{col}"] = cf_panel.groupby("fund_id")[col].diff()

print(f"  Cashflow fund-quarters: {len(cf_panel):,}")
print(f"  With d_call_pct: {cf_panel['d_call_pct'].notna().sum():,}")

# === Events ===
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
co_det = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_priv = set(co_det[(co_det["country"] == "United States") & (co_det["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
events = events[events["companyid"].isin(us_priv)]
noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)"]
events = events[~events["eventtype"].isin(noise)]
events["quarter"] = events["announcedate"].dt.to_period("Q")
events["material"] = events["eventtype"].apply(lambda x: "M&A" in str(x) or "Bankruptcy" in str(x) or "Executive/Board" in str(x))
events["is_ma"] = events["eventtype"].str.contains("M&A", na=False)

# Event counts per VC-quarter (also compute lead/lag quarters)
vc_q_events = []
for vc_cid in set(xwalk["ciq_vc_companyid"].astype(str)):
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
    ).reset_index()
    qc["ciq_vc_companyid"] = vc_cid
    vc_q_events.append(qc)
evt_counts = pd.concat(vc_q_events, ignore_index=True)

# === Merge and build panels ===

# Performance panel
pp = perf.merge(evt_counts, on=["ciq_vc_companyid", "quarter"], how="left")
for c in ["n_events", "n_material", "n_ma"]:
    pp[c] = pp[c].fillna(0).astype(int)
pp["has_event"] = (pp["n_events"] > 0).astype(int)
pp["has_material"] = (pp["n_material"] > 0).astype(int)
pp["has_ma"] = (pp["n_ma"] > 0).astype(int)
pp["post_2020"] = (pp["year"] >= 2020).astype(int)
pp["log_events"] = np.log1p(pp["n_events"])

# Also get LEAD event (next quarter has events)
pp["next_quarter"] = pp["quarter"] + 1
pp_next = evt_counts.rename(columns={"quarter": "next_quarter",
                                      "n_events": "n_events_next",
                                      "n_material": "n_material_next",
                                      "n_ma": "n_ma_next"})
pp = pp.merge(pp_next, on=["ciq_vc_companyid", "next_quarter"], how="left")
pp["has_event_next"] = (pp["n_events_next"].fillna(0) > 0).astype(int)

# LAG event (previous quarter had events)
pp["prev_quarter"] = pp["quarter"] - 1
pp_prev = evt_counts.rename(columns={"quarter": "prev_quarter",
                                      "n_events": "n_events_prev",
                                      "n_material": "n_material_prev",
                                      "n_ma": "n_ma_prev"})
pp = pp.merge(pp_prev, on=["ciq_vc_companyid", "prev_quarter"], how="left")
pp["has_event_prev"] = (pp["n_events_prev"].fillna(0) > 0).astype(int)

# Cashflow panel
cp = cf_panel.merge(evt_counts, on=["ciq_vc_companyid", "quarter"], how="left")
for c in ["n_events", "n_material", "n_ma"]:
    cp[c] = cp[c].fillna(0).astype(int)
cp["has_event"] = (cp["n_events"] > 0).astype(int)
cp["has_material"] = (cp["n_material"] > 0).astype(int)
cp["post_2020"] = (cp["year"] >= 2020).astype(int)

# === Regressions ===
print(f"\n\n{'=' * 90}")
print("RESULTS")
print(f"{'=' * 90}")

year_dum_pp = pd.get_dummies(pp["year"], prefix="yr", drop_first=True).astype(float)
year_dum_cp = pd.get_dummies(cp["year"], prefix="yr", drop_first=True).astype(float)

def run_reg(panel, y_col, x_cols, year_dum, label):
    """Run with HC1, fund-cl, firm-cl, and fund FE."""
    y = panel[y_col].dropna()
    if len(y) < 200:
        print(f"    {label}: too few obs ({len(y)})")
        return

    lo, hi = y.quantile([0.01, 0.99])
    y = y.clip(lo, hi)
    idx = y.index

    X = panel.loc[idx, x_cols].copy()
    X = pd.concat([X, year_dum.loc[idx]], axis=1)
    X = sm.add_constant(X)

    # Fund FE version
    fund_mean = panel.loc[idx].groupby("fund_id")[y_col].transform("mean")
    y_fe = y - fund_mean.loc[idx]
    X_fe = panel.loc[idx, x_cols].copy()
    X_fe = sm.add_constant(X_fe)

    specs = [
        ("HC1", y, X, "HC1", {}),
        ("Firm-cl", y, X, "cluster", {"groups": panel.loc[idx, "firm_id"]}),
        ("Fund FE", y_fe, X_fe, "HC1", {}),
    ]

    for sname, dep, xmat, cov, kwds in specs:
        try:
            m = sm.OLS(dep, xmat).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
            parts = []
            for xc in x_cols:
                b = m.params.get(xc, np.nan)
                p = m.pvalues.get(xc, np.nan)
                s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
                parts.append(f"b({xc[:12]})={b:>8.4f}{s:<3} p={p:.3f}")
            print(f"    {sname:<10} N={len(dep):>6,}  {'  '.join(parts)}")
        except Exception as e:
            print(f"    {sname:<10} Error: {str(e)[:50]}")


# =====================================================================
# TEST 1: Acceleration in profitability (second difference)
# =====================================================================
print(f"\n{'─' * 90}")
print("  TEST 1: Performance ACCELERATION (d2 = change in the change)")
print(f"{'─' * 90}")
print("  Does fund performance growth ACCELERATE in event quarters?")

for dv, dv_name in [("d2_multiple_num", "TVPI acceleration"),
                     ("d2_distr_dpi_pcent_num", "DPI acceleration"),
                     ("d2_value_rvpi_pcent_num", "RVPI acceleration")]:
    print(f"\n  DV: {dv_name}")
    for treat in ["has_event", "has_material"]:
        print(f"    Treatment: {treat}")
        run_reg(pp, dv, [treat], year_dum_pp, dv_name)

# =====================================================================
# TEST 2: Level changes with lead/lag (event window approach)
# =====================================================================
print(f"\n{'─' * 90}")
print("  TEST 2: Event Window (quarter before, during, after)")
print(f"{'─' * 90}")
print("  Do changes happen BEFORE, DURING, or AFTER events?")

for dv, dv_name in [("d1_multiple_num", "Delta TVPI"),
                     ("d1_distr_dpi_pcent_num", "Delta DPI")]:
    print(f"\n  DV: {dv_name}")
    run_reg(pp, dv, ["has_event_prev", "has_event", "has_event_next"], year_dum_pp, dv_name)

# =====================================================================
# TEST 3: Between-quarter changes in cashflow activity
# =====================================================================
print(f"\n{'─' * 90}")
print("  TEST 3: Changes in Portfolio Activity (Bradley-style)")
print(f"{'─' * 90}")
print("  Does the VC change its investment/exit behavior around events?")

for dv, dv_name in [("d_call_pct", "Change in call rate"),
                     ("d_dist_pct", "Change in dist rate"),
                     ("d_net_pct", "Change in net cash"),
                     ("d_n_dists", "Change in # distributions")]:
    print(f"\n  DV: {dv_name}")
    for treat in ["has_event", "has_material"]:
        print(f"    Treatment: {treat}")
        run_reg(cp, dv, [treat], year_dum_cp, dv_name)

# =====================================================================
# TEST 4: Post-2020 interaction on best DVs
# =====================================================================
print(f"\n{'─' * 90}")
print("  TEST 4: NVCA 2020 DiD on best DVs")
print(f"{'─' * 90}")

pp["evt_x_post"] = pp["has_event"] * pp["post_2020"]
pp["mat_x_post"] = pp["has_material"] * pp["post_2020"]
cp["evt_x_post"] = cp["has_event"] * cp["post_2020"]

for dv, dv_name, panel, yd in [
    ("d1_multiple_num", "Delta TVPI", pp, year_dum_pp),
    ("d2_multiple_num", "TVPI acceleration", pp, year_dum_pp),
    ("d1_distr_dpi_pcent_num", "Delta DPI", pp, year_dum_pp),
    ("d_dist_pct", "Change in dist rate", cp, year_dum_cp),
]:
    print(f"\n  DV: {dv_name}")
    run_reg(panel, dv, ["has_event", "post_2020", "evt_x_post"], yd, dv_name)

# =====================================================================
# SUBSAMPLE MEANS: Event window
# =====================================================================
print(f"\n\n{'─' * 90}")
print("  SUBSAMPLE MEANS: Event vs No-Event Quarters")
print(f"{'─' * 90}")

dvs_perf = [("d1_multiple_num", "Delta TVPI"),
            ("d2_multiple_num", "TVPI accel"),
            ("d1_distr_dpi_pcent_num", "Delta DPI"),
            ("d1_value_rvpi_pcent_num", "Delta RVPI"),
            ("d1_called_pcent_num", "Delta Called%")]

print(f"\n  {'DV':<25} {'Event Q':>10} {'N':>6}  {'No-Evt Q':>10} {'N':>6}  {'Diff':>10}")
print(f"  {'-' * 75}")
for dv, name in dvs_perf:
    ev = pp[pp["has_event"] == 1][dv]
    ne = pp[pp["has_event"] == 0][dv]
    diff = ev.mean() - ne.mean() if ev.notna().sum() > 0 and ne.notna().sum() > 0 else np.nan
    print(f"  {name:<25} {ev.mean():>10.4f} {ev.notna().sum():>5}  {ne.mean():>10.4f} {ne.notna().sum():>5}  {diff:>10.4f}")

print(f"\n  Pre-2020 event quarters vs Post-2020 event quarters:")
print(f"  {'DV':<25} {'Pre-2020':>10} {'N':>6}  {'Post-2020':>10} {'N':>6}")
print(f"  {'-' * 60}")
for dv, name in dvs_perf:
    pre = pp[(pp["has_event"] == 1) & (pp["year"] < 2020)][dv]
    post = pp[(pp["has_event"] == 1) & (pp["year"] >= 2020)][dv]
    print(f"  {name:<25} {pre.mean():>10.4f} {pre.notna().sum():>5}  {post.mean():>10.4f} {post.notna().sum():>5}")

print("\n\nDone.")
