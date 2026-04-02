"""
Fund performance v3: Full FE and clustering specification battery.

Specs:
  (1) HC1 robust
  (2) Fund-clustered
  (3) Firm-clustered (VC firm level)
  (4) Vintage FE + Firm-clustered
  (5) Fund FE (within-transformation) + HC1
  (6) Vintage FE + Fund-clustered

Focus on the strongest DVs from v2:
  - Delta TVPI (raw)
  - Delta TVPI (benchmark-adjusted)
  - Delta DPI (realized)
  - Delta Called %
  - Has Distribution (0/1)
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
print("FUND PERFORMANCE v3: Full Specification Battery")
print("=" * 90)

# === Load everything (same as v2, condensed) ===
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

for col in ["multiple_num", "distr_dpi_pcent_num", "value_rvpi_pcent_num", "called_pcent_num"]:
    perf[f"d_{col}"] = perf.groupby("fund_id")[col].diff()

# Benchmark-adjusted
bench = pd.read_csv(os.path.join(preqin_dir, "benchmarks.csv"))
vc_bench = bench[bench["benchmark_fundtype_name"].isin(["Venture", "Early Stage"])]
bench_med = vc_bench.groupby(["fund_vintage", "qdate"]).agg(
    bench_mult=("multiple_med", "first")).reset_index()
bench_med["fund_vintage"] = bench_med["fund_vintage"].astype(float)
bench_med["qdate"] = pd.to_datetime(bench_med["qdate"], errors="coerce")
perf = perf.merge(bench_med, left_on=[perf["vintage"].astype(float), "date_reported"],
                   right_on=["fund_vintage", "qdate"], how="left")
perf["adj_multiple"] = perf["multiple_num"] - perf["bench_mult"]
perf["d_adj_multiple"] = perf.groupby("fund_id")["adj_multiple"].diff()

# Cashflows
cf = pd.read_csv(os.path.join(preqin_dir, "cashflows_full.csv"))
cf = cf[cf["fund_id"].isin(vc_fund_ids)].copy()
cf["transaction_date"] = pd.to_datetime(cf["transaction_date"], errors="coerce")
cf = cf.dropna(subset=["transaction_date"])
cf["quarter"] = cf["transaction_date"].dt.to_period("Q")

calls = cf[cf["transaction_type"] == "Capital Call"].groupby(["fund_id", "quarter"])["transaction_amount"].sum().reset_index(name="q_calls")
dists = cf[cf["transaction_type"] == "Distribution"].groupby(["fund_id", "quarter"])["transaction_amount"].sum().reset_index(name="q_dists")
cf_q = calls.merge(dists, on=["fund_id", "quarter"], how="outer").fillna(0)
cf_q["has_dist"] = (cf_q["q_dists"] > 0).astype(int)
cf_q["has_call"] = (cf_q["q_calls"] > 0).astype(int)
cf_q = cf_q.merge(vc_funds[["fund_id", "firm_id", "vintage", "final_size_usd"]], on="fund_id", how="left")
cf_q["ciq_vc_companyid"] = cf_q["firm_id"].map(firm_to_ciq).astype(str)
cf_q["year"] = cf_q["quarter"].dt.year

# Events
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
events["material"] = events["eventtype"].apply(lambda et: "M&A" in str(et) or "Bankruptcy" in str(et) or "Executive/Board" in str(et) or "Restructuring" in str(et))
events["is_ma"] = events["eventtype"].str.contains("M&A", na=False)

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

# Build panels
perf_panel = perf.merge(evt_counts, on=["ciq_vc_companyid", "quarter"], how="left")
for c in ["n_events", "n_material", "n_ma"]:
    perf_panel[c] = perf_panel[c].fillna(0).astype(int)
perf_panel["has_event"] = (perf_panel["n_events"] > 0).astype(int)
perf_panel["has_material"] = (perf_panel["n_material"] > 0).astype(int)
perf_panel["has_ma"] = (perf_panel["n_ma"] > 0).astype(int)
perf_panel["post_2020"] = (perf_panel["year"] >= 2020).astype(int)
perf_panel["ln_size"] = np.log(perf_panel["final_size_usd"].clip(lower=1))

cf_panel = cf_q.merge(evt_counts, on=["ciq_vc_companyid", "quarter"], how="left")
for c in ["n_events", "n_material", "n_ma"]:
    cf_panel[c] = cf_panel[c].fillna(0).astype(int)
cf_panel["has_event"] = (cf_panel["n_events"] > 0).astype(int)
cf_panel["has_material"] = (cf_panel["n_material"] > 0).astype(int)
cf_panel["has_ma"] = (cf_panel["n_ma"] > 0).astype(int)
cf_panel["post_2020"] = (cf_panel["year"] >= 2020).astype(int)

print(f"\n  Performance panel: {len(perf_panel):,} obs, {perf_panel['fund_id'].nunique():,} funds, {perf_panel['firm_id'].nunique():,} firms")
print(f"  Cashflow panel: {len(cf_panel):,} obs, {cf_panel['fund_id'].nunique():,} funds")

# =====================================================================
# Run full spec battery
# =====================================================================
print(f"\n\n{'=' * 90}")
print("RESULTS: 6 Specifications")
print(f"{'=' * 90}")

def run_specs(panel, dv_col, treat_col, dv_name, treat_name):
    """Run 6 specifications for one DV-treatment combination."""
    panel = panel.copy()
    panel["treat_x_post"] = panel[treat_col] * panel["post_2020"]

    y_raw = panel[dv_col].dropna()
    if len(y_raw) < 200:
        print(f"    Too few obs ({len(y_raw)}), skipping")
        return

    # Winsorize
    lo, hi = y_raw.quantile([0.01, 0.99])
    y = y_raw.clip(lo, hi)
    idx = y.index

    # Base X
    base_vars = [treat_col, "post_2020", "treat_x_post"]
    X_base = panel.loc[idx, base_vars].copy()

    # Year dummies
    yr_dum = pd.get_dummies(panel.loc[idx, "year"], prefix="yr", drop_first=True).astype(float)

    # Vintage dummies (for vintage FE)
    vint = panel.loc[idx, "vintage"].fillna(0).astype(int)
    vint_dum = pd.get_dummies(vint, prefix="vint", drop_first=True).astype(float)

    # Fund FE: within-transformation
    fund_means = panel.loc[idx].groupby("fund_id")[dv_col].transform("mean")
    y_demean = y - fund_means.loc[idx]

    specs = [
        ("(1) HC1",              y,        pd.concat([X_base, yr_dum.loc[idx]], axis=1), "HC1",     {}),
        ("(2) Fund-cl",          y,        pd.concat([X_base, yr_dum.loc[idx]], axis=1), "cluster", {"groups": panel.loc[idx, "fund_id"]}),
        ("(3) Firm-cl",          y,        pd.concat([X_base, yr_dum.loc[idx]], axis=1), "cluster", {"groups": panel.loc[idx, "firm_id"]}),
        ("(4) VintFE+Firm-cl",   y,        pd.concat([X_base, yr_dum.loc[idx], vint_dum.loc[idx]], axis=1), "cluster", {"groups": panel.loc[idx, "firm_id"]}),
        ("(5) FundFE+HC1",       y_demean, X_base,                                       "HC1",     {}),
        ("(6) VintFE+Fund-cl",   y,        pd.concat([X_base, yr_dum.loc[idx], vint_dum.loc[idx]], axis=1), "cluster", {"groups": panel.loc[idx, "fund_id"]}),
    ]

    for spec_name, dep, X, cov_type, cov_kwds in specs:
        X_const = sm.add_constant(X)
        try:
            if cov_type == "HC1":
                m = sm.OLS(dep, X_const).fit(cov_type="HC1")
            else:
                m = sm.OLS(dep, X_const).fit(cov_type="cluster", cov_kwds=cov_kwds)

            bt = m.params.get(treat_col, np.nan)
            pt = m.pvalues.get(treat_col, np.nan)
            bi = m.params.get("treat_x_post", np.nan)
            pi = m.pvalues.get("treat_x_post", np.nan)
            st = "***" if pt < 0.01 else "**" if pt < 0.05 else "*" if pt < 0.10 else ""
            si = "***" if pi < 0.01 else "**" if pi < 0.05 else "*" if pi < 0.10 else ""
            print(f"    {spec_name:<22} b({treat_col[:8]})={bt:>9.4f}{st:<3} p={pt:>5.3f}  b(TxP20)={bi:>9.4f}{si:<3} p={pi:>5.3f}")
        except Exception as e:
            print(f"    {spec_name:<22} Error: {str(e)[:50]}")


# === Performance DVs ===
perf_tests = [
    ("d_multiple_num", "Delta TVPI (raw)"),
    ("d_adj_multiple", "Delta TVPI (bench-adj)"),
    ("d_distr_dpi_pcent_num", "Delta DPI (realized)"),
    ("d_called_pcent_num", "Delta Called %"),
]

treatments = [("has_event", "Any Event"), ("has_material", "Material"), ("has_ma", "M&A")]

for dv_col, dv_name in perf_tests:
    print(f"\n{'─' * 90}")
    print(f"  DV: {dv_name}")
    print(f"{'─' * 90}")
    for treat_col, treat_name in treatments:
        print(f"\n  Treatment: {treat_name}")
        run_specs(perf_panel, dv_col, treat_col, dv_name, treat_name)

# === Cashflow DVs ===
cf_tests = [
    ("has_dist", "Has Distribution (0/1)"),
    ("has_call", "Has Capital Call (0/1)"),
]

print(f"\n\n{'─' * 90}")
print(f"  CASHFLOW PANEL")
print(f"{'─' * 90}")

for dv_col, dv_name in cf_tests:
    print(f"\n  DV: {dv_name}")
    for treat_col, treat_name in treatments:
        print(f"\n  Treatment: {treat_name}")
        run_specs(cf_panel, dv_col, treat_col, dv_name, treat_name)

print("\n\nDone.")
