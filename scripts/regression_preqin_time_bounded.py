"""
Time-bounded Preqin fund performance test.

Improvement: use Private Placement dates from CIQ to infer WHEN the VC
invested in each observed company, then only link fund performance to
events AFTER the investment date.

Logic:
  CIQ says: Observer X works at VC Y, observes at Company A
  CIQ events say: Company A had Private Placement on 2019-03-15
  Inference: VC Y invested in Company A around 2019-Q1
  → Only count events at A that happen AFTER 2019-Q1 as treatment for VC Y
  → Only attribute to funds with vintage >= 2019 (or active at investment time)
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
print("TIME-BOUNDED PREQIN TEST")
print("Using Private Placement dates to anchor observer timing")
print("=" * 90)

# === Load all data ===
print("\n--- Loading data ---")

# Observer network
tb = pd.read_csv(os.path.join(data_dir, "table_b_observer_network.csv"))
tb["vc_firm_companyid"] = tb["vc_firm_companyid"].astype(str).str.replace(".0", "", regex=False)
tb["observed_companyid"] = tb["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
tb["observer_personid"] = tb["observer_personid"].astype(str).str.replace(".0", "", regex=False)

# Events
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
events["material"] = events["eventtype"].apply(
    lambda x: "M&A" in str(x) or "Bankruptcy" in str(x) or "Executive/Board" in str(x) or "Restructuring" in str(x))

# Private Placements = investment date proxy
pp_events = events[events["eventtype"] == "Private Placements"]
first_pp = pp_events.groupby("companyid")["announcedate"].min().reset_index()
first_pp.columns = ["companyid", "first_investment_date"]
first_pp_dict = dict(zip(first_pp["companyid"], first_pp["first_investment_date"]))
print(f"  Companies with investment date proxy: {len(first_pp):,}")

# Preqin crosswalk + funds + performance
xwalk = pd.read_csv(os.path.join(preqin_dir, "vc_preqin_crosswalk_validated.csv"))
xwalk = xwalk[xwalk["quality"].isin(["high", "medium"])]
firm_to_ciq = xwalk.drop_duplicates("preqin_firm_id").set_index("preqin_firm_id")["ciq_vc_companyid"].to_dict()
ciq_to_firm = {str(v): int(k) for k, v in firm_to_ciq.items()}
matched_firm_ids = set(xwalk["preqin_firm_id"].dropna().astype(int))

funds = pd.read_csv(os.path.join(preqin_dir, "fund_details_full.csv"))
funds = funds[funds["firm_id"].isin(matched_firm_ids)]
vc_funds = funds[funds["fund_type"].str.contains("Venture|Seed|Early", case=False, na=False)].copy()
vc_fund_ids = set(vc_funds["fund_id"].dropna().astype(int))

perf = pd.read_csv(os.path.join(preqin_dir, "fund_performance_full.csv"), low_memory=False)
perf = perf[perf["fund_id"].isin(vc_fund_ids)].copy()
perf["date_reported"] = pd.to_datetime(perf["date_reported"], errors="coerce")
perf = perf.dropna(subset=["date_reported"]).sort_values(["fund_id", "date_reported"])
perf["multiple_num"] = pd.to_numeric(perf["multiple"], errors="coerce")
perf["dpi_num"] = pd.to_numeric(perf["distr_dpi_pcent"], errors="coerce")
perf["d_multiple"] = perf.groupby("fund_id")["multiple_num"].diff()
perf["d_dpi"] = perf.groupby("fund_id")["dpi_num"].diff()
perf = perf.merge(vc_funds[["fund_id", "firm_id", "vintage", "final_size_usd"]],
                   on="fund_id", how="left", suffixes=("", "_fund"))
perf["ciq_vc_companyid"] = perf["firm_id"].map(firm_to_ciq).astype(str)
perf["quarter"] = perf["date_reported"].dt.to_period("Q")
perf["year"] = perf["date_reported"].dt.year

print(f"  Performance records: {len(perf):,}, {perf['fund_id'].nunique():,} funds")

# === Build TIME-BOUNDED event counts ===
print("\n--- Building time-bounded event counts ---")

# For each VC-observed company pair, find the investment date
# Then only count events at that company AFTER the investment date
vc_to_obs = {}
for _, r in tb.iterrows():
    vc = r["vc_firm_companyid"]
    oc = r["observed_companyid"]
    if vc not in vc_to_obs:
        vc_to_obs[vc] = set()
    vc_to_obs[vc].add(oc)

matched_ciq_vcs = set(xwalk["ciq_vc_companyid"].astype(str))

# Build: for each VC, for each quarter, count events only at companies
# where (a) there's a PP date and (b) the event is after the PP date
vc_q_events_bounded = []
vc_q_events_unbounded = []  # for comparison

n_bounded = 0
n_unbounded = 0
n_dropped_no_pp = 0
n_dropped_before_pp = 0

for vc_cid in matched_ciq_vcs:
    obs_cos = vc_to_obs.get(vc_cid, set())
    if not obs_cos:
        continue

    # All events at this VC's observed companies
    vc_events_all = events[events["companyid"].isin(obs_cos) & events["material"]]
    if len(vc_events_all) == 0:
        continue

    # Unbounded (for comparison)
    qc_unb = vc_events_all.groupby("quarter").size().reset_index(name="n_events_unbounded")
    qc_unb["ciq_vc_companyid"] = vc_cid
    vc_q_events_unbounded.append(qc_unb)
    n_unbounded += len(vc_events_all)

    # Bounded: only events after the company's first PP date
    bounded_events = []
    for _, evt in vc_events_all.iterrows():
        oc = evt["companyid"]
        pp_date = first_pp_dict.get(oc)
        if pp_date is None:
            n_dropped_no_pp += 1
            continue
        if evt["announcedate"] < pp_date:
            n_dropped_before_pp += 1
            continue
        bounded_events.append(evt)
        n_bounded += 1

    if bounded_events:
        bdf = pd.DataFrame(bounded_events)
        qc_b = bdf.groupby("quarter").size().reset_index(name="n_events_bounded")
        qc_b["ciq_vc_companyid"] = vc_cid
        vc_q_events_bounded.append(qc_b)

evt_bounded = pd.concat(vc_q_events_bounded, ignore_index=True) if vc_q_events_bounded else pd.DataFrame()
evt_unbounded = pd.concat(vc_q_events_unbounded, ignore_index=True) if vc_q_events_unbounded else pd.DataFrame()

print(f"  Unbounded events: {n_unbounded:,}")
print(f"  Bounded events: {n_bounded:,}")
print(f"  Dropped (no PP date): {n_dropped_no_pp:,}")
print(f"  Dropped (before PP): {n_dropped_before_pp:,}")
print(f"  Retention: {n_bounded/max(n_unbounded,1)*100:.1f}%")

# Also: restrict to funds whose vintage >= investment year
# For each VC, get the earliest PP date across all observed companies
vc_earliest_pp = {}
for vc_cid in matched_ciq_vcs:
    obs_cos = vc_to_obs.get(vc_cid, set())
    dates = [first_pp_dict[oc] for oc in obs_cos if oc in first_pp_dict]
    if dates:
        vc_earliest_pp[vc_cid] = min(dates)

# === Build panels ===
print("\n--- Building panels ---")

# Unbounded panel (same as before, for comparison)
panel_unb = perf.merge(evt_unbounded, on=["ciq_vc_companyid", "quarter"], how="left")
panel_unb["n_events"] = panel_unb["n_events_unbounded"].fillna(0).astype(int)
panel_unb["has_event"] = (panel_unb["n_events"] > 0).astype(int)
panel_unb["post_2020"] = (panel_unb["year"] >= 2020).astype(int)

# Bounded panel
panel_b = perf.merge(evt_bounded, on=["ciq_vc_companyid", "quarter"], how="left")
panel_b["n_events"] = panel_b["n_events_bounded"].fillna(0).astype(int)
panel_b["has_event"] = (panel_b["n_events"] > 0).astype(int)
panel_b["post_2020"] = (panel_b["year"] >= 2020).astype(int)

# Further restriction: only fund-quarters where vintage >= earliest PP year
panel_b_strict = panel_b.copy()
panel_b_strict["earliest_pp_year"] = panel_b_strict["ciq_vc_companyid"].map(
    lambda x: vc_earliest_pp.get(x, pd.Timestamp("1900-01-01")).year if x in vc_earliest_pp else 1900
)
panel_b_strict["fund_active"] = panel_b_strict["vintage"] >= panel_b_strict["earliest_pp_year"] - 2  # allow 2yr overlap
panel_b_strict_active = panel_b_strict[panel_b_strict["fund_active"]].copy()

print(f"  Unbounded panel: {len(panel_unb):,} obs, event quarters: {panel_unb['has_event'].sum():,}")
print(f"  Bounded panel: {len(panel_b):,} obs, event quarters: {panel_b['has_event'].sum():,}")
print(f"  Bounded + vintage filter: {len(panel_b_strict_active):,} obs, event quarters: {panel_b_strict_active['has_event'].sum():,}")

# === Regressions ===
print(f"\n\n{'=' * 90}")
print("RESULTS: Comparing Unbounded vs Bounded vs Bounded+Vintage")
print(f"{'=' * 90}")

yr_dum_unb = pd.get_dummies(panel_unb["year"], prefix="yr", drop_first=True).astype(float)
yr_dum_b = pd.get_dummies(panel_b["year"], prefix="yr", drop_first=True).astype(float)
yr_dum_bs = pd.get_dummies(panel_b_strict_active["year"], prefix="yr", drop_first=True).astype(float)

def run_panel(panel, yr_dum, label, dv_col, treat_col="has_event"):
    """Run HC1, Firm-cl, Fund FE + HC1, Firm FE + Vintage FE"""
    panel = panel.copy()
    panel["treat_x_post"] = panel[treat_col] * panel["post_2020"]

    y = panel[dv_col].dropna()
    if len(y) < 200:
        print(f"    {label}: N={len(y)}, too few")
        return
    lo, hi = y.quantile([0.01, 0.99])
    y = y.clip(lo, hi)
    idx = y.index

    X = panel.loc[idx, [treat_col, "post_2020", "treat_x_post"]].copy()
    X_yr = pd.concat([X, yr_dum.loc[idx]], axis=1)

    # Fund FE
    fund_mean = panel.loc[idx].groupby("fund_id")[dv_col].transform("mean")
    y_fe = y - fund_mean.loc[idx]

    # Firm FE (if enough firms)
    n_firms = panel.loc[idx, "firm_id"].nunique()

    specs = [
        ("HC1+YrFE", y, X_yr, "HC1", {}),
        ("Firm-cl+YrFE", y, X_yr, "cluster", {"groups": panel.loc[idx, "firm_id"]}),
        ("FundFE+HC1", y_fe, X.copy(), "HC1", {}),
    ]

    if n_firms >= 50:
        # Firm FE via demeaning
        firm_mean = panel.loc[idx].groupby("firm_id")[dv_col].transform("mean")
        y_firmfe = y - firm_mean.loc[idx]
        specs.append(("FirmFE+HC1", y_firmfe, X.copy(), "HC1", {}))

    n_evt = int(panel.loc[idx, treat_col].sum())
    print(f"\n    {label} (N={len(y):,}, events={n_evt:,}, funds={panel.loc[idx, 'fund_id'].nunique():,})")

    for sname, dep, xmat, cov, kwds in specs:
        xmat = sm.add_constant(xmat)
        try:
            m = sm.OLS(dep, xmat).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
            bt = m.params.get(treat_col, np.nan)
            pt = m.pvalues.get(treat_col, np.nan)
            bi = m.params.get("treat_x_post", np.nan)
            pi = m.pvalues.get("treat_x_post", np.nan)
            st = "***" if pt < 0.01 else "**" if pt < 0.05 else "*" if pt < 0.10 else ""
            si = "***" if pi < 0.01 else "**" if pi < 0.05 else "*" if pi < 0.10 else ""
            print(f"      {sname:<18} b(event)={bt:>8.4f}{st:<3} p={pt:.3f}  b(evtXpost20)={bi:>8.4f}{si:<3} p={pi:.3f}")
        except Exception as e:
            print(f"      {sname:<18} Error: {str(e)[:50]}")


for dv, dv_name in [("d_multiple", "Delta TVPI"), ("d_dpi", "Delta DPI")]:
    print(f"\n{'=' * 90}")
    print(f"  DV: {dv_name}")
    print(f"{'=' * 90}")

    run_panel(panel_unb, yr_dum_unb, "UNBOUNDED (original)", dv)
    run_panel(panel_b, yr_dum_b, "BOUNDED (events after PP date)", dv)
    run_panel(panel_b_strict_active, yr_dum_bs, "BOUNDED + VINTAGE FILTER", dv)

# === Cross-sectional: within-firm with time-bounded observer count ===
print(f"\n\n{'=' * 90}")
print("CROSS-SECTIONAL: Observer count with time-bounded measurement")
print(f"{'=' * 90}")

# For each fund, count observers that are:
# (a) at the same VC firm
# (b) at companies with PP dates within [vintage-2, vintage+5] year range
fund_obs_counts = []
for _, fund in vc_funds.iterrows():
    fid = fund["fund_id"]
    firm_id = fund["firm_id"]
    vintage = fund.get("vintage", 2000)
    if pd.isna(vintage):
        continue

    ciq_vc = firm_to_ciq.get(firm_id)
    if not ciq_vc:
        continue

    obs_cos = vc_to_obs.get(str(ciq_vc), set())
    n_total = len(obs_cos)

    # Time-bounded: only count companies invested during fund's active period
    n_bounded = 0
    for oc in obs_cos:
        pp_date = first_pp_dict.get(oc)
        if pp_date and (vintage - 2) <= pp_date.year <= (vintage + 5):
            n_bounded += 1

    fund_obs_counts.append({
        "fund_id": fid,
        "firm_id": firm_id,
        "vintage": vintage,
        "n_obs_total": n_total,
        "n_obs_bounded": n_bounded,
        "final_size_usd": fund.get("final_size_usd", np.nan),
    })

foc = pd.DataFrame(fund_obs_counts)

# Get final performance per fund
final_perf = perf.sort_values("date_reported").drop_duplicates("fund_id", keep="last")
final_perf = final_perf[["fund_id", "multiple_num", "dpi_num"]].rename(
    columns={"multiple_num": "final_tvpi", "dpi_num": "final_dpi"})

foc = foc.merge(final_perf, on="fund_id", how="left")
foc = foc.dropna(subset=["final_tvpi"])
foc["ln_size"] = np.log(foc["final_size_usd"].clip(lower=1))

print(f"\n  Funds with both measures: {len(foc):,}")
print(f"  Mean n_obs_total: {foc['n_obs_total'].mean():.2f}")
print(f"  Mean n_obs_bounded: {foc['n_obs_bounded'].mean():.2f}")
print(f"  Correlation total vs bounded: {foc['n_obs_total'].corr(foc['n_obs_bounded']):.3f}")

# Run: TVPI ~ n_observers with Firm FE
for obs_measure, label in [("n_obs_total", "Total observers (unbounded)"),
                            ("n_obs_bounded", "Time-bounded observers")]:
    print(f"\n  {label}:")

    for dv, dv_name in [("final_tvpi", "TVPI"), ("final_dpi", "Final DPI")]:
        y = foc[dv].dropna()
        lo, hi = y.quantile([0.01, 0.99])
        y = y.clip(lo, hi)
        idx = y.index

        X = foc.loc[idx, [obs_measure, "ln_size"]].copy()
        vint_dum = pd.get_dummies(foc.loc[idx, "vintage"].astype(int), prefix="v", drop_first=True).astype(float)

        # HC1 + vintage FE
        X_v = pd.concat([X, vint_dum], axis=1)
        X_v = sm.add_constant(X_v)
        try:
            m = sm.OLS(y, X_v).fit(cov_type="HC1")
            b = m.params.get(obs_measure, np.nan)
            p = m.pvalues.get(obs_measure, np.nan)
            s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            print(f"    {dv_name}: HC1+VintFE: b={b:.4f}{s} p={p:.3f} (N={len(y):,})")
        except Exception as e:
            print(f"    {dv_name}: Error: {str(e)[:50]}")

        # Firm FE
        n_firms = foc.loc[idx, "firm_id"].nunique()
        if n_firms >= 20:
            firm_mean = foc.loc[idx].groupby("firm_id")[dv].transform("mean")
            y_fe = y - firm_mean.loc[idx]
            X_fe = foc.loc[idx, [obs_measure]].copy()
            X_fe = sm.add_constant(X_fe)
            try:
                m = sm.OLS(y_fe, X_fe).fit(cov_type="HC1")
                b = m.params.get(obs_measure, np.nan)
                p = m.pvalues.get(obs_measure, np.nan)
                s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
                print(f"    {dv_name}: FirmFE: b={b:.4f}{s} p={p:.3f}")
            except Exception as e:
                print(f"    {dv_name}: FirmFE Error: {str(e)[:50]}")

print("\n\nDone.")
