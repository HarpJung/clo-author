"""Check if the quarterly DPI result holds for funds WITH vs WITHOUT cashflow data."""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd, numpy as np, os
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
preqin_dir = os.path.join(data_dir, "Preqin")

# Load crosswalk
xwalk = pd.read_csv(os.path.join(preqin_dir, "vc_preqin_crosswalk_validated.csv"))
xwalk = xwalk[xwalk["quality"].isin(["high", "medium"])]
firm_to_ciq = xwalk.drop_duplicates("preqin_firm_id").set_index("preqin_firm_id")["ciq_vc_companyid"].to_dict()
matched_firm_ids = set(xwalk["preqin_firm_id"].dropna().astype(int))

# Load funds
funds = pd.read_csv(os.path.join(preqin_dir, "fund_details_full.csv"))
funds = funds[funds["firm_id"].isin(matched_firm_ids)]
vc_funds = funds[funds["fund_type"].str.contains("Venture|Seed|Early", case=False, na=False)]
vc_fund_ids = set(vc_funds["fund_id"].dropna().astype(int))

# Which funds have cashflow data?
cf = pd.read_csv(os.path.join(preqin_dir, "cashflows_full.csv"))
cf_fund_ids = set(cf[cf["fund_id"].isin(vc_fund_ids)]["fund_id"].dropna().astype(int))

print(f"VC funds total: {len(vc_fund_ids):,}")
print(f"With cashflow data: {len(cf_fund_ids):,}")
print(f"Without cashflow data: {len(vc_fund_ids - cf_fund_ids):,}")

# Load performance
perf = pd.read_csv(os.path.join(preqin_dir, "fund_performance_full.csv"), low_memory=False)
perf = perf[perf["fund_id"].isin(vc_fund_ids)].copy()
perf["date_reported"] = pd.to_datetime(perf["date_reported"], errors="coerce")
perf = perf.dropna(subset=["date_reported"]).sort_values(["fund_id", "date_reported"])
perf["dpi_num"] = pd.to_numeric(perf["distr_dpi_pcent"], errors="coerce")
perf["d_dpi"] = perf.groupby("fund_id")["dpi_num"].diff()
perf = perf.merge(vc_funds[["fund_id", "firm_id"]], on="fund_id", how="left", suffixes=("", "_fund"))
perf["ciq_vc_companyid"] = perf["firm_id"].map(firm_to_ciq).astype(str)
perf["quarter"] = perf["date_reported"].dt.to_period("Q")
perf["year"] = perf["date_reported"].dt.year
perf["has_cf"] = perf["fund_id"].isin(cf_fund_ids).astype(int)

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

vc_q_events = []
for vc_cid in set(xwalk["ciq_vc_companyid"].astype(str)):
    obs_cos = vc_to_obs.get(vc_cid, set())
    if not obs_cos:
        continue
    vc_evt = events[events["companyid"].isin(obs_cos)]
    if len(vc_evt) == 0:
        continue
    qc = vc_evt.groupby("quarter").agg(n_events=("companyid", "count")).reset_index()
    qc["ciq_vc_companyid"] = vc_cid
    vc_q_events.append(qc)
evt_counts = pd.concat(vc_q_events, ignore_index=True)

# Merge events with performance
panel = perf.merge(evt_counts, on=["ciq_vc_companyid", "quarter"], how="left")
panel["n_events"] = panel["n_events"].fillna(0).astype(int)
panel["has_event"] = (panel["n_events"] > 0).astype(int)

# Lead/lag
next_evt = evt_counts.copy()
next_evt["quarter"] = next_evt["quarter"] - 1
next_evt = next_evt.rename(columns={"n_events": "n_evt_next"})
prev_evt = evt_counts.copy()
prev_evt["quarter"] = prev_evt["quarter"] + 1
prev_evt = prev_evt.rename(columns={"n_events": "n_evt_prev"})
panel = panel.merge(next_evt[["ciq_vc_companyid", "quarter", "n_evt_next"]], on=["ciq_vc_companyid", "quarter"], how="left")
panel = panel.merge(prev_evt[["ciq_vc_companyid", "quarter", "n_evt_prev"]], on=["ciq_vc_companyid", "quarter"], how="left")
panel["has_evt_prev"] = (panel["n_evt_prev"].fillna(0) > 0).astype(int)
panel["has_evt_next"] = (panel["n_evt_next"].fillna(0) > 0).astype(int)

yr_dum = pd.get_dummies(panel["year"], prefix="yr", drop_first=True).astype(float)

# Run on three subsets
print("\n" + "=" * 90)
print("DPI EVENT WINDOW: ALL vs WITH-CASHFLOW vs WITHOUT-CASHFLOW")
print("=" * 90)

for subset_name, mask in [("ALL FUNDS", panel["fund_id"].notna()),
                           ("WITH CASHFLOW DATA (480 funds)", panel["has_cf"] == 1),
                           ("WITHOUT CASHFLOW DATA (811 funds)", panel["has_cf"] == 0)]:
    sub = panel[mask].copy()
    y = sub["d_dpi"].dropna()
    if len(y) < 200:
        print(f"\n  {subset_name}: too few obs ({len(y)})")
        continue

    lo, hi = y.quantile([0.01, 0.99])
    y = y.clip(lo, hi)
    idx = y.index

    X = sub.loc[idx, ["has_evt_prev", "has_event", "has_evt_next"]].copy()
    X = pd.concat([X, yr_dum.loc[idx]], axis=1)
    X = sm.add_constant(X)

    # Fund FE
    fund_mean = sub.loc[idx].groupby("fund_id")["d_dpi"].transform("mean")
    y_fe = y - fund_mean.loc[idx]
    X_fe = sub.loc[idx, ["has_evt_prev", "has_event", "has_evt_next"]].copy()
    X_fe = sm.add_constant(X_fe)

    print(f"\n  {subset_name}")
    print(f"  Funds: {sub.loc[idx, 'fund_id'].nunique():,}  Obs: {len(y):,}  Event Q: {sub.loc[idx, 'has_event'].sum():,}")

    for sname, dep, xmat, cov, kwds in [
        ("HC1", y, X, "HC1", {}),
        ("Firm-cl", y, X, "cluster", {"groups": sub.loc[idx, "firm_id"]}),
        ("Fund FE", y_fe, X_fe, "HC1", {}),
    ]:
        try:
            m = sm.OLS(dep, xmat).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
            bp = m.params.get("has_evt_prev", np.nan)
            pp = m.pvalues.get("has_evt_prev", np.nan)
            be = m.params.get("has_event", np.nan)
            pe = m.pvalues.get("has_event", np.nan)
            bn = m.params.get("has_evt_next", np.nan)
            pn = m.pvalues.get("has_evt_next", np.nan)
            sp = "***" if pp < 0.01 else "**" if pp < 0.05 else "*" if pp < 0.10 else ""
            se = "***" if pe < 0.01 else "**" if pe < 0.05 else "*" if pe < 0.10 else ""
            sn = "***" if pn < 0.01 else "**" if pn < 0.05 else "*" if pn < 0.10 else ""
            print(f"    {sname:<10} Prev: {bp:>7.3f}{sp:<3} p={pp:.3f}  Event: {be:>7.3f}{se:<3} p={pe:.3f}  Next: {bn:>7.3f}{sn:<3} p={pn:.3f}")
        except Exception as e:
            print(f"    {sname:<10} Error: {str(e)[:50]}")

    ev = sub[sub["has_event"] == 1]["d_dpi"]
    ne = sub[sub["has_event"] == 0]["d_dpi"]
    diff = ev.mean() - ne.mean() if ev.notna().sum() > 0 and ne.notna().sum() > 0 else np.nan
    print(f"    Means: event={ev.mean():.4f} (N={ev.notna().sum():,})  no-event={ne.mean():.4f} (N={ne.notna().sum():,})  diff={diff:.4f}")

print("\nDone.")
