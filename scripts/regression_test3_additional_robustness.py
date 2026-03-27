"""Test 3: Additional robustness checks.
3. Winsorization (1st/99th percentile)
7. Tighter industry match (SIC3 instead of SIC2)
8. Exclude penny stocks (price < $5)
9. Number of connections (intensity)
11. Buy-and-hold abnormal returns (BHARs)

Runs for M&A Buyer and Bankruptcy (the two key event groups).
Market-adjusted, event-clustered.
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv, time
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 110)
print("TEST 3: ADDITIONAL ROBUSTNESS")
print("=" * 110)

# =====================================================================
# LOAD DATA (same pipeline)
# =====================================================================
print("\n--- Loading ---")

events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])
events["event_year"] = events["announcedate"].dt.year
events = events[(events["event_year"] >= 2015) & (events["event_year"] <= 2025)]

pub_cids = set()
with open(os.path.join(ciq_dir, "04_observer_company_details.csv"), "r", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        cid = str(r.get("companyid", "")).strip().replace(".0", "")
        if "public" in str(r.get("companytypename", "")).lower():
            pub_cids.add(cid)
events = events[~events["companyid"].isin(pub_cids)]

ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0"), errors="coerce")
cid_to_cik = dict(zip(ciq_xwalk["cid"], ciq_xwalk["cik_int"]))

panel_b_xwalk = pd.read_csv(os.path.join(data_dir, "Panel_B_Outcomes", "01_identifier_crosswalk.csv"))
panel_b_xwalk["cik_int"] = pd.to_numeric(panel_b_xwalk["cik"], errors="coerce")
panel_b_xwalk["linkdt"] = pd.to_datetime(panel_b_xwalk["linkdt"], errors="coerce")
panel_b_xwalk["linkenddt"] = pd.to_datetime(panel_b_xwalk["linkenddt"], errors="coerce").fillna(pd.Timestamp("2099-12-31"))
listing = panel_b_xwalk.groupby("cik_int").agg(first_listed=("linkdt", "min"), last_listed=("linkenddt", "max")).reset_index()
events["cik_int"] = events["companyid"].map(cid_to_cik)
events = events.merge(listing, on="cik_int", how="left")
events["was_public"] = (events["announcedate"] >= events["first_listed"]) & (events["announcedate"] <= events["last_listed"])
events = events[~events["was_public"].fillna(False)]

noise_types = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
               "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)",
               "Annual General Meeting", "Special/Extraordinary Shareholders Meeting",
               "Shareholder/Analyst Calls", "Special Calls", "Ex-Div Date (Regular)", "Ex-Div Date (Special)"]
events = events[~events["eventtype"].isin(noise_types)]

# Network
edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")
pxw = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
pxw["cik_int"] = pd.to_numeric(pxw["cik"], errors="coerce")
edges = edges.merge(pxw.drop_duplicates("cik_int", keep="first")[["cik_int", "permno"]].rename(
    columns={"cik_int": "portfolio_cik_int"}), on="portfolio_cik_int", how="inner")
edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce").astype("Int64")

# Industry — both SIC2 and SIC3
industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
industry["sic3"] = industry["sic"].astype(str).str[:3]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))
cik_to_sic3 = dict(zip(industry["cik_int"], industry["sic3"]))

edges["same_ind_sic2"] = (edges["observed_companyid"].map(cid_to_cik).map(cik_to_sic2) ==
                            edges["portfolio_cik_int"].map(cik_to_sic2)).astype(int)
edges["same_ind_sic3"] = (edges["observed_companyid"].map(cid_to_cik).map(cik_to_sic3) ==
                            edges["portfolio_cik_int"].map(cik_to_sic3)).astype(int)

# Number of connections per (observed company, portfolio company)
edge_counts = edges.groupby(["observed_companyid", "permno"]).size().reset_index(name="n_connections")
edges = edges.merge(edge_counts, on=["observed_companyid", "permno"], how="left")

connected_set = set()
for _, row in edges.iterrows():
    connected_set.add((row["observed_companyid"], int(row["permno"])))

pmcik = dict(zip(pxw["permno"].dropna().astype(int), pxw["cik_int"].dropna().astype(int)))
pm_sic2 = {p: cik_to_sic2.get(c, "") for p, c in pmcik.items()}
pm_sic3 = {p: cik_to_sic3.get(c, "") for p, c in pmcik.items()}
obs_with_edges = set(edges["observed_companyid"])

# Returns with price for penny stock filter
pd_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
pd_daily["date"] = pd.to_datetime(pd_daily["date"])
pd_daily["permno"] = pd.to_numeric(pd_daily["permno"], errors="coerce").dropna().astype(int)
pd_daily["ret"] = pd.to_numeric(pd_daily["ret"], errors="coerce")
pd_daily["prc"] = pd.to_numeric(pd_daily["prc"], errors="coerce").abs()  # CRSP uses negative for bid
pd_daily = pd_daily.dropna(subset=["ret"]).sort_values(["permno", "date"])
mkt_ret = pd_daily.groupby("date")["ret"].mean().rename("mkt_ret")
pd_daily = pd_daily.merge(mkt_ret, on="date", how="left")
pd_daily["aret"] = pd_daily["ret"] - pd_daily["mkt_ret"]
all_permnos = sorted(pd_daily["permno"].unique())

# Average price per stock (for penny stock filter)
avg_price = pd_daily.groupby("permno")["prc"].mean()
penny_stocks = set(avg_price[avg_price < 5].index)
print(f"  Penny stocks (avg price < $5): {len(penny_stocks)}")

pmdata = {}
for p, g in pd_daily.groupby("permno"):
    pmdata[p] = (g["date"].values, g["ret"].values, g["aret"].values)

car_windows = [("car_30", -30, -1), ("car_10", -10, -1), ("car_5", -5, -1), ("car_1", -1, 0)]


def calc_cars_full(permno, event_np):
    """Returns dict with raw CARs, market-adj CARs, and BHARs."""
    if permno not in pmdata:
        return None
    dates, rets, arets = pmdata[permno]
    if len(dates) < 30:
        return None
    diffs = (dates - event_np).astype("timedelta64[D]").astype(int)
    result = {}
    for wn, d0, d1 in car_windows:
        mask = (diffs >= d0) & (diffs <= d1)
        wr = rets[mask]
        war = arets[mask]
        if len(wr) >= max(2, abs(d1 - d0) * 0.3):
            result[wn] = float(np.sum(war))  # market-adjusted CAR
            # BHAR: product of (1+r) - 1
            result[f"{wn}_bhar"] = float(np.prod(1 + war) - 1)
    return result if result else None


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


wins = [("car_30", "[-30,-1]"), ("car_10", "[-10,-1]"), ("car_5", "[-5,-1]"), ("car_1", "[-1,0]")]

# Event groups
bankruptcy_types = [t for t in events["eventtype"].unique() if "Bankruptcy" in str(t) or "bankrupt" in str(t).lower()]

event_groups = [
    ("M&A Buyer", lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Buyer")]),
    ("Bankruptcy", lambda df: df[df["eventtype"].isin(bankruptcy_types)]),
]

# =====================================================================
# FOR EACH EVENT GROUP: build data and run robustness tests
# =====================================================================
for group_name, group_fn in event_groups:
    grp = group_fn(events)
    grp = grp[grp["companyid"].isin(obs_with_edges)]
    grp_df = grp[["companyid", "announcedate", "event_year"]].drop_duplicates(subset=["companyid", "announcedate"])

    if len(grp_df) < 20:
        continue

    print(f"\n\n{'='*110}")
    print(f"{group_name} ({len(grp_df)} events)")
    print(f"{'='*110}")

    # Build control group
    all_obs = []
    event_id = 0
    evl = grp_df.to_dict("records")

    for ei, ev in enumerate(evl):
        enp = np.datetime64(ev["announcedate"])
        ecid = ev["companyid"]
        eyr = ev["event_year"]
        obs_cik = cid_to_cik.get(ecid)
        obs_sic2 = cik_to_sic2.get(obs_cik, "") if obs_cik else ""
        obs_sic3 = cik_to_sic3.get(obs_cik, "") if obs_cik else ""

        for pmi, pm in enumerate(all_permnos):
            is_conn = 1 if (ecid, pm) in connected_set else 0
            if not is_conn and pmi % 10 != 0:
                continue

            cars = calc_cars_full(pm, enp)
            if not cars:
                continue

            psic2 = pm_sic2.get(pm, "")
            psic3 = pm_sic3.get(pm, "")
            si2 = 1 if (obs_sic2 and psic2 and obs_sic2 == psic2) else 0
            si3 = 1 if (obs_sic3 and psic3 and obs_sic3 == psic3) else 0

            # Number of connections
            n_conn = 0
            if is_conn:
                ec = edge_counts[(edge_counts["observed_companyid"] == ecid) &
                                  (edge_counts["permno"] == pm)]
                if len(ec) > 0:
                    n_conn = ec.iloc[0]["n_connections"]

            is_penny = 1 if pm in penny_stocks else 0

            all_obs.append({
                "event_id": event_id, "permno": pm, "event_year": eyr,
                "connected": is_conn, "same_ind_sic2": si2, "same_ind_sic3": si3,
                "n_connections": n_conn, "is_penny": is_penny,
                **cars,
            })
        event_id += 1

    df = pd.DataFrame(all_obs)
    df["cx2"] = df["connected"] * df["same_ind_sic2"]
    df["cx3"] = df["connected"] * df["same_ind_sic3"]
    df["eid_str"] = df["event_id"].astype(str)
    n_conn = df["connected"].sum()
    print(f"  Obs: {len(df):,} (conn={n_conn:,})")

    # =====================================================================
    # TEST A: BASELINE (same as before, for reference)
    # =====================================================================
    print(f"\n  --- A. BASELINE (mkt-adj, event-clustered, SIC2) ---")
    print(f"  {'Window':<10} {'conn x same':>12} {'p':>8}")
    print(f"  {'-'*30}")
    for var, label in wins:
        s = df.dropna(subset=[var]).copy().reset_index(drop=True)
        if len(s) < 200: continue
        try:
            m = smf.ols(f"{var} ~ connected + same_ind_sic2 + cx2", data=s).fit(
                cov_type="cluster", cov_kwds={"groups": s["eid_str"]})
            print(f"  {label:<10} {m.params['cx2']:>+10.5f}{sig(m.pvalues['cx2'])} {m.pvalues['cx2']:>8.4f}")
        except:
            print(f"  {label:<10} ERROR")

    # =====================================================================
    # TEST B: WINSORIZED at 1st/99th percentile
    # =====================================================================
    print(f"\n  --- B. WINSORIZED (1st/99th percentile) ---")
    print(f"  {'Window':<10} {'conn x same':>12} {'p':>8}")
    print(f"  {'-'*30}")
    for var, label in wins:
        s = df.dropna(subset=[var]).copy()
        p1, p99 = s[var].quantile(0.01), s[var].quantile(0.99)
        s[var] = s[var].clip(p1, p99)
        s = s.reset_index(drop=True)
        if len(s) < 200: continue
        try:
            m = smf.ols(f"{var} ~ connected + same_ind_sic2 + cx2", data=s).fit(
                cov_type="cluster", cov_kwds={"groups": s["eid_str"]})
            print(f"  {label:<10} {m.params['cx2']:>+10.5f}{sig(m.pvalues['cx2'])} {m.pvalues['cx2']:>8.4f}")
        except:
            print(f"  {label:<10} ERROR")

    # =====================================================================
    # TEST C: EXCLUDE PENNY STOCKS (avg price < $5)
    # =====================================================================
    print(f"\n  --- C. EXCLUDE PENNY STOCKS (avg price < $5) ---")
    no_penny = df[df["is_penny"] == 0]
    print(f"  Dropped {len(df) - len(no_penny):,} obs ({len(penny_stocks)} penny stocks)")
    print(f"  {'Window':<10} {'conn x same':>12} {'p':>8}")
    print(f"  {'-'*30}")
    for var, label in wins:
        s = no_penny.dropna(subset=[var]).copy().reset_index(drop=True)
        if len(s) < 200: continue
        try:
            m = smf.ols(f"{var} ~ connected + same_ind_sic2 + cx2", data=s).fit(
                cov_type="cluster", cov_kwds={"groups": s["eid_str"]})
            print(f"  {label:<10} {m.params['cx2']:>+10.5f}{sig(m.pvalues['cx2'])} {m.pvalues['cx2']:>8.4f}")
        except:
            print(f"  {label:<10} ERROR")

    # =====================================================================
    # TEST D: TIGHTER INDUSTRY MATCH (SIC3)
    # =====================================================================
    print(f"\n  --- D. TIGHTER INDUSTRY (SIC3 instead of SIC2) ---")
    n_sic3_same = df["cx3"].sum()
    print(f"  SIC3 same-ind connected obs: {int(n_sic3_same)} (vs SIC2: {int(df['cx2'].sum())})")
    print(f"  {'Window':<10} {'conn x same':>12} {'p':>8}")
    print(f"  {'-'*30}")
    for var, label in wins:
        s = df.dropna(subset=[var]).copy().reset_index(drop=True)
        if len(s) < 200 or s["cx3"].sum() < 5: continue
        try:
            m = smf.ols(f"{var} ~ connected + same_ind_sic3 + cx3", data=s).fit(
                cov_type="cluster", cov_kwds={"groups": s["eid_str"]})
            print(f"  {label:<10} {m.params['cx3']:>+10.5f}{sig(m.pvalues['cx3'])} {m.pvalues['cx3']:>8.4f}")
        except:
            print(f"  {label:<10} ERROR")

    # =====================================================================
    # TEST E: NUMBER OF CONNECTIONS (intensity)
    # =====================================================================
    print(f"\n  --- E. CONNECTION INTENSITY (n_connections as continuous) ---")
    print(f"  Max connections: {df['n_connections'].max()}, Mean (connected): {df[df['connected']==1]['n_connections'].mean():.1f}")
    print(f"  {'Window':<10} {'n_conn coef':>12} {'p':>8} {'n_conn x same':>14} {'p':>8}")
    print(f"  {'-'*52}")
    for var, label in wins:
        s = df.dropna(subset=[var]).copy().reset_index(drop=True)
        if len(s) < 200: continue
        s["nc_same"] = s["n_connections"] * s["same_ind_sic2"]
        try:
            m = smf.ols(f"{var} ~ n_connections + same_ind_sic2 + nc_same", data=s).fit(
                cov_type="cluster", cov_kwds={"groups": s["eid_str"]})
            c1 = m.params["n_connections"]; p1 = m.pvalues["n_connections"]
            c2 = m.params["nc_same"]; p2 = m.pvalues["nc_same"]
            print(f"  {label:<10} {c1:>+10.5f}{sig(p1)} {p1:>8.4f} {c2:>+12.5f}{sig(p2)} {p2:>8.4f}")
        except:
            print(f"  {label:<10} ERROR")

    # =====================================================================
    # TEST F: BUY-AND-HOLD ABNORMAL RETURNS (BHARs)
    # =====================================================================
    print(f"\n  --- F. BHARs (buy-and-hold instead of cumulative) ---")
    print(f"  {'Window':<10} {'conn x same':>12} {'p':>8} {'(vs CAR p)':>12}")
    print(f"  {'-'*42}")
    for var, label in wins:
        bvar = f"{var}_bhar"
        s = df.dropna(subset=[bvar]).copy().reset_index(drop=True)
        if len(s) < 200: continue
        s["cx2_b"] = s["connected"] * s["same_ind_sic2"]
        try:
            m = smf.ols(f"{bvar} ~ connected + same_ind_sic2 + cx2_b", data=s).fit(
                cov_type="cluster", cov_kwds={"groups": s["eid_str"]})
            # Also get baseline CAR p for comparison
            s2 = df.dropna(subset=[var]).copy().reset_index(drop=True)
            m2 = smf.ols(f"{var} ~ connected + same_ind_sic2 + cx2", data=s2).fit(
                cov_type="cluster", cov_kwds={"groups": s2["eid_str"]})
            print(f"  {label:<10} {m.params['cx2_b']:>+10.5f}{sig(m.pvalues['cx2_b'])} {m.pvalues['cx2_b']:>8.4f} (CAR: {m2.pvalues['cx2']:.4f})")
        except:
            print(f"  {label:<10} ERROR")

    # =====================================================================
    # COMPACT SUMMARY
    # =====================================================================
    print(f"\n  --- COMPACT: conn x same_ind at [-30,-1] across all robustness checks ---")
    var = "car_30"
    results = []

    for test_name, test_fn in [
        ("A. Baseline", lambda d: d),
        ("B. Winsorized", lambda d: d.assign(**{var: d[var].clip(d[var].quantile(0.01), d[var].quantile(0.99))})),
        ("C. No penny", lambda d: d[d["is_penny"] == 0]),
    ]:
        s = test_fn(df.dropna(subset=[var]).copy()).reset_index(drop=True)
        if len(s) < 200: continue
        try:
            m = smf.ols(f"{var} ~ connected + same_ind_sic2 + cx2", data=s).fit(
                cov_type="cluster", cov_kwds={"groups": s["eid_str"]})
            results.append((test_name, m.params["cx2"], m.pvalues["cx2"], len(s)))
        except:
            pass

    # SIC3
    s = df.dropna(subset=[var]).copy().reset_index(drop=True)
    if s["cx3"].sum() >= 5:
        try:
            m = smf.ols(f"{var} ~ connected + same_ind_sic3 + cx3", data=s).fit(
                cov_type="cluster", cov_kwds={"groups": s["eid_str"]})
            results.append(("D. SIC3 match", m.params["cx3"], m.pvalues["cx3"], len(s)))
        except:
            pass

    # BHAR
    bvar = f"{var}_bhar"
    s = df.dropna(subset=[bvar]).copy().reset_index(drop=True)
    s["cx2_b"] = s["connected"] * s["same_ind_sic2"]
    if len(s) >= 200:
        try:
            m = smf.ols(f"{bvar} ~ connected + same_ind_sic2 + cx2_b", data=s).fit(
                cov_type="cluster", cov_kwds={"groups": s["eid_str"]})
            results.append(("F. BHAR", m.params["cx2_b"], m.pvalues["cx2_b"], len(s)))
        except:
            pass

    print(f"  {'Test':<20} {'Coefficient':>12} {'p':>10} {'N':>10}")
    print(f"  {'-'*52}")
    for name, coef, p, n in results:
        print(f"  {name:<20} {coef:>+10.5f}{sig(p)} {p:>10.4f} {n:>10,}")


print("\n\nDone.")
