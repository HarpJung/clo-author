"""Test 3: Abnormal Volume.
Do connected stocks show higher trading volume before events?
Compute abnormal volume (actual / average) at connected vs non-connected
portfolio companies around private firm events.

Same event groups: M&A Buyer, Bankruptcy, Exec/Board Changes.
Same control group structure.
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
print("TEST 3: ABNORMAL VOLUME")
print("=" * 110)

# =====================================================================
# LOAD (same pipeline)
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

edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")
pxw = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
pxw["cik_int"] = pd.to_numeric(pxw["cik"], errors="coerce")
edges = edges.merge(pxw.drop_duplicates("cik_int", keep="first")[["cik_int", "permno"]].rename(
    columns={"cik_int": "portfolio_cik_int"}), on="portfolio_cik_int", how="inner")
edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce").astype("Int64")

industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))
edges["same_industry"] = (edges["observed_companyid"].map(cid_to_cik).map(cik_to_sic2) ==
                           edges["portfolio_cik_int"].map(cik_to_sic2)).astype(int)

connected_set = set()
for _, row in edges.iterrows():
    connected_set.add((row["observed_companyid"], int(row["permno"])))

pmcik = dict(zip(pxw["permno"].dropna().astype(int), pxw["cik_int"].dropna().astype(int)))
pm_sic2 = {p: cik_to_sic2.get(c, "") for p, c in pmcik.items()}
obs_with_edges = set(edges["observed_companyid"])

# Load returns WITH volume
pd_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
pd_daily["date"] = pd.to_datetime(pd_daily["date"])
pd_daily["permno"] = pd.to_numeric(pd_daily["permno"], errors="coerce").dropna().astype(int)
pd_daily["vol"] = pd.to_numeric(pd_daily["vol"], errors="coerce")
pd_daily["ret"] = pd.to_numeric(pd_daily["ret"], errors="coerce")
pd_daily = pd_daily.dropna(subset=["ret"]).sort_values(["permno", "date"])
all_permnos = sorted(pd_daily["permno"].unique())

# Pre-compute per-stock data including volume
pmdata = {}
for p, g in pd_daily.groupby("permno"):
    dates = g["date"].values
    vols = g["vol"].values
    rets = g["ret"].values
    # Compute average volume over estimation window [-250, -60] for each date
    # For simplicity, use trailing 120-day average volume
    avg_vol = pd.Series(vols).rolling(120, min_periods=60).mean().values
    pmdata[p] = (dates, rets, vols, avg_vol)

print(f"  Stocks with volume: {len(pmdata):,}")

# =====================================================================
# CAR and Abnormal Volume computation
# =====================================================================

vol_windows = [
    ("avol_30", -30, -1), ("avol_20", -20, -1), ("avol_10", -10, -1),
    ("avol_5", -5, -1), ("avol_3", -3, -1),
    ("avol_post5", 0, 5),
]

car_windows = [
    ("car_30", -30, -1), ("car_10", -10, -1), ("car_5", -5, -1),
]


def calc_all(permno, event_np):
    """Compute CARs and abnormal volume ratios."""
    if permno not in pmdata:
        return None
    dates, rets, vols, avg_vol = pmdata[permno]
    if len(dates) < 60:
        return None
    diffs = (dates - event_np).astype("timedelta64[D]").astype(int)

    result = {}

    # CARs (market-unadjusted for simplicity — focus is volume)
    for wn, d0, d1 in car_windows:
        mask = (diffs >= d0) & (diffs <= d1)
        wr = rets[mask]
        if len(wr) >= max(2, abs(d1 - d0) * 0.3):
            result[wn] = float(np.sum(wr))

    # Abnormal volume = mean(event window volume) / mean(baseline volume)
    # Baseline: [-120, -31] (before event window)
    baseline_mask = (diffs >= -120) & (diffs <= -31)
    baseline_vol = vols[baseline_mask]
    if len(baseline_vol) < 30 or np.nanmean(baseline_vol) <= 0:
        return result if result else None

    baseline_mean = np.nanmean(baseline_vol)

    for wn, d0, d1 in vol_windows:
        mask = (diffs >= d0) & (diffs <= d1)
        wv = vols[mask]
        if len(wv) >= max(2, abs(d1 - d0) * 0.3):
            event_mean = np.nanmean(wv)
            if baseline_mean > 0 and not np.isnan(event_mean):
                # Abnormal volume ratio (1.0 = normal, >1 = above average)
                result[wn] = float(event_mean / baseline_mean)
                # Also log ratio for regression (more normally distributed)
                result[f"{wn}_log"] = float(np.log(event_mean / baseline_mean))

    return result if result else None


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


# =====================================================================
# Event groups
# =====================================================================
bankruptcy_types = [t for t in events["eventtype"].unique() if "Bankruptcy" in str(t) or "bankrupt" in str(t).lower()]

event_groups = [
    ("M&A Buyer", lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Buyer")]),
    ("Bankruptcy", lambda df: df[df["eventtype"].isin(bankruptcy_types)]),
    ("Exec/Board", lambda df: df[df["eventtype"] == "Executive/Board Changes - Other"]),
]

vol_wins = [("avol_30", "Vol[-30,-1]"), ("avol_20", "Vol[-20,-1]"), ("avol_10", "Vol[-10,-1]"),
            ("avol_5", "Vol[-5,-1]"), ("avol_3", "Vol[-3,-1]"), ("avol_post5", "Vol[0,+5]")]

for group_name, group_fn in event_groups:
    grp = group_fn(events)
    grp = grp[grp["companyid"].isin(obs_with_edges)]
    grp_df = grp[["companyid", "announcedate", "event_year"]].drop_duplicates(subset=["companyid", "announcedate"])

    if len(grp_df) < 20:
        continue

    print(f"\n\n{'='*110}")
    print(f"{group_name} ({len(grp_df)} events)")
    print(f"{'='*110}")

    # Build control group with volume
    all_obs = []
    event_id = 0
    evl = grp_df.to_dict("records")

    for ei, ev in enumerate(evl):
        enp = np.datetime64(ev["announcedate"])
        ecid = ev["companyid"]
        obs_cik = cid_to_cik.get(ecid)
        obs_sic2 = cik_to_sic2.get(obs_cik, "") if obs_cik else ""

        for pmi, pm in enumerate(all_permnos):
            is_conn = 1 if (ecid, pm) in connected_set else 0
            if not is_conn and pmi % 10 != 0:
                continue

            result = calc_all(pm, enp)
            if not result:
                continue

            psic = pm_sic2.get(pm, "")
            si = 1 if (obs_sic2 and psic and obs_sic2 == psic) else 0

            all_obs.append({
                "event_id": event_id, "connected": is_conn,
                "same_industry": si, "event_year": ev["event_year"],
                **result,
            })
        event_id += 1

    if not all_obs:
        continue

    df = pd.DataFrame(all_obs)
    df["cx"] = df["connected"] * df["same_industry"]
    df["eid_str"] = df["event_id"].astype(str)
    n_conn = df["connected"].sum()
    print(f"  Obs: {len(df):,} (conn={n_conn:,})")

    # =====================================================================
    # TABLE A: Connected vs Non-Connected Volume Ratios
    # =====================================================================
    print(f"\n  --- A. VOLUME RATIOS (mean abnormal volume, >1 = above normal) ---")
    print(f"  {'Window':<14} {'Connected':>12} {'Non-conn':>12} {'Diff':>10} {'t-test p':>10}")
    print(f"  {'-'*58}")

    for var, label in vol_wins:
        conn = df[(df["connected"] == 1)].dropna(subset=[var])[var]
        nonconn = df[(df["connected"] == 0)].dropna(subset=[var])[var]
        if len(conn) < 20 or len(nonconn) < 20:
            continue
        t, p = stats.ttest_ind(conn, nonconn, equal_var=False)
        diff = conn.mean() - nonconn.mean()
        print(f"  {label:<14} {conn.mean():>10.4f}{sig(stats.ttest_1samp(conn, 1)[1])} {nonconn.mean():>10.4f}{sig(stats.ttest_1samp(nonconn, 1)[1])} {diff:>+8.4f}{sig(p)} {p:>10.4f}")

    # Same-industry breakdown
    print(f"\n  --- B. SAME-INDUSTRY VOLUME RATIOS ---")
    print(f"  {'Window':<14} {'Conn+Same':>12} {'Conn+Diff':>12} {'NonC+Same':>12} {'NonC+Diff':>12}")
    print(f"  {'-'*62}")

    for var, label in vol_wins:
        vals = []
        for gname, gfn in [
            ("CS", lambda d: d[(d["connected"] == 1) & (d["same_industry"] == 1)]),
            ("CD", lambda d: d[(d["connected"] == 1) & (d["same_industry"] == 0)]),
            ("NS", lambda d: d[(d["connected"] == 0) & (d["same_industry"] == 1)]),
            ("ND", lambda d: d[(d["connected"] == 0) & (d["same_industry"] == 0)]),
        ]:
            g = gfn(df).dropna(subset=[var])[var]
            if len(g) >= 10:
                vals.append(f"{g.mean():>10.4f}{sig(stats.ttest_1samp(g, 1)[1])}")
            else:
                vals.append(f"{'--':>12}")
        print(f"  {label:<14} {'  '.join(vals)}")

    # =====================================================================
    # TABLE C: Regression — log abnormal volume
    # =====================================================================
    print(f"\n  --- C. REGRESSION: log(abnormal volume) = connected + same_ind + conn x same ---")
    print(f"  Event-clustered")
    print(f"  {'Window':<14} {'connected':>12} {'p':>8} {'same_ind':>12} {'p':>8} {'conn x same':>12} {'p':>8}")
    print(f"  {'-'*74}")

    for var, label in vol_wins:
        logvar = f"{var}_log"
        s = df.dropna(subset=[logvar]).copy().reset_index(drop=True)
        if len(s) < 200:
            continue
        s["cx"] = s["connected"] * s["same_industry"]
        try:
            m = smf.ols(f"{logvar} ~ connected + same_industry + cx", data=s).fit(
                cov_type="cluster", cov_kwds={"groups": s["eid_str"]})
            c1 = m.params["connected"]; p1 = m.pvalues["connected"]
            c2 = m.params["same_industry"]; p2 = m.pvalues["same_industry"]
            c3 = m.params["cx"]; p3 = m.pvalues["cx"]
            print(f"  {label:<14} {c1:>+10.5f}{sig(p1)} {p1:>8.4f} {c2:>+10.5f}{sig(p2)} {p2:>8.4f} {c3:>+10.5f}{sig(p3)} {p3:>8.4f}")
        except Exception as e:
            print(f"  {label:<14} ERROR: {str(e)[:50]}")

    # =====================================================================
    # TABLE D: Do high-volume connected stocks also show high CARs?
    # =====================================================================
    print(f"\n  --- D. VOLUME-RETURN RELATIONSHIP (connected stocks only) ---")
    conn_only = df[df["connected"] == 1].copy()
    if len(conn_only) > 50:
        print(f"  Correlation between abnormal volume [-10,-1] and CAR[-10,-1]:")
        for vol_var, car_var in [("avol_10", "car_10"), ("avol_5", "car_5"), ("avol_30", "car_30")]:
            sub = conn_only.dropna(subset=[vol_var, car_var])
            if len(sub) > 20:
                r, p = stats.pearsonr(sub[vol_var], sub[car_var])
                print(f"    {vol_var} vs {car_var}: r={r:.4f}, p={p:.4f}{sig(p)}, N={len(sub)}")


print("\n\nDone.")
