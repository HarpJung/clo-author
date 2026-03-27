"""Test 3: Expanded events from full CIQ pull.
Groups: Distress, M&A Target, M&A Buyer, CEO/CFO, Bankruptcy, Private Placements
For each: connected means (overall/same/diff) + control group regression (mkt-adj, event-cl)
Filters: private companies, not CRSP-listed, 2015-2025
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
print("TEST 3: EXPANDED EVENTS BY CATEGORY")
print("=" * 110)

# =====================================================================
# LOAD
# =====================================================================
print("\n--- Loading ---")

# Full events with role types
events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])
events["event_year"] = events["announcedate"].dt.year
events = events[(events["event_year"] >= 2015) & (events["event_year"] <= 2025)]

# Filter: private companies only
pub_cids = set()
with open(os.path.join(ciq_dir, "04_observer_company_details.csv"), "r", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        cid = str(r.get("companyid", "")).strip().replace(".0", "")
        if "public" in str(r.get("companytypename", "")).lower():
            pub_cids.add(cid)
events = events[~events["companyid"].isin(pub_cids)]

# Filter: not CRSP-listed at event date
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

# Drop earnings and conferences (noise)
noise_types = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
               "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)",
               "Annual General Meeting", "Special/Extraordinary Shareholders Meeting",
               "Shareholder/Analyst Calls", "Special Calls", "Ex-Div Date (Regular)", "Ex-Div Date (Special)"]
events = events[~events["eventtype"].isin(noise_types)]

print(f"  Filtered events: {len(events):,}, Companies: {events['companyid'].nunique():,}")

# Network
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

# Returns + market adjustment
pd_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
pd_daily["date"] = pd.to_datetime(pd_daily["date"])
pd_daily["permno"] = pd.to_numeric(pd_daily["permno"], errors="coerce").dropna().astype(int)
pd_daily["ret"] = pd.to_numeric(pd_daily["ret"], errors="coerce")
pd_daily = pd_daily.dropna(subset=["ret"]).sort_values(["permno", "date"])
mkt_ret = pd_daily.groupby("date")["ret"].mean().rename("mkt_ret")
pd_daily = pd_daily.merge(mkt_ret, on="date", how="left")
pd_daily["aret"] = pd_daily["ret"] - pd_daily["mkt_ret"]
all_permnos = sorted(pd_daily["permno"].unique())

pmdata_adj = {}
for p, g in pd_daily.groupby("permno"):
    pmdata_adj[p] = (g["date"].values, g["aret"].values)
print(f"  Stocks: {len(pmdata_adj):,}")

car_windows = [
    ("car_30", -30, -1), ("car_20", -20, -1), ("car_10", -10, -1),
    ("car_5", -5, -1), ("car_3", -3, -1), ("car_1", -1, 0),
    ("car_post5", 0, 5),
]

def calc_cars(permno, event_np):
    if permno not in pmdata_adj: return None
    dates, rets = pmdata_adj[permno]
    if len(dates) < 30: return None
    diffs = (dates - event_np).astype("timedelta64[D]").astype(int)
    cars = {}
    for wn, d0, d1 in car_windows:
        mask = (diffs >= d0) & (diffs <= d1)
        wr = rets[mask]
        if len(wr) >= max(2, abs(d1 - d0) * 0.3):
            cars[wn] = float(np.sum(wr))
    return cars if cars else None

def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "

wins = [("car_30", "[-30,-1]"), ("car_20", "[-20,-1]"), ("car_10", "[-10,-1]"),
        ("car_5", "[-5,-1]"), ("car_3", "[-3,-1]"), ("car_1", "[-1,0]"),
        ("car_post5", "[0,+5]")]

# =====================================================================
# DEFINE EVENT GROUPS
# =====================================================================
distress_types = [
    "Bankruptcy - Filing", "Bankruptcy - Other", "Bankruptcy - Reorganization",
    "Bankruptcy - Asset Sale/Liquidation", "Bankruptcy - Financing",
    "Bankruptcy - Emergence/Exit", "Bankruptcy - Conclusion",
    "Auditor Going Concern Doubts", "Impairments/Write Offs",
    "Delayed SEC Filings", "Delistings", "Discontinued Operations/Downsizings",
    "Restatements of Operating Results", "Debt Defaults",
    "Halt/Resume of Operations - Unusual Events", "Business Reorganizations",
    "Regulatory Authority - Enforcement Actions", "Labor-related Announcements",
    "Lawsuits & Legal Issues", "Considering Multiple Strategic Alternatives",
    "Corporate Guidance - Lowered",
]
# Fix unicode dash variants
distress_types_fix = []
for t in distress_types:
    distress_types_fix.append(t)
    distress_types_fix.append(t.replace(" - ", " – "))
    distress_types_fix.append(t.replace(" – ", " - "))

bankruptcy_types = [t for t in events["eventtype"].unique() if "Bankruptcy" in str(t)]

event_groups = [
    ("All Distress", lambda df: df[df["eventtype"].isin(distress_types_fix)]),
    ("Bankruptcy (all)", lambda df: df[df["eventtype"].isin(bankruptcy_types)]),
    ("M&A Target", lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Target")]),
    ("M&A Buyer", lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Buyer")]),
    ("CEO/CFO Changes", lambda df: df[df["eventtype"].isin(["Executive Changes - CEO", "Executive Changes - CFO"])]),
    ("Private Placements", lambda df: df[df["eventtype"] == "Private Placements"]),
    ("Seeking to Sell", lambda df: df[df["eventtype"].isin(["Seeking to Sell/Divest", "Considering Multiple Strategic Alternatives"])]),
    ("Exec/Board Changes", lambda df: df[df["eventtype"] == "Executive/Board Changes - Other"]),
    ("Product/Client", lambda df: df[df["eventtype"].isin(["Product-Related Announcements", "Client Announcements"])]),
]

# =====================================================================
# RUN TESTS
# =====================================================================
for group_name, group_fn in event_groups:
    grp = group_fn(events)
    grp = grp[grp["companyid"].isin(obs_with_edges)]
    grp_df = grp[["companyid", "announcedate", "event_year", "eventtype", "objectroletype"]].drop_duplicates(
        subset=["companyid", "announcedate"])

    if len(grp_df) < 20:
        print(f"\n{'='*110}")
        print(f"{group_name}: only {len(grp_df)} events — SKIPPING")
        continue

    print(f"\n{'='*110}")
    print(f"{group_name} ({len(grp_df):,} events, {grp_df['companyid'].nunique():,} companies)")
    print(f"{'='*110}")

    # Compute connected CARs
    grp_ee = grp_df.merge(edges, left_on="companyid", right_on="observed_companyid", how="inner")
    conn_results = []
    event_id = 0
    eid_map = {}

    for _, row in grp_ee.iterrows():
        ekey = (row["companyid"], str(row["announcedate"]))
        if ekey not in eid_map:
            eid_map[ekey] = event_id
            event_id += 1
        cars = calc_cars(int(row["permno"]), np.datetime64(row["announcedate"]))
        if cars:
            conn_results.append({
                "vc_firm": str(row.get("vc_firm_companyid", "")),
                "same_industry": row["same_industry"],
                "event_id": eid_map[ekey],
                "connected": 1,
                **cars,
            })

    if not conn_results:
        print("  No connected CARs")
        continue

    conn_df = pd.DataFrame(conn_results)
    conn_df = conn_df[conn_df["vc_firm"] != ""].reset_index(drop=True)
    n_same = (conn_df["same_industry"] == 1).sum()
    print(f"  Connected CARs: {len(conn_df):,} (same={n_same:,}, diff={len(conn_df)-n_same:,})")

    # TABLE A: Connected means
    print(f"\n  TABLE A: Connected Means (VC-clustered, market-adjusted)")
    print(f"  {'Window':<10} {'Overall':>12} {'Same-ind':>12} {'Diff-ind':>12}")
    print(f"  {'-'*46}")

    for var, label in wins:
        row_str = f"  {label:<10}"
        for sfn in [lambda d: d, lambda d: d[d["same_industry"] == 1], lambda d: d[d["same_industry"] == 0]]:
            s = sfn(conn_df).dropna(subset=[var]).copy().reset_index(drop=True)
            if len(s) < 10:
                row_str += f" {'--':>12}"
                continue
            try:
                m = smf.ols(f"{var} ~ 1", data=s).fit(
                    cov_type="cluster", cov_kwds={"groups": s["vc_firm"]})
                p = m.pvalues["Intercept"]
                row_str += f" {s[var].mean():>+8.4f}{sig(p)}"
            except:
                row_str += f" {'ERR':>12}"
        print(row_str)

    # TABLE B: Control group regression (sample 20% of non-connected for speed)
    if len(grp_df) >= 30:
        ctrl_results = []
        evl = grp_df.to_dict("records")
        for ei, ev in enumerate(evl):
            enp = np.datetime64(ev["announcedate"])
            ecid = ev["companyid"]
            obs_cik = cid_to_cik.get(ecid)
            obs_sic2 = cik_to_sic2.get(obs_cik, "") if obs_cik else ""
            eid = eid_map.get((ecid, str(ev["announcedate"])), -1)

            for pmi, pm in enumerate(all_permnos):
                if (ecid, pm) in connected_set:
                    cars = calc_cars(pm, enp)
                    if cars:
                        psic = pm_sic2.get(pm, "")
                        si = 1 if (obs_sic2 and psic and obs_sic2 == psic) else 0
                        ctrl_results.append({"event_id": eid, "connected": 1, "same_industry": si, **cars})
                elif pmi % 10 == 0:  # 10% sample
                    cars = calc_cars(pm, enp)
                    if cars:
                        psic = pm_sic2.get(pm, "")
                        si = 1 if (obs_sic2 and psic and obs_sic2 == psic) else 0
                        ctrl_results.append({"event_id": eid, "connected": 0, "same_industry": si, **cars})

        if ctrl_results:
            ctrl_df = pd.DataFrame(ctrl_results)
            ctrl_df["eid_str"] = ctrl_df["event_id"].astype(str)
            n_conn = ctrl_df["connected"].sum()
            n_ctrl = len(ctrl_df) - n_conn

            print(f"\n  TABLE B: Control Group (Mkt-adj, Event-clustered)")
            print(f"  Obs: {len(ctrl_df):,} (conn={n_conn:,}, ctrl={n_ctrl:,})")
            print(f"  {'Window':<10} {'connected':>12} {'p':>8} {'same_ind':>12} {'p':>8} {'conn x same':>12} {'p':>8}")
            print(f"  {'-'*68}")

            for var, label in wins:
                s = ctrl_df.dropna(subset=[var]).copy().reset_index(drop=True)
                if len(s) < 200: continue
                s["cx"] = s["connected"] * s["same_industry"]
                try:
                    m = smf.ols(f"{var} ~ connected + same_industry + cx", data=s).fit(
                        cov_type="cluster", cov_kwds={"groups": s["eid_str"]})
                    c1 = m.params["connected"]; p1 = m.pvalues["connected"]
                    c2 = m.params["same_industry"]; p2 = m.pvalues["same_industry"]
                    c3 = m.params["cx"]; p3 = m.pvalues["cx"]
                    print(f"  {label:<10} {c1:>+10.5f}{sig(p1)} {p1:>8.4f} {c2:>+10.5f}{sig(p2)} {p2:>8.4f} {c3:>+10.5f}{sig(p3)} {p3:>8.4f}")
                except Exception as e:
                    print(f"  {label:<10} ERROR: {str(e)[:50]}")


print("\n\nDone.")
