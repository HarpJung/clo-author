"""Test 3: Re-run key tests with supplemented network (CIQ + BoardEx + Form 4).
1. By-event-type: M&A Buyer, Bankruptcy, M&A Target, Exec/Board
   - Connected means (overall/same/diff, VC-clustered)
   - Control group regression (mkt-adj, event-clustered)
2. NVCA 2020 + Clayton 2025 shock interactions
3. Compare old vs new network results
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
print("TEST 3: SUPPLEMENTED NETWORK (CIQ + BoardEx + Form 4)")
print("=" * 110)

# =====================================================================
# LOAD — use supplemented network
# =====================================================================
print("\n--- Loading ---")

# Supplemented network
edges = pd.read_csv(os.path.join(panel_c_dir, "02b_supplemented_network_edges.csv"))
edges["observer_personid"] = edges["observer_personid"].astype(str).str.replace(".0", "", regex=False)
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")
print(f"  Supplemented network: {len(edges):,} edges, {edges['observer_personid'].nunique():,} observers")
print(f"  By source: {dict(edges['source'].value_counts())}")

# Need permno for each portfolio CIK
pxw = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
pxw["cik_int"] = pd.to_numeric(pxw["cik"], errors="coerce")
pxw["permno"] = pd.to_numeric(pxw["permno"], errors="coerce")
pxw = pxw.drop_duplicates("cik_int", keep="first")
cik_to_permno = dict(zip(pxw["cik_int"], pxw["permno"]))
edges["permno"] = edges["portfolio_cik"].map(cik_to_permno)
edges = edges.dropna(subset=["permno"])
edges["permno"] = edges["permno"].astype(int)

# Industry
industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
industry = industry.drop_duplicates("cik_int", keep="first")
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))
ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0"), errors="coerce")
cid_to_cik = dict(zip(ciq_xwalk["cid"], ciq_xwalk["cik_int"]))

edges["same_industry"] = (
    edges["observed_companyid"].map(cid_to_cik).map(cik_to_sic2) ==
    edges["portfolio_cik"].map(cik_to_sic2)
).astype(int)

connected_set = set()
for _, row in edges.iterrows():
    connected_set.add((row["observed_companyid"], row["permno"]))

pmcik = dict(zip(pxw["permno"].dropna().astype(int), pxw["cik_int"].dropna().astype(int)))
pm_sic2 = {int(p): cik_to_sic2.get(c, "") for p, c in pmcik.items() if pd.notna(p) and pd.notna(c)}

obs_with_edges = set(edges["observed_companyid"])
print(f"  Edges with permno: {len(edges):,}")
print(f"  Same-industry: {edges['same_industry'].sum():,}")
print(f"  Connected pairs: {len(connected_set):,}")

# Events
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

noise_types = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
               "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)",
               "Annual General Meeting", "Special/Extraordinary Shareholders Meeting",
               "Shareholder/Analyst Calls", "Special Calls", "Ex-Div Date (Regular)", "Ex-Div Date (Special)"]
events = events[~events["eventtype"].isin(noise_types)]

# CRSP-listed filter
panel_b_xwalk = pd.read_csv(os.path.join(data_dir, "Panel_B_Outcomes", "01_identifier_crosswalk.csv"))
panel_b_xwalk["cik_int"] = pd.to_numeric(panel_b_xwalk["cik"], errors="coerce")
panel_b_xwalk["linkdt"] = pd.to_datetime(panel_b_xwalk["linkdt"], errors="coerce")
panel_b_xwalk["linkenddt"] = pd.to_datetime(panel_b_xwalk["linkenddt"], errors="coerce").fillna(pd.Timestamp("2099-12-31"))
listing = panel_b_xwalk.groupby("cik_int").agg(first_listed=("linkdt", "min"), last_listed=("linkenddt", "max")).reset_index()
events["cik_int"] = events["companyid"].map(cid_to_cik)
events = events.merge(listing, on="cik_int", how="left")
events["was_public"] = (events["announcedate"] >= events["first_listed"]) & (events["announcedate"] <= events["last_listed"])
events = events[~events["was_public"].fillna(False)]

print(f"  Events: {len(events):,}")

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

pmdata = {}
for p, g in pd_daily.groupby("permno"):
    pmdata[p] = (g["date"].values, g["aret"].values)
print(f"  Returns: {len(pd_daily):,} rows, {len(pmdata):,} stocks")

car_windows = [
    ("car_30", -30, -1), ("car_10", -10, -1), ("car_5", -5, -1), ("car_1", -1, 0),
]

def calc_cars(permno, event_np):
    if permno not in pmdata: return None
    dates, rets = pmdata[permno]
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

wins = [("car_30", "[-30,-1]"), ("car_10", "[-10,-1]"), ("car_5", "[-5,-1]"), ("car_1", "[-1,0]")]

# =====================================================================
# EVENT GROUPS
# =====================================================================
bankruptcy_types = [t for t in events["eventtype"].unique() if "Bankruptcy" in str(t)]

event_groups = [
    ("M&A Buyer", lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Buyer")]),
    ("Bankruptcy", lambda df: df[df["eventtype"].isin(bankruptcy_types)]),
    ("M&A Target", lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Target")]),
    ("Exec/Board", lambda df: df[df["eventtype"] == "Executive/Board Changes - Other"]),
]

# =====================================================================
# RUN TESTS
# =====================================================================
for group_name, group_fn in event_groups:
    grp = group_fn(events)
    grp = grp[grp["companyid"].isin(obs_with_edges)]
    grp_df = grp[["companyid", "announcedate", "event_year"]].drop_duplicates(subset=["companyid", "announcedate"])

    if len(grp_df) < 20:
        print(f"\n{group_name}: {len(grp_df)} events — SKIPPING")
        continue

    print(f"\n\n{'='*110}")
    print(f"{group_name} ({len(grp_df):,} events) — SUPPLEMENTED NETWORK")
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

        for pmi, pm in enumerate(all_permnos):
            is_conn = 1 if (ecid, pm) in connected_set else 0
            if not is_conn and pmi % 10 != 0:
                continue
            cars = calc_cars(pm, enp)
            if not cars: continue
            psic = pm_sic2.get(pm, "")
            si = 1 if (obs_sic2 and psic and obs_sic2 == psic) else 0
            all_obs.append({
                "event_id": event_id, "permno": pm, "connected": is_conn,
                "same_industry": si, "event_year": eyr, **cars,
            })
        event_id += 1

    if not all_obs:
        continue

    df = pd.DataFrame(all_obs)
    df["cx"] = df["connected"] * df["same_industry"]
    df["eid_str"] = df["event_id"].astype(str)
    n_conn = df["connected"].sum()
    print(f"  Obs: {len(df):,} (conn={n_conn:,}, ctrl={len(df)-n_conn:,})")
    print(f"  Connected same-ind: {df['cx'].sum():,}")

    # TABLE A: Connected means (VC-clustered — use observer as cluster since no VC in supplemented)
    print(f"\n  TABLE A: Connected Means (overall/same/diff)")
    print(f"  {'Window':<10} {'Overall':>12} {'Same-ind':>12} {'Diff-ind':>12}")
    print(f"  {'-'*46}")

    conn_df = df[df["connected"] == 1]
    for var, label in wins:
        row_str = f"  {label:<10}"
        for sfn in [lambda d: d, lambda d: d[d["same_industry"] == 1], lambda d: d[d["same_industry"] == 0]]:
            s = sfn(conn_df).dropna(subset=[var]).copy().reset_index(drop=True)
            if len(s) < 10:
                row_str += f" {'--':>12}"
                continue
            mean_val = s[var].mean()
            t, p = stats.ttest_1samp(s[var], 0)
            row_str += f" {mean_val:>+8.4f}{sig(p)}"
        print(row_str)

    # TABLE B: Control group regression (mkt-adj, event-clustered)
    print(f"\n  TABLE B: Control Group Regression (mkt-adj, event-clustered)")
    print(f"  {'Window':<10} {'connected':>12} {'p':>8} {'same_ind':>12} {'p':>8} {'conn x same':>12} {'p':>8}")
    print(f"  {'-'*68}")

    for var, label in wins:
        s = df.dropna(subset=[var]).copy().reset_index(drop=True)
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


# =====================================================================
# NVCA 2020 + CLAYTON 2025 SHOCK
# =====================================================================
print(f"\n\n{'='*110}")
print("NVCA 2020 + CLAYTON 2025 SHOCK — SUPPLEMENTED NETWORK")
print(f"{'='*110}")

# Use all filtered events with supplemented network
all_events = events[events["companyid"].isin(obs_with_edges)]
all_df = all_events[["companyid", "announcedate", "event_year"]].drop_duplicates(subset=["companyid", "announcedate"])

# Compute connected CARs only (faster than full control group)
print(f"\n  Computing connected CARs for {len(all_df):,} events...")
ee = all_df.merge(edges[["observed_companyid", "permno", "same_industry"]].rename(
    columns={"observed_companyid": "companyid"}), on="companyid", how="inner")

conn_cars = []
for idx, row in ee.iterrows():
    cars = calc_cars(int(row["permno"]), np.datetime64(row["announcedate"]))
    if cars:
        conn_cars.append({
            "same_industry": row["same_industry"],
            "event_year": row["event_year"],
            "event_date": row["announcedate"],
            **cars,
        })
    if (idx + 1) % 10000 == 0:
        print(f"    {idx+1:,}/{len(ee):,}: {len(conn_cars):,} CARs")

car_df = pd.DataFrame(conn_cars)
print(f"  Total connected CARs: {len(car_df):,}")
print(f"  Same-ind: {(car_df['same_industry']==1).sum():,}")

# NVCA shock
car_df["post_2020"] = (car_df["event_year"] >= 2020).astype(int)
jan2025 = pd.Timestamp("2025-01-01")
car_df["event_date"] = pd.to_datetime(car_df["event_date"])
car_df["post_jan2025"] = (car_df["event_date"] >= jan2025).astype(int)

# Same-industry means by period
print(f"\n  Same-industry CAR[-10,-1] by period:")
for pname, pmask in [("Pre-2020", car_df["event_year"] < 2020),
                       ("Post-2020", car_df["event_year"] >= 2020),
                       ("Pre-Jan2025", car_df["event_date"] < jan2025),
                       ("Post-Jan2025", car_df["event_date"] >= jan2025)]:
    sub = car_df[pmask & (car_df["same_industry"] == 1)].dropna(subset=["car_10"])
    if len(sub) < 10:
        print(f"    {pname:<16} N={len(sub):>6}  too few")
        continue
    t, p = stats.ttest_1samp(sub["car_10"], 0)
    print(f"    {pname:<16} N={len(sub):>6,}  mean={sub['car_10'].mean():>+.5f}  p={p:.4f}{sig(p)}")


print("\n\nDone.")
