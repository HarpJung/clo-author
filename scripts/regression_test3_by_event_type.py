"""Test 3: By event type — CIQ Key Dev events.
For each event type, run:
- Connected-only means (overall/same/diff, VC-clustered)
- Market-adjusted connected + same_ind + conn x same (event-clustered)
- Event-level collapse (market-adjusted)

Event types: Executive/Board Changes, Strategic Alliances,
Seeking Financing, Bankruptcy, Lawsuits, Downsizings
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
print("TEST 3: BY EVENT TYPE")
print("=" * 110)

# =====================================================================
# LOAD DATA
# =====================================================================
print("\n--- Loading ---")
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
ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0"), errors="coerce")
cid_to_cik = dict(zip(ciq_xwalk["cid"], ciq_xwalk["cik_int"]))
edges["same_industry"] = (edges["observed_companyid"].map(cid_to_cik).map(cik_to_sic2) ==
                           edges["portfolio_cik_int"].map(cik_to_sic2)).astype(int)

connected_set = set()
for _, row in edges.iterrows():
    connected_set.add((row["observed_companyid"], int(row["permno"])))

pmcik = dict(zip(pxw["permno"].dropna().astype(int), pxw["cik_int"].dropna().astype(int)))
pm_sic2 = {p: cik_to_sic2.get(c, "") for p, c in pmcik.items()}

# Load events with event type
events = pd.read_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
events["cid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
pub = set()
with open(os.path.join(ciq_dir, "04_observer_company_details.csv"), "r", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        cid = str(r.get("companyid", "")).strip().replace(".0", "")
        if "public" in str(r.get("companytypename", "")).lower():
            pub.add(cid)
events = events[~events["cid"].isin(pub)]
events["event_date"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["event_date"])
events["event_year"] = events["event_date"].dt.year
events = events[(events["event_year"] >= 2015) & (events["event_year"] <= 2025)]
events = events[events["keydeveventtypename"] != "Announcements of Earnings"]

# CRSP filter
panel_b_xwalk = pd.read_csv(os.path.join(data_dir, "Panel_B_Outcomes", "01_identifier_crosswalk.csv"))
panel_b_xwalk["cik_int"] = pd.to_numeric(panel_b_xwalk["cik"], errors="coerce")
panel_b_xwalk["linkdt"] = pd.to_datetime(panel_b_xwalk["linkdt"], errors="coerce")
panel_b_xwalk["linkenddt"] = pd.to_datetime(panel_b_xwalk["linkenddt"], errors="coerce").fillna(pd.Timestamp("2099-12-31"))
listing = panel_b_xwalk.groupby("cik_int").agg(first_listed=("linkdt", "min"), last_listed=("linkenddt", "max")).reset_index()
events["cik_int"] = events["cid"].map(cid_to_cik)
events = events.merge(listing, on="cik_int", how="left")
events["was_public"] = (events["event_date"] >= events["first_listed"]) & (events["event_date"] <= events["last_listed"])
events = events[~events["was_public"].fillna(False)]

obs_with_edges = set(edges["observed_companyid"])
events = events[events["cid"].isin(obs_with_edges)]

print(f"  Events by type:")
for et, n in events["keydeveventtypename"].value_counts().items():
    print(f"    {et:<45} {n:>6,}")

# Load returns + market return
pd_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
pd_daily["date"] = pd.to_datetime(pd_daily["date"])
pd_daily["permno"] = pd.to_numeric(pd_daily["permno"], errors="coerce").dropna().astype(int)
pd_daily["ret"] = pd.to_numeric(pd_daily["ret"], errors="coerce")
pd_daily = pd_daily.dropna(subset=["ret"]).sort_values(["permno", "date"])
all_permnos = sorted(pd_daily["permno"].unique())

mkt_ret = pd_daily.groupby("date")["ret"].mean().rename("mkt_ret")
pd_daily = pd_daily.merge(mkt_ret, on="date", how="left")
pd_daily["aret"] = pd_daily["ret"] - pd_daily["mkt_ret"]

pmdata_raw = {}
pmdata_adj = {}
for p, g in pd_daily.groupby("permno"):
    pmdata_raw[p] = (g["date"].values, g["ret"].values)
    pmdata_adj[p] = (g["date"].values, g["aret"].values)

car_windows = [
    ("car_30", -30, -1), ("car_20", -20, -1), ("car_15", -15, -1),
    ("car_10", -10, -1), ("car_5", -5, -1), ("car_3", -3, -1),
    ("car_2", -2, -1), ("car_1", -1, 0), ("car_post3", 0, 3), ("car_post5", 0, 5),
]


def calc_cars(permno, event_np, data_dict):
    if permno not in data_dict: return None
    dates, rets = data_dict[permno]
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


wins = [("car_30", "[-30,-1]"), ("car_20", "[-20,-1]"), ("car_15", "[-15,-1]"),
        ("car_10", "[-10,-1]"), ("car_5", "[-5,-1]"), ("car_3", "[-3,-1]"),
        ("car_2", "[-2,-1]"), ("car_1", "[-1,0]"), ("car_post3", "[0,+3]"), ("car_post5", "[0,+5]")]


# =====================================================================
# RUN BY EVENT TYPE
# =====================================================================
event_types = events["keydeveventtypename"].value_counts()
# Only run for types with >= 50 events
event_types = event_types[event_types >= 50]

for et_name, et_count in event_types.items():
    print(f"\n\n{'='*110}")
    print(f"EVENT TYPE: {et_name} (N={et_count:,} events)")
    print(f"{'='*110}")

    et_events = events[events["keydeveventtypename"] == et_name]
    et_df = et_events[["cid", "event_date", "event_year"]].drop_duplicates()

    # Compute connected CARs
    et_ee = et_df.merge(edges, left_on="cid", right_on="observed_companyid", how="inner")

    conn_results = []
    for _, row in et_ee.iterrows():
        cars = calc_cars(int(row["permno"]), np.datetime64(row["event_date"]), pmdata_raw)
        cars_adj = calc_cars(int(row["permno"]), np.datetime64(row["event_date"]), pmdata_adj)
        if cars:
            r = {
                "vc_firm": str(row.get("vc_firm_companyid", "")),
                "same_industry": row["same_industry"],
                "event_year": row["event_year"],
                "connected": 1,
            }
            for wn, _, _ in car_windows:
                r[wn] = cars.get(wn, np.nan)
                r[f"{wn}_adj"] = cars_adj.get(wn, np.nan) if cars_adj else np.nan
            conn_results.append(r)

    if not conn_results:
        print("  No connected CARs computed")
        continue

    conn_df = pd.DataFrame(conn_results)
    conn_df = conn_df[conn_df["vc_firm"] != ""].reset_index(drop=True)
    n_same = (conn_df["same_industry"] == 1).sum()
    n_diff = (conn_df["same_industry"] == 0).sum()
    print(f"  Connected CARs: {len(conn_df):,} (same={n_same:,}, diff={n_diff:,})")

    # --- TABLE A: Connected means (VC-clustered) ---
    print(f"\n  TABLE A: Connected Means (VC-clustered)")
    print(f"  {'Window':<10} {'Overall':>12} {'Same-ind':>12} {'Diff-ind':>12}")
    print(f"  {'-'*46}")

    for var, label in wins:
        row_str = f"  {label:<10}"
        for sname, sfn in [("all", lambda d: d),
                            ("same", lambda d: d[d["same_industry"] == 1]),
                            ("diff", lambda d: d[d["same_industry"] == 0])]:
            s = sfn(conn_df).dropna(subset=[var]).copy().reset_index(drop=True)
            if len(s) < 10:
                row_str += f" {'--':>12}"
                continue
            try:
                m = smf.ols(f"{var} ~ 1", data=s).fit(
                    cov_type="cluster", cov_kwds={"groups": s["vc_firm"]})
                p = m.pvalues["Intercept"]
                mean_val = s[var].mean()
                row_str += f" {mean_val:>+8.4f}{sig(p)}"
            except:
                row_str += f" {'ERR':>12}"
        print(row_str)

    # --- TABLE B: Market-adjusted, event-clustered control group ---
    # Only if we have enough events (>= 30)
    if len(et_df) >= 30:
        print(f"\n  TABLE B: Control Group Regression (Mkt-adj, Event-clustered)")

        # Compute control CARs for this event type
        ctrl_results = []
        event_id = 0
        evl = et_df.to_dict("records")

        for ei, ev in enumerate(evl):
            enp = np.datetime64(ev["event_date"])
            ecid = ev["cid"]
            obs_cik = cid_to_cik.get(ecid)
            obs_sic2 = cik_to_sic2.get(obs_cik, "") if obs_cik else ""

            # Sample non-connected stocks (take every 5th to keep manageable)
            for pmi, pm in enumerate(all_permnos):
                if (ecid, pm) in connected_set:
                    # Always include connected
                    pass
                elif pmi % 5 != 0:
                    continue  # sample 20% of non-connected for speed

                cars_adj = calc_cars(pm, enp, pmdata_adj)
                if not cars_adj: continue
                is_conn = 1 if (ecid, pm) in connected_set else 0
                psic = pm_sic2.get(pm, "")
                si = 1 if (obs_sic2 and psic and obs_sic2 == psic) else 0
                r = {"event_id": event_id, "connected": is_conn, "same_industry": si}
                for wn, _, _ in car_windows:
                    r[f"{wn}_adj"] = cars_adj.get(wn, np.nan)
                ctrl_results.append(r)
            event_id += 1

        if ctrl_results:
            ctrl_df = pd.DataFrame(ctrl_results)
            ctrl_df["event_id_str"] = ctrl_df["event_id"].astype(str)
            n_conn_ctrl = ctrl_df["connected"].sum()
            n_nonconn_ctrl = len(ctrl_df) - n_conn_ctrl

            print(f"  Obs: {len(ctrl_df):,} (conn={n_conn_ctrl:,}, ctrl={n_nonconn_ctrl:,}, events={ctrl_df['event_id'].nunique():,})")
            print(f"  {'Window':<10} {'connected':>12} {'p':>8} {'same_ind':>12} {'p':>8} {'conn x same':>12} {'p':>8}")
            print(f"  {'-'*68}")

            for var, label in wins:
                adjvar = f"{var}_adj"
                s = ctrl_df.dropna(subset=[adjvar, "connected", "same_industry"]).copy().reset_index(drop=True)
                if len(s) < 200: continue
                s["cx"] = s["connected"] * s["same_industry"]
                try:
                    m = smf.ols(f"{adjvar} ~ connected + same_industry + cx", data=s).fit(
                        cov_type="cluster", cov_kwds={"groups": s["event_id_str"]})
                    c1 = m.params["connected"]; p1 = m.pvalues["connected"]
                    c2 = m.params["same_industry"]; p2 = m.pvalues["same_industry"]
                    c3 = m.params["cx"]; p3 = m.pvalues["cx"]
                    print(f"  {label:<10} {c1:>+10.5f}{sig(p1)} {p1:>8.4f} {c2:>+10.5f}{sig(p2)} {p2:>8.4f} {c3:>+10.5f}{sig(p3)} {p3:>8.4f}")
                except Exception as e:
                    print(f"  {label:<10} ERROR: {str(e)[:50]}")


print("\n\nDone.")
