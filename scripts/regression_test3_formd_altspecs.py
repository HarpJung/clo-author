"""Test 3 Form D: Alternative specifications.
1) Market-adjusted CARs (subtract value-weighted market return)
2) Event-level collapse (average within event x group cells)
3) Event-level spread (connected minus non-connected per event)
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv, glob, time
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")
formd_dir = os.path.join(data_dir, "FormD")

print("=" * 110)
print("TEST 3 FORM D: ALTERNATIVE SPECIFICATIONS")
print("=" * 110)

# =====================================================================
# LOAD DATA (same pipeline, abbreviated)
# =====================================================================
print("\n--- Loading ---")
formd_events = []
for qdir in sorted(glob.glob(os.path.join(formd_dir, "20*"))):
    subdirs = glob.glob(os.path.join(qdir, "*_d"))
    if not subdirs: continue
    ddir = subdirs[0]
    sf = os.path.join(ddir, "FORMDSUBMISSION.tsv")
    isf = os.path.join(ddir, "ISSUERS.tsv")
    if not os.path.exists(sf) or not os.path.exists(isf): continue
    try:
        sub = pd.read_csv(sf, sep="\t", dtype=str, low_memory=False)
        iss = pd.read_csv(isf, sep="\t", dtype=str, low_memory=False)
        m = sub[["ACCESSIONNUMBER", "FILING_DATE"]].merge(
            iss[iss["IS_PRIMARYISSUER_FLAG"] == "YES"][["ACCESSIONNUMBER", "CIK"]],
            on="ACCESSIONNUMBER", how="inner")
        formd_events.append(m)
    except:
        pass

fd = pd.concat(formd_events, ignore_index=True)
fd["FILING_DATE"] = pd.to_datetime(fd["FILING_DATE"], format="mixed", dayfirst=False, errors="coerce")
fd["CIK"] = fd["CIK"].astype(str).str.strip().str.lstrip("0")
fd = fd.dropna(subset=["FILING_DATE"])
fd["event_year"] = fd["FILING_DATE"].dt.year
fd = fd[(fd["event_year"] >= 2015) & (fd["event_year"] <= 2025)]

ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_str"] = ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0")
cik_to_cid = dict(zip(ciq_xwalk["cik_str"], ciq_xwalk["cid"]))
fd["cid"] = fd["CIK"].map(cik_to_cid)
fd = fd.dropna(subset=["cid"])

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
cxn = ciq_xwalk.copy()
cxn["cik_int"] = pd.to_numeric(cxn["cik_str"], errors="coerce")
cid_to_cik = dict(zip(cxn["cid"], cxn["cik_int"]))
edges["same_industry"] = (edges["observed_companyid"].map(cid_to_cik).map(cik_to_sic2) ==
                           edges["portfolio_cik_int"].map(cik_to_sic2)).astype(int)

connected_set = set()
for _, row in edges.iterrows():
    connected_set.add((row["observed_companyid"], int(row["permno"])))

pmcik = dict(zip(pxw["permno"].dropna().astype(int), pxw["cik_int"].dropna().astype(int)))
pm_sic2 = {p: cik_to_sic2.get(c, "") for p, c in pmcik.items()}

obs_with_edges = set(edges["observed_companyid"])
fd = fd[fd["cid"].isin(obs_with_edges)]
events_df = fd[["cid", "FILING_DATE", "event_year"]].drop_duplicates(subset=["cid", "FILING_DATE"])
ee = events_df.merge(edges, left_on="cid", right_on="observed_companyid", how="inner")
print(f"  Events: {len(events_df):,}, Edge pairs: {len(ee):,}")

# Load returns
pd_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
pd_daily["date"] = pd.to_datetime(pd_daily["date"])
pd_daily["permno"] = pd.to_numeric(pd_daily["permno"], errors="coerce").dropna().astype(int)
pd_daily["ret"] = pd.to_numeric(pd_daily["ret"], errors="coerce")
pd_daily = pd_daily.dropna(subset=["ret"]).sort_values(["permno", "date"])
all_permnos = sorted(pd_daily["permno"].unique())

# Compute daily market return (equal-weighted across all portfolio stocks)
mkt_ret = pd_daily.groupby("date")["ret"].mean().rename("mkt_ret")
pd_daily = pd_daily.merge(mkt_ret, on="date", how="left")
pd_daily["aret"] = pd_daily["ret"] - pd_daily["mkt_ret"]  # abnormal return

pmdata_raw = {}
pmdata_adj = {}
for p, g in pd_daily.groupby("permno"):
    pmdata_raw[p] = (g["date"].values, g["ret"].values)
    pmdata_adj[p] = (g["date"].values, g["aret"].values)
print(f"  Stocks: {len(pmdata_raw):,}, Market return computed")

# =====================================================================
# CAR COMPUTATION
# =====================================================================
car_windows = [
    ("car_30", -30, -1), ("car_20", -20, -1), ("car_15", -15, -1),
    ("car_10", -10, -1), ("car_5", -5, -1), ("car_3", -3, -1),
    ("car_2", -2, -1), ("car_1", -1, 0), ("car_post3", 0, 3), ("car_post5", 0, 5),
]


def calc_cars(permno, event_np, data_dict):
    if permno not in data_dict:
        return None
    dates, rets = data_dict[permno]
    if len(dates) < 30:
        return None
    diffs = (dates - event_np).astype("timedelta64[D]").astype(int)
    cars = {}
    for wn, d0, d1 in car_windows:
        mask = (diffs >= d0) & (diffs <= d1)
        wr = rets[mask]
        if len(wr) >= max(2, abs(d1 - d0) * 0.3):
            cars[wn] = float(np.sum(wr))
    return cars if cars else None


# Compute both raw and market-adjusted CARs for connected + control
print("\n--- Computing CARs (connected + control, raw + market-adjusted) ---")
t0 = time.time()
all_results = []
evlist = events_df.to_dict("records")
event_id = 0

for ei, ev in enumerate(evlist):
    enp = np.datetime64(ev["FILING_DATE"])
    ecid = ev["cid"]
    eyr = ev["event_year"]
    obs_cik = cid_to_cik.get(ecid)
    obs_sic2 = cik_to_sic2.get(obs_cik, "") if obs_cik else ""

    for pm in all_permnos:
        is_conn = 1 if (ecid, pm) in connected_set else 0
        cars_raw = calc_cars(pm, enp, pmdata_raw)
        cars_adj = calc_cars(pm, enp, pmdata_adj)
        if not cars_raw:
            continue

        psic = pm_sic2.get(pm, "")
        si = 1 if (obs_sic2 and psic and obs_sic2 == psic) else 0

        row = {
            "event_id": event_id,
            "permno": pm,
            "connected": is_conn,
            "same_industry": si,
            "event_year": eyr,
        }
        for wn, _, _ in car_windows:
            row[wn] = cars_raw.get(wn, np.nan)
            row[f"{wn}_adj"] = cars_adj.get(wn, np.nan) if cars_adj else np.nan
        all_results.append(row)

    event_id += 1
    if (ei + 1) % 100 == 0:
        el = time.time() - t0
        rm = (len(evlist) - ei - 1) / (ei + 1) * el / 60
        print(f"    Event {ei+1:,}/{len(evlist):,} | {len(all_results):,} obs | ~{rm:.0f}min")

df = pd.DataFrame(all_results)
print(f"\n  Total obs: {len(df):,}")
print(f"  Connected: {df['connected'].sum():,}, Control: {(df['connected']==0).sum():,}")


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


wins = [("car_30", "[-30,-1]"), ("car_20", "[-20,-1]"), ("car_15", "[-15,-1]"),
        ("car_10", "[-10,-1]"), ("car_5", "[-5,-1]"), ("car_3", "[-3,-1]"),
        ("car_2", "[-2,-1]"), ("car_1", "[-1,0]"), ("car_post3", "[0,+3]"), ("car_post5", "[0,+5]")]


# =====================================================================
# SPEC 1: Market-adjusted CARs — stock-level regression, event-clustered
# =====================================================================
print("\n" + "=" * 110)
print("SPEC 1: MARKET-ADJUSTED CARs — connected + same_ind + conn x same")
print("Event-clustered SEs")
print("=" * 110)

print(f"\n  {'Window':<10} {'connected':>12} {'p':>8} {'same_ind':>12} {'p':>8} {'conn x same':>12} {'p':>8} {'N':>10}")
print(f"  {'-'*80}")

df["event_id_str"] = df["event_id"].astype(str)

for var, label in wins:
    adjvar = f"{var}_adj"
    s = df.dropna(subset=[adjvar, "connected", "same_industry"]).copy().reset_index(drop=True)
    if len(s) < 500:
        continue
    s["cx"] = s["connected"] * s["same_industry"]
    try:
        m = smf.ols(f"{adjvar} ~ connected + same_industry + cx", data=s).fit(
            cov_type="cluster", cov_kwds={"groups": s["event_id_str"]})
        c1 = m.params["connected"]; p1 = m.pvalues["connected"]
        c2 = m.params["same_industry"]; p2 = m.pvalues["same_industry"]
        c3 = m.params["cx"]; p3 = m.pvalues["cx"]
        print(f"  {label:<10} {c1:>+10.5f}{sig(p1)} {p1:>8.4f} {c2:>+10.5f}{sig(p2)} {p2:>8.4f} {c3:>+10.5f}{sig(p3)} {p3:>8.4f} {len(s):>10,}")
    except Exception as e:
        print(f"  {label:<10} ERROR: {str(e)[:60]}")

# Also with Year FE + event-cluster
print(f"\n  With Year FE + event-cluster:")
print(f"  {'Window':<10} {'connected':>12} {'p':>8} {'same_ind':>12} {'p':>8} {'conn x same':>12} {'p':>8}")
print(f"  {'-'*68}")

for var, label in wins:
    adjvar = f"{var}_adj"
    s = df.dropna(subset=[adjvar, "connected", "same_industry"]).copy().reset_index(drop=True)
    if len(s) < 500:
        continue
    s["cx"] = s["connected"] * s["same_industry"]
    xvars = [adjvar, "connected", "same_industry", "cx"]
    dm = s[xvars].copy()
    gm = dm.groupby(s["event_year"]).transform("mean")
    dm = dm - gm
    try:
        m = smf.ols(f"{adjvar} ~ connected + same_industry + cx - 1", data=dm).fit(
            cov_type="cluster", cov_kwds={"groups": s["event_id_str"]})
        c1 = m.params["connected"]; p1 = m.pvalues["connected"]
        c2 = m.params["same_industry"]; p2 = m.pvalues["same_industry"]
        c3 = m.params["cx"]; p3 = m.pvalues["cx"]
        print(f"  {label:<10} {c1:>+10.5f}{sig(p1)} {p1:>8.4f} {c2:>+10.5f}{sig(p2)} {p2:>8.4f} {c3:>+10.5f}{sig(p3)} {p3:>8.4f}")
    except Exception as e:
        print(f"  {label:<10} ERROR: {str(e)[:60]}")


# =====================================================================
# SPEC 2: Event-level collapse
# =====================================================================
print("\n\n" + "=" * 110)
print("SPEC 2: EVENT-LEVEL COLLAPSE")
print("Average CARs within each event x connected x same_industry cell")
print("Then regress on event-level observations")
print("=" * 110)

# For each event, compute mean CAR by (connected, same_industry)
for use_adj, adj_label in [(False, "Raw CARs"), (True, "Market-adjusted CARs")]:
    print(f"\n  {adj_label}:")
    print(f"  {'Window':<10} {'connected':>12} {'p':>8} {'same_ind':>12} {'p':>8} {'conn x same':>12} {'p':>8} {'N events':>10}")
    print(f"  {'-'*80}")

    for var, label in wins:
        v = f"{var}_adj" if use_adj else var
        s = df.dropna(subset=[v, "connected", "same_industry"]).copy()

        # Collapse to event x group level
        collapsed = s.groupby(["event_id", "event_year", "connected", "same_industry"])[v].mean().reset_index()
        collapsed["cx"] = collapsed["connected"] * collapsed["same_industry"]

        if len(collapsed) < 50:
            continue

        try:
            m = smf.ols(f"{v} ~ connected + same_industry + cx", data=collapsed).fit(cov_type="HC1")
            c1 = m.params["connected"]; p1 = m.pvalues["connected"]
            c2 = m.params["same_industry"]; p2 = m.pvalues["same_industry"]
            c3 = m.params["cx"]; p3 = m.pvalues["cx"]
            n_ev = collapsed["event_id"].nunique()
            print(f"  {label:<10} {c1:>+10.5f}{sig(p1)} {p1:>8.4f} {c2:>+10.5f}{sig(p2)} {p2:>8.4f} {c3:>+10.5f}{sig(p3)} {p3:>8.4f} {n_ev:>10,}")
        except Exception as e:
            print(f"  {label:<10} ERROR: {str(e)[:60]}")


# =====================================================================
# SPEC 3: Event-level spread (connected minus non-connected)
# =====================================================================
print("\n\n" + "=" * 110)
print("SPEC 3: EVENT-LEVEL SPREAD")
print("For each event: mean CAR(connected) - mean CAR(non-connected)")
print("Test if spread > 0")
print("=" * 110)

for use_adj, adj_label in [(False, "Raw CARs"), (True, "Market-adjusted CARs")]:
    print(f"\n  {adj_label}:")
    print(f"  {'Window':<10} {'Spread':>10} {'t':>8} {'p':>8} {'N events':>10} {'Same-ind spread':>16} {'p':>8} {'N':>6}")
    print(f"  {'-'*86}")

    for var, label in wins:
        v = f"{var}_adj" if use_adj else var
        s = df.dropna(subset=[v]).copy()

        # Compute per-event means for connected and non-connected
        ev_conn = s[s["connected"] == 1].groupby("event_id")[v].mean().rename("conn_mean")
        ev_nonconn = s[s["connected"] == 0].groupby("event_id")[v].mean().rename("nonconn_mean")
        spread = pd.concat([ev_conn, ev_nonconn], axis=1).dropna()
        spread["diff"] = spread["conn_mean"] - spread["nonconn_mean"]

        if len(spread) < 20:
            continue

        t_all, p_all = stats.ttest_1samp(spread["diff"], 0)

        # Same-industry spread
        ev_conn_si = s[(s["connected"] == 1) & (s["same_industry"] == 1)].groupby("event_id")[v].mean().rename("conn_si")
        ev_nonconn_si = s[(s["connected"] == 0) & (s["same_industry"] == 1)].groupby("event_id")[v].mean().rename("nonconn_si")
        spread_si = pd.concat([ev_conn_si, ev_nonconn_si], axis=1).dropna()
        spread_si["diff"] = spread_si["conn_si"] - spread_si["nonconn_si"]

        if len(spread_si) >= 10:
            t_si, p_si = stats.ttest_1samp(spread_si["diff"], 0)
            print(f"  {label:<10} {spread['diff'].mean():>+8.5f} {t_all:>8.2f} {p_all:>8.4f}{sig(p_all)} {len(spread):>10,} {spread_si['diff'].mean():>+14.5f} {p_si:>8.4f}{sig(p_si)} {len(spread_si):>6,}")
        else:
            print(f"  {label:<10} {spread['diff'].mean():>+8.5f} {t_all:>8.2f} {p_all:>8.4f}{sig(p_all)} {len(spread):>10,} {'too few':>16}")


# =====================================================================
# SPEC 4: Connected-only, market-adjusted, same_industry coef
# =====================================================================
print("\n\n" + "=" * 110)
print("SPEC 4: CONNECTED-ONLY, MARKET-ADJUSTED same_industry coefficient")
print("=" * 110)

conn = df[df["connected"] == 1].copy()
conn["vc_firm"] = ""
# Get vc_firm from edges
edge_vc = edges[["observed_companyid", "permno", "vc_firm_companyid"]].drop_duplicates()
edge_vc["permno"] = edge_vc["permno"].astype(int)
# This is approximate — just need a VC firm for clustering
# Use events_df to map event_id back to cid
eid_cid = {}
for ei, ev in enumerate(evlist):
    eid_cid[ei] = ev["cid"]
conn["cid"] = conn["event_id"].map(eid_cid)
conn = conn.merge(edge_vc.rename(columns={"observed_companyid": "cid", "vc_firm_companyid": "vc_firm_id"}),
                   on=["cid", "permno"], how="left")
conn["vc_firm_id"] = conn["vc_firm_id"].fillna("unknown").astype(str)

print(f"\n  {'Window':<10} {'HC1':>12} {'VC-cl':>12} {'Evt-cl':>12} {'MktAdj+HC1':>12} {'MktAdj+VC':>12} {'MktAdj+Evt':>12}")
print(f"  {'-'*82}")

for var, label in wins:
    adjvar = f"{var}_adj"
    s = conn.dropna(subset=[var, adjvar, "same_industry"]).copy().reset_index(drop=True)
    if len(s) < 100:
        continue

    results = []
    for v, cl in [(var, "HC1"), (var, "vc"), (var, "event"),
                   (adjvar, "HC1"), (adjvar, "vc"), (adjvar, "event")]:
        try:
            if cl == "HC1":
                m = smf.ols(f"{v} ~ same_industry", data=s).fit(cov_type="HC1")
            elif cl == "vc":
                m = smf.ols(f"{v} ~ same_industry", data=s).fit(
                    cov_type="cluster", cov_kwds={"groups": s["vc_firm_id"]})
            elif cl == "event":
                m = smf.ols(f"{v} ~ same_industry", data=s).fit(
                    cov_type="cluster", cov_kwds={"groups": s["event_id"].astype(str)})
            p = m.pvalues["same_industry"]
            results.append(f"{p:>9.4f}{sig(p)}")
        except:
            results.append(f"{'ERR':>12}")

    print(f"  {label:<10} {'  '.join(results)}")


print("\n\nDone.")
