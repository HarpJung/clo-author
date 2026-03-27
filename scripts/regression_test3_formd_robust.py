"""Test 3 Form D: Robust FE/clustering analysis.
Connected-only tests: VC-cluster, event-cluster, Year FE
Control group tests: event-cluster, stock-cluster, two-way, Year FE, Stock FE

All tables show: Overall, Same-ind, Diff-ind
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
print("TEST 3 FORM D: ROBUST FE/CLUSTERING ANALYSIS")
print("=" * 110)

# =====================================================================
# DATA LOADING (same as formd_full)
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
        m = sub[["ACCESSIONNUMBER","FILING_DATE"]].merge(
            iss[iss["IS_PRIMARYISSUER_FLAG"]=="YES"][["ACCESSIONNUMBER","CIK","ENTITYNAME"]],
            on="ACCESSIONNUMBER", how="inner")
        formd_events.append(m)
    except: pass

fd = pd.concat(formd_events, ignore_index=True)
fd["FILING_DATE"] = pd.to_datetime(fd["FILING_DATE"], format="mixed", dayfirst=False, errors="coerce")
fd["CIK"] = fd["CIK"].astype(str).str.strip().str.lstrip("0")
fd = fd.dropna(subset=["FILING_DATE"])
fd["event_year"] = fd["FILING_DATE"].dt.year
fd = fd[(fd["event_year"] >= 2015) & (fd["event_year"] <= 2025)]

ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0","",regex=False)
ciq_xwalk["cik_str"] = ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0")
cik_to_cid = dict(zip(ciq_xwalk["cik_str"], ciq_xwalk["cid"]))
fd["cid"] = fd["CIK"].map(cik_to_cid)
fd = fd.dropna(subset=["cid"])

edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0","",regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")
pxw = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
pxw["cik_int"] = pd.to_numeric(pxw["cik"], errors="coerce")
edges = edges.merge(pxw.drop_duplicates("cik_int",keep="first")[["cik_int","permno"]].rename(
    columns={"cik_int":"portfolio_cik_int"}), on="portfolio_cik_int", how="inner")
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
pm_sic2 = {p: cik_to_sic2.get(c,"") for p,c in pmcik.items()}

obs_with_edges = set(edges["observed_companyid"])
fd = fd[fd["cid"].isin(obs_with_edges)]
events_df = fd[["cid","FILING_DATE","event_year","ACCESSIONNUMBER"]].drop_duplicates(subset=["cid","FILING_DATE"])
print(f"  Events: {len(events_df):,}, Companies: {events_df['cid'].nunique()}")

ee = events_df.merge(edges, left_on="cid", right_on="observed_companyid", how="inner")
print(f"  Event-edge pairs: {len(ee):,}")

pd_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
pd_daily["date"] = pd.to_datetime(pd_daily["date"])
pd_daily["permno"] = pd.to_numeric(pd_daily["permno"], errors="coerce").dropna().astype(int)
pd_daily["ret"] = pd.to_numeric(pd_daily["ret"], errors="coerce")
pd_daily = pd_daily.dropna(subset=["ret"]).sort_values(["permno","date"])
all_permnos = sorted(pd_daily["permno"].unique())

pmdata = {}
for p, g in pd_daily.groupby("permno"):
    pmdata[p] = (g["date"].values, g["ret"].values)
print(f"  Stocks: {len(pmdata):,}")

# =====================================================================
# COMPUTE CARs
# =====================================================================
car_windows = [
    ("car_30",-30,-1),("car_20",-20,-1),("car_15",-15,-1),
    ("car_10",-10,-1),("car_5",-5,-1),("car_3",-3,-1),
    ("car_2",-2,-1),("car_1",-1,0),("car_post3",0,3),("car_post5",0,5),
]

def calc_cars(permno, event_np):
    if permno not in pmdata: return None
    dates, rets = pmdata[permno]
    if len(dates) < 30: return None
    diffs = (dates - event_np).astype("timedelta64[D]").astype(int)
    cars = {}
    for wn,d0,d1 in car_windows:
        mask = (diffs >= d0) & (diffs <= d1)
        wr = rets[mask]
        if len(wr) >= max(2, abs(d1-d0)*0.3):
            cars[wn] = float(np.sum(wr))
    return cars if cars else None

# Connected CARs
print("\n--- Connected CARs ---")
conn_results = []
event_id = 0
event_id_map = {}
for idx, row in ee.iterrows():
    ekey = (row["cid"], str(row["FILING_DATE"]))
    if ekey not in event_id_map:
        event_id_map[ekey] = event_id
        event_id += 1
    cars = calc_cars(int(row["permno"]), np.datetime64(row["FILING_DATE"]))
    if cars:
        obs_cik = cid_to_cik.get(row["cid"])
        obs_sic2 = cik_to_sic2.get(obs_cik,"") if obs_cik else ""
        conn_results.append({
            "vc_firm": str(row.get("vc_firm_companyid","")),
            "permno": int(row["permno"]),
            "event_id": event_id_map[ekey],
            "same_industry": row["same_industry"],
            "event_year": row["event_year"],
            "connected": 1,
            **cars,
        })

conn_df = pd.DataFrame(conn_results)
conn_df = conn_df[conn_df["vc_firm"]!=""].reset_index(drop=True)
print(f"  Connected: {len(conn_df):,}, Events: {conn_df['event_id'].nunique()}, VC: {conn_df['vc_firm'].nunique()}, Stocks: {conn_df['permno'].nunique()}")

# Control CARs
print("\n--- Control CARs ---")
t0 = time.time()
ctrl_results = []
evlist = events_df.to_dict("records")
for ei, ev in enumerate(evlist):
    enp = np.datetime64(ev["FILING_DATE"])
    ecid = ev["cid"]
    eyr = ev["event_year"]
    ekey = (ecid, str(ev["FILING_DATE"]))
    eid = event_id_map.get(ekey, -1)
    obs_cik = cid_to_cik.get(ecid)
    obs_sic2 = cik_to_sic2.get(obs_cik,"") if obs_cik else ""
    for pm in all_permnos:
        if (ecid, pm) in connected_set: continue
        cars = calc_cars(pm, enp)
        if not cars: continue
        psic = pm_sic2.get(pm,"")
        si = 1 if (obs_sic2 and psic and obs_sic2==psic) else 0
        ctrl_results.append({
            "permno": pm, "event_id": eid, "same_industry": si,
            "event_year": eyr, "connected": 0, **cars,
        })
    if (ei+1) % 100 == 0:
        el = time.time()-t0
        rm = (len(evlist)-ei-1)/(ei+1)*el/60
        print(f"    Event {ei+1:,}/{len(evlist):,} | {len(ctrl_results):,} | ~{rm:.0f}min")

ctrl_df = pd.DataFrame(ctrl_results)
print(f"  Control: {len(ctrl_df):,}")

# Add vc_firm="" for control, and stock_id
ctrl_df["vc_firm"] = ""
combined = pd.concat([conn_df, ctrl_df], ignore_index=True)
combined["stock_id"] = combined["permno"].astype(str)
combined["event_id_str"] = combined["event_id"].astype(str)
print(f"  Combined: {len(combined):,}")


def sig(p):
    if p<0.01: return "***"
    if p<0.05: return "**"
    if p<0.10: return "*"
    return "   "


wins = [("car_30","CAR[-30,-1]"),("car_20","CAR[-20,-1]"),("car_15","CAR[-15,-1]"),
        ("car_10","CAR[-10,-1]"),("car_5","CAR[-5,-1]"),("car_3","CAR[-3,-1]"),
        ("car_2","CAR[-2,-1]"),("car_1","CAR[-1,0]"),("car_post3","CAR[0,+3]"),("car_post5","CAR[0,+5]")]

subs = [("Overall", lambda df: df),
        ("Same-ind", lambda df: df[df["same_industry"]==1]),
        ("Diff-ind", lambda df: df[df["same_industry"]==0])]


# =====================================================================
# PART 1: CONNECTED-ONLY — means with different clustering
# =====================================================================
print("\n" + "=" * 110)
print("PART 1: CONNECTED-ONLY MEANS — VC-cluster vs Event-cluster vs Year FE")
print("=" * 110)

for var, label in wins:
    print(f"\n  {label}")
    print(f"  {'Subsample':<12} {'N':>7} {'Mean':>9} {'No cl p':>9} {'VC-cl p':>10} {'Evt-cl p':>10} {'YrFE+VC p':>10} {'YrFE+Evt p':>11}")
    print(f"  {'-'*78}")
    for sn, sf in subs:
        s = sf(conn_df).dropna(subset=[var]).copy().reset_index(drop=True)
        n = len(s)
        if n < 20: continue
        mv = s[var].mean()
        # No clustering
        m0 = smf.ols(f"{var} ~ 1", data=s).fit(cov_type="HC1")
        p0 = m0.pvalues["Intercept"]
        # VC cluster
        try:
            m1 = smf.ols(f"{var} ~ 1", data=s).fit(cov_type="cluster", cov_kwds={"groups": s["vc_firm"]})
            p1 = m1.pvalues["Intercept"]
        except: p1 = np.nan
        # Event cluster
        try:
            m2 = smf.ols(f"{var} ~ 1", data=s).fit(cov_type="cluster", cov_kwds={"groups": s["event_id"]})
            p2 = m2.pvalues["Intercept"]
        except: p2 = np.nan
        # Year FE + VC cluster
        try:
            yrm = s.groupby("event_year")[var].transform("mean")
            s["_dm"] = s[var] - yrm
            m3 = smf.ols("_dm ~ 1", data=s).fit(cov_type="cluster", cov_kwds={"groups": s["vc_firm"]})
            p3 = m3.pvalues["Intercept"]
        except: p3 = np.nan
        # Year FE + Event cluster
        try:
            m4 = smf.ols("_dm ~ 1", data=s).fit(cov_type="cluster", cov_kwds={"groups": s["event_id"]})
            p4 = m4.pvalues["Intercept"]
        except: p4 = np.nan
        print(f"  {sn:<12} {n:>7,} {mv:>+8.5f} {p0:>7.4f}{sig(p0)} {p1:>7.4f}{sig(p1)} {p2:>7.4f}{sig(p2)} {p3:>7.4f}{sig(p3)} {p4:>8.4f}{sig(p4)}")


# =====================================================================
# PART 2: CONTROL GROUP REGRESSION — all FE/cluster combos
# =====================================================================
print("\n\n" + "=" * 110)
print("PART 2: CONTROL GROUP REGRESSION — connected + same_ind + conn x same")
print("All FE/cluster combinations")
print("=" * 110)

specs = [
    ("HC1 only",        None,  "HC1"),
    ("Event-cluster",   None,  "event"),
    ("Stock-cluster",   None,  "stock"),
    ("YrFE + HC1",      "yr",  "HC1"),
    ("YrFE + Event-cl", "yr",  "event"),
    ("YrFE + Stock-cl", "yr",  "stock"),
    ("StockFE + HC1",   "stock", "HC1"),
    ("StockFE + Evt-cl","stock", "event"),
]

for var, label in wins:
    s = combined.dropna(subset=[var,"connected","same_industry"]).copy().reset_index(drop=True)
    if len(s) < 500: continue
    s["cx"] = s["connected"] * s["same_industry"]

    print(f"\n  {label} (N={len(s):,}, conn={s['connected'].sum():,}, ctrl={len(s)-s['connected'].sum():,})")
    print(f"  {'Spec':<22} {'connected':>12} {'p':>8} {'same_ind':>12} {'p':>8} {'conn x same':>12} {'p':>8}")
    print(f"  {'-'*82}")

    for spec_name, fe_type, cl_type in specs:
        try:
            # Apply FE via demeaning
            dm = s[[var,"connected","same_industry","cx"]].copy()
            if fe_type == "yr":
                gm = dm.groupby(s["event_year"]).transform("mean")
                dm = dm - gm
            elif fe_type == "stock":
                gm = dm.groupby(s["permno"]).transform("mean")
                dm = dm - gm

            formula = f"{var} ~ connected + same_industry + cx - 1" if fe_type else f"{var} ~ connected + same_industry + cx"

            if cl_type == "HC1":
                m = smf.ols(formula, data=dm).fit(cov_type="HC1")
            elif cl_type == "event":
                m = smf.ols(formula, data=dm).fit(cov_type="cluster", cov_kwds={"groups": s["event_id_str"]})
            elif cl_type == "stock":
                m = smf.ols(formula, data=dm).fit(cov_type="cluster", cov_kwds={"groups": s["stock_id"]})

            c1 = m.params["connected"]; p1 = m.pvalues["connected"]
            c2 = m.params["same_industry"]; p2 = m.pvalues["same_industry"]
            c3 = m.params["cx"]; p3 = m.pvalues["cx"]
            print(f"  {spec_name:<22} {c1:>+10.5f}{sig(p1)} {p1:>8.4f} {c2:>+10.5f}{sig(p2)} {p2:>8.4f} {c3:>+10.5f}{sig(p3)} {p3:>8.4f}")
        except Exception as e:
            print(f"  {spec_name:<22} ERROR: {str(e)[:60]}")


# =====================================================================
# PART 3: CONNECTED-ONLY — same_industry coef with all FE/cluster combos
# =====================================================================
print("\n\n" + "=" * 110)
print("PART 3: CONNECTED-ONLY — same_industry regression coefficient")
print("=" * 110)

conn_specs = [
    ("HC1 only",         None,  "HC1"),
    ("VC-cluster",       None,  "vc"),
    ("Event-cluster",    None,  "event"),
    ("YrFE + VC-cl",     "yr",  "vc"),
    ("YrFE + Event-cl",  "yr",  "event"),
]

for var, label in wins:
    s = conn_df.dropna(subset=[var,"same_industry"]).copy().reset_index(drop=True)
    if len(s) < 100: continue

    print(f"\n  {label} (N={len(s):,})")
    print(f"  {'Spec':<22} {'same_ind coef':>14} {'p':>10}")
    print(f"  {'-'*46}")

    for spec_name, fe_type, cl_type in conn_specs:
        try:
            dm = s[[var,"same_industry"]].copy()
            if fe_type == "yr":
                gm = dm.groupby(s["event_year"]).transform("mean")
                dm = dm - gm
            formula = f"{var} ~ same_industry - 1" if fe_type else f"{var} ~ same_industry"

            if cl_type == "HC1":
                m = smf.ols(formula, data=dm).fit(cov_type="HC1")
            elif cl_type == "vc":
                m = smf.ols(formula, data=dm).fit(cov_type="cluster", cov_kwds={"groups": s["vc_firm"]})
            elif cl_type == "event":
                m = smf.ols(formula, data=dm).fit(cov_type="cluster", cov_kwds={"groups": s["event_id"]})

            c = m.params["same_industry"]; p = m.pvalues["same_industry"]
            print(f"  {spec_name:<22} {c:>+12.5f}{sig(p)} {p:>10.4f}")
        except Exception as e:
            print(f"  {spec_name:<22} ERROR: {str(e)[:60]}")


print("\n\nDone.")
