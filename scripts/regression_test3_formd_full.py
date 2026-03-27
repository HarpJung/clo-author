"""Test 3 FULL BATTERY using Form D filing dates as events.
Replicates all CIQ tests: subsample means, pre/post NVCA, pre/post Clayton,
shock interactions, year-by-year, connected vs non-connected control group.
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

print("=" * 100)
print("TEST 3 FULL BATTERY: FORM D EVENTS")
print("=" * 100)

# =====================================================================
# STEP 1: Load Form D filings -> match to CIQ observer companies
# =====================================================================
print("\n--- Loading Form D ---")
formd_events = []
for qdir in sorted(glob.glob(os.path.join(formd_dir, "20*"))):
    subdirs = glob.glob(os.path.join(qdir, "*_d"))
    if not subdirs:
        continue
    ddir = subdirs[0]
    sub_file = os.path.join(ddir, "FORMDSUBMISSION.tsv")
    iss_file = os.path.join(ddir, "ISSUERS.tsv")
    if not os.path.exists(sub_file) or not os.path.exists(iss_file):
        continue
    try:
        sub = pd.read_csv(sub_file, sep="\t", dtype=str, low_memory=False)
        iss = pd.read_csv(iss_file, sep="\t", dtype=str, low_memory=False)
        merged = sub[["ACCESSIONNUMBER", "FILING_DATE"]].merge(
            iss[iss["IS_PRIMARYISSUER_FLAG"] == "YES"][["ACCESSIONNUMBER", "CIK", "ENTITYNAME"]],
            on="ACCESSIONNUMBER", how="inner")
        formd_events.append(merged)
    except:
        pass

formd_all = pd.concat(formd_events, ignore_index=True)
formd_all["FILING_DATE"] = pd.to_datetime(formd_all["FILING_DATE"], format="mixed", dayfirst=False, errors="coerce")
formd_all["CIK"] = formd_all["CIK"].astype(str).str.strip().str.lstrip("0")
formd_all = formd_all.dropna(subset=["FILING_DATE"])
formd_all["event_year"] = formd_all["FILING_DATE"].dt.year
formd_all = formd_all[(formd_all["event_year"] >= 2015) & (formd_all["event_year"] <= 2025)]

ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["companyid_str"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_str"] = ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0")
cik_to_cid = dict(zip(ciq_xwalk["cik_str"], ciq_xwalk["companyid_str"]))

formd_all["companyid_str"] = formd_all["CIK"].map(cik_to_cid)
formd_matched = formd_all.dropna(subset=["companyid_str"])
print(f"  Form D matched to CIQ: {len(formd_matched):,} filings, {formd_matched['companyid_str'].nunique():,} companies")

# =====================================================================
# STEP 2: Load network
# =====================================================================
print("\n--- Loading network ---")
edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")
port_xwalk = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
port_xwalk["cik_int"] = pd.to_numeric(port_xwalk["cik"], errors="coerce")
edges = edges.merge(port_xwalk.drop_duplicates("cik_int", keep="first")[["cik_int", "permno"]].rename(
    columns={"cik_int": "portfolio_cik_int"}), on="portfolio_cik_int", how="inner")
edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce").astype("Int64")

industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))
ciq_xwalk_num = ciq_xwalk.copy()
ciq_xwalk_num["cik_int"] = pd.to_numeric(ciq_xwalk_num["cik_str"], errors="coerce")
companyid_to_cik = dict(zip(ciq_xwalk_num["companyid_str"], ciq_xwalk_num["cik_int"]))
edges["same_industry"] = (edges["observed_companyid"].map(companyid_to_cik).map(cik_to_sic2) ==
                           edges["portfolio_cik_int"].map(cik_to_sic2)).astype(int)

# Connected set for control group
connected_set = set()
for _, row in edges.iterrows():
    connected_set.add((row["observed_companyid"], int(row["permno"])))

permno_to_cik_map = dict(zip(port_xwalk["permno"].dropna().astype(int), port_xwalk["cik_int"].dropna().astype(int)))
permno_to_sic2 = {p: cik_to_sic2.get(c, "") for p, c in permno_to_cik_map.items()}

observed_with_edges = set(edges["observed_companyid"])
formd_matched = formd_matched[formd_matched["companyid_str"].isin(observed_with_edges)]
events_df = formd_matched[["companyid_str", "FILING_DATE", "event_year"]].drop_duplicates()
print(f"  Events with edges: {len(events_df):,}, {events_df['companyid_str'].nunique():,} companies")

event_edges = events_df.merge(edges, left_on="companyid_str", right_on="observed_companyid", how="inner")
print(f"  Event-edge pairs: {len(event_edges):,}")

# =====================================================================
# STEP 3: Load returns
# =====================================================================
print("\n--- Loading returns ---")
port_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["permno"] = pd.to_numeric(port_daily["permno"], errors="coerce").dropna().astype(int)
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
port_daily = port_daily.dropna(subset=["ret"]).sort_values(["permno", "date"])
all_permnos = sorted(port_daily["permno"].unique())
print(f"  Returns: {len(port_daily):,}, {len(all_permnos):,} stocks")

# Pre-compute per-permno data
permno_data = {}
for permno, group in port_daily.groupby("permno"):
    permno_data[permno] = (group["date"].values, group["ret"].values)

# =====================================================================
# STEP 4: Compute CARs — connected sample
# =====================================================================
print("\n--- Computing CARs (connected) ---")

car_windows = [
    ("car_30", -30, -1), ("car_20", -20, -1), ("car_15", -15, -1),
    ("car_10", -10, -1), ("car_5", -5, -1), ("car_3", -3, -1),
    ("car_2", -2, -1), ("car_1", -1, 0),
    ("car_post3", 0, 3), ("car_post5", 0, 5),
]

def compute_cars(permno, event_date_np, rets_dates_dict):
    if permno not in rets_dates_dict:
        return None
    dates, rets = rets_dates_dict[permno]
    if len(dates) < 30:
        return None
    diffs = (dates - event_date_np).astype("timedelta64[D]").astype(int)
    cars = {}
    for wn, d0, d1 in car_windows:
        mask = (diffs >= d0) & (diffs <= d1)
        wr = rets[mask]
        if len(wr) >= max(2, abs(d1 - d0) * 0.3):
            cars[wn] = float(np.sum(wr))
    return cars if cars else None

car_results = []
for idx, row in event_edges.iterrows():
    cars = compute_cars(int(row["permno"]), np.datetime64(row["FILING_DATE"]), permno_data)
    if cars:
        car_results.append({
            "vc_firm_companyid": str(row.get("vc_firm_companyid", "")),
            "same_industry": row["same_industry"],
            "event_year": row["event_year"],
            "event_date": row["FILING_DATE"],
            "permno": int(row["permno"]),
            "companyid_str": row["companyid_str"],
            "connected": 1,
            **cars,
        })
    if (idx + 1) % 2000 == 0:
        print(f"    {idx+1:,}/{len(event_edges):,}: {len(car_results):,} CARs")

car_df = pd.DataFrame(car_results)
car_df = car_df[car_df["vc_firm_companyid"] != ""].reset_index(drop=True)
car_df["event_date"] = pd.to_datetime(car_df["event_date"])
print(f"  Connected CARs: {len(car_df):,}, VC firms: {car_df['vc_firm_companyid'].nunique():,}")
print(f"  Same-ind: {(car_df['same_industry']==1).sum():,}, Diff-ind: {(car_df['same_industry']==0).sum():,}")
print(f"  By year: {dict(car_df.groupby('event_year').size())}")

# =====================================================================
# STEP 5: Compute CARs — control group (non-connected)
# =====================================================================
print("\n--- Computing CARs (control group: all stocks x all events) ---")
t0 = time.time()
control_results = []
events_list = events_df.to_dict("records")

for ev_idx, ev in enumerate(events_list):
    event_np = np.datetime64(ev["FILING_DATE"])
    observed_cid = ev["companyid_str"]
    event_year = ev["event_year"]
    obs_cik = companyid_to_cik.get(observed_cid)
    obs_sic2 = cik_to_sic2.get(obs_cik, "") if obs_cik else ""

    for permno in all_permnos:
        if (observed_cid, permno) in connected_set:
            continue  # skip connected — already in car_df
        cars = compute_cars(permno, event_np, permno_data)
        if not cars:
            continue
        port_sic2 = permno_to_sic2.get(permno, "")
        same_ind = 1 if (obs_sic2 and port_sic2 and obs_sic2 == port_sic2) else 0
        control_results.append({
            "same_industry": same_ind,
            "event_year": event_year,
            "connected": 0,
            **cars,
        })

    if (ev_idx + 1) % 50 == 0:
        elapsed = time.time() - t0
        rate = (ev_idx + 1) / elapsed
        remaining = (len(events_list) - ev_idx - 1) / rate / 60
        print(f"    Event {ev_idx+1:,}/{len(events_list):,} | {len(control_results):,} control CARs | ~{remaining:.0f}min left")

control_df = pd.DataFrame(control_results)
print(f"  Control CARs: {len(control_df):,}")

# Combine
combined = pd.concat([
    car_df[["connected", "same_industry", "event_year"] + [w[0] for w in car_windows]],
    control_df
], ignore_index=True)
print(f"  Combined: {len(combined):,} (connected={len(car_df):,}, control={len(control_df):,})")


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


windows = [
    ("car_30", "CAR[-30,-1]"), ("car_20", "CAR[-20,-1]"), ("car_15", "CAR[-15,-1]"),
    ("car_10", "CAR[-10,-1]"), ("car_5", "CAR[-5,-1]"), ("car_3", "CAR[-3,-1]"),
    ("car_2", "CAR[-2,-1]"), ("car_1", "CAR[-1,0]"),
    ("car_post3", "CAR[0,+3]"), ("car_post5", "CAR[0,+5]"),
]

subsamples = [
    ("Overall", lambda df: df),
    ("Same-ind", lambda df: df[df["same_industry"] == 1]),
    ("Diff-ind", lambda df: df[df["same_industry"] == 0]),
]

jan2025 = pd.Timestamp("2025-01-01")


# =====================================================================
# TABLE 1: Full sample means (VC-clustered) — connected only
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 1: CONNECTED SAMPLE MEANS (VC-clustered)")
print("=" * 100)
print(f"\n  {'Window':<14} {'Subsample':<12} {'N':>8} {'Mean CAR':>10} {'p (VC-cl)':>12} {'Clusters':>10}")
print(f"  {'-'*66}")

for var, label in windows:
    for sname, sfn in subsamples:
        sub = sfn(car_df).dropna(subset=[var]).copy()
        sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
        n = len(sub)
        if n < 20:
            continue
        nvc = sub["vc_firm_companyid"].nunique()
        mean_val = sub[var].mean()
        try:
            m = smf.ols(f"{var} ~ 1", data=sub).fit(
                cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
            pcl = m.pvalues["Intercept"]
        except:
            pcl = np.nan
        print(f"  {label:<14} {sname:<12} {n:>8,} {mean_val:>+10.5f} {pcl:>9.4f}{sig(pcl)} {nvc:>10,}")
    print()


# =====================================================================
# TABLE 2: Pre-2020 vs Post-2020 means
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 2: PRE-2020 vs POST-2020")
print("=" * 100)
print(f"\n  {'Window':<14} {'Period':<14} {'Subsample':<12} {'N':>8} {'Mean CAR':>10} {'p (VC-cl)':>12}")
print(f"  {'-'*70}")

for var, label in windows:
    for pname, pfn in [("Pre-2020", lambda df: df[df["event_year"] < 2020]),
                        ("Post-2020", lambda df: df[df["event_year"] >= 2020])]:
        for sname, sfn in subsamples:
            sub = sfn(pfn(car_df)).dropna(subset=[var]).copy()
            sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
            if len(sub) < 10:
                continue
            mean_val = sub[var].mean()
            try:
                m = smf.ols(f"{var} ~ 1", data=sub).fit(
                    cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
                pcl = m.pvalues["Intercept"]
            except:
                pcl = np.nan
            print(f"  {label:<14} {pname:<14} {sname:<12} {len(sub):>8,} {mean_val:>+10.5f} {pcl:>9.4f}{sig(pcl)}")
        print()


# =====================================================================
# TABLE 3: Pre vs Post Clayton Act
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 3: PRE vs POST CLAYTON ACT (Jan 2025)")
print("=" * 100)
print(f"\n  {'Window':<14} {'Period':<14} {'Subsample':<12} {'N':>8} {'Mean CAR':>10} {'p (VC-cl)':>12}")
print(f"  {'-'*70}")

for var, label in windows:
    for pname, pfn in [("Pre-Jan2025", lambda df: df[df["event_date"] < jan2025]),
                        ("Post-Jan2025", lambda df: df[df["event_date"] >= jan2025])]:
        for sname, sfn in subsamples:
            sub = sfn(pfn(car_df)).dropna(subset=[var]).copy()
            sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
            if len(sub) < 10:
                print(f"  {label:<14} {pname:<14} {sname:<12} {len(sub):>8}  too few")
                continue
            mean_val = sub[var].mean()
            try:
                m = smf.ols(f"{var} ~ 1", data=sub).fit(
                    cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
                pcl = m.pvalues["Intercept"]
            except:
                pcl = np.nan
            print(f"  {label:<14} {pname:<14} {sname:<12} {len(sub):>8,} {mean_val:>+10.5f} {pcl:>9.4f}{sig(pcl)}")
        print()


# =====================================================================
# TABLE 4: Shock interactions (Year FE + VC-clustered)
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 4: SHOCK INTERACTIONS (Year FE + VC-clustered)")
print("=" * 100)

car_df["post_2020"] = (car_df["event_year"] >= 2020).astype(int)
car_df["post_jan2025"] = (car_df["event_date"] >= jan2025).astype(int)

nvca_df = car_df[car_df["event_year"] <= 2024].copy()
clayton_df = car_df[car_df["event_year"] >= 2020].copy()

print(f"\n  {'Window':<14} {'NVCA coef':>12} {'p':>10} {'Clayton coef':>14} {'p':>10}")
print(f"  {'-'*60}")

for var, label in windows:
    results = []
    for shock_name, shock_df, post_col in [
        ("NVCA", nvca_df, "post_2020"),
        ("Clayton", clayton_df, "post_jan2025"),
    ]:
        sub = shock_df.dropna(subset=[var, "same_industry"]).copy()
        sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
        n_post = sub[post_col].sum()
        if len(sub) < 100 or n_post < 10:
            results.append((np.nan, np.nan))
            continue
        sub["same_x_post"] = sub["same_industry"] * sub[post_col]
        xvars = [var, "same_industry", "same_x_post"]
        sub_dm = sub[xvars].copy()
        yr_m = sub_dm.groupby(sub["event_year"]).transform("mean")
        sub_dm = sub_dm - yr_m
        try:
            m = smf.ols(f"{var} ~ same_industry + same_x_post - 1", data=sub_dm).fit(
                cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
            results.append((m.params["same_x_post"], m.pvalues["same_x_post"]))
        except:
            results.append((np.nan, np.nan))

    c1, p1 = results[0]
    c2, p2 = results[1]
    s1 = f"{c1:>+10.5f}{sig(p1)}" if not np.isnan(c1) else f"{'N/A':>14}"
    s2 = f"{c2:>+12.5f}{sig(p2)}" if not np.isnan(c2) else f"{'N/A':>14}"
    p1s = f"{p1:>10.4f}" if not np.isnan(p1) else f"{'':>10}"
    p2s = f"{p2:>10.4f}" if not np.isnan(p2) else f"{'':>10}"
    print(f"  {label:<14} {s1} {p1s} {s2} {p2s}")

print("  NVCA: expect + (loosened), Clayton: expect - (tightened)")


# =====================================================================
# TABLE 5: Year-by-year same-industry
# =====================================================================
print("\n\n" + "=" * 100)
print("TABLE 5: YEAR-BY-YEAR SAME-INDUSTRY")
print("=" * 100)

for var, label in [("car_10", "CAR[-10,-1]"), ("car_30", "CAR[-30,-1]"),
                    ("car_5", "CAR[-5,-1]"), ("car_3", "CAR[-3,-1]")]:
    print(f"\n  {label} (Same-industry only):")
    print(f"  {'Year':<8} {'N':>6} {'Mean CAR':>10} {'p (VC-cl)':>12}")
    print(f"  {'-'*36}")
    for yr in range(2015, 2026):
        sub = car_df[(car_df["event_year"] == yr) & (car_df["same_industry"] == 1)].dropna(subset=[var]).copy()
        sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
        if len(sub) < 5:
            print(f"  {yr:<8} {len(sub):>6}  too few")
            continue
        mean_val = sub[var].mean()
        try:
            m = smf.ols(f"{var} ~ 1", data=sub).fit(
                cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
            pcl = m.pvalues["Intercept"]
        except:
            pcl = np.nan
        marker = ""
        if yr == 2020: marker = "  <-- NVCA"
        if yr == 2025: marker = "  <-- Clayton"
        print(f"  {yr:<8} {len(sub):>6} {mean_val:>+10.5f} {pcl:>9.4f}{sig(pcl)}{marker}")


# =====================================================================
# TABLE 6: Connected vs Non-Connected (control group)
# =====================================================================
print("\n\n" + "=" * 100)
print("TABLE 6: CONNECTED vs NON-CONNECTED (control group)")
print("=" * 100)

print(f"\n  {'Window':<14} {'Connected':>12} {'Non-conn':>12} {'Diff':>12} {'p(diff)':>10}")
print(f"  {'-'*60}")

for var, label in windows:
    conn = combined[(combined["connected"] == 1)].dropna(subset=[var])[var]
    nonconn = combined[(combined["connected"] == 0)].dropna(subset=[var])[var]
    if len(conn) < 20 or len(nonconn) < 20:
        continue
    t, p = stats.ttest_ind(conn, nonconn, equal_var=False)
    diff = conn.mean() - nonconn.mean()
    print(f"  {label:<14} {conn.mean():>+10.5f}{sig(stats.ttest_1samp(conn,0)[1])} {nonconn.mean():>+10.5f}{sig(stats.ttest_1samp(nonconn,0)[1])} {diff:>+10.5f}{sig(p)} {p:>10.4f}")


# TABLE 7: Control group regression
print("\n\n" + "=" * 100)
print("TABLE 7: REGRESSION — connected + same_ind + connected x same_ind (Year FE, HC1)")
print("=" * 100)
print(f"\n  {'Window':<14} {'connected':>14} {'p':>8} {'same_ind':>14} {'p':>8} {'conn x same':>14} {'p':>8} {'N':>10}")
print(f"  {'-'*90}")

for var, label in windows:
    sub = combined.dropna(subset=[var, "connected", "same_industry"]).copy().reset_index(drop=True)
    if len(sub) < 200:
        continue
    sub["conn_x_same"] = sub["connected"] * sub["same_industry"]
    xvars = [var, "connected", "same_industry", "conn_x_same"]
    sub_dm = sub[xvars].copy()
    yr_m = sub_dm.groupby(sub["event_year"]).transform("mean")
    sub_dm = sub_dm - yr_m
    m = smf.ols(f"{var} ~ connected + same_industry + conn_x_same - 1", data=sub_dm).fit(cov_type="HC1")
    print(f"  {label:<14} {m.params['connected']:>+12.6f}{sig(m.pvalues['connected'])} {m.pvalues['connected']:>8.4f} {m.params['same_industry']:>+12.6f}{sig(m.pvalues['same_industry'])} {m.pvalues['same_industry']:>8.4f} {m.params['conn_x_same']:>+12.6f}{sig(m.pvalues['conn_x_same'])} {m.pvalues['conn_x_same']:>8.4f} {len(sub):>10,}")


print("\n\nDone.")
