"""Test 3: Fine-grained CAR windows.
[-30,-1] [-20,-1] [-15,-1] [-10,-1] [-5,-1] [-3,-1] [-2,-1] [-1,0] [0,+3] [0,+5]
Shows: Overall, Same-ind, Diff-ind for each window.
VC-clustered. Also NVCA and Clayton shock interactions."""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 100)
print("TEST 3: FINE-GRAINED CAR WINDOWS")
print("=" * 100)

# --- Load ---
print("\n--- Loading data ---")
edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")
port_xwalk = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
port_xwalk["cik_int"] = pd.to_numeric(port_xwalk["cik"], errors="coerce")
edges = edges.merge(port_xwalk.drop_duplicates("cik_int", keep="first")[["cik_int", "permno"]].rename(
    columns={"cik_int": "portfolio_cik_int"}), on="portfolio_cik_int", how="inner")
edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce")

industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))
ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["companyid_str"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.lstrip("0"), errors="coerce")
companyid_to_cik = dict(zip(ciq_xwalk["companyid_str"], ciq_xwalk["cik_int"]))
edges["same_industry"] = (edges["observed_companyid"].map(companyid_to_cik).map(cik_to_sic2) ==
                           edges["portfolio_cik_int"].map(cik_to_sic2)).astype(int)

events = pd.read_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
events["companyid_str"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
pub = set()
with open(os.path.join(ciq_dir, "04_observer_company_details.csv"), "r", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        cid = str(r.get("companyid", "")).strip().replace(".0", "")
        if "public" in str(r.get("companytypename", "")).lower():
            pub.add(cid)
events = events[~events["companyid_str"].isin(pub)]
events["event_date"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["event_date"])
events["event_year"] = events["event_date"].dt.year
events = events[(events["event_year"] >= 2015) & (events["event_year"] <= 2025)]

# Drop earnings announcements
events = events[events["keydeveventtypename"] != "Announcements of Earnings"]

# Filter out events when company was CRSP-listed
cid_to_cik = dict(zip(ciq_xwalk["companyid_str"], ciq_xwalk["cik_int"]))
panel_b_xwalk = pd.read_csv(os.path.join(data_dir, "Panel_B_Outcomes", "01_identifier_crosswalk.csv"))
panel_b_xwalk["cik_int"] = pd.to_numeric(panel_b_xwalk["cik"], errors="coerce")
panel_b_xwalk["linkdt"] = pd.to_datetime(panel_b_xwalk["linkdt"], errors="coerce")
panel_b_xwalk["linkenddt"] = pd.to_datetime(panel_b_xwalk["linkenddt"], errors="coerce").fillna(pd.Timestamp("2099-12-31"))
listing = panel_b_xwalk.groupby("cik_int").agg(first_listed=("linkdt", "min"), last_listed=("linkenddt", "max")).reset_index()

events["cik_int"] = events["companyid_str"].map(cid_to_cik)
events = events.merge(listing, on="cik_int", how="left")
events["was_public"] = (events["event_date"] >= events["first_listed"]) & (events["event_date"] <= events["last_listed"])
events = events[~events["was_public"].fillna(False)]

print(f"  Events (private, no earnings, not CRSP-listed): {len(events):,}")
print(f"  Companies: {events['companyid_str'].nunique():,}")

event_edges = events.merge(edges, left_on="companyid_str", right_on="observed_companyid", how="inner")
print(f"  Event-edge pairs: {len(event_edges):,}")

port_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["permno"] = pd.to_numeric(port_daily["permno"], errors="coerce")
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
port_daily = port_daily.dropna(subset=["permno", "ret", "date"]).sort_values(["permno", "date"])
print(f"  Daily returns: {len(port_daily):,}")

# --- Compute CARs ---
print("\n--- Computing CARs (10 windows) ---")
np.random.seed(42)
if len(event_edges) > 120000:
    event_edges = event_edges.sample(120000).reset_index(drop=True)

car_windows = [
    ("car_30", -30, -1), ("car_20", -20, -1), ("car_15", -15, -1),
    ("car_10", -10, -1), ("car_5", -5, -1), ("car_3", -3, -1),
    ("car_2", -2, -1), ("car_1", -1, 0),
    ("car_post3", 0, 3), ("car_post5", 0, 5),
]

car_results = []
chunk_size = 10000
total_chunks = (len(event_edges) + chunk_size - 1) // chunk_size

for chunk_idx in range(total_chunks):
    chunk = event_edges.iloc[chunk_idx * chunk_size:(chunk_idx + 1) * chunk_size]
    for _, row in chunk.iterrows():
        pdata = port_daily[port_daily["permno"] == row["permno"]]
        if len(pdata) < 30:
            continue
        dates = pdata["date"].values
        rets = pdata["ret"].values
        event_np = np.datetime64(row["event_date"])
        diffs = (dates - event_np).astype("timedelta64[D]").astype(int)
        cars = {}
        for wn, d0, d1 in car_windows:
            mask = (diffs >= d0) & (diffs <= d1)
            wr = rets[mask]
            if len(wr) >= max(2, abs(d1 - d0) * 0.3):
                cars[wn] = float(np.sum(wr))
        if cars:
            ed = row["event_date"]
            if not isinstance(ed, pd.Timestamp):
                ed = pd.Timestamp(ed)
            car_results.append({
                "vc_firm_companyid": str(row.get("vc_firm_companyid", "")),
                "same_industry": row["same_industry"],
                "event_year": ed.year,
                "event_date": ed,
                **cars,
            })
    if (chunk_idx + 1) % 3 == 0 or chunk_idx == total_chunks - 1:
        print(f"    Chunk {chunk_idx+1}/{total_chunks}: {len(car_results):,} CARs")

car_df = pd.DataFrame(car_results)
car_df = car_df[car_df["vc_firm_companyid"] != ""].reset_index(drop=True)
car_df["event_date"] = pd.to_datetime(car_df["event_date"])
print(f"\n  Total CARs: {len(car_df):,}, VC firms: {car_df['vc_firm_companyid'].nunique():,}")


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


# =====================================================================
# TABLE 1: Full sample means
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 1: SUBSAMPLE MEANS (full sample, VC-clustered)")
print("=" * 100)
print(f"\n  {'Window':<14} {'Subsample':<12} {'N':>8} {'Mean CAR':>10} {'p (VC-cl)':>12} {'Clusters':>10}")
print(f"  {'-'*66}")

for var, label in windows:
    for sname, sfn in subsamples:
        sub = sfn(car_df).dropna(subset=[var]).copy()
        sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
        n = len(sub)
        if n < 30:
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
# TABLE 2: Pre-2020 vs Post-2020 (NVCA shock)
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 2: PRE-2020 vs POST-2020 (NVCA shock)")
print("=" * 100)
print(f"\n  {'Window':<14} {'Period':<14} {'Subsample':<12} {'N':>8} {'Mean CAR':>10} {'p (VC-cl)':>12}")
print(f"  {'-'*70}")

for var, label in windows:
    for pname, pfn in [("Pre-2020", lambda df: df[df["event_year"] < 2020]),
                        ("Post-2020", lambda df: df[df["event_year"] >= 2020])]:
        for sname, sfn in subsamples:
            sub = sfn(pfn(car_df)).dropna(subset=[var]).copy()
            sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
            n = len(sub)
            if n < 30:
                continue
            mean_val = sub[var].mean()
            try:
                m = smf.ols(f"{var} ~ 1", data=sub).fit(
                    cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
                pcl = m.pvalues["Intercept"]
            except:
                pcl = np.nan
            print(f"  {label:<14} {pname:<14} {sname:<12} {n:>8,} {mean_val:>+10.5f} {pcl:>9.4f}{sig(pcl)}")
        print()


# =====================================================================
# TABLE 3: Pre vs Post Clayton Act (Jan 2025)
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 3: PRE vs POST CLAYTON ACT (Jan 2025)")
print("=" * 100)
jan2025 = pd.Timestamp("2025-01-01")
print(f"\n  {'Window':<14} {'Period':<14} {'Subsample':<12} {'N':>8} {'Mean CAR':>10} {'p (VC-cl)':>12}")
print(f"  {'-'*70}")

for var, label in windows:
    for pname, pfn in [("Pre-Jan2025", lambda df: df[df["event_date"] < jan2025]),
                        ("Post-Jan2025", lambda df: df[df["event_date"] >= jan2025])]:
        for sname, sfn in subsamples:
            sub = sfn(pfn(car_df)).dropna(subset=[var]).copy()
            sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
            n = len(sub)
            if n < 20:
                print(f"  {label:<14} {pname:<14} {sname:<12} {n:>8}  too few")
                continue
            mean_val = sub[var].mean()
            try:
                m = smf.ols(f"{var} ~ 1", data=sub).fit(
                    cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
                pcl = m.pvalues["Intercept"]
            except:
                pcl = np.nan
            print(f"  {label:<14} {pname:<14} {sname:<12} {n:>8,} {mean_val:>+10.5f} {pcl:>9.4f}{sig(pcl)}")
        print()


# =====================================================================
# TABLE 4: Shock interactions (Year FE + VC-clustered)
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 4: SHOCK INTERACTIONS (Year FE + VC-clustered)")
print("=" * 100)

car_df["post_2020"] = (car_df["event_year"] >= 2020).astype(int)
car_df["post_jan2025"] = (car_df["event_date"] >= jan2025).astype(int)

# NVCA: use 2015-2024 to avoid Oct2025 contamination
nvca_df = car_df[car_df["event_year"] <= 2024].copy()
# Clayton: use post-2020 only
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
        if len(sub) < 200 or n_post < 20:
            results.append((np.nan, np.nan))
            continue
        sub["same_x_post"] = sub["same_industry"] * sub[post_col]
        xvars = [var, "same_industry", "same_x_post"]
        sub_dm = sub[xvars].copy()
        yr_m = sub_dm.groupby(sub["event_year"]).transform("mean")
        sub_dm = sub_dm - yr_m
        m = smf.ols(f"{var} ~ same_industry + same_x_post - 1", data=sub_dm).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
        results.append((m.params["same_x_post"], m.pvalues["same_x_post"]))

    c1, p1 = results[0]
    c2, p2 = results[1]
    s1 = f"{c1:>+10.5f}{sig(p1)}" if not np.isnan(c1) else f"{'N/A':>14}"
    s2 = f"{c2:>+12.5f}{sig(p2)}" if not np.isnan(c2) else f"{'N/A':>14}"
    p1s = f"{p1:>10.4f}" if not np.isnan(p1) else f"{'':>10}"
    p2s = f"{p2:>10.4f}" if not np.isnan(p2) else f"{'':>10}"
    print(f"  {label:<14} {s1} {p1s} {s2} {p2s}")

print("\n  NVCA: expect + (loosened), Clayton: expect - (tightened)")

print("\n\nDone.")
