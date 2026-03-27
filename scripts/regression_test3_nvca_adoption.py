"""Test 3: NVCA adoption timing — gradual trend vs sharp cutoff.
Option 3: Linear trend (year as continuous)
Option 4: 2021 cutoff instead of 2020
Option 5: 2020 cutoff (baseline)
Also: rolling 2-year windows to visualize when the effect appears.

Uses connected CIQ events, same_industry interaction, Year FE + VC-clustered.
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 110)
print("TEST 3: NVCA ADOPTION TIMING")
print("=" * 110)

# Load connected CARs (recompute from filtered CIQ events)
print("\n--- Loading ---")
edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")
pxw = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
pxw["cik_int"] = pd.to_numeric(pxw["cik"], errors="coerce")
edges = edges.merge(pxw.drop_duplicates("cik_int", keep="first")[["cik_int", "permno"]].rename(
    columns={"cik_int": "portfolio_cik_int"}), on="portfolio_cik_int", how="inner")
edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce")

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

events = pd.read_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
events["cid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
pub_cids = set()
with open(os.path.join(ciq_dir, "04_observer_company_details.csv"), "r", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        cid = str(r.get("companyid", "")).strip().replace(".0", "")
        if "public" in str(r.get("companytypename", "")).lower():
            pub_cids.add(cid)
events = events[~events["cid"].isin(pub_cids)]
events["event_date"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["event_date"])
events["event_year"] = events["event_date"].dt.year
events = events[(events["event_year"] >= 2015) & (events["event_year"] <= 2025)]
events = events[events["keydeveventtypename"] != "Announcements of Earnings"]

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

event_edges = events.merge(edges, left_on="cid", right_on="observed_companyid", how="inner")

port_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["permno"] = pd.to_numeric(port_daily["permno"], errors="coerce")
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
port_daily = port_daily.dropna(subset=["permno", "ret", "date"]).sort_values(["permno", "date"])

pmdata = {}
for p, g in port_daily.groupby("permno"):
    pmdata[int(p)] = (g["date"].values, g["ret"].values)

# Compute CARs
print("Computing CARs...")
np.random.seed(42)
if len(event_edges) > 120000:
    event_edges = event_edges.sample(120000).reset_index(drop=True)

car_results = []
for idx, row in event_edges.iterrows():
    pm = int(row["permno"])
    if pm not in pmdata:
        continue
    dates, rets = pmdata[pm]
    if len(dates) < 30:
        continue
    event_np = np.datetime64(row["event_date"])
    diffs = (dates - event_np).astype("timedelta64[D]").astype(int)

    cars = {}
    for wn, d0, d1 in [("car_30", -30, -1), ("car_10", -10, -1), ("car_5", -5, -1)]:
        mask = (diffs >= d0) & (diffs <= d1)
        wr = rets[mask]
        if len(wr) >= max(2, abs(d1 - d0) * 0.3):
            cars[wn] = float(np.sum(wr))

    if cars:
        car_results.append({
            "vc_firm": str(row.get("vc_firm_companyid", "")),
            "same_industry": row["same_industry"],
            "event_year": int(row["event_date"].year if hasattr(row["event_date"], "year") else pd.Timestamp(row["event_date"]).year),
            **cars,
        })

car_df = pd.DataFrame(car_results)
car_df = car_df[car_df["vc_firm"] != ""].reset_index(drop=True)
print(f"CARs: {len(car_df):,}, VC firms: {car_df['vc_firm'].nunique():,}")


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


wins = [("car_30", "CAR[-30,-1]"), ("car_10", "CAR[-10,-1]"), ("car_5", "CAR[-5,-1]")]


# =====================================================================
# OPTION 5: Sharp 2020 cutoff (baseline)
# =====================================================================
print("\n" + "=" * 100)
print("OPTION 5: SHARP 2020 CUTOFF (baseline)")
print("=" * 100)

car_df["post_2020"] = (car_df["event_year"] >= 2020).astype(int)

for var, label in wins:
    sub = car_df.dropna(subset=[var, "same_industry"]).copy().reset_index(drop=True)
    sub["same_x_post"] = sub["same_industry"] * sub["post_2020"]
    xvars = [var, "same_industry", "same_x_post"]
    dm = sub[xvars].copy()
    yr_m = dm.groupby(sub["event_year"]).transform("mean")
    dm = dm - yr_m
    m = smf.ols(f"{var} ~ same_industry + same_x_post - 1", data=dm).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm"]})
    print(f"  {label}: same_ind x post_2020 = {m.params['same_x_post']:+.5f} (p={m.pvalues['same_x_post']:.4f}{sig(m.pvalues['same_x_post'])})")


# =====================================================================
# OPTION 4: Sharp 2021 cutoff (skip COVID year)
# =====================================================================
print("\n" + "=" * 100)
print("OPTION 4: SHARP 2021 CUTOFF (skip COVID)")
print("=" * 100)

car_no_2020 = car_df[car_df["event_year"] != 2020].copy()
car_no_2020["post_2021"] = (car_no_2020["event_year"] >= 2021).astype(int)

for var, label in wins:
    sub = car_no_2020.dropna(subset=[var, "same_industry"]).copy().reset_index(drop=True)
    sub["same_x_post"] = sub["same_industry"] * sub["post_2021"]
    xvars = [var, "same_industry", "same_x_post"]
    dm = sub[xvars].copy()
    yr_m = dm.groupby(sub["event_year"]).transform("mean")
    dm = dm - yr_m
    m = smf.ols(f"{var} ~ same_industry + same_x_post - 1", data=dm).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm"]})
    print(f"  {label}: same_ind x post_2021 = {m.params['same_x_post']:+.5f} (p={m.pvalues['same_x_post']:.4f}{sig(m.pvalues['same_x_post'])})")


# =====================================================================
# OPTION 3: Linear trend (year as continuous variable)
# =====================================================================
print("\n" + "=" * 100)
print("OPTION 3: LINEAR TREND (same_industry x year)")
print("=" * 100)

car_df["year_centered"] = car_df["event_year"] - 2020  # center at 2020

for var, label in wins:
    sub = car_df.dropna(subset=[var, "same_industry"]).copy().reset_index(drop=True)
    sub["same_x_year"] = sub["same_industry"] * sub["year_centered"]
    m = smf.ols(f"{var} ~ same_industry + year_centered + same_x_year", data=sub).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm"]})
    print(f"  {label}: same_ind x year = {m.params['same_x_year']:+.5f} (p={m.pvalues['same_x_year']:.4f}{sig(m.pvalues['same_x_year'])})")
    print(f"         Interpretation: each additional year adds {m.params['same_x_year']*100:.2f}pp to same-industry CARs")


# =====================================================================
# OPTION 3b: Quadratic trend (allows for acceleration/deceleration)
# =====================================================================
print("\n" + "=" * 100)
print("OPTION 3b: QUADRATIC TREND")
print("=" * 100)

for var, label in wins:
    sub = car_df.dropna(subset=[var, "same_industry"]).copy().reset_index(drop=True)
    sub["year_c"] = sub["event_year"] - 2020
    sub["year_c2"] = sub["year_c"] ** 2
    sub["same_x_year"] = sub["same_industry"] * sub["year_c"]
    sub["same_x_year2"] = sub["same_industry"] * sub["year_c2"]
    m = smf.ols(f"{var} ~ same_industry + year_c + year_c2 + same_x_year + same_x_year2", data=sub).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm"]})
    print(f"  {label}:")
    print(f"    same_ind x year  = {m.params['same_x_year']:+.5f} (p={m.pvalues['same_x_year']:.4f}{sig(m.pvalues['same_x_year'])})")
    print(f"    same_ind x year2 = {m.params['same_x_year2']:+.5f} (p={m.pvalues['same_x_year2']:.4f}{sig(m.pvalues['same_x_year2'])})")


# =====================================================================
# ROLLING 2-YEAR WINDOWS: When does the effect appear?
# =====================================================================
print("\n" + "=" * 100)
print("ROLLING 2-YEAR WINDOWS: Same-industry CAR means (VC-clustered)")
print("=" * 100)

for var, label in wins:
    print(f"\n  {label}:")
    print(f"  {'Window':<14} {'N same':>8} {'Mean':>10} {'p (VC-cl)':>12}")
    print(f"  {'-'*44}")

    for start_yr in range(2015, 2025):
        end_yr = start_yr + 1
        sub = car_df[(car_df["event_year"] >= start_yr) & (car_df["event_year"] <= end_yr) &
                      (car_df["same_industry"] == 1)].dropna(subset=[var]).copy()
        sub = sub[sub["vc_firm"] != ""].reset_index(drop=True)
        if len(sub) < 10:
            print(f"  {start_yr}-{end_yr:<9} {len(sub):>8}  too few")
            continue
        try:
            m = smf.ols(f"{var} ~ 1", data=sub).fit(
                cov_type="cluster", cov_kwds={"groups": sub["vc_firm"]})
            p = m.pvalues["Intercept"]
            marker = ""
            if start_yr == 2019: marker = " <-- straddles NVCA"
            if start_yr == 2020: marker = " <-- post-NVCA"
            print(f"  {start_yr}-{end_yr:<9} {len(sub):>8} {sub[var].mean():>+10.5f} {p:>9.4f}{sig(p)}{marker}")
        except:
            print(f"  {start_yr}-{end_yr:<9} {len(sub):>8}  error")


# =====================================================================
# STAGGERED ADOPTION: Compare early vs late adopters
# =====================================================================
print("\n" + "=" * 100)
print("ALTERNATIVE CUTOFFS: Interaction p-values across different break years")
print("=" * 100)

print(f"\n  {'Break Year':<12}", end="")
for var, label in wins:
    print(f" {label:>16}", end="")
print()
print(f"  {'-'*60}")

for break_yr in [2018, 2019, 2020, 2021, 2022]:
    row = f"  {break_yr:<12}"
    for var, label in wins:
        sub = car_df.dropna(subset=[var, "same_industry"]).copy().reset_index(drop=True)
        sub["post"] = (sub["event_year"] >= break_yr).astype(int)
        sub["same_x_post"] = sub["same_industry"] * sub["post"]
        xvars = [var, "same_industry", "same_x_post"]
        dm = sub[xvars].copy()
        yr_m = dm.groupby(sub["event_year"]).transform("mean")
        dm = dm - yr_m
        try:
            m = smf.ols(f"{var} ~ same_industry + same_x_post - 1", data=dm).fit(
                cov_type="cluster", cov_kwds={"groups": sub["vc_firm"]})
            p = m.pvalues["same_x_post"]
            coef = m.params["same_x_post"]
            row += f" {coef:>+8.4f}{sig(p):>3} p={p:.3f}"
        except:
            row += f" {'ERROR':>16}"
    print(row)


print("\n\nDone.")
