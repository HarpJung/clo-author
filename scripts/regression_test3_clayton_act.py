"""Test 3: DOJ/FTC Clayton Act Section 8 Extension to Observers (Jan 2025)
DOJ/FTC explicitly stated board observers are subject to interlocking
directorate prohibition. Same-industry connections become riskier.

Prediction: same-industry spillover DECREASES after Jan 2025.

Design: same_industry x post_jan2025 interaction
Sample: 2020-2025 (post-NVCA-loosening period)
Shows: Overall, Same-ind, Diff-ind for all windows
Specs: No FE + VC-cl, Year FE + VC-cl
Also: placebo at Jan 2024
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 95)
print("TEST 3: CLAYTON ACT SECTION 8 SHOCK (Jan 2025)")
print("DOJ/FTC extended interlocking directorate rules to board observers")
print("=" * 95)

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

event_edges = events.merge(edges, left_on="companyid_str", right_on="observed_companyid", how="inner")

port_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["permno"] = pd.to_numeric(port_daily["permno"], errors="coerce")
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
port_daily = port_daily.dropna(subset=["permno", "ret", "date"]).sort_values(["permno", "date"])
print(f"  Events: {len(events):,}, Edge pairs: {len(event_edges):,}")
print(f"  Daily returns: {len(port_daily):,} (through {port_daily['date'].max().date()})")

# --- Compute CARs ---
print("\n--- Computing CARs ---")
np.random.seed(42)
if len(event_edges) > 150000:
    event_edges = event_edges.sample(150000).reset_index(drop=True)

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
        for wn, d0, d1 in [("car_30", -30, -1), ("car_20", -20, -1),
                            ("car_10", -10, -1), ("car_post5", 0, 5),
                            ("car_post10", 0, 10)]:
            mask = (diffs >= d0) & (diffs <= d1)
            wr = rets[mask]
            if len(wr) >= max(3, abs(d1 - d0) * 0.3):
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
    if (chunk_idx + 1) % 4 == 0 or chunk_idx == total_chunks - 1:
        print(f"    Chunk {chunk_idx+1}/{total_chunks}: {len(car_results):,} CARs")

car_df = pd.DataFrame(car_results)
car_df = car_df[car_df["vc_firm_companyid"] != ""].reset_index(drop=True)
car_df["event_date"] = pd.to_datetime(car_df["event_date"])

jan2025 = pd.Timestamp("2025-01-01")
car_df["post_jan2025"] = (car_df["event_date"] >= jan2025).astype(int)

print(f"\n  Total CARs: {len(car_df):,}")
print(f"  Pre-Jan2025: {(car_df['post_jan2025']==0).sum():,}")
print(f"  Post-Jan2025: {(car_df['post_jan2025']==1).sum():,}")
print(f"  VC firms: {car_df['vc_firm_companyid'].nunique():,}")


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


windows = [("car_30", "CAR[-30,-1]"), ("car_20", "CAR[-20,-1]"),
           ("car_10", "CAR[-10,-1]"), ("car_post5", "CAR[0,+5]"),
           ("car_post10", "CAR[0,+10]")]

subsamples = [
    ("Overall", lambda df: df),
    ("Same-ind", lambda df: df[df["same_industry"] == 1]),
    ("Diff-ind", lambda df: df[df["same_industry"] == 0]),
]


# =====================================================================
# TABLE 1: Subsample means by period (VC-clustered)
# =====================================================================
print("\n\n" + "=" * 100)
print("TABLE 1: SUBSAMPLE MEANS (VC-clustered) Pre vs Post Jan 2025")
print("=" * 100)

for var, label in windows:
    print(f"\n  {label}")
    print(f"  {'Period':<16} {'Subsample':<12} {'N':>8} {'Mean':>10} {'p (VC-cl)':>12} {'Clusters':>10}")
    print(f"  {'-'*68}")
    for pname, pfn in [("Pre-Jan2025", lambda df: df[df["post_jan2025"] == 0]),
                        ("Post-Jan2025", lambda df: df[df["post_jan2025"] == 1]),
                        ("Full", lambda df: df)]:
        for sname, sfn in subsamples:
            sub = sfn(pfn(car_df)).dropna(subset=[var]).copy()
            sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
            n = len(sub)
            if n < 20:
                print(f"  {pname:<16} {sname:<12} {n:>8}  too few")
                continue
            nvc = sub["vc_firm_companyid"].nunique()
            mean_val = sub[var].mean()
            try:
                m = smf.ols(f"{var} ~ 1", data=sub).fit(
                    cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
                pcl = m.pvalues["Intercept"]
            except:
                pcl = np.nan
            print(f"  {pname:<16} {sname:<12} {n:>8,} {mean_val:>+10.5f} {pcl:>9.4f}{sig(pcl)} {nvc:>10,}")
        print()


# =====================================================================
# TABLE 2: Clayton Act shock interaction
# Three samples: (A) Full 2015-2025, (B) Post-2020 only, (C) Placebo @Jan2024
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 2: CLAYTON ACT SHOCK -- same_industry x post_jan2025")
print("=" * 100)

shock_configs = [
    ("Full 2015-2025", car_df.copy(), "post_jan2025"),
    ("Post-2020 only", car_df[car_df["event_year"] >= 2020].copy(), "post_jan2025"),
    ("Placebo @Jan2024", None, "post_jan2024"),  # built below
]

# Build placebo: use 2020-2024 data, fake break at Jan 2024
placebo_df = car_df[(car_df["event_year"] >= 2020) & (car_df["event_year"] <= 2024)].copy()
placebo_df["post_jan2024"] = (placebo_df["event_date"] >= pd.Timestamp("2024-01-01")).astype(int)
shock_configs[2] = ("Placebo @Jan2024", placebo_df, "post_jan2024")

for var, label in windows:
    print(f"\n  {label}")
    print(f"  {'Sample':<22} {'Spec':<18} {'Coef(interact)':>14} {'p':>10} {'N':>8} {'N_post':>8}")
    print(f"  {'-'*80}")

    for sample_name, sample_df, post_col in shock_configs:
        sub = sample_df.dropna(subset=[var, "same_industry"]).copy()
        sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
        n = len(sub)
        n_post = sub[post_col].sum()
        if n < 200 or n_post < 20:
            print(f"  {sample_name:<22} {'--':<18} {'too few post obs':>14} {'':>10} {n:>8,} {n_post:>8,}")
            continue

        sub["same_x_post"] = sub["same_industry"] * sub[post_col]

        # (A) No FE + VC-cluster
        formula = f"{var} ~ same_industry + {post_col} + same_x_post"
        m_a = smf.ols(formula, data=sub).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

        # (B) Year FE + VC-cluster
        xvars = [var, "same_industry", "same_x_post"]
        sub_dm = sub[xvars].copy()
        yr_m = sub_dm.groupby(sub["event_year"]).transform("mean")
        sub_dm = sub_dm - yr_m
        m_b = smf.ols(f"{var} ~ same_industry + same_x_post - 1", data=sub_dm).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

        c_a = m_a.params["same_x_post"]
        p_a = m_a.pvalues["same_x_post"]
        c_b = m_b.params["same_x_post"]
        p_b = m_b.pvalues["same_x_post"]

        print(f"  {sample_name:<22} {'No FE + VC-cl':<18} {c_a:>+12.5f}{sig(p_a)} {p_a:>10.4f} {n:>8,} {n_post:>8,}")
        print(f"  {'':<22} {'Yr FE + VC-cl':<18} {c_b:>+12.5f}{sig(p_b)} {p_b:>10.4f} {n:>8,} {n_post:>8,}")
    print()


# =====================================================================
# TABLE 3: All three shocks compact summary (Year FE + VC-cl)
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 3: ALL SHOCKS COMPACT SUMMARY (Year FE + VC-clustered)")
print("=" * 100)

# Shock 1: NVCA 2020 (use 2015-2024 to avoid Oct2025 contamination)
shock1_df = car_df[car_df["event_year"] <= 2024].copy()
shock1_df["post_2020"] = (shock1_df["event_year"] >= 2020).astype(int)

# Shock 2: Clayton Act Jan 2025 (use 2020-2025)
shock2_df = car_df[car_df["event_year"] >= 2020].copy()
shock2_df["post_jan2025"] = (shock2_df["event_date"] >= jan2025).astype(int)

print(f"\n  {'Window':<16} {'NVCA 2020':>16} {'p':>10} {'Clayton Jan25':>16} {'p':>10} {'Pattern':>10}")
print(f"  {'-'*78}")

for var, label in windows:
    results = []
    for shock_name, shock_df, post_col in [
        ("NVCA 2020", shock1_df, "post_2020"),
        ("Clayton Jan25", shock2_df, "post_jan2025"),
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

    # Pattern: NVCA should be +, Clayton should be -
    if not np.isnan(c1) and not np.isnan(c2):
        if c1 > 0 and c2 < 0:
            pattern = "CONSISTENT"
        elif c1 > 0:
            pattern = "S1 only"
        elif c2 < 0:
            pattern = "S2 only"
        else:
            pattern = "neither"
    else:
        pattern = "N/A"

    s1 = f"{c1:>+12.5f}{sig(p1)}" if not np.isnan(c1) else f"{'N/A':>16}"
    s2 = f"{c2:>+12.5f}{sig(p2)}" if not np.isnan(c2) else f"{'N/A':>16}"
    p1s = f"{p1:>10.4f}" if not np.isnan(p1) else f"{'':>10}"
    p2s = f"{p2:>10.4f}" if not np.isnan(p2) else f"{'':>10}"
    print(f"  {label:<16} {s1} {p1s} {s2} {p2s} {pattern:>10}")

print("\n  NVCA 2020: expect + (fiduciary removal -> more spillover)")
print("  Clayton Jan 2025: expect - (antitrust scrutiny -> less spillover)")
print("  CONSISTENT = NVCA positive AND Clayton negative")

print("\nDone.")
