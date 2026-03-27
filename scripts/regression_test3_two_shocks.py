"""Test 3: Two NVCA Shocks
Shock 1 (2020): Fiduciary language removed -> predict spillover INCREASES
Shock 2 (Oct 2025): Competitive harm carve-outs expanded -> predict spillover DECREASES

Three periods: Pre-2020 / 2020-Sep2025 / Oct2025+
All results shown for: Overall, Same-industry, Diff-industry
Specs: No FE + VC-cluster, Year FE + VC-cluster
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
print("TEST 3: TWO NVCA SHOCKS (2020 loosen + Oct 2025 tighten)")
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
# Now include 2015-2025
events = events[(events["event_year"] >= 2015) & (events["event_year"] <= 2025)]
print(f"  Private events (2015-2025): {len(events):,}")

event_edges = events.merge(edges, left_on="companyid_str", right_on="observed_companyid", how="inner")
print(f"  Event-edge pairs: {len(event_edges):,}")

port_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["permno"] = pd.to_numeric(port_daily["permno"], errors="coerce")
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
port_daily = port_daily.dropna(subset=["permno", "ret", "date"]).sort_values(["permno", "date"])
print(f"  Daily returns: {len(port_daily):,} rows (through {port_daily['date'].max().date()})")

# --- Compute CARs ---
print("\n--- Computing CARs (2015-2025) ---")
np.random.seed(42)
if len(event_edges) > 150000:
    event_edges = event_edges.sample(150000).reset_index(drop=True)
    print(f"  Sampled to {len(event_edges):,}")

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
        for wn, d0, d1 in [("car_60", -60, -1), ("car_30", -30, -1),
                            ("car_10", -10, -1), ("car_post", 0, 5)]:
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

# Create period indicators
car_df["event_date"] = pd.to_datetime(car_df["event_date"])
oct2025 = pd.Timestamp("2025-10-01")
car_df["period"] = "pre2020"
car_df.loc[car_df["event_year"] >= 2020, "period"] = "mid"  # 2020 to Sep 2025
car_df.loc[car_df["event_date"] >= oct2025, "period"] = "post_oct2025"

print(f"\n  Total CARs: {len(car_df):,}")
print(f"  VC firms: {car_df['vc_firm_companyid'].nunique():,}")
print(f"  Period breakdown:")
for p in ["pre2020", "mid", "post_oct2025"]:
    n = (car_df["period"] == p).sum()
    print(f"    {p}: {n:,}")

# Year distribution
print("\n  By year:")
for yr, n in car_df.groupby("event_year").size().items():
    print(f"    {yr}: {n:,}")


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


windows = [("car_60", "CAR[-60,-1]"), ("car_30", "CAR[-30,-1]"),
           ("car_10", "CAR[-10,-1]"), ("car_post", "CAR[0,+5]")]

subsamples = [
    ("Overall", lambda df: df),
    ("Same-ind", lambda df: df[df["same_industry"] == 1]),
    ("Diff-ind", lambda df: df[df["same_industry"] == 0]),
]

periods_list = [
    ("Pre-2020", lambda df: df[df["period"] == "pre2020"]),
    ("2020-Sep2025", lambda df: df[df["period"] == "mid"]),
    ("Oct2025+", lambda df: df[df["period"] == "post_oct2025"]),
    ("Full", lambda df: df),
]


# =====================================================================
# TABLE 1: Subsample means by period (VC-clustered)
# =====================================================================
print("\n\n" + "=" * 100)
print("TABLE 1: SUBSAMPLE MEANS (VC-clustered) across three periods")
print("=" * 100)

for var, label in windows:
    print(f"\n  {label}")
    print(f"  {'Period':<16} {'Subsample':<12} {'N':>8} {'Mean':>10} {'p (VC-cl)':>12} {'Clusters':>10}")
    print(f"  {'-'*68}")
    for pname, pfn in periods_list:
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
# TABLE 2: Shock 1 interaction (same_industry x post_2020)
# Using only 2015-Sep2025 (exclude Oct2025+ to isolate Shock 1)
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 2: SHOCK 1 — same_industry x post_2020 (2015-2019 vs 2020-Sep2025)")
print("Excludes Oct2025+ to isolate the first shock")
print("=" * 100)

shock1_df = car_df[car_df["period"] != "post_oct2025"].copy()
shock1_df["post"] = (shock1_df["event_year"] >= 2020).astype(int)

for var, label in windows:
    sub = shock1_df.dropna(subset=[var, "same_industry"]).copy()
    sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
    if len(sub) < 200:
        continue
    n = len(sub)
    n_pre = (sub["post"] == 0).sum()
    n_post = (sub["post"] == 1).sum()

    sub["same_x_post"] = sub["same_industry"] * sub["post"]

    # No FE + VC-cluster
    formula = f"{var} ~ same_industry + post + same_x_post"
    m_a = smf.ols(formula, data=sub).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

    # Year FE + VC-cluster
    xvars = [var, "same_industry", "same_x_post"]
    sub_dm = sub[xvars].copy()
    yr_m = sub_dm.groupby(sub["event_year"]).transform("mean")
    sub_dm = sub_dm - yr_m
    m_b = smf.ols(f"{var} ~ same_industry + same_x_post - 1", data=sub_dm).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

    print(f"\n  {label} | N={n:,} (pre={n_pre:,}, post={n_post:,})")
    print(f"    {'Spec':<20} {'Coef(interact)':>14} {'p':>10}")
    print(f"    {'-'*44}")
    print(f"    {'No FE + VC-cl':<20} {m_a.params['same_x_post']:>+12.5f}{sig(m_a.pvalues['same_x_post'])} {m_a.pvalues['same_x_post']:>10.4f}")
    print(f"    {'Yr FE + VC-cl':<20} {m_b.params['same_x_post']:>+12.5f}{sig(m_b.pvalues['same_x_post'])} {m_b.pvalues['same_x_post']:>10.4f}")


# =====================================================================
# TABLE 3: Shock 2 interaction (same_industry x post_oct2025)
# Using only 2020-2025 (post-Shock1 period, testing Shock 2)
# =====================================================================
print("\n\n" + "=" * 100)
print("TABLE 3: SHOCK 2 — same_industry x post_oct2025 (2020-Sep2025 vs Oct2025+)")
print("Uses only post-2020 data to isolate the second shock")
print("=" * 100)

shock2_df = car_df[car_df["event_year"] >= 2020].copy()
shock2_df["post_oct"] = (shock2_df["event_date"] >= oct2025).astype(int)

for var, label in windows:
    sub = shock2_df.dropna(subset=[var, "same_industry"]).copy()
    sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
    n = len(sub)
    n_pre = (sub["post_oct"] == 0).sum()
    n_post = (sub["post_oct"] == 1).sum()
    if n < 200 or n_post < 20:
        print(f"\n  {label}: N={n:,} (pre={n_pre:,}, post_oct={n_post:,}) -- too few post obs")
        continue

    sub["same_x_post"] = sub["same_industry"] * sub["post_oct"]

    # No FE + VC-cluster
    formula = f"{var} ~ same_industry + post_oct + same_x_post"
    m_a = smf.ols(formula, data=sub).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

    # Year FE + VC-cluster
    xvars = [var, "same_industry", "same_x_post"]
    sub_dm = sub[xvars].copy()
    yr_m = sub_dm.groupby(sub["event_year"]).transform("mean")
    sub_dm = sub_dm - yr_m
    m_b = smf.ols(f"{var} ~ same_industry + same_x_post - 1", data=sub_dm).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

    print(f"\n  {label} | N={n:,} (2020-Sep25={n_pre:,}, Oct25+={n_post:,})")
    print(f"    {'Spec':<20} {'Coef(interact)':>14} {'p':>10} {'Prediction':>12}")
    print(f"    {'-'*56}")
    pred = "DECREASE" if "post" in label.lower() else "DECREASE"
    print(f"    {'No FE + VC-cl':<20} {m_a.params['same_x_post']:>+12.5f}{sig(m_a.pvalues['same_x_post'])} {m_a.pvalues['same_x_post']:>10.4f} {'(expect -)':>12}")
    print(f"    {'Yr FE + VC-cl':<20} {m_b.params['same_x_post']:>+12.5f}{sig(m_b.pvalues['same_x_post'])} {m_b.pvalues['same_x_post']:>10.4f} {'(expect -)':>12}")


# =====================================================================
# TABLE 4: Compact two-shock summary
# =====================================================================
print("\n\n" + "=" * 100)
print("COMPACT SUMMARY: Two Shocks Side by Side")
print("=" * 100)
print(f"\n  {'Window':<16} {'Shock 1 (2020)':>20} {'p':>10} {'Shock 2 (Oct25)':>20} {'p':>10} {'Consistent?':>12}")
print(f"  {'-'*88}")

for var, label in windows:
    # Shock 1
    sub1 = shock1_df.dropna(subset=[var, "same_industry"]).copy()
    sub1 = sub1[sub1["vc_firm_companyid"] != ""].reset_index(drop=True)
    sub1["same_x_post"] = sub1["same_industry"] * sub1["post"]
    if len(sub1) < 200:
        continue
    xvars1 = [var, "same_industry", "same_x_post"]
    sub1_dm = sub1[xvars1].copy()
    yr_m1 = sub1_dm.groupby(sub1["event_year"]).transform("mean")
    sub1_dm = sub1_dm - yr_m1
    m1 = smf.ols(f"{var} ~ same_industry + same_x_post - 1", data=sub1_dm).fit(
        cov_type="cluster", cov_kwds={"groups": sub1["vc_firm_companyid"]})
    c1 = m1.params["same_x_post"]
    p1 = m1.pvalues["same_x_post"]

    # Shock 2
    sub2 = shock2_df.dropna(subset=[var, "same_industry"]).copy()
    sub2 = sub2[sub2["vc_firm_companyid"] != ""].reset_index(drop=True)
    sub2["post_oct"] = (sub2["event_date"] >= oct2025).astype(int)
    sub2["same_x_post"] = sub2["same_industry"] * sub2["post_oct"]
    n_post2 = sub2["post_oct"].sum()
    if n_post2 < 20:
        print(f"  {label:<16} {c1:>+16.5f}{sig(p1)} {p1:>10.4f} {'too few obs':>20} {'':>10} {'':>12}")
        continue
    xvars2 = [var, "same_industry", "same_x_post"]
    sub2_dm = sub2[xvars2].copy()
    yr_m2 = sub2_dm.groupby(sub2["event_year"]).transform("mean")
    sub2_dm = sub2_dm - yr_m2
    m2 = smf.ols(f"{var} ~ same_industry + same_x_post - 1", data=sub2_dm).fit(
        cov_type="cluster", cov_kwds={"groups": sub2["vc_firm_companyid"]})
    c2 = m2.params["same_x_post"]
    p2 = m2.pvalues["same_x_post"]

    # Consistent = shock1 positive AND shock2 negative
    consistent = "YES" if c1 > 0 and c2 < 0 else "PARTIAL" if (c1 > 0 or c2 < 0) else "NO"
    print(f"  {label:<16} {c1:>+16.5f}{sig(p1)} {p1:>10.4f} {c2:>+16.5f}{sig(p2)} {p2:>10.4f} {consistent:>12}")

print("\n  Shock 1 prediction: positive (fiduciary removal -> more spillover)")
print("  Shock 2 prediction: negative (info restriction -> less spillover)")
print("  Consistent = Shock 1 positive AND Shock 2 negative")

print("\nDone.")
