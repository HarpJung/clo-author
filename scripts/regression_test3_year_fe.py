"""Test 3 with VC FE + Year FE (double demeaning).
Also: NVCA shock interaction with year FE absorbing the post dummy.

Year FE absorbs market-wide annual shocks so identification comes from:
within the same year AND same VC firm, do same-industry connections
show higher CARs than different-industry connections?

For NVCA shock: post_2020 is collinear with year FE, so we drop the
post main effect and keep only same_industry x post_2020 interaction.
Year FE absorbs any level shift; the interaction captures whether the
same-industry DIFFERENTIAL changed after 2020.
"""

import os, numpy as np, pandas as pd, csv
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 95)
print("TEST 3: VC FE + YEAR FE + NVCA SHOCK INTERACTION")
print("=" * 95)

# =====================================================================
# STEP 1: Load data (same pipeline)
# =====================================================================
print("\n--- Loading data ---")

edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")

port_xwalk = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
port_xwalk["cik_int"] = pd.to_numeric(port_xwalk["cik"], errors="coerce")
edges = edges.merge(
    port_xwalk.drop_duplicates("cik_int", keep="first")[["cik_int", "permno"]].rename(
        columns={"cik_int": "portfolio_cik_int"}),
    on="portfolio_cik_int", how="inner")
edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce")
edges["is_director_at_portfolio"] = edges["portfolio_title"].str.contains(
    "Director|Chairman|Board Member", case=False, na=False).astype(int)

industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))

ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["companyid_str"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.lstrip("0"), errors="coerce")
companyid_to_cik = dict(zip(ciq_xwalk["companyid_str"], ciq_xwalk["cik_int"]))
edges["same_industry"] = (
    edges["observed_companyid"].map(companyid_to_cik).map(cik_to_sic2) ==
    edges["portfolio_cik_int"].map(cik_to_sic2)
).astype(int)

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
events = events[(events["event_year"] >= 2015) & (events["event_year"] <= 2024)]
print(f"  Private events (2015-2024): {len(events):,}")

event_edges = events.merge(edges, left_on="companyid_str", right_on="observed_companyid", how="inner")
print(f"  Event-edge pairs: {len(event_edges):,}")

port_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["permno"] = pd.to_numeric(port_daily["permno"], errors="coerce")
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
port_daily = port_daily.dropna(subset=["permno", "ret", "date"]).sort_values(["permno", "date"])
print(f"  Daily returns: {len(port_daily):,} rows")

# =====================================================================
# STEP 2: Compute CARs
# =====================================================================
print("\n--- Computing CARs ---")
np.random.seed(42)
if len(event_edges) > 120000:
    event_edges = event_edges.sample(120000).reset_index(drop=True)
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
            ey = row["event_date"].year if hasattr(row["event_date"], "year") else pd.Timestamp(row["event_date"]).year
            car_results.append({
                "vc_firm_companyid": str(row.get("vc_firm_companyid", "")),
                "portfolio_id": str(row["permno"]),
                "same_industry": row["same_industry"],
                "is_director_at_portfolio": row["is_director_at_portfolio"],
                "event_year": int(ey),
                **cars,
            })
    if (chunk_idx + 1) % 4 == 0 or chunk_idx == total_chunks - 1:
        print(f"    Chunk {chunk_idx+1}/{total_chunks}: {len(car_results):,} CARs")

car_df = pd.DataFrame(car_results)
car_df = car_df[car_df["vc_firm_companyid"] != ""].reset_index(drop=True)
car_df["post_2020"] = (car_df["event_year"] >= 2020).astype(int)
print(f"\n  Total CARs: {len(car_df):,}")
print(f"  VC firms: {car_df['vc_firm_companyid'].nunique():,}")
print(f"  Years: {sorted(car_df['event_year'].unique())}")


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


windows = [
    ("car_60", "CAR[-60,-1]"),
    ("car_30", "CAR[-30,-1]"),
    ("car_10", "CAR[-10,-1]"),
    ("car_post", "CAR[0,+5]"),
]

# =====================================================================
# PART A: Baseline Test 3 — VC FE + Year FE
# =====================================================================
print("\n\n" + "=" * 95)
print("PART A: BASELINE — same_industry coefficient with different FE combos")
print("=" * 95)

print(f"\n  {'Window':<16} {'(1) None':>14} {'(2) VC-cl':>14} {'(3) VC FE':>14} {'(4) Yr FE':>14} {'(5) VC+Yr FE':>14} {'(6) VC+Yr+cl':>14}")
print(f"  {'-'*100}")

for var, label in windows:
    sub = car_df.dropna(subset=[var, "same_industry"]).copy()
    sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
    if len(sub) < 100:
        continue

    n = len(sub)
    n_vc = sub["vc_firm_companyid"].nunique()
    formula = f"{var} ~ same_industry"

    # (1) OLS HC1
    m1 = smf.ols(formula, data=sub).fit(cov_type="HC1")

    # (2) OLS VC-clustered
    m2 = smf.ols(formula, data=sub).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

    # (3) VC FE only (demean by VC)
    dm_vc = sub.groupby("vc_firm_companyid")[[var, "same_industry"]].transform("mean")
    sub_vc = sub[[var, "same_industry"]] - dm_vc
    m3 = smf.ols(f"{var} ~ same_industry - 1", data=sub_vc).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

    # (4) Year FE only (demean by year)
    dm_yr = sub.groupby("event_year")[[var, "same_industry"]].transform("mean")
    sub_yr = sub[[var, "same_industry"]] - dm_yr
    m4 = smf.ols(f"{var} ~ same_industry - 1", data=sub_yr).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

    # (5) VC FE + Year FE (double demean: Frisch-Waugh iterative)
    # Simple approach: demean by VC, then demean residuals by year
    sub_dm = sub[[var, "same_industry"]].copy()
    # Iterate demeaning (converges quickly for two-way FE)
    for _ in range(10):
        gm_vc = sub.groupby("vc_firm_companyid")[[]].transform("size")  # dummy
        sub_dm_tmp = sub_dm.copy()
        # Demean by VC
        vc_means = sub_dm_tmp.groupby(sub["vc_firm_companyid"]).transform("mean")
        sub_dm_tmp = sub_dm_tmp - vc_means
        # Demean by year
        yr_means = sub_dm_tmp.groupby(sub["event_year"]).transform("mean")
        sub_dm_tmp = sub_dm_tmp - yr_means
        sub_dm = sub_dm_tmp

    m5_hc = smf.ols(f"{var} ~ same_industry - 1", data=sub_dm).fit(cov_type="HC1")

    # (6) VC FE + Year FE + VC clustering
    m6 = smf.ols(f"{var} ~ same_industry - 1", data=sub_dm).fit(
        cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

    row = f"  {label:<16}"
    row += f" {m1.pvalues['same_industry']:>10.4f}{sig(m1.pvalues['same_industry'])}"
    row += f" {m2.pvalues['same_industry']:>10.4f}{sig(m2.pvalues['same_industry'])}"
    row += f" {m3.pvalues['same_industry']:>10.4f}{sig(m3.pvalues['same_industry'])}"
    row += f" {m4.pvalues['same_industry']:>10.4f}{sig(m4.pvalues['same_industry'])}"
    row += f" {m5_hc.pvalues['same_industry']:>10.4f}{sig(m5_hc.pvalues['same_industry'])}"
    row += f" {m6.pvalues['same_industry']:>10.4f}{sig(m6.pvalues['same_industry'])}"
    print(row)

    # Also print coefficients
    row2 = f"  {'  (coef)':<16}"
    row2 += f" {m1.params['same_industry']:>+10.5f} "
    row2 += f" {m2.params['same_industry']:>+10.5f} "
    row2 += f" {m3.params['same_industry']:>+10.5f} "
    row2 += f" {m4.params['same_industry']:>+10.5f} "
    row2 += f" {m5_hc.params['same_industry']:>+10.5f} "
    row2 += f" {m6.params['same_industry']:>+10.5f} "
    print(row2)

print(f"\n  N ~ {len(car_df):,}, VC clusters ~ {car_df['vc_firm_companyid'].nunique():,}")
print("  Specs: (1) OLS+HC1, (2) OLS+VC-cl, (3) VC FE+VC-cl, (4) Yr FE+VC-cl,")
print("         (5) VC+Yr FE+HC1, (6) VC+Yr FE+VC-cl")


# =====================================================================
# PART B: NVCA Shock with Year FE
# =====================================================================
# With year FE, post_2020 main effect is absorbed. We only estimate:
#   CAR = VC_FE + Year_FE + b1*same_industry + b2*(same_ind x post_2020) + e
# b2 captures: did the same-industry DIFFERENTIAL change after 2020,
# controlling for year-level shocks and VC-level heterogeneity?

print("\n\n" + "=" * 95)
print("PART B: NVCA SHOCK — same_industry x post_2020 with Year FE")
print("=" * 95)

samples = [
    ("Full 2015-2024", car_df.copy()),
    ("Ex-COVID (no 2020)", car_df[car_df["event_year"] != 2020].copy()),
    ("Placebo (2015-19, break@2018)", car_df[car_df["event_year"] <= 2019].copy()),
]
# Fix the placebo post indicator
samples[2][1]["post_2020"] = (samples[2][1]["event_year"] >= 2018).astype(int)

print(f"\n  {'Window':<14} {'Sample':<26} {'Spec':<20} {'Coef':>10} {'p':>10} {'Sig':>4} {'N':>8}")
print(f"  {'-'*96}")

for var, label in windows:
    for sample_name, sample_df in samples:
        sub = sample_df.dropna(subset=[var, "same_industry"]).copy()
        sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
        if len(sub) < 200:
            continue

        sub["same_x_post"] = sub["same_industry"] * sub["post_2020"]
        n = len(sub)
        n_vc = sub["vc_firm_companyid"].nunique()

        # --- Spec A: OLS + VC-cluster (no FE) ---
        formula_ols = f"{var} ~ same_industry + post_2020 + same_x_post"
        m_ols = smf.ols(formula_ols, data=sub).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

        # --- Spec B: VC FE + VC-cluster (demean by VC only) ---
        xvars_b = [var, "same_industry", "post_2020", "same_x_post"]
        dm_vc = sub.groupby("vc_firm_companyid")[xvars_b].transform("mean")
        sub_dm_vc = sub[xvars_b] - dm_vc
        m_vcfe = smf.ols(f"{var} ~ same_industry + post_2020 + same_x_post - 1",
                         data=sub_dm_vc).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

        # --- Spec C: VC FE + Year FE + VC-cluster (double demean) ---
        # post_2020 is collinear with year FE, so drop it
        xvars_c = [var, "same_industry", "same_x_post"]
        sub_dm2 = sub[xvars_c].copy()
        for _ in range(10):
            vc_m = sub_dm2.groupby(sub["vc_firm_companyid"]).transform("mean")
            sub_dm2 = sub_dm2 - vc_m
            yr_m = sub_dm2.groupby(sub["event_year"]).transform("mean")
            sub_dm2 = sub_dm2 - yr_m

        m_both = smf.ols(f"{var} ~ same_industry + same_x_post - 1",
                         data=sub_dm2).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})

        # Print interaction coefficient across specs
        for spec_name, model, param in [
            ("OLS+VC-cl", m_ols, "same_x_post"),
            ("VC FE+VC-cl", m_vcfe, "same_x_post"),
            ("VC+Yr FE+cl", m_both, "same_x_post"),
        ]:
            if param in model.params:
                coef = model.params[param]
                p = model.pvalues[param]
                print(f"  {label:<14} {sample_name:<26} {spec_name:<20} {coef:>+10.5f} {p:>10.4f} {sig(p):>4} {n:>8,}")

        # Blank line between samples
        if sample_name != samples[-1][0]:
            pass
    print()  # blank between windows


# =====================================================================
# PART C: Subsample means by period x industry, with year FE context
# =====================================================================
print("\n" + "=" * 95)
print("PART C: SUBSAMPLE MEANS by Period x Industry (for intuition)")
print("=" * 95)

for var, label in windows:
    sub = car_df.dropna(subset=[var]).copy()
    print(f"\n  {label}:")
    print(f"  {'Period x Industry':<30} {'N':>7} {'Mean':>10} {'t-test p':>10} {'VC-cl p':>10}")
    print(f"  {'-'*67}")

    for period_name, period_mask in [("Pre-2020", sub["event_year"] < 2020),
                                      ("Post-2020", sub["event_year"] >= 2020)]:
        for ind_name, ind_val in [("Same-ind", 1), ("Diff-ind", 0)]:
            cell = sub[period_mask & (sub["same_industry"] == ind_val)][var]
            if len(cell) < 30:
                continue
            t, p_t = stats.ttest_1samp(cell, 0)
            # VC-clustered
            cell_df = sub[period_mask & (sub["same_industry"] == ind_val)].copy()
            cell_df = cell_df[cell_df["vc_firm_companyid"] != ""]
            try:
                m_cl = smf.ols(f"{var} ~ 1", data=cell_df).fit(
                    cov_type="cluster", cov_kwds={"groups": cell_df["vc_firm_companyid"]})
                p_cl = m_cl.pvalues["Intercept"]
            except:
                p_cl = np.nan

            print(f"  {period_name + ' ' + ind_name:<30} {len(cell):>7,} {cell.mean():>+10.5f} {p_t:>7.4f}{sig(p_t)} {p_cl:>7.4f}{sig(p_cl)}")


print("\n\nDone.")
