"""Test 3 Subsample Means: Overall, Same-Industry, Diff-Industry
With VC-clustering and VC FE coefficient for comparison."""

import os, numpy as np, pandas as pd, csv
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

# --- Load data ---
print("Loading data...")
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

event_edges = events.merge(edges, left_on="companyid_str", right_on="observed_companyid", how="inner")

port_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["permno"] = pd.to_numeric(port_daily["permno"], errors="coerce")
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
port_daily = port_daily.dropna(subset=["permno", "ret", "date"]).sort_values(["permno", "date"])

np.random.seed(42)
if len(event_edges) > 80000:
    event_edges = event_edges.sample(80000).reset_index(drop=True)

# --- Compute CARs ---
print("Computing CARs...")
car_results = []
for idx, row in event_edges.iterrows():
    pdata = port_daily[port_daily["permno"] == row["permno"]]
    if len(pdata) < 30:
        continue
    dates = pdata["date"].values
    rets = pdata["ret"].values
    event_np = np.datetime64(row["event_date"])
    diffs = (dates - event_np).astype("timedelta64[D]").astype(int)
    cars = {}
    for wn, d0, d1 in [("car_60", -60, -1), ("car_30", -30, -1), ("car_10", -10, -1), ("car_post", 0, 5)]:
        mask = (diffs >= d0) & (diffs <= d1)
        wr = rets[mask]
        if len(wr) >= max(3, abs(d1 - d0) * 0.3):
            cars[wn] = float(np.sum(wr))
    if cars:
        car_results.append({
            "vc_firm_companyid": str(row.get("vc_firm_companyid", "")),
            "portfolio_id": str(row["permno"]),
            "same_industry": row["same_industry"],
            **cars,
        })
    if len(car_results) % 5000 == 0 and len(car_results) > 0:
        print(f"  {len(car_results):,} CARs...")

car_df = pd.DataFrame(car_results)
car_df = car_df[car_df["vc_firm_companyid"] != ""].reset_index(drop=True)
print(f"Total: {len(car_df):,} CARs, {car_df['vc_firm_companyid'].nunique():,} VC firms\n")


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

print("=" * 95)
print("SUBSAMPLE MEAN CARs + VC FE COEFFICIENT")
print("=" * 95)

for var, label in windows:
    full = car_df.dropna(subset=[var, "same_industry"]).copy().reset_index(drop=True)
    n_full = len(full)
    n_vc = full["vc_firm_companyid"].nunique()

    print(f"\n  {'=' * 88}")
    print(f"  {label} | N = {n_full:,} | VC firms = {n_vc:,}")
    print(f"  {'=' * 88}")
    print(f"  {'Subsample':<22} {'N':>7} {'Mean CAR':>10} {'(1) t-test':>12} {'(2) VC-clust':>14} {'VC clusters':>12}")
    print(f"  {'-' * 77}")

    for ss_name, ss_fn in [
        ("Overall", lambda df: df),
        ("Same-industry", lambda df: df[df["same_industry"] == 1]),
        ("Diff-industry", lambda df: df[df["same_industry"] == 0]),
    ]:
        sub = ss_fn(full).reset_index(drop=True)
        n = len(sub)
        if n < 30:
            print(f"  {ss_name:<22} {n:>7}  too few")
            continue

        mean_val = sub[var].mean()
        n_vc_sub = sub["vc_firm_companyid"].nunique()

        # (1) Simple t-test (unclustered)
        t1, p1 = stats.ttest_1samp(sub[var], 0)

        # (2) VC-firm clustered
        m2 = smf.ols(f"{var} ~ 1", data=sub).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
        p2 = m2.pvalues["Intercept"]

        print(f"  {ss_name:<22} {n:>7} {mean_val:>+10.5f} {p1:>8.4f}{sig(p1)} {p2:>10.4f}{sig(p2)} {n_vc_sub:>12,}")

    # VC FE same_industry coefficient (within-VC comparison)
    dm_cols = [var, "same_industry"]
    gm = full.groupby("vc_firm_companyid")[dm_cols].transform("mean")
    full_dm = full[dm_cols] - gm

    m_fe_hc = smf.ols(f"{var} ~ same_industry - 1", data=full_dm).fit(cov_type="HC1")
    m_fe_cl = smf.ols(f"{var} ~ same_industry - 1", data=full_dm).fit(
        cov_type="cluster", cov_kwds={"groups": full["vc_firm_companyid"]})

    coef = m_fe_cl.params["same_industry"]
    p_hc = m_fe_hc.pvalues["same_industry"]
    p_cl = m_fe_cl.pvalues["same_industry"]

    print(f"  {'-' * 77}")
    print(f"  {'VC FE: same_ind coef':<22} {n_full:>7} {coef:>+10.5f} {p_hc:>8.4f}{sig(p_hc)} {p_cl:>10.4f}{sig(p_cl)} {n_vc:>12,}")
    print(f"  (Within-VC: same-ind vs diff-ind, absorbing VC means)")

print("\n\nDone.")
