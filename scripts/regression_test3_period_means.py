"""Subsample means for all periods x all subsamples.
Period breaks: NVCA (pre-2020 / 2020-2024 / post-Oct2025)
               Clayton (pre-Jan2025 / post-Jan2025)
Subsamples: Overall, Same-ind, Diff-ind
Windows: [-30,-1], [-20,-1], [-10,-1], [0,+5], [0,+10]
All VC-firm clustered."""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 100)
print("SUBSAMPLE MEANS: All Periods x All Subsamples")
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

event_edges = events.merge(edges, left_on="companyid_str", right_on="observed_companyid", how="inner")

port_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["permno"] = pd.to_numeric(port_daily["permno"], errors="coerce")
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
port_daily = port_daily.dropna(subset=["permno", "ret", "date"]).sort_values(["permno", "date"])
print(f"  Events: {len(events):,}, Edge pairs: {len(event_edges):,}")
print(f"  Returns: {len(port_daily):,} (through {port_daily['date'].max().date()})")

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
print(f"\n  Total CARs: {len(car_df):,}, VC firms: {car_df['vc_firm_companyid'].nunique():,}")


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


def print_means_table(title, car_data, period_defs):
    """Print means for all windows x subsamples x periods."""
    print(f"\n{'='*105}")
    print(title)
    print(f"{'='*105}")

    for var, label in windows:
        print(f"\n  {label}")
        print(f"  {'Period':<20} {'Subsample':<12} {'N':>8} {'Mean CAR':>10} {'p (VC-cl)':>12} {'Clusters':>10}")
        print(f"  {'-'*72}")

        for pname, pfn in period_defs:
            for sname, sfn in subsamples:
                sub = sfn(pfn(car_data)).dropna(subset=[var]).copy()
                sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
                n = len(sub)
                if n < 20:
                    print(f"  {pname:<20} {sname:<12} {n:>8}  too few")
                    continue
                nvc = sub["vc_firm_companyid"].nunique()
                mean_val = sub[var].mean()
                try:
                    m = smf.ols(f"{var} ~ 1", data=sub).fit(
                        cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
                    pcl = m.pvalues["Intercept"]
                except:
                    pcl = np.nan
                print(f"  {pname:<20} {sname:<12} {n:>8,} {mean_val:>+10.5f} {pcl:>9.4f}{sig(pcl)} {nvc:>10,}")
            print()


# =====================================================================
# PANEL A: NVCA Three Periods (Pre-2020 / 2020-2024 / Post-Oct2025)
# =====================================================================
oct2025 = pd.Timestamp("2025-10-01")

nvca_periods = [
    ("Pre-2020 (baseline)", lambda df: df[df["event_year"] < 2020]),
    ("2020-2024 (loosened)", lambda df: df[(df["event_year"] >= 2020) & (df["event_date"] < oct2025)]),
    ("Oct2025+ (tightened)", lambda df: df[df["event_date"] >= oct2025]),
    ("Full", lambda df: df),
]

print_means_table(
    "PANEL A: NVCA THREE PERIODS\n"
    "  Pre-2020 = before fiduciary removal\n"
    "  2020-2024 = after fiduciary removal, before competitive carve-outs\n"
    "  Oct2025+ = after competitive carve-outs expanded",
    car_df, nvca_periods
)


# =====================================================================
# PANEL B: Clayton Act Two Periods (Pre-Jan2025 / Post-Jan2025)
# =====================================================================
jan2025 = pd.Timestamp("2025-01-01")

clayton_periods = [
    ("Pre-Jan2025", lambda df: df[df["event_date"] < jan2025]),
    ("Post-Jan2025", lambda df: df[df["event_date"] >= jan2025]),
    ("Full", lambda df: df),
]

print_means_table(
    "PANEL B: CLAYTON ACT SECTION 8 TWO PERIODS\n"
    "  Pre-Jan2025 = before DOJ/FTC extended Section 8 to observers\n"
    "  Post-Jan2025 = after extension (antitrust scrutiny on same-ind observers)",
    car_df, clayton_periods
)


# =====================================================================
# PANEL C: Combined four periods
# =====================================================================
combined_periods = [
    ("Pre-2020", lambda df: df[df["event_year"] < 2020]),
    ("2020 to Dec2024", lambda df: df[(df["event_year"] >= 2020) & (df["event_date"] < jan2025)]),
    ("Jan-Sep 2025", lambda df: df[(df["event_date"] >= jan2025) & (df["event_date"] < oct2025)]),
    ("Oct2025+", lambda df: df[df["event_date"] >= oct2025]),
    ("Full", lambda df: df),
]

print_means_table(
    "PANEL C: COMBINED FOUR PERIODS (finest granularity)\n"
    "  Pre-2020: no shocks yet\n"
    "  2020-Dec2024: NVCA loosened, no Clayton\n"
    "  Jan-Sep 2025: NVCA loosened + Clayton tightened\n"
    "  Oct2025+: NVCA loosened + Clayton tightened + NVCA re-tightened",
    car_df, combined_periods
)


# =====================================================================
# PANEL D: Year-by-year for CAR[-10,-1] same-industry (the key result)
# =====================================================================
print(f"\n{'='*105}")
print("PANEL D: YEAR-BY-YEAR for Same-Industry CAR[-10,-1] (the key result)")
print(f"{'='*105}")
print(f"  {'Year':<8} {'N':>8} {'Mean CAR':>10} {'p (VC-cl)':>12} {'Clusters':>10}")
print(f"  {'-'*48}")

for yr in range(2015, 2026):
    sub = car_df[(car_df["event_year"] == yr) & (car_df["same_industry"] == 1)].dropna(subset=["car_10"]).copy()
    sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
    n = len(sub)
    if n < 10:
        print(f"  {yr:<8} {n:>8}  too few")
        continue
    nvc = sub["vc_firm_companyid"].nunique()
    mean_val = sub["car_10"].mean()
    try:
        m = smf.ols("car_10 ~ 1", data=sub).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
        pcl = m.pvalues["Intercept"]
    except:
        pcl = np.nan
    marker = ""
    if yr == 2020: marker = "  <-- NVCA fiduciary removal"
    if yr == 2025: marker = "  <-- Clayton Act + NVCA carve-outs"
    print(f"  {yr:<8} {n:>8,} {mean_val:>+10.5f} {pcl:>9.4f}{sig(pcl)} {nvc:>10,}{marker}")


# Also year-by-year for CAR[-30,-1] same-industry
print(f"\n\nPANEL D2: YEAR-BY-YEAR for Same-Industry CAR[-30,-1]")
print(f"  {'Year':<8} {'N':>8} {'Mean CAR':>10} {'p (VC-cl)':>12} {'Clusters':>10}")
print(f"  {'-'*48}")

for yr in range(2015, 2026):
    sub = car_df[(car_df["event_year"] == yr) & (car_df["same_industry"] == 1)].dropna(subset=["car_30"]).copy()
    sub = sub[sub["vc_firm_companyid"] != ""].reset_index(drop=True)
    n = len(sub)
    if n < 10:
        print(f"  {yr:<8} {n:>8}  too few")
        continue
    nvc = sub["vc_firm_companyid"].nunique()
    mean_val = sub["car_30"].mean()
    try:
        m = smf.ols("car_30 ~ 1", data=sub).fit(
            cov_type="cluster", cov_kwds={"groups": sub["vc_firm_companyid"]})
        pcl = m.pvalues["Intercept"]
    except:
        pcl = np.nan
    marker = ""
    if yr == 2020: marker = "  <-- NVCA fiduciary removal"
    if yr == 2025: marker = "  <-- Clayton Act + NVCA carve-outs"
    print(f"  {yr:<8} {n:>8,} {mean_val:>+10.5f} {pcl:>9.4f}{sig(pcl)} {nvc:>10,}{marker}")


print("\n\nDone.")
