"""Test 3 with Control Group: Connected vs Non-Connected Portfolio Companies.
For each event, compute CARs at ALL portfolio stocks, then compare
connected (through observer network) vs non-connected.

CAR = b1(connected) + b2(same_industry) + b3(connected x same_ind) + Year FE + e

Uses pre-computed cumulative returns for efficiency.
NO SAMPLING — runs everything.
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv
import statsmodels.formula.api as smf
from scipy import stats
import time

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 100)
print("TEST 3 WITH CONTROL GROUP: Connected vs Non-Connected")
print("=" * 100)

# =====================================================================
# STEP 1: Load network edges (to identify connected pairs)
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

# Build connected set: {(observed_companyid, permno)} for fast lookup
connected_set = set()
for _, row in edges.iterrows():
    connected_set.add((row["observed_companyid"], int(row["permno"])))
print(f"  Connected pairs: {len(connected_set):,}")

# Industry mapping
industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))

ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["companyid_str"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.lstrip("0"), errors="coerce")
companyid_to_cik = dict(zip(ciq_xwalk["companyid_str"], ciq_xwalk["cik_int"]))

# Portfolio permno -> SIC2
permno_to_cik = dict(zip(
    port_xwalk["permno"].dropna().astype(int),
    port_xwalk["cik_int"].dropna().astype(int)
))
permno_to_sic2 = {p: cik_to_sic2.get(c, "") for p, c in permno_to_cik.items()}

# =====================================================================
# STEP 2: Load events (filtered)
# =====================================================================
print("\n--- Loading events ---")
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
events = events[events["keydeveventtypename"] != "Announcements of Earnings"]

# Filter CRSP-listed
panel_b_xwalk = pd.read_csv(os.path.join(data_dir, "Panel_B_Outcomes", "01_identifier_crosswalk.csv"))
panel_b_xwalk["cik_int"] = pd.to_numeric(panel_b_xwalk["cik"], errors="coerce")
panel_b_xwalk["linkdt"] = pd.to_datetime(panel_b_xwalk["linkdt"], errors="coerce")
panel_b_xwalk["linkenddt"] = pd.to_datetime(panel_b_xwalk["linkenddt"], errors="coerce").fillna(pd.Timestamp("2099-12-31"))
listing = panel_b_xwalk.groupby("cik_int").agg(first_listed=("linkdt", "min"), last_listed=("linkenddt", "max")).reset_index()
events["cik_int"] = events["companyid_str"].map(companyid_to_cik)
events = events.merge(listing, on="cik_int", how="left")
events["was_public"] = (events["event_date"] >= events["first_listed"]) & (events["event_date"] <= events["last_listed"])
events = events[~events["was_public"].fillna(False)]

# Only keep events from companies that have at least one network edge
observed_with_edges = set(edges["observed_companyid"].unique())
events = events[events["companyid_str"].isin(observed_with_edges)]

events = events[["companyid_str", "event_date", "event_year", "keydeveventtypename"]].drop_duplicates()
print(f"  Events: {len(events):,} from {events['companyid_str'].nunique():,} companies")

# =====================================================================
# STEP 3: Load and pre-process daily returns
# =====================================================================
print("\n--- Loading daily returns ---")
port_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
port_daily["date"] = pd.to_datetime(port_daily["date"])
port_daily["permno"] = pd.to_numeric(port_daily["permno"], errors="coerce").dropna().astype(int)
port_daily["ret"] = pd.to_numeric(port_daily["ret"], errors="coerce")
port_daily = port_daily.dropna(subset=["ret"]).sort_values(["permno", "date"])

all_permnos = sorted(port_daily["permno"].unique())
print(f"  Returns: {len(port_daily):,} rows, {len(all_permnos):,} stocks")

# Pre-compute: for each permno, store dates and cumulative returns as arrays
print("  Pre-computing cumulative returns...")
permno_data = {}
for permno, group in port_daily.groupby("permno"):
    dates = group["date"].values  # numpy datetime64
    rets = group["ret"].values
    cumret = np.cumsum(rets)  # cumulative sum of returns
    permno_data[permno] = (dates, rets, cumret)
print(f"  Pre-computed for {len(permno_data):,} stocks")

# =====================================================================
# STEP 4: Compute CARs for ALL stocks around each event
# =====================================================================
print("\n--- Computing CARs (all stocks x all events) ---")

car_windows = [
    ("car_30", -30, -1), ("car_20", -20, -1), ("car_15", -15, -1),
    ("car_10", -10, -1), ("car_5", -5, -1), ("car_3", -3, -1),
    ("car_2", -2, -1), ("car_1", -1, 0),
    ("car_post3", 0, 3), ("car_post5", 0, 5),
]

n_events = len(events)
n_stocks = len(all_permnos)
print(f"  {n_events:,} events x {n_stocks:,} stocks = {n_events * n_stocks:,} potential pairs")

car_results = []
t0 = time.time()

for ev_idx, (_, ev_row) in enumerate(events.iterrows()):
    event_date = ev_row["event_date"]
    event_np = np.datetime64(event_date)
    observed_cid = ev_row["companyid_str"]
    event_year = ev_row["event_year"]

    # Get observed company's SIC2
    obs_cik = companyid_to_cik.get(observed_cid)
    obs_sic2 = cik_to_sic2.get(obs_cik, "") if obs_cik else ""

    for permno in all_permnos:
        if permno not in permno_data:
            continue

        dates, rets, cumret = permno_data[permno]
        diffs = (dates - event_np).astype("timedelta64[D]").astype(int)

        cars = {}
        for wn, d0, d1 in car_windows:
            mask = (diffs >= d0) & (diffs <= d1)
            wr = rets[mask]
            if len(wr) >= max(2, abs(d1 - d0) * 0.3):
                cars[wn] = float(np.sum(wr))

        if not cars:
            continue

        is_connected = 1 if (observed_cid, permno) in connected_set else 0
        port_sic2 = permno_to_sic2.get(permno, "")
        same_ind = 1 if (obs_sic2 and port_sic2 and obs_sic2 == port_sic2) else 0

        # Get VC firm for clustering (use first edge if connected)
        vc_firm = ""
        if is_connected:
            edge_match = edges[(edges["observed_companyid"] == observed_cid) &
                               (edges["permno"] == permno)]
            if len(edge_match) > 0:
                vc_firm = str(edge_match.iloc[0].get("vc_firm_companyid", ""))

        car_results.append({
            "permno": permno,
            "event_year": event_year,
            "connected": is_connected,
            "same_industry": same_ind,
            "vc_firm": vc_firm,
            **cars,
        })

    if (ev_idx + 1) % 100 == 0:
        elapsed = time.time() - t0
        rate = (ev_idx + 1) / elapsed
        remaining = (n_events - ev_idx - 1) / rate / 60
        print(f"    Event {ev_idx+1:,}/{n_events:,} | {len(car_results):,} CARs | {elapsed:.0f}s | ~{remaining:.0f}min remaining")

car_df = pd.DataFrame(car_results)
print(f"\n  Total CARs: {len(car_df):,}")
print(f"  Connected: {(car_df['connected']==1).sum():,}")
print(f"  Non-connected: {(car_df['connected']==0).sum():,}")

elapsed_total = time.time() - t0
print(f"  Time: {elapsed_total/60:.1f} minutes")


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

# =====================================================================
# TABLE 1: Means by connected/not-connected and same/diff industry
# =====================================================================
print("\n" + "=" * 100)
print("TABLE 1: MEANS by Connection Status x Industry Match")
print("=" * 100)

for var, label in windows:
    sub = car_df.dropna(subset=[var]).copy()
    print(f"\n  {label}")
    print(f"  {'Group':<30} {'N':>10} {'Mean CAR':>10} {'t-test p':>10}")
    print(f"  {'-'*60}")

    for gname, gfn in [
        ("Connected", lambda df: df[df["connected"] == 1]),
        ("Connected + Same-ind", lambda df: df[(df["connected"] == 1) & (df["same_industry"] == 1)]),
        ("Connected + Diff-ind", lambda df: df[(df["connected"] == 1) & (df["same_industry"] == 0)]),
        ("Non-connected", lambda df: df[df["connected"] == 0]),
        ("Non-conn + Same-ind", lambda df: df[(df["connected"] == 0) & (df["same_industry"] == 1)]),
        ("Non-conn + Diff-ind", lambda df: df[(df["connected"] == 0) & (df["same_industry"] == 0)]),
    ]:
        g = gfn(sub)
        n = len(g)
        if n < 30:
            continue
        mean_val = g[var].mean()
        t, p = stats.ttest_1samp(g[var], 0)
        print(f"  {gname:<30} {n:>10,} {mean_val:>+10.6f} {p:>7.4f}{sig(p)}")

    # Difference: connected vs non-connected
    conn = sub[sub["connected"] == 1][var].values
    nonconn = sub[sub["connected"] == 0][var].values
    if len(conn) > 30 and len(nonconn) > 30:
        t_diff, p_diff = stats.ttest_ind(conn, nonconn, equal_var=False)
        print(f"  {'Diff (conn - non-conn)':<30} {'':>10} {conn.mean()-nonconn.mean():>+10.6f} {p_diff:>7.4f}{sig(p_diff)}")


# =====================================================================
# TABLE 2: Regression with connected indicator
# =====================================================================
print("\n\n" + "=" * 100)
print("TABLE 2: REGRESSION — CAR = connected + same_ind + connected x same_ind")
print("Year FE (demeaned), robust SEs")
print("=" * 100)

print(f"\n  {'Window':<14} {'connected':>14} {'p':>8} {'same_ind':>14} {'p':>8} {'conn x same':>14} {'p':>8} {'N':>10}")
print(f"  {'-'*90}")

for var, label in windows:
    sub = car_df.dropna(subset=[var, "connected", "same_industry"]).copy()
    sub = sub.reset_index(drop=True)
    if len(sub) < 200:
        continue

    sub["conn_x_same"] = sub["connected"] * sub["same_industry"]

    # Year FE via demeaning
    xvars = [var, "connected", "same_industry", "conn_x_same"]
    sub_dm = sub[xvars].copy()
    yr_m = sub_dm.groupby(sub["event_year"]).transform("mean")
    sub_dm = sub_dm - yr_m

    m = smf.ols(f"{var} ~ connected + same_industry + conn_x_same - 1", data=sub_dm).fit(cov_type="HC1")

    c1 = m.params["connected"]
    p1 = m.pvalues["connected"]
    c2 = m.params["same_industry"]
    p2 = m.pvalues["same_industry"]
    c3 = m.params["conn_x_same"]
    p3 = m.pvalues["conn_x_same"]

    print(f"  {label:<14} {c1:>+12.6f}{sig(p1)} {p1:>8.4f} {c2:>+12.6f}{sig(p2)} {p2:>8.4f} {c3:>+12.6f}{sig(p3)} {p3:>8.4f} {len(sub):>10,}")


# =====================================================================
# TABLE 3: Same regression but only pre-2020 vs post-2020
# =====================================================================
print("\n\n" + "=" * 100)
print("TABLE 3: REGRESSION by Period (Year FE, HC1)")
print("=" * 100)

for period_name, period_fn in [("Pre-2020", lambda df: df[df["event_year"] < 2020]),
                                 ("Post-2020", lambda df: df[df["event_year"] >= 2020])]:
    print(f"\n  {period_name}:")
    print(f"  {'Window':<14} {'connected':>14} {'p':>8} {'same_ind':>14} {'p':>8} {'conn x same':>14} {'p':>8} {'N':>10}")
    print(f"  {'-'*90}")

    for var, label in windows:
        sub = period_fn(car_df).dropna(subset=[var, "connected", "same_industry"]).copy()
        sub = sub.reset_index(drop=True)
        if len(sub) < 200:
            continue

        sub["conn_x_same"] = sub["connected"] * sub["same_industry"]

        xvars = [var, "connected", "same_industry", "conn_x_same"]
        sub_dm = sub[xvars].copy()
        yr_m = sub_dm.groupby(sub["event_year"]).transform("mean")
        sub_dm = sub_dm - yr_m

        m = smf.ols(f"{var} ~ connected + same_industry + conn_x_same - 1", data=sub_dm).fit(cov_type="HC1")

        c1 = m.params["connected"]
        p1 = m.pvalues["connected"]
        c2 = m.params["same_industry"]
        p2 = m.pvalues["same_industry"]
        c3 = m.params["conn_x_same"]
        p3 = m.pvalues["conn_x_same"]

        print(f"  {label:<14} {c1:>+12.6f}{sig(p1)} {p1:>8.4f} {c2:>+12.6f}{sig(p2)} {p2:>8.4f} {c3:>+12.6f}{sig(p3)} {p3:>8.4f} {len(sub):>10,}")


print("\n\nDone.")
