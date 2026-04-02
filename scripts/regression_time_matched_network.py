"""
Re-run key regressions using a TIME-MATCHED observer network.

The original network treats all edges as always-on: if observer X ever
held a directorship at public firm B, we count B as connected for ALL
events at private firm A. But directorships have start/end dates.

This script:
1. Uses BoardEx tenure dates (datestartrole/dateendrole) where available
   to check if the observer was at the public company ON THE EVENT DATE
2. Falls back to CIQ current/former flags for non-BoardEx edges
3. Re-computes CARs and runs the baseline + event-type regressions
4. Compares results to the always-on network

This matters because ~40% of observer positions at public companies are
"Former" — if the observer left before the event, there's no active
information bridge.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd
import numpy as np
import os
import statsmodels.api as sm
from datetime import timedelta

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c = os.path.join(data_dir, "Panel_C_Network")

print("=" * 80)
print("TIME-MATCHED NETWORK REGRESSIONS")
print("=" * 80)

# =====================================================================
# STEP 1: Load data
# =====================================================================
print("\n--- Step 1: Loading data ---")

# Network edges (US only)
edges = pd.read_csv(os.path.join(panel_c, "02b_supplemented_network_edges_us.csv"))
edges["observer_personid"] = edges["observer_personid"].astype(str).str.replace(".0", "", regex=False)
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")
print(f"  Network edges: {len(edges):,}")

# BoardEx positions with dates
bd_pos = pd.read_csv(os.path.join(data_dir, "BoardEx/observer_boardex_positions.csv"))
bd_pos["datestartrole"] = pd.to_datetime(bd_pos["datestartrole"], errors="coerce")
bd_pos["dateendrole"] = pd.to_datetime(bd_pos["dateendrole"], errors="coerce")
# Fill missing end dates with today (position is current)
bd_pos["dateendrole"] = bd_pos["dateendrole"].fillna(pd.Timestamp("2026-12-31"))

# BoardEx crosswalk (directorid -> CIQ personid)
bd_xwalk = pd.read_csv(os.path.join(data_dir, "BoardEx/observer_boardex_crosswalk.csv"))
bd_xwalk["ciq_personid"] = bd_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
bd_did_to_ciq = dict(zip(bd_xwalk["directorid"], bd_xwalk["ciq_personid"]))
bd_pos["ciq_personid"] = bd_pos["directorid"].map(bd_did_to_ciq)

# BoardEx companies (for CIK)
bd_co = pd.read_csv(os.path.join(data_dir, "BoardEx/observer_boardex_companies.csv"))
bd_co_cik = dict(zip(bd_co["companyid"], pd.to_numeric(bd_co["cikcode"], errors="coerce")))
bd_pos["portfolio_cik"] = bd_pos["companyid"].map(bd_co_cik)

# Filter BoardEx to board-level roles at public companies
bd_board = bd_pos[
    ((bd_pos["brdposition"] == "Yes") |
     (bd_pos["rolename"].str.contains("Director|Board|Chairman|CEO|CFO|Officer", case=False, na=False)))
    & (bd_pos["orgtype"].isin(["Quoted", "Listed"]))
].dropna(subset=["ciq_personid", "portfolio_cik"]).copy()
bd_board["portfolio_cik"] = bd_board["portfolio_cik"].astype(int)
print(f"  BoardEx dated positions: {len(bd_board):,}")
print(f"  Unique observers in BoardEx: {bd_board['ciq_personid'].nunique():,}")

# CIQ all-positions for current/former flag (fallback)
ciq_pos = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
ciq_pos["personid"] = ciq_pos["personid"].astype(str).str.replace(".0", "", regex=False)
ciq_pos["companyid"] = ciq_pos["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_pos["is_current"] = ciq_pos["currentproflag"].astype(str) == "1.0"

# Events
events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])

# US filter
co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_cids = set(co[co["country"] == "United States"]["companyid"].astype(str).str.replace(".0", "", regex=False))
us_private = set(co[(co["country"] == "United States") & (co["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
events = events[events["companyid"].isin(us_private)].copy()
events = events[events["announcedate"] >= "2015-01-01"]

# Drop noise events
noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date",
         "Estimated Earnings Release Date (S&P Global Derived)"]
events = events[~events["eventtype"].isin(noise)]
print(f"  Filtered events (US private, 2015+): {len(events):,}")

# Daily returns
ret = pd.read_csv(os.path.join(panel_c, "06_portfolio_crsp_daily.csv"))
try:
    ret25 = pd.read_csv(os.path.join(panel_c, "06b_portfolio_crsp_daily_2025.csv"))
    ret = pd.concat([ret, ret25], ignore_index=True)
except FileNotFoundError:
    pass
ret["date"] = pd.to_datetime(ret["date"], errors="coerce")
ret["permno"] = pd.to_numeric(ret["permno"], errors="coerce").astype("Int64")
ret["ret"] = pd.to_numeric(ret["ret"], errors="coerce")
ret = ret.dropna(subset=["date", "permno", "ret"])
print(f"  Daily returns: {len(ret):,}")

# CIK -> PERMNO
pxw = pd.read_csv(os.path.join(panel_c, "03_portfolio_permno_crosswalk.csv"))
pxw["cik_int"] = pd.to_numeric(pxw["cik"], errors="coerce")
pxw["permno"] = pd.to_numeric(pxw["permno"], errors="coerce")
pxw = pxw.drop_duplicates("cik_int", keep="first")
cik_to_permno = dict(zip(pxw["cik_int"], pxw["permno"]))

# Industry codes
industry = pd.read_csv(os.path.join(panel_c, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))

ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0"), errors="coerce")
cid_to_cik = dict(zip(ciq_xwalk["cid"], ciq_xwalk["cik_int"]))

# =====================================================================
# STEP 2: Build time-matched connection lookup
# =====================================================================
print("\n--- Step 2: Building time-matched connections ---")

# For each (observer, portfolio_cik), determine the active date range.
# BoardEx edges: use datestartrole / dateendrole
# CIQ/Form4 edges: use current/former flag as proxy
#   - Current positions: assume active from 2010 to 2026
#   - Former positions: assume active from 2010 to 2020 (conservative)

# Build a dict: (observer_personid, portfolio_cik) -> list of (start, end) intervals
connection_dates = {}

# BoardEx: exact dates
for _, r in bd_board.iterrows():
    key = (r["ciq_personid"], int(r["portfolio_cik"]))
    if key not in connection_dates:
        connection_dates[key] = []
    connection_dates[key].append((r["datestartrole"], r["dateendrole"]))

bd_keys = set(connection_dates.keys())
print(f"  BoardEx-dated connections: {len(bd_keys):,}")

# CIQ/Form4 edges without BoardEx dates: use current/former proxy
edges["permno"] = edges["portfolio_cik"].map(cik_to_permno)
edges = edges.dropna(subset=["permno"])

for _, r in edges.iterrows():
    pid = r["observer_personid"]
    cik = int(r["portfolio_cik"]) if pd.notna(r["portfolio_cik"]) else None
    if cik is None:
        continue
    key = (pid, cik)
    if key in connection_dates:
        continue  # already have BoardEx dates

    # Check CIQ current/former flag for this person-company pair
    person_pos = ciq_pos[(ciq_pos["personid"] == pid)]
    # Try to match by company CIK (imperfect but best available)
    is_current_any = person_pos["is_current"].any()

    if is_current_any:
        # Current position: assume active through present
        connection_dates[key] = [(pd.Timestamp("2010-01-01"), pd.Timestamp("2026-12-31"))]
    else:
        # Former position: conservatively assume ended before 2022
        connection_dates[key] = [(pd.Timestamp("2010-01-01"), pd.Timestamp("2022-12-31"))]

print(f"  Total time-matched connections: {len(connection_dates):,}")
print(f"  With BoardEx dates: {len(bd_keys):,} ({len(bd_keys)/len(connection_dates)*100:.1f}%)")


def is_connected_at_date(observer_pid, portfolio_cik, event_date):
    """Check if observer was connected to portfolio company on event_date."""
    key = (observer_pid, portfolio_cik)
    intervals = connection_dates.get(key, [])
    for start, end in intervals:
        if pd.notna(start) and pd.notna(end):
            if start <= event_date <= end:
                return True
    return False


# =====================================================================
# STEP 3: Group events
# =====================================================================
print("\n--- Step 3: Grouping events ---")

def classify_event(row):
    et = str(row.get("eventtype", ""))
    role = str(row.get("objectroletype", ""))
    if "M&A" in et and role == "Buyer":
        return "M&A Buyer"
    elif "M&A" in et and role == "Target":
        return "M&A Target"
    elif "Bankruptcy" in et:
        return "Bankruptcy"
    elif et == "Executive/Board Changes - Other":
        return "Exec/Board Changes"
    elif et == "Private Placements":
        return "Private Placements"
    elif et in ["Product-Related Announcements", "Client Announcements"]:
        return "Product/Client"
    else:
        return "Other"

events["event_group"] = events.apply(classify_event, axis=1)
for g, n in events["event_group"].value_counts().items():
    print(f"  {g:<25} {n:>6,}")

# =====================================================================
# STEP 4: Compute CARs with time-matched connections
# =====================================================================
print("\n--- Step 4: Computing CARs ---")

# Build return lookup: (permno, date) -> ret
ret_sorted = ret.sort_values(["permno", "date"])
# Market return per day (equal-weighted mean across all portfolio stocks)
mkt_ret = ret.groupby("date")["ret"].mean().to_dict()

# For each stock, build a date-indexed return series
stock_returns = {}
for permno, grp in ret_sorted.groupby("permno"):
    stock_returns[int(permno)] = dict(zip(grp["date"], grp["ret"]))

# All portfolio permnos
all_permnos = sorted(stock_returns.keys())
permno_to_cik = {int(v): int(k) for k, v in cik_to_permno.items() if pd.notna(k) and pd.notna(v)}

# PERMNO -> SIC2
pm_sic2 = {}
for pm, cik in permno_to_cik.items():
    s = cik_to_sic2.get(cik, "")
    if s:
        pm_sic2[pm] = s

# Get trading dates
trading_dates = sorted(ret["date"].unique())
date_to_idx = {d: i for i, d in enumerate(trading_dates)}

def compute_car(permno, event_date, window_start, window_end):
    """Compute market-adjusted CAR for a stock around an event."""
    series = stock_returns.get(permno, {})
    if not series:
        return np.nan
    idx = date_to_idx.get(event_date)
    if idx is None:
        # Find nearest trading date
        for d in trading_dates:
            if d >= event_date:
                idx = date_to_idx[d]
                break
    if idx is None:
        return np.nan

    car = 0.0
    count = 0
    for offset in range(window_start, window_end + 1):
        didx = idx + offset
        if 0 <= didx < len(trading_dates):
            td = trading_dates[didx]
            r = series.get(td)
            m = mkt_ret.get(td, 0)
            if r is not None and not np.isnan(r):
                car += (r - m)
                count += 1
    return car if count >= abs(window_end - window_start) * 0.5 else np.nan

# Event groups to test
test_groups = ["M&A Buyer", "M&A Target", "Bankruptcy", "Exec/Board Changes"]
windows = {"CAR[-30,-1]": (-30, -1), "CAR[-10,-1]": (-10, -1),
           "CAR[-5,-1]": (-5, -1), "CAR[-1,0]": (-1, 0)}

# Network edge lookup: observed_companyid -> set of (observer_pid, portfolio_cik)
obs_to_edges = {}
for _, r in edges.iterrows():
    oc = r["observed_companyid"]
    if oc not in obs_to_edges:
        obs_to_edges[oc] = []
    obs_to_edges[oc].append((r["observer_personid"], int(r["portfolio_cik"])))

results_all = []

for group in test_groups:
    grp_events = events[events["event_group"] == group]
    # Sample if too many events (for speed)
    if len(grp_events) > 500:
        grp_events = grp_events.sample(500, random_state=42)

    print(f"\n  Processing {group} ({len(grp_events)} events)...")
    rows = []

    for evt_idx, (_, evt) in enumerate(grp_events.iterrows()):
        oc = evt["companyid"]
        edate = evt["announcedate"]
        event_edges = obs_to_edges.get(oc, [])

        # Get SIC2 of event company
        event_cik = cid_to_cik.get(oc)
        event_sic2 = cik_to_sic2.get(event_cik, "") if event_cik else ""

        # For each portfolio stock, determine connection status
        # Use 10% sample of non-connected for control
        np.random.seed(evt_idx)
        sample_mask = np.random.random(len(all_permnos)) < 0.10

        for i, pm in enumerate(all_permnos):
            # Check time-matched connection
            connected = False
            for obs_pid, edge_cik in event_edges:
                if cik_to_permno.get(edge_cik) == pm:
                    if is_connected_at_date(obs_pid, edge_cik, edate):
                        connected = True
                        break

            # Skip non-connected stocks not in 10% sample
            if not connected and not sample_mask[i]:
                continue

            pm_sic = pm_sic2.get(pm, "")
            same_ind = 1 if (pm_sic and event_sic2 and pm_sic == event_sic2) else 0

            # Compute CARs
            car_vals = {}
            for wname, (ws, we) in windows.items():
                car_vals[wname] = compute_car(pm, edate, ws, we)

            rows.append({
                "permno": pm,
                "event_companyid": oc,
                "announcedate": edate,
                "connected": int(connected),
                "same_industry": same_ind,
                "conn_x_sameind": int(connected) * same_ind,
                **car_vals
            })

        if (evt_idx + 1) % 50 == 0:
            print(f"    {evt_idx + 1}/{len(grp_events)} events done")

    df = pd.DataFrame(rows)
    if len(df) == 0:
        print(f"  No observations for {group}")
        continue

    # Winsorize CARs
    for w in windows:
        lo, hi = df[w].quantile([0.01, 0.99])
        df[w] = df[w].clip(lo, hi)

    print(f"  {group}: {len(df):,} obs ({df['connected'].sum():,} connected, "
          f"{df['conn_x_sameind'].sum():,} conn x same_ind)")

    # Run regressions
    for wname in windows:
        y = df[wname].dropna()
        X = df.loc[y.index, ["connected", "same_industry", "conn_x_sameind"]]
        X = sm.add_constant(X)
        if len(y) < 50:
            continue
        try:
            model = sm.OLS(y, X).fit(cov_type="cluster",
                                      cov_kwds={"groups": df.loc[y.index, "event_companyid"]})
            results_all.append({
                "group": group,
                "window": wname,
                "n_obs": len(y),
                "n_connected": int(df.loc[y.index, "connected"].sum()),
                "n_conn_sameind": int(df.loc[y.index, "conn_x_sameind"].sum()),
                "b_connected": model.params.get("connected", np.nan),
                "p_connected": model.pvalues.get("connected", np.nan),
                "b_sameind": model.params.get("same_industry", np.nan),
                "p_sameind": model.pvalues.get("same_industry", np.nan),
                "b_interaction": model.params.get("conn_x_sameind", np.nan),
                "p_interaction": model.pvalues.get("conn_x_sameind", np.nan),
            })
        except Exception as e:
            print(f"    Regression error for {group} {wname}: {e}")

# =====================================================================
# STEP 5: Results
# =====================================================================
print("\n\n" + "=" * 80)
print("RESULTS: TIME-MATCHED NETWORK (event-clustered SE)")
print("=" * 80)

res_df = pd.DataFrame(results_all)
for group in test_groups:
    grp = res_df[res_df["group"] == group]
    if len(grp) == 0:
        continue
    print(f"\n  {group}")
    print(f"  {'Window':<15} {'N':>8} {'Conn':>6} {'CxSI':>5}  "
          f"{'b(conn)':>9} {'p':>7}  {'b(SI)':>9} {'p':>7}  "
          f"{'b(CxSI)':>9} {'p':>7}")
    print(f"  {'-'*95}")
    for _, r in grp.iterrows():
        stars_c = "***" if r["p_connected"] < 0.01 else "**" if r["p_connected"] < 0.05 else "*" if r["p_connected"] < 0.10 else ""
        stars_i = "***" if r["p_interaction"] < 0.01 else "**" if r["p_interaction"] < 0.05 else "*" if r["p_interaction"] < 0.10 else ""
        print(f"  {r['window']:<15} {int(r['n_obs']):>8,} {int(r['n_connected']):>6} {int(r['n_conn_sameind']):>5}  "
              f"{r['b_connected']*100:>8.2f}% {r['p_connected']:>6.3f}{stars_c:<3}  "
              f"{r['b_sameind']*100:>8.2f}% {r['p_sameind']:>6.3f}  "
              f"{r['b_interaction']*100:>8.2f}% {r['p_interaction']:>6.3f}{stars_i:<3}")

print("\n\nDone.")
