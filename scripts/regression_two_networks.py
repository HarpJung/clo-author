"""
Compare two network definitions and run regressions on both.

NETWORK 1 — PERSON-LEVEL (current approach):
  Observer X observes at Private Firm A
  Observer X is director at Public Firm B
  => A is connected to B through X personally

NETWORK 2 — VC-LEVEL (new):
  Observer X observes at Private Firm A
  Observer X works at VC Firm Y
  ANY person at VC Firm Y is director at Public Firm B
  => A is connected to B through VC Firm Y

Network 2 is much larger because it includes public companies where
other VC partners (not the observer themselves) hold board seats.
The information story is: observer learns at Private Firm A's board,
shares with VC colleagues, and information reaches Public Firm B
through a different partner who sits on B's board.

Both networks are time-matched where possible using BoardEx dates.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd
import numpy as np
import os
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c = os.path.join(data_dir, "Panel_C_Network")

print("=" * 90)
print("TWO-NETWORK COMPARISON: Person-Level vs VC-Level")
print("=" * 90)

# =====================================================================
# STEP 1: Load all data
# =====================================================================
print("\n--- Step 1: Loading data ---")

# All positions for all observers
pos = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
pos["personid"] = pos["personid"].astype(str).str.replace(".0", "", regex=False)
pos["companyid"] = pos["companyid"].astype(str).str.replace(".0", "", regex=False)
pos["is_current"] = pos["currentproflag"].astype(str) == "1.0"
print(f"  All positions: {len(pos):,}")

# Observer records (to know who is an observer and where)
obs_rec = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs_rec["personid"] = obs_rec["personid"].astype(str).str.replace(".0", "", regex=False)
obs_rec["companyid"] = obs_rec["companyid"].astype(str).str.replace(".0", "", regex=False)

# US filter
co_det = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_cids = set(co_det[co_det["country"] == "United States"]["companyid"].astype(str).str.replace(".0", "", regex=False))
us_private = set(co_det[(co_det["country"] == "United States") & (co_det["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
obs_rec = obs_rec[obs_rec["companyid"].isin(us_cids)]

# CIK-to-PERMNO
pxw = pd.read_csv(os.path.join(panel_c, "03_portfolio_permno_crosswalk.csv"))
pxw["cik_int"] = pd.to_numeric(pxw["cik"], errors="coerce")
pxw["permno"] = pd.to_numeric(pxw["permno"], errors="coerce")
pxw = pxw.drop_duplicates("cik_int", keep="first")
cik_to_permno = dict(zip(pxw["cik_int"], pxw["permno"]))

# CIQ companyid -> CIK
ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0"), errors="coerce")
cid_to_cik = dict(zip(ciq_xwalk["cid"], ciq_xwalk["cik_int"]))

# Public company CIK lookup from wrds_cik for portfolio companies
pub_cik = pd.read_csv(os.path.join(panel_c, "01_public_portfolio_companies.csv"))
pub_cik["companyid"] = pub_cik["companyid"].astype(str).str.replace(".0", "", regex=False)
pub_cik["cik"] = pd.to_numeric(pub_cik["cik"], errors="coerce")
pubcid_to_cik = dict(zip(pub_cik["companyid"], pub_cik["cik"]))

# Industry
industry = pd.read_csv(os.path.join(panel_c, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))

# Daily returns
ret = pd.read_csv(os.path.join(panel_c, "06_portfolio_crsp_daily.csv"))
try:
    ret25 = pd.read_csv(os.path.join(panel_c, "06b_portfolio_crsp_daily_2025.csv"))
    ret = pd.concat([ret, ret25], ignore_index=True)
except FileNotFoundError:
    pass
ret["date"] = pd.to_datetime(ret["date"], errors="coerce")
ret["permno"] = pd.to_numeric(ret["permno"], errors="coerce")
ret["ret"] = pd.to_numeric(ret["ret"], errors="coerce")
ret = ret.dropna(subset=["date", "permno", "ret"])
print(f"  Daily returns: {len(ret):,}")

# Events
events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])
events = events[events["companyid"].isin(us_private)]
events = events[events["announcedate"] >= "2015-01-01"]
noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date",
         "Estimated Earnings Release Date (S&P Global Derived)"]
events = events[~events["eventtype"].isin(noise)]
print(f"  Filtered events: {len(events):,}")

# =====================================================================
# STEP 2: Build PERSON-LEVEL network (Network 1)
# =====================================================================
print("\n--- Step 2: Build person-level network ---")

# For each observer, find their public company positions
# Observer person -> observed companies
obs_person_to_cos = {}
for _, r in obs_rec.iterrows():
    pid = r["personid"]
    if pid not in obs_person_to_cos:
        obs_person_to_cos[pid] = set()
    obs_person_to_cos[pid].add(r["companyid"])

# Observer person -> public company PERMNOs
pub_positions = pos[pos["companytypename"] == "Public Company"].copy()
person_to_pub_permnos = {}
for _, r in pub_positions.iterrows():
    pid = r["personid"]
    if pid not in obs_person_to_cos:
        continue  # Not an observer
    cik = pubcid_to_cik.get(r["companyid"])
    if cik and not pd.isna(cik):
        pm = cik_to_permno.get(cik)
        if pm and not pd.isna(pm):
            if pid not in person_to_pub_permnos:
                person_to_pub_permnos[pid] = set()
            person_to_pub_permnos[pid].add(int(pm))

# Build Network 1: observed_companyid -> set of connected PERMNOs
net1 = {}  # observed_cid -> set of permnos
for pid, observed_cos in obs_person_to_cos.items():
    pub_pms = person_to_pub_permnos.get(pid, set())
    for oc in observed_cos:
        if oc not in net1:
            net1[oc] = set()
        net1[oc].update(pub_pms)

n1_edges = sum(len(v) for v in net1.values())
n1_obs_cos = len(net1)
n1_pub = len(set().union(*net1.values())) if net1 else 0
print(f"  Network 1 (person-level): {n1_edges:,} edges, {n1_obs_cos:,} observed cos, {n1_pub:,} public stocks")

# =====================================================================
# STEP 3: Build VC-LEVEL network (Network 2)
# =====================================================================
print("\n--- Step 3: Build VC-level network ---")

# Step 3a: For each observer, find their VC firm(s)
vc_types = {"Private Investment Firm", "Public Investment Firm", "Private Fund"}
vc_positions = pos[pos["companytypename"].isin(vc_types)]

# observer person -> set of VC firm companyids
obs_person_to_vcs = {}
for _, r in vc_positions.iterrows():
    pid = r["personid"]
    if pid in obs_person_to_cos:  # Only for our observers
        if pid not in obs_person_to_vcs:
            obs_person_to_vcs[pid] = set()
        obs_person_to_vcs[pid].add(r["companyid"])

# Step 3b: For each VC firm, find ALL people who work there
vc_to_people = {}
for _, r in vc_positions.iterrows():
    vc_cid = r["companyid"]
    if vc_cid not in vc_to_people:
        vc_to_people[vc_cid] = set()
    vc_to_people[vc_cid].add(r["personid"])

print(f"  VC firms identified: {len(vc_to_people):,}")
print(f"  Total VC employees: {len(set().union(*vc_to_people.values())):,}")

# Step 3c: For each VC employee, find their public company positions
# (not just observers — ALL people at the VC firm)
all_vc_people = set().union(*vc_to_people.values())
vc_person_pub = {}
for _, r in pub_positions.iterrows():
    pid = r["personid"]
    if pid not in all_vc_people:
        continue
    # Must be a board-level role
    title = str(r.get("title", "")).lower()
    board = str(r.get("boardflag", "")) == "1.0" or str(r.get("currentboardflag", "")) == "1.0"
    if not board and "director" not in title and "chairman" not in title:
        continue
    cik = pubcid_to_cik.get(r["companyid"])
    if cik and not pd.isna(cik):
        pm = cik_to_permno.get(cik)
        if pm and not pd.isna(pm):
            if pid not in vc_person_pub:
                vc_person_pub[pid] = set()
            vc_person_pub[pid].add(int(pm))

# Step 3d: Build time-aware lookups for VC-level connections
# We need to check: was the VC partner at the public company at the time
# of the event? Use BoardEx dates where available, CIQ current flag as fallback.

# BoardEx: directorid -> ciq_personid
bd_xwalk = pd.read_csv(os.path.join(data_dir, "BoardEx/observer_boardex_crosswalk.csv"))
bd_xwalk["ciq_personid"] = bd_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
bd_did_to_ciq = dict(zip(bd_xwalk["directorid"], bd_xwalk["ciq_personid"]))
ciq_to_bd_did = {}
for did, cpid in bd_did_to_ciq.items():
    if cpid not in ciq_to_bd_did:
        ciq_to_bd_did[cpid] = []
    ciq_to_bd_did[cpid].append(did)

# BoardEx positions with dates
bd_pos_raw = pd.read_csv(os.path.join(data_dir, "BoardEx/observer_boardex_positions.csv"))
bd_pos_raw["datestartrole"] = pd.to_datetime(bd_pos_raw["datestartrole"], errors="coerce")
bd_pos_raw["dateendrole"] = pd.to_datetime(bd_pos_raw["dateendrole"], errors="coerce")
bd_pos_raw["dateendrole"] = bd_pos_raw["dateendrole"].fillna(pd.Timestamp("2026-12-31"))
bd_co_raw = pd.read_csv(os.path.join(data_dir, "BoardEx/observer_boardex_companies.csv"))
bd_co_cik_map = dict(zip(bd_co_raw["companyid"], pd.to_numeric(bd_co_raw["cikcode"], errors="coerce")))

# Build: ciq_personid -> list of (permno, start_date, end_date)
person_pub_dated = {}  # ciq_personid -> [(permno, start, end), ...]
bd_pub_pos = bd_pos_raw[bd_pos_raw["orgtype"].isin(["Quoted", "Listed"])].copy()
bd_pub_pos["ciq_personid"] = bd_pub_pos["directorid"].map(bd_did_to_ciq)
bd_pub_pos["cik"] = bd_pub_pos["companyid"].map(bd_co_cik_map)
bd_pub_pos = bd_pub_pos.dropna(subset=["ciq_personid", "cik"])
bd_pub_pos["permno"] = bd_pub_pos["cik"].map(cik_to_permno)
bd_pub_pos = bd_pub_pos.dropna(subset=["permno"])

for _, r in bd_pub_pos.iterrows():
    pid = r["ciq_personid"]
    if pid not in person_pub_dated:
        person_pub_dated[pid] = []
    person_pub_dated[pid].append((int(r["permno"]), r["datestartrole"], r["dateendrole"]))

print(f"  BoardEx-dated person-pub connections: {len(person_pub_dated):,} people")

# For people NOT in BoardEx: use CIQ current flag
# Current = assume active 2010-2026; Former = assume active 2010-2020
for _, r in pub_positions.iterrows():
    pid = r["personid"]
    if pid in person_pub_dated:
        continue  # already have BoardEx dates
    if pid not in all_vc_people:
        continue
    title = str(r.get("title", "")).lower()
    board = str(r.get("boardflag", "")) == "1.0" or str(r.get("currentboardflag", "")) == "1.0"
    if not board and "director" not in title and "chairman" not in title:
        continue
    cik = pubcid_to_cik.get(r["companyid"])
    if cik and not pd.isna(cik):
        pm = cik_to_permno.get(cik)
        if pm and not pd.isna(pm):
            if pid not in person_pub_dated:
                person_pub_dated[pid] = []
            is_cur = str(r.get("currentproflag", "")) == "1.0"
            if is_cur:
                person_pub_dated[pid].append((int(pm), pd.Timestamp("2010-01-01"), pd.Timestamp("2026-12-31")))
            else:
                person_pub_dated[pid].append((int(pm), pd.Timestamp("2010-01-01"), pd.Timestamp("2020-12-31")))

print(f"  Total person-pub dated connections: {len(person_pub_dated):,} people")

# Step 3d: Build FAST lookup dicts for time-matched connections
#
# Pre-compute: observed_companyid -> list of observer personids
# This avoids iterating over ALL observers for every event.
obs_cos_to_persons = {}  # observed_cid -> [observer_pid1, pid2, ...]
for obs_pid, obs_cos in obs_person_to_cos.items():
    for oc in obs_cos:
        if oc not in obs_cos_to_persons:
            obs_cos_to_persons[oc] = []
        obs_cos_to_persons[oc].append(obs_pid)

# Pre-compute: observer_pid -> set of all VC people (via their VC firms)
obs_to_vc_people = {}  # observer_pid -> set of all people at their VCs
for obs_pid in obs_person_to_cos:
    vc_firms = obs_person_to_vcs.get(obs_pid, set())
    all_vp = set()
    for vc_cid in vc_firms:
        all_vp.update(vc_to_people.get(vc_cid, set()))
    if all_vp:
        obs_to_vc_people[obs_pid] = all_vp

print(f"  Pre-built lookup: {len(obs_cos_to_persons):,} observed cos -> observer lists")
print(f"  Pre-built lookup: {len(obs_to_vc_people):,} observers -> VC colleague sets")

def get_person_connections_at_date(observed_cid, event_date):
    """Get person-level connected PERMNOs on a given date (FAST version)."""
    connected_pms = set()
    for obs_pid in obs_cos_to_persons.get(observed_cid, []):
        for pm, start, end in person_pub_dated.get(obs_pid, []):
            if start <= event_date <= end:
                connected_pms.add(pm)
    return connected_pms

def get_vc_connections_at_date(observed_cid, event_date):
    """Get VC-level connected PERMNOs on a given date (FAST version).

    Chain: observed_cid -> observers at that company -> their VC firms ->
           all people at those VCs -> their public company positions active on event_date
    """
    connected_pms = set()
    for obs_pid in obs_cos_to_persons.get(observed_cid, []):
        vc_people_set = obs_to_vc_people.get(obs_pid, set())
        for vp in vc_people_set:
            for pm, start, end in person_pub_dated.get(vp, []):
                if start <= event_date <= end:
                    connected_pms.add(pm)
    return connected_pms

# Quick stats: sample a few events to see network sizes
import random
sample_events = events.sample(min(50, len(events)), random_state=42)
n1_sizes, n2_sizes = [], []
for _, evt in sample_events.iterrows():
    p1 = get_person_connections_at_date(evt["companyid"], evt["announcedate"])
    p2 = get_vc_connections_at_date(evt["companyid"], evt["announcedate"])
    n1_sizes.append(len(p1))
    n2_sizes.append(len(p2))

print(f"\n  Time-matched network sizes (sample of {len(sample_events)} events):")
print(f"    Person-level: mean {np.mean(n1_sizes):.1f}, median {np.median(n1_sizes):.0f}, max {max(n1_sizes)}")
print(f"    VC-level:     mean {np.mean(n2_sizes):.1f}, median {np.median(n2_sizes):.0f}, max {max(n2_sizes)}")
print(f"    Expansion:    {np.mean(n2_sizes)/max(np.mean(n1_sizes),0.01):.1f}x")

# VC-level network is now time-matched (callable), so we report
# the sample-based stats from above instead of static edge counts.
print(f"  Network 2 (VC-level):     time-matched (callable), ~{np.mean(n2_sizes):.1f} connections/event avg")
print(f"  Expansion factor:         ~{np.mean(n2_sizes)/max(np.mean(n1_sizes),0.01):.1f}x vs person-level")

# =====================================================================
# STEP 4: Compute CARs and run regressions for both networks
# =====================================================================
print("\n--- Step 4: Regressions ---")

# Market return per day
mkt_ret = ret.groupby("date")["ret"].mean().to_dict()
trading_dates = sorted(ret["date"].unique())
date_to_idx = {d: i for i, d in enumerate(trading_dates)}

# Per-stock returns
stock_returns = {}
for pm, grp in ret.groupby("permno"):
    stock_returns[int(pm)] = dict(zip(grp["date"], grp["ret"]))

all_permnos = sorted(stock_returns.keys())
pm_sic2 = {}
for pm in all_permnos:
    cik = {int(v): int(k) for k, v in cik_to_permno.items() if pd.notna(k) and pd.notna(v)}.get(pm)
    if cik:
        pm_sic2[pm] = cik_to_sic2.get(cik, "")

def compute_car(permno, event_date, ws, we):
    series = stock_returns.get(permno, {})
    if not series:
        return np.nan
    idx = date_to_idx.get(event_date)
    if idx is None:
        for d in trading_dates:
            if d >= event_date:
                idx = date_to_idx[d]
                break
    if idx is None:
        return np.nan
    car = 0.0
    count = 0
    for offset in range(ws, we + 1):
        didx = idx + offset
        if 0 <= didx < len(trading_dates):
            td = trading_dates[didx]
            r = series.get(td)
            m = mkt_ret.get(td, 0)
            if r is not None and not np.isnan(r):
                car += (r - m)
                count += 1
    return car if count >= max(1, abs(we - ws) * 0.5) else np.nan

# Event grouping
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
    else:
        return None

events["event_group"] = events.apply(classify_event, axis=1)
events = events.dropna(subset=["event_group"])

test_groups = ["M&A Buyer", "M&A Target", "Bankruptcy", "Exec/Board Changes"]
windows = {"CAR[-30,-1]": (-30, -1), "CAR[-10,-1]": (-10, -1),
           "CAR[-5,-1]": (-5, -1), "CAR[-1,0]": (-1, 0)}

def run_regressions(network_func, network_name, events_df, max_events=300):
    """Run regressions for one network definition.

    network_func: either a dict (static) or callable (time-matched)
        If dict: network_func[observed_cid] -> set of permnos
        If callable: network_func(observed_cid, event_date) -> set of permnos
    """
    is_callable = callable(network_func)
    all_results = []

    for group in test_groups:
        grp_events = events_df[events_df["event_group"] == group]
        if len(grp_events) > max_events:
            grp_events = grp_events.sample(max_events, random_state=42)

        print(f"\n  [{network_name}] {group}: {len(grp_events)} events")
        rows = []

        for evt_i, (_, evt) in enumerate(grp_events.iterrows()):
            oc = evt["companyid"]
            edate = evt["announcedate"]
            if is_callable:
                connected_pms = network_func(oc, edate)
            else:
                connected_pms = network_func.get(oc, set())

            event_cik = cid_to_cik.get(oc)
            event_sic2 = cik_to_sic2.get(event_cik, "") if event_cik else ""

            np.random.seed(evt_i)
            sample_mask = np.random.random(len(all_permnos)) < 0.10

            for i, pm in enumerate(all_permnos):
                conn = 1 if pm in connected_pms else 0
                if conn == 0 and not sample_mask[i]:
                    continue

                si = 1 if (pm_sic2.get(pm, "") and event_sic2 and pm_sic2[pm] == event_sic2) else 0
                cars = {}
                for wn, (ws, we) in windows.items():
                    cars[wn] = compute_car(pm, edate, ws, we)

                rows.append({
                    "permno": pm, "event_cid": oc, "announcedate": edate,
                    "connected": conn, "same_industry": si,
                    "conn_x_sameind": conn * si, **cars
                })

            if (evt_i + 1) % 100 == 0:
                print(f"    {evt_i+1}/{len(grp_events)}")

        df = pd.DataFrame(rows)
        if len(df) == 0:
            continue

        for w in windows:
            lo, hi = df[w].quantile([0.01, 0.99])
            df[w] = df[w].clip(lo, hi)

        n_conn = df["connected"].sum()
        n_cxsi = df["conn_x_sameind"].sum()
        print(f"    {len(df):,} obs, {n_conn:,} connected, {n_cxsi:,} conn x same_ind")

        for wn in windows:
            y = df[wn].dropna()
            X = df.loc[y.index, ["connected", "same_industry", "conn_x_sameind"]]
            X = sm.add_constant(X)
            if len(y) < 50:
                continue
            try:
                m = sm.OLS(y, X).fit(cov_type="cluster",
                                      cov_kwds={"groups": df.loc[y.index, "event_cid"]})
                all_results.append({
                    "network": network_name, "group": group, "window": wn,
                    "n_obs": len(y), "n_connected": int(n_conn), "n_cxsi": int(n_cxsi),
                    "b_conn": m.params.get("connected", np.nan),
                    "p_conn": m.pvalues.get("connected", np.nan),
                    "b_si": m.params.get("same_industry", np.nan),
                    "p_si": m.pvalues.get("same_industry", np.nan),
                    "b_cxsi": m.params.get("conn_x_sameind", np.nan),
                    "p_cxsi": m.pvalues.get("conn_x_sameind", np.nan),
                })
            except Exception as e:
                print(f"    Error {wn}: {e}")

    return pd.DataFrame(all_results)

# Run both networks (time-matched versions)
res1 = run_regressions(get_person_connections_at_date, "Person", events, max_events=200)
res2 = run_regressions(get_vc_connections_at_date, "VC-Level", events, max_events=200)
all_res = pd.concat([res1, res2], ignore_index=True)

# =====================================================================
# STEP 5: Display comparison
# =====================================================================
print("\n\n" + "=" * 90)
print("RESULTS COMPARISON: Person-Level vs VC-Level Network")
print("=" * 90)

for group in test_groups:
    print(f"\n{'─'*90}")
    print(f"  {group}")
    print(f"{'─'*90}")
    print(f"  {'Network':<10} {'Window':<15} {'N':>8} {'Conn':>6} {'CxSI':>5}  "
          f"{'b(Conn)':>9} {'p':>6}  {'b(CxSI)':>9} {'p':>6}")

    for net_name in ["Person", "VC-Level"]:
        grp = all_res[(all_res["group"] == group) & (all_res["network"] == net_name)]
        for _, r in grp.iterrows():
            sc = "***" if r["p_conn"] < 0.01 else "**" if r["p_conn"] < 0.05 else "*" if r["p_conn"] < 0.10 else ""
            si = "***" if r["p_cxsi"] < 0.01 else "**" if r["p_cxsi"] < 0.05 else "*" if r["p_cxsi"] < 0.10 else ""
            print(f"  {net_name:<10} {r['window']:<15} {int(r['n_obs']):>8,} {int(r['n_connected']):>6,} {int(r['n_cxsi']):>5}  "
                  f"{r['b_conn']*100:>8.2f}%{sc:<3} {r['p_conn']:>5.3f}  "
                  f"{r['b_cxsi']*100:>8.2f}%{si:<3} {r['p_cxsi']:>5.3f}")

# Save
all_res.to_csv(os.path.join(data_dir, "Analysis_Ready/two_network_comparison.csv"), index=False)
print(f"\nSaved: Analysis_Ready/two_network_comparison.csv")
print("\nDone.")
