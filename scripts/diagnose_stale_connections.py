"""
Diagnose how many connections in the original always-on network were
actually active at the time of each event vs. stale (observer had left).

For every (event, connected stock) pair in the original regressions,
check whether the observer was still at the public company on the event date
using BoardEx tenure dates where available and CIQ current/former flag otherwise.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd
import numpy as np
import os

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c = os.path.join(data_dir, "Panel_C_Network")

print("=" * 80)
print("STALE vs ACTIVE CONNECTION DIAGNOSTIC")
print("=" * 80)

# =====================================================================
# Load all data
# =====================================================================
print("\n--- Loading data ---")

# Network edges (US only, always-on)
edges = pd.read_csv(os.path.join(panel_c, "02b_supplemented_network_edges_us.csv"))
edges["observer_personid"] = edges["observer_personid"].astype(str).str.replace(".0", "", regex=False)
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")

# CIK -> PERMNO
pxw = pd.read_csv(os.path.join(panel_c, "03_portfolio_permno_crosswalk.csv"))
pxw["cik_int"] = pd.to_numeric(pxw["cik"], errors="coerce")
pxw["permno"] = pd.to_numeric(pxw["permno"], errors="coerce")
pxw = pxw.drop_duplicates("cik_int", keep="first")
cik_to_permno = dict(zip(pxw["cik_int"], pxw["permno"]))
edges["permno"] = edges["portfolio_cik"].map(cik_to_permno)
edges = edges.dropna(subset=["permno"])
edges["permno"] = edges["permno"].astype(int)

# Industry
industry = pd.read_csv(os.path.join(panel_c, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))

ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0"), errors="coerce")
cid_to_cik = dict(zip(ciq_xwalk["cid"], ciq_xwalk["cik_int"]))

permno_to_cik = {int(v): int(k) for k, v in cik_to_permno.items() if pd.notna(k) and pd.notna(v)}
pm_sic2 = {pm: cik_to_sic2.get(cik, "") for pm, cik in permno_to_cik.items()}

# BoardEx positions with dates
bd_pos = pd.read_csv(os.path.join(data_dir, "BoardEx/observer_boardex_positions.csv"))
bd_pos["datestartrole"] = pd.to_datetime(bd_pos["datestartrole"], errors="coerce")
bd_pos["dateendrole"] = pd.to_datetime(bd_pos["dateendrole"], errors="coerce")
bd_pos["dateendrole"] = bd_pos["dateendrole"].fillna(pd.Timestamp("2026-12-31"))

bd_xwalk = pd.read_csv(os.path.join(data_dir, "BoardEx/observer_boardex_crosswalk.csv"))
bd_xwalk["ciq_personid"] = bd_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
bd_did_to_ciq = dict(zip(bd_xwalk["directorid"], bd_xwalk["ciq_personid"]))
bd_pos["ciq_personid"] = bd_pos["directorid"].map(bd_did_to_ciq)

bd_co = pd.read_csv(os.path.join(data_dir, "BoardEx/observer_boardex_companies.csv"))
bd_co_cik = dict(zip(bd_co["companyid"], pd.to_numeric(bd_co["cikcode"], errors="coerce")))
bd_pos["portfolio_cik"] = bd_pos["companyid"].map(bd_co_cik)

# Build BoardEx date lookup: (ciq_personid, portfolio_cik) -> [(start, end), ...]
bd_pub = bd_pos[bd_pos["orgtype"].isin(["Quoted", "Listed"])].dropna(subset=["ciq_personid", "portfolio_cik"])
bd_dates = {}
for _, r in bd_pub.iterrows():
    key = (r["ciq_personid"], int(r["portfolio_cik"]))
    if key not in bd_dates:
        bd_dates[key] = []
    bd_dates[key].append((r["datestartrole"], r["dateendrole"]))

print(f"  BoardEx dated (person, public co) pairs: {len(bd_dates):,}")

# CIQ current/former flag
ciq_pos = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
ciq_pos["personid"] = ciq_pos["personid"].astype(str).str.replace(".0", "", regex=False)
ciq_pos["companyid"] = ciq_pos["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_pos["is_current"] = ciq_pos["currentproflag"].astype(str) == "1.0"

# Build CIQ current lookup: (personid, companyid) -> is_current
ciq_current = {}
pub_cik_lookup = pd.read_csv(os.path.join(panel_c, "01_public_portfolio_companies.csv"))
pub_cik_lookup["companyid"] = pub_cik_lookup["companyid"].astype(str).str.replace(".0", "", regex=False)
pub_cik_lookup["cik"] = pd.to_numeric(pub_cik_lookup["cik"], errors="coerce")
pubcid_to_cik = dict(zip(pub_cik_lookup["companyid"], pub_cik_lookup["cik"]))

for _, r in ciq_pos[ciq_pos["companytypename"] == "Public Company"].iterrows():
    pid = r["personid"]
    cik = pubcid_to_cik.get(r["companyid"])
    if cik and not pd.isna(cik):
        key = (pid, int(cik))
        # Keep as current if ANY position at that company is current
        if key not in ciq_current:
            ciq_current[key] = r["is_current"]
        elif r["is_current"]:
            ciq_current[key] = True

print(f"  CIQ current/former (person, CIK) pairs: {len(ciq_current):,}")

# Events
events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])

co_det = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_private = set(co_det[(co_det["country"] == "United States") & (co_det["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
events = events[events["companyid"].isin(us_private)]
events = events[events["announcedate"] >= "2015-01-01"]

noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date",
         "Estimated Earnings Release Date (S&P Global Derived)"]
events = events[~events["eventtype"].isin(noise)]

# Classify events
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

# =====================================================================
# Build edge lookup: observed_companyid -> [(observer_pid, permno, portfolio_cik), ...]
# =====================================================================
edge_lookup = {}
for _, r in edges.iterrows():
    oc = r["observed_companyid"]
    if oc not in edge_lookup:
        edge_lookup[oc] = []
    edge_lookup[oc].append((r["observer_personid"], r["permno"], int(r["portfolio_cik"])))


def check_connection_status(observer_pid, portfolio_cik, event_date):
    """
    Check if the observer was at the public company on the event date.
    Returns: 'active_boardex', 'active_ciq_current', 'stale_boardex',
             'stale_ciq_former', 'unknown'
    """
    key = (observer_pid, portfolio_cik)

    # Check BoardEx first (has dates)
    intervals = bd_dates.get(key, [])
    if intervals:
        for start, end in intervals:
            if pd.notna(start) and pd.notna(end) and start <= event_date <= end:
                return "active_boardex"
        return "stale_boardex"

    # Fall back to CIQ current/former
    is_current = ciq_current.get(key)
    if is_current is True:
        return "active_ciq_current"
    elif is_current is False:
        return "stale_ciq_former"

    return "unknown"


# =====================================================================
# For each event group, check every connected observation
# =====================================================================
print("\n--- Checking connection status for all event groups ---")

test_groups = ["M&A Buyer", "M&A Target", "Bankruptcy", "Exec/Board Changes",
               "Private Placements", "Product/Client"]

for group in test_groups:
    grp_events = events[events["event_group"] == group]
    print(f"\n{'='*80}")
    print(f"  {group}: {len(grp_events):,} events")
    print(f"{'='*80}")

    # For each event, find connected stocks and check status
    statuses_all = []
    statuses_same_ind = []

    for _, evt in grp_events.iterrows():
        oc = evt["companyid"]
        edate = evt["announcedate"]
        event_cik = cid_to_cik.get(oc)
        event_sic2 = cik_to_sic2.get(event_cik, "") if event_cik else ""

        for obs_pid, permno, port_cik in edge_lookup.get(oc, []):
            status = check_connection_status(obs_pid, port_cik, edate)
            port_sic2 = pm_sic2.get(permno, "")
            same_ind = (port_sic2 and event_sic2 and port_sic2 == event_sic2)

            statuses_all.append(status)
            if same_ind:
                statuses_same_ind.append(status)

    # Summarize
    from collections import Counter
    all_counts = Counter(statuses_all)
    si_counts = Counter(statuses_same_ind)

    total = len(statuses_all)
    total_si = len(statuses_same_ind)

    print(f"\n  ALL CONNECTED OBSERVATIONS: {total:,}")
    if total > 0:
        for status in ["active_boardex", "active_ciq_current", "stale_boardex", "stale_ciq_former", "unknown"]:
            n = all_counts.get(status, 0)
            pct = n / total * 100
            label = {
                "active_boardex": "ACTIVE (BoardEx dated)",
                "active_ciq_current": "ACTIVE (CIQ current flag)",
                "stale_boardex": "STALE (BoardEx: observer left)",
                "stale_ciq_former": "STALE (CIQ: former position)",
                "unknown": "UNKNOWN (no date info)",
            }[status]
            print(f"    {label:<45} {n:>6,}  ({pct:>5.1f}%)")

        active = all_counts.get("active_boardex", 0) + all_counts.get("active_ciq_current", 0)
        stale = all_counts.get("stale_boardex", 0) + all_counts.get("stale_ciq_former", 0)
        print(f"    {'TOTAL ACTIVE':<45} {active:>6,}  ({active/total*100:>5.1f}%)")
        print(f"    {'TOTAL STALE':<45} {stale:>6,}  ({stale/total*100:>5.1f}%)")

    print(f"\n  SAME-INDUSTRY CONNECTED OBSERVATIONS: {total_si:,}")
    if total_si > 0:
        for status in ["active_boardex", "active_ciq_current", "stale_boardex", "stale_ciq_former", "unknown"]:
            n = si_counts.get(status, 0)
            pct = n / total_si * 100
            label = {
                "active_boardex": "ACTIVE (BoardEx dated)",
                "active_ciq_current": "ACTIVE (CIQ current flag)",
                "stale_boardex": "STALE (BoardEx: observer left)",
                "stale_ciq_former": "STALE (CIQ: former position)",
                "unknown": "UNKNOWN (no date info)",
            }[status]
            print(f"    {label:<45} {n:>6,}  ({pct:>5.1f}%)")

        active_si = si_counts.get("active_boardex", 0) + si_counts.get("active_ciq_current", 0)
        stale_si = si_counts.get("stale_boardex", 0) + si_counts.get("stale_ciq_former", 0)
        print(f"    {'TOTAL ACTIVE':<45} {active_si:>6,}  ({active_si/total_si*100:>5.1f}%)")
        print(f"    {'TOTAL STALE':<45} {stale_si:>6,}  ({stale_si/total_si*100:>5.1f}%)")

print("\n\nDone.")
