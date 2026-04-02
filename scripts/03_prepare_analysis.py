"""
03_prepare_analysis.py
======================
Build all analysis-ready datasets from the observer network and returns data.

This script is the most complex in the pipeline. It reads pre-built CSV files
(no WRDS queries) and produces the final datasets used in regressions.

INPUTS (all CSVs):
  - Panel_C_Network/02b_supplemented_network_edges_us.csv  (supplemented network, US only)
  - Panel_C_Network/02_observer_public_portfolio_edges_us.csv (original CIQ network, US only)
  - CIQ_Extract/06d_observer_all_events_full.csv          (400K+ events)
  - Panel_C_Network/06_portfolio_crsp_daily.csv            (daily returns 2015-2024)
  - Panel_C_Network/06b_portfolio_crsp_daily_2025.csv      (daily returns 2025)
  - Panel_C_Network/05_industry_codes.csv                  (SIC codes)
  - Panel_C_Network/03_portfolio_permno_crosswalk.csv      (CIK -> PERMNO mapping)
  - Form4/observer_form4_trades.csv                        (insider trades)
  - CIQ_Extract/08_observer_tr_insider_crosswalk.csv       (TR -> CIQ person mapping)

OUTPUTS (all to Data/Analysis_Ready/):
  - control_group_{network}_{event_type}.csv   -- event x stock CARs with controls
  - connected_{network}_{event_type}.csv       -- connected stocks only (shock tests)
  - form4_trades.csv                           -- insider trading matched to events

Pipeline position: Runs AFTER 01/02 scripts build the network and pull returns.
                   Runs BEFORE all regression scripts.

Author: Harp Jung
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import os
import csv
import time
import numpy as np
import pandas as pd
from scipy.stats.mstats import winsorize


# =============================================================================
# CONFIGURATION
# =============================================================================

data_dir  = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir   = os.path.join(data_dir, "CIQ_Extract")
panel_c   = os.path.join(data_dir, "Panel_C_Network")
panel_b   = os.path.join(data_dir, "Panel_B_Outcomes")
form4_dir = os.path.join(data_dir, "Form4")
out_dir   = os.path.join(data_dir, "Analysis_Ready")
os.makedirs(out_dir, exist_ok=True)

# Sample period
YEAR_MIN = 2015
YEAR_MAX = 2025

# Penny stock threshold: drop stocks with average price below this
PENNY_THRESHOLD = 5.0

# CAR windows: (label, start_day_relative, end_day_relative)
# Negative days are pre-event; day 0 is the event date.
CAR_WINDOWS = [
    ("car_30",    -30, -1),   # 30-day pre-event drift
    ("car_20",    -20, -1),   # 20-day pre-event drift
    ("car_15",    -15, -1),   # 15-day pre-event drift
    ("car_10",    -10, -1),   # 10-day pre-event drift
    ("car_5",      -5, -1),   # 5-day pre-event drift
    ("car_3",      -3, -1),   # 3-day pre-event drift
    ("car_1",      -1,  0),   # immediate pre-event (1 day + event day)
    ("car_post5",   0, +5),   # 5-day post-event
    ("car_post10",  0, +10),  # 10-day post-event
]

# Abnormal volume windows
AVOL_WINDOWS = [
    ("avol_30", -30, -1),
    ("avol_10", -10, -1),
    ("avol_5",   -5, -1),
]

# Abnormal volume baseline period: [-120, -31] trading days before event
AVOL_BASELINE_START = -120
AVOL_BASELINE_END   = -31
AVOL_MIN_BASELINE_DAYS = 30  # need at least this many days for stable baseline

# Control group sampling rate: for non-connected stocks, keep 1 in N
CONTROL_SAMPLE_RATE = 10  # keep every 10th non-connected stock

# Noise event types to exclude from analysis.
# Earnings announcements are contaminated by foreign-listed firms that appear
# in CIQ as non-US companies. Conference calls and presentations are routine
# and reflect no material information.
NOISE_EVENT_TYPES = [
    "Announcements of Earnings",
    "Conferences",
    "Company Conference Presentations",
    "Earnings Calls",
    "Earnings Release Date",
    "Estimated Earnings Release Date (S&P Global Derived)",
    "Annual General Meeting",
    "Special/Extraordinary Shareholders Meeting",
    "Shareholder/Analyst Calls",
    "Special Calls",
    "Ex-Div Date (Regular)",
    "Ex-Div Date (Special)",
]

print("=" * 100)
print("03_prepare_analysis.py — Build Analysis-Ready Datasets")
print("=" * 100)


# =============================================================================
# SECTION A: LOAD ALL INPUTS
# =============================================================================
#
# This section loads eight CSV files and builds lookup dictionaries that the
# rest of the script depends on. No filtering happens here — just loading
# and basic type conversion.
# =============================================================================

print("\n" + "=" * 100)
print("SECTION A: LOAD ALL INPUTS")
print("=" * 100)

# ---- A1: Industry codes (SIC) ----
# Maps CIK -> 2-digit SIC and 3-digit SIC for same-industry classification.
# Source: Compustat via WRDS (pulled in earlier scripts).
print("\n  [A1] Loading industry codes...")
industry = pd.read_csv(os.path.join(panel_c, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]  # first 2 digits
industry["sic3"] = industry["sic"].astype(str).str[:3]  # first 3 digits
industry = industry.drop_duplicates("cik_int", keep="first")
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))
cik_to_sic3 = dict(zip(industry["cik_int"], industry["sic3"]))
print(f"       {len(cik_to_sic2):,} CIKs with SIC codes")

# ---- A2: CIQ company ID -> CIK crosswalk ----
# CIQ uses internal companyids; SEC filings use CIKs. This crosswalk lets
# us map between the two identifier systems.
print("  [A2] Loading CIQ-CIK crosswalk...")
ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(
    ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0"), errors="coerce"
)
cid_to_cik = dict(zip(ciq_xwalk["cid"], ciq_xwalk["cik_int"]))
print(f"       {len(cid_to_cik):,} CIQ companyid -> CIK mappings")

# ---- A3: CIK -> PERMNO crosswalk ----
# PERMNO is CRSP's stock identifier. We need this to link network edges
# (which have CIKs) to daily return data (which uses PERMNOs).
print("  [A3] Loading CIK-PERMNO crosswalk...")
pxw = pd.read_csv(os.path.join(panel_c, "03_portfolio_permno_crosswalk.csv"))
pxw["cik_int"] = pd.to_numeric(pxw["cik"], errors="coerce")
pxw["permno"] = pd.to_numeric(pxw["permno"], errors="coerce")
pxw = pxw.drop_duplicates("cik_int", keep="first")
cik_to_permno = dict(zip(pxw["cik_int"], pxw["permno"]))
print(f"       {len(cik_to_permno):,} CIK -> PERMNO mappings")

# Build PERMNO -> SIC lookup (used for same-industry flagging at the stock level)
# Chain: PERMNO -> CIK -> SIC
pmcik = dict(
    zip(
        pxw["permno"].dropna().astype(int),
        pxw["cik_int"].dropna().astype(int),
    )
)
pm_sic2 = {
    int(p): cik_to_sic2.get(c, "")
    for p, c in pmcik.items()
    if pd.notna(p) and pd.notna(c)
}
pm_sic3 = {
    int(p): cik_to_sic3.get(c, "")
    for p, c in pmcik.items()
    if pd.notna(p) and pd.notna(c)
}

# ---- A4: Observer network edges (two versions) ----
# We test both the original CIQ-only network and the supplemented network
# (which adds BoardEx-derived edges). Each edge represents:
#   observer_personid --[sits on board of]--> observed_companyid
#   observer_personid --[is officer/director at]--> portfolio_cik (public)
# So the "connected pair" is (observed_companyid, portfolio_permno).

def load_network(filepath, network_name):
    """Load a network edge file and build the connected-pair set.

    Returns:
        edges (DataFrame): Full edge-level data with PERMNO and same_industry.
        connected (set): Set of (observed_companyid, permno) pairs indicating
                         which portfolio stocks are connected to which private
                         companies through the observer network.
    """
    print(f"  [A4] Loading {network_name} network from {os.path.basename(filepath)}...")
    edges = pd.read_csv(filepath)

    # Clean IDs: CIQ stores numeric IDs that pandas may read as floats
    if "observer_personid" in edges.columns:
        edges["observer_personid"] = (
            edges["observer_personid"].astype(str).str.replace(".0", "", regex=False)
        )
    edges["observed_companyid"] = (
        edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
    )
    edges["portfolio_cik"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")

    # Map portfolio CIK to PERMNO (CRSP stock identifier)
    if "permno" not in edges.columns:
        edges["permno"] = edges["portfolio_cik"].map(cik_to_permno)
    else:
        edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce")

    # Drop edges where we cannot find a PERMNO (company not in CRSP)
    edges = edges.dropna(subset=["permno"])
    edges["permno"] = edges["permno"].astype(int)

    # Flag same-industry pairs (2-digit SIC match between observed and portfolio)
    edges["same_industry"] = (
        edges["observed_companyid"].map(cid_to_cik).map(cik_to_sic2)
        == edges["portfolio_cik"].map(cik_to_sic2)
    ).astype(int)

    # Build the connected-pair lookup set
    # Key insight: a stock is "connected" to a private company event if there
    # exists an observer who sits on the private company's board AND is an
    # officer/director at the public company.
    connected = set()
    for _, row in edges.iterrows():
        connected.add((row["observed_companyid"], row["permno"]))

    print(f"       {len(edges):,} edges, {len(connected):,} unique (company, permno) pairs")
    return edges, connected


orig_edges, orig_connected = load_network(
    os.path.join(panel_c, "02_observer_public_portfolio_edges_us.csv"),
    "original (CIQ-only)",
)
supp_edges, supp_connected = load_network(
    os.path.join(panel_c, "02b_supplemented_network_edges_us.csv"),
    "supplemented (CIQ + BoardEx)",
)

# ---- A5: Events (400K+ key development events) ----
# These are material corporate events from S&P Capital IQ (CIQ) KeyDev.
# Each row is one event at one company on one date.
print("\n  [A5] Loading events...")
events = pd.read_csv(
    os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False
)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])
events["event_year"] = events["announcedate"].dt.year
print(f"       {len(events):,} total events loaded")

# ---- A5b: Filter events to US-only companies ----
# The network is US-only, so we also restrict events to US companies.
# Load company details to get the US company ID set.
co_details = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_cids = set(
    co_details[co_details["country"] == "United States"]["companyid"]
    .astype(str).str.replace(".0", "", regex=False)
)
before_evt = len(events)
events = events[events["companyid"].isin(us_cids)].copy()
print(f"       After US-only filter: {len(events):,} events (dropped {before_evt - len(events):,} non-US)")

# ---- A6: Daily stock returns ----
# Two files: main (2015-2024) and supplement (2025).
# Columns: permno, date, ret, prc, vol
print("\n  [A6] Loading daily returns...")
ret_main = pd.read_csv(os.path.join(panel_c, "06_portfolio_crsp_daily.csv"))
ret_2025 = pd.read_csv(os.path.join(panel_c, "06b_portfolio_crsp_daily_2025.csv"))

# Harmonize columns: the 2025 file has an extra 'shrout' column
common_cols = ["permno", "date", "ret", "prc", "vol"]
ret_main = ret_main[common_cols]
ret_2025 = ret_2025[[c for c in common_cols if c in ret_2025.columns]]

# Concatenate and clean
daily = pd.concat([ret_main, ret_2025], ignore_index=True)
daily["date"] = pd.to_datetime(daily["date"])
daily["permno"] = pd.to_numeric(daily["permno"], errors="coerce").dropna().astype(int)
daily["ret"] = pd.to_numeric(daily["ret"], errors="coerce")
daily["vol"] = pd.to_numeric(daily["vol"], errors="coerce")
daily["prc"] = pd.to_numeric(daily["prc"], errors="coerce").abs()  # CRSP stores neg for bid/ask avg
daily = daily.dropna(subset=["ret"]).sort_values(["permno", "date"])
print(f"       {len(daily):,} daily return observations, {daily['permno'].nunique():,} stocks")

# ---- A7: Form 4 insider trades ----
print("\n  [A7] Loading Form 4 trades...")
trades_raw = pd.read_csv(os.path.join(form4_dir, "observer_form4_trades.csv"))
print(f"       {len(trades_raw):,} raw trade records")

# ---- A8: TR-CIQ person crosswalk (for Form 4 matching) ----
print("\n  [A8] Loading TR-CIQ insider crosswalk...")
tr_xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
print(f"       {len(tr_xwalk):,} person-level links")

# ---- A9: Company details (for public/private classification) ----
print("\n  [A9] Loading company details (for public company filter)...")
pub_cids = set()
with open(os.path.join(ciq_dir, "04_observer_company_details.csv"), "r", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        cid = str(r.get("companyid", "")).strip().replace(".0", "")
        if "public" in str(r.get("companytypename", "")).lower():
            pub_cids.add(cid)
print(f"       {len(pub_cids):,} public companies identified")

# ---- A10: Panel B crosswalk (for CRSP-listing filter) ----
# This crosswalk has linkdt/linkenddt showing when a company was listed on
# an exchange. We use it to drop events that occurred while the "observed"
# company was actually publicly traded.
print("\n  [A10] Loading Panel B identifier crosswalk (CRSP listing dates)...")
panel_b_xwalk = pd.read_csv(os.path.join(panel_b, "01_identifier_crosswalk.csv"))
panel_b_xwalk["cik_int"] = pd.to_numeric(panel_b_xwalk["cik"], errors="coerce")
panel_b_xwalk["linkdt"] = pd.to_datetime(panel_b_xwalk["linkdt"], errors="coerce")
panel_b_xwalk["linkenddt"] = pd.to_datetime(
    panel_b_xwalk["linkenddt"], errors="coerce"
).fillna(pd.Timestamp("2099-12-31"))
# For each CIK, find the earliest and latest dates it was exchange-listed
listing = (
    panel_b_xwalk.groupby("cik_int")
    .agg(first_listed=("linkdt", "min"), last_listed=("linkenddt", "max"))
    .reset_index()
)
print(f"       {len(listing):,} CIKs with CRSP listing periods")

# ---- A11: Observer records (for Form 4 event matching) ----
print("\n  [A11] Loading observer records (person -> company links)...")
obs_records = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs_records["personid"] = (
    obs_records["personid"].astype(str).str.replace(".0", "", regex=False)
)
obs_records["companyid"] = (
    obs_records["companyid"].astype(str).str.replace(".0", "", regex=False)
)
print(f"       {len(obs_records):,} observer-company records")


# =============================================================================
# SECTION B: FILTER EVENTS
# =============================================================================
#
# We need a clean set of PRIVATE company events. The raw events file contains
# events at all companies (public and private) for all types. We apply three
# filters:
#
#   B1. Keep only the sample period (2015-2025)
#   B2. Drop events at public companies (we study private-company information)
#   B3. Drop events occurring while company was CRSP-listed
#       (some CIQ "private" companies had prior/subsequent public listings)
#   B4. Drop noise event types (earnings, conferences, dividends, AGMs)
#   B5. Group remaining events into analytical categories
# =============================================================================

print("\n" + "=" * 100)
print("SECTION B: FILTER EVENTS")
print("=" * 100)

# B1: Sample period
events = events[(events["event_year"] >= YEAR_MIN) & (events["event_year"] <= YEAR_MAX)]
print(f"\n  [B1] After year filter ({YEAR_MIN}-{YEAR_MAX}): {len(events):,}")

# B2: Drop public companies
# CIQ classifies companies by type. We drop any with "Public" in companytypename.
# Rationale: our research question is about information flowing from PRIVATE
# company boardrooms to PUBLIC stock portfolios. Public-company events are
# observable to all market participants, so they don't test our mechanism.
events = events[~events["companyid"].isin(pub_cids)]
print(f"  [B2] After dropping public companies: {len(events):,}")

# B3: Drop events while company was CRSP-listed
# Some companies appear "private" in CIQ but were actually exchange-listed
# at the time of the event (e.g., post-IPO but still in CIQ's private list).
# We match via: event companyid -> CIK -> listing period, then check if the
# event date falls within a listing period.
events["cik_int"] = events["companyid"].map(cid_to_cik)
events = events.merge(listing, on="cik_int", how="left")
events["was_public"] = (
    (events["announcedate"] >= events["first_listed"])
    & (events["announcedate"] <= events["last_listed"])
)
events = events[~events["was_public"].fillna(False)]
print(f"  [B3] After dropping CRSP-listed events: {len(events):,}")

# B4: Drop noise event types
# Earnings announcements: contaminated by foreign-listed firms that CIQ
#   classifies as non-US private but which actually trade on foreign exchanges.
# Conferences/calls: routine IR events, no material private information.
# AGMs/shareholder meetings: public information for public companies.
# Dividends: routine distributions, no information content for our test.
events = events[~events["eventtype"].isin(NOISE_EVENT_TYPES)]
print(f"  [B4] After dropping noise events: {len(events):,}")

# B5: Group events into analytical categories
# Each event type is tested separately in regressions because different event
# types carry different information content and may trigger different trading.
#
# We define groups using lambda functions that filter the events DataFrame:
#   - ma_buyer:    M&A where the observed company is the acquirer
#   - ma_target:   M&A where the observed company is the target
#   - bankruptcy:  Any bankruptcy-related event
#   - exec_board:  Executive or board membership changes
#   - ceo_cfo:     CEO or CFO changes specifically (subset of exec_board)
#   - all_events:  Everything (used for aggregate tests)

bankruptcy_types = [t for t in events["eventtype"].unique() if "Bankruptcy" in str(t)]

event_groups = {
    "ma_buyer": lambda df: df[
        (df["eventtype"] == "M&A Transaction Announcements")
        & (df["objectroletype"] == "Buyer")
    ],
    "ma_target": lambda df: df[
        (df["eventtype"] == "M&A Transaction Announcements")
        & (df["objectroletype"] == "Target")
    ],
    "bankruptcy": lambda df: df[df["eventtype"].isin(bankruptcy_types)],
    "exec_board": lambda df: df[
        df["eventtype"] == "Executive/Board Changes - Other"
    ],
    "ceo_cfo": lambda df: df[
        df["eventtype"].isin(
            ["Executive Changes - CEO", "Executive Changes - CFO"]
        )
    ],
    "all_events": lambda df: df,
}

# Print event type distribution for diagnostics
print("\n  Event type counts (top 20):")
for etype, count in events["eventtype"].value_counts().head(20).items():
    print(f"    {etype:<55} {count:>8,}")

print(f"\n  Event groups defined: {list(event_groups.keys())}")
print(f"  Bankruptcy types found: {bankruptcy_types}")


# =============================================================================
# SECTION C: COMPUTE CARs
# =============================================================================
#
# For each event, we compute Cumulative Abnormal Returns (CARs) at ALL
# portfolio stocks over multiple windows. This is the core empirical measure:
# if connected stocks show abnormal returns before private events, that is
# evidence of information permeability through the observer network.
#
# Methodology:
#   1. Market adjustment: subtract the equal-weighted mean daily return
#      across all portfolio stocks on each day (our own portfolio-level
#      market return, not CRSP value-weighted).
#   2. Raw CAR = sum of raw daily returns in window
#   3. Market-adjusted CAR = sum of (ret - market_ret) in window
#   4. BHAR = product of (1 + abnormal_ret) - 1  (buy-and-hold)
#   5. Abnormal volume = event-window mean volume / baseline mean volume
#
# We also identify and drop penny stocks (avg price < $5) because their
# returns are noisy and can dominate CAR distributions.
# =============================================================================

print("\n" + "=" * 100)
print("SECTION C: COMPUTE CARs (preparation)")
print("=" * 100)

# C1: Compute the equal-weighted portfolio-level market return
# This is the mean daily return across all portfolio stocks on each trading day.
# We use this as our market adjustment factor rather than CRSP VW or SP500
# because our portfolio stocks are the relevant comparison set.
print("\n  [C1] Computing portfolio-level market return...")
mkt_ret = daily.groupby("date")["ret"].mean().rename("mkt_ret")
daily = daily.merge(mkt_ret, on="date", how="left")
daily["aret"] = daily["ret"] - daily["mkt_ret"]  # abnormal return
print(f"       Market return computed for {len(mkt_ret):,} trading days")

# C2: Identify penny stocks
# Stocks with average price < $5 have very noisy returns and bid-ask bounce
# effects that can create spurious CARs. We flag them here; they will be
# included in the output but flagged for optional exclusion in regressions.
print("  [C2] Identifying penny stocks...")
avg_price = daily.groupby("permno")["prc"].mean()
penny_stocks = set(avg_price[avg_price < PENNY_THRESHOLD].index)
print(f"       {len(penny_stocks):,} penny stocks (avg price < ${PENNY_THRESHOLD})")

# C3: Build per-stock return arrays for fast lookup
# Instead of merging on every event, we pre-index returns by PERMNO.
# For each stock, we store sorted arrays of (dates, returns, abnormal_returns,
# volumes). This makes CAR computation O(1) lookup + vectorized sum.
print("  [C3] Building per-stock return lookup (this takes a moment)...")
all_permnos = sorted(daily["permno"].unique())
pmdata = {}
for p, g in daily.groupby("permno"):
    pmdata[p] = (
        g["date"].values,   # numpy datetime64 array
        g["ret"].values,     # raw returns
        g["aret"].values,    # abnormal returns (market-adjusted)
        g["vol"].values,     # trading volume
    )
print(f"       {len(pmdata):,} stocks indexed")


def compute_cars_and_volume(permno, event_date_np):
    """Compute all CAR measures and abnormal volume for one (stock, event) pair.

    Args:
        permno: CRSP PERMNO identifier for the portfolio stock.
        event_date_np: numpy datetime64 of the event announcement date.

    Returns:
        dict with keys like 'car_10', 'car_10_adj', 'car_10_bhar', 'avol_10',
        or None if insufficient data.

    Method:
        1. Look up the stock's daily return array.
        2. Compute calendar-day offset of each trading day from event date.
        3. For each CAR window, sum returns of trading days within that offset range.
        4. Require at least 30% of expected trading days to be present (accounts
           for holidays, weekends, halts).
        5. For volume, compute ratio of event-window mean to baseline mean.
    """
    if permno not in pmdata:
        return None
    dates, rets, arets, vols = pmdata[permno]

    # Need at least 60 trading days of history for meaningful CARs
    if len(dates) < 60:
        return None

    # Compute calendar-day offsets: how many calendar days is each trading day
    # from the event date? Negative = before event, positive = after.
    diffs = (dates - event_date_np).astype("timedelta64[D]").astype(int)

    result = {}

    # --- CARs ---
    for window_name, day_start, day_end in CAR_WINDOWS:
        mask = (diffs >= day_start) & (diffs <= day_end)
        window_rets = rets[mask]
        window_arets = arets[mask]

        # Require at least 30% of expected trading days, minimum 2 days.
        # This handles holidays, weekends, and short trading halts.
        expected_days = abs(day_end - day_start)
        min_days = max(2, int(expected_days * 0.3))

        if len(window_rets) >= min_days:
            result[window_name] = float(np.sum(window_rets))             # raw CAR
            result[f"{window_name}_adj"] = float(np.sum(window_arets))   # mkt-adjusted
            result[f"{window_name}_bhar"] = float(np.prod(1 + window_arets) - 1)  # BHAR

    # --- Abnormal volume ---
    # Baseline: mean daily volume in [-120, -31] trading days before event.
    # Event window: mean daily volume in the specified window.
    # Ratio > 1 means above-normal trading activity.
    baseline_mask = (diffs >= AVOL_BASELINE_START) & (diffs <= AVOL_BASELINE_END)
    baseline_vol = vols[baseline_mask]

    if len(baseline_vol) >= AVOL_MIN_BASELINE_DAYS and np.nanmean(baseline_vol) > 0:
        baseline_mean = np.nanmean(baseline_vol)
        for window_name, day_start, day_end in AVOL_WINDOWS:
            mask = (diffs >= day_start) & (diffs <= day_end)
            window_vol = vols[mask]
            expected_days = abs(day_end - day_start)
            min_days = max(2, int(expected_days * 0.3))
            if len(window_vol) >= min_days:
                event_mean = np.nanmean(window_vol)
                if not np.isnan(event_mean) and baseline_mean > 0:
                    result[window_name] = float(event_mean / baseline_mean)

    return result if result else None


# =============================================================================
# SECTION D: BUILD CONNECTED AND CONTROL GROUP DATASETS
# =============================================================================
#
# For each (network_type x event_group) combination, we:
#   1. Filter events to those where the private company appears in the network
#   2. For each event, compute CARs at ALL portfolio stocks
#   3. Flag which stocks are "connected" through the observer network
#   4. Flag same-industry matches (2-digit and 3-digit SIC)
#   5. Sample non-connected stocks at 10% to keep file sizes manageable
#   6. Save two files:
#      - control_group_*.csv: connected + 10% sample of non-connected
#      - connected_*.csv: connected stocks only (for regulatory shock tests)
#
# The control group design follows the event-study approach in finance:
# connected stocks are the "treatment" and non-connected stocks from the same
# event are the "control." Including all non-connected stocks would create
# enormous files (N_events x N_stocks), so we sample.
# =============================================================================

print("\n" + "=" * 100)
print("SECTION D: BUILD CONNECTED AND CONTROL GROUP DATASETS")
print("=" * 100)

# January 2025 cutoff for the supplemented network period indicator
jan2025 = pd.Timestamp("2025-01-01")

# We iterate over both networks and all event groups
networks = [
    ("original", orig_connected, orig_edges),
    ("supplemented", supp_connected, supp_edges),
]

for net_name, connected_set, net_edges in networks:
    # Set of private companies that actually appear in this network.
    # We only compute CARs for events at these companies because events at
    # companies with no network connection are uninformative for our test.
    companies_in_network = set(net_edges["observed_companyid"])

    for grp_name, grp_filter_fn in event_groups.items():
        # Apply the group filter to get events of this type
        grp = grp_filter_fn(events)

        # Keep only events where the private company appears in the network
        grp = grp[grp["companyid"].isin(companies_in_network)]

        # Deduplicate: one row per (company, date) — multiple event rows for
        # the same company on the same date are effectively the same event
        grp_df = grp[["companyid", "announcedate", "event_year"]].drop_duplicates(
            subset=["companyid", "announcedate"]
        )

        if len(grp_df) < 20:
            print(f"\n  {net_name}/{grp_name}: {len(grp_df)} events — SKIP (too few)")
            continue

        print(f"\n  {net_name}/{grp_name}: {len(grp_df):,} events — computing CARs...")
        t0 = time.time()

        all_obs = []    # accumulates all (event, stock) observations
        event_id = 0    # unique event counter
        evl = grp_df.to_dict("records")

        for ei, ev in enumerate(evl):
            event_np = np.datetime64(ev["announcedate"])
            event_cid = ev["companyid"]
            event_year = ev["event_year"]
            event_date = ev["announcedate"]

            # Look up the industry of the event company
            obs_cik = cid_to_cik.get(event_cid)
            obs_sic2 = cik_to_sic2.get(obs_cik, "") if obs_cik else ""
            obs_sic3 = cik_to_sic3.get(obs_cik, "") if obs_cik else ""

            # Iterate over ALL portfolio stocks
            for pmi, pm in enumerate(all_permnos):
                is_connected = 1 if (event_cid, pm) in connected_set else 0

                # CONTROL GROUP SAMPLING: for non-connected stocks, keep only
                # every Nth stock to avoid enormous output files.
                # Connected stocks are always kept.
                if not is_connected and (pmi % CONTROL_SAMPLE_RATE != 0):
                    continue

                # Compute CARs and abnormal volume for this (stock, event) pair
                result = compute_cars_and_volume(pm, event_np)
                if not result:
                    continue

                # Same-industry flags: does the portfolio stock share a 2-digit
                # or 3-digit SIC code with the private event company?
                psic2 = pm_sic2.get(pm, "")
                psic3 = pm_sic3.get(pm, "")
                same_ind_sic2 = 1 if (obs_sic2 and psic2 and obs_sic2 == psic2) else 0
                same_ind_sic3 = 1 if (obs_sic3 and psic3 and obs_sic3 == psic3) else 0

                all_obs.append(
                    {
                        "event_id": event_id,
                        "permno": pm,
                        "event_year": event_year,
                        "event_date": (
                            str(event_date.date())
                            if hasattr(event_date, "date")
                            else str(event_date)
                        ),
                        "connected": is_connected,
                        "same_ind_sic2": same_ind_sic2,
                        "same_ind_sic3": same_ind_sic3,
                        "is_penny": 1 if pm in penny_stocks else 0,
                        "post_2020": 1 if event_year >= 2020 else 0,
                        "post_jan2025": 1 if pd.Timestamp(event_date) >= jan2025 else 0,
                        **result,
                    }
                )

            event_id += 1

            # Progress reporting every 500 events
            if (ei + 1) % 500 == 0:
                elapsed = time.time() - t0
                events_per_sec = (ei + 1) / elapsed
                remaining_events = len(evl) - ei - 1
                remaining_min = remaining_events / events_per_sec / 60
                print(
                    f"    Event {ei + 1:,}/{len(evl):,} | "
                    f"{len(all_obs):,} obs | "
                    f"~{remaining_min:.0f} min remaining"
                )

        if not all_obs:
            print(f"    No observations generated — SKIP")
            continue

        # Build the output DataFrame
        df = pd.DataFrame(all_obs)

        # D1: Winsorize CAR columns at 1st/99th percentiles
        # Extreme returns (e.g., from stock splits, delistings, data errors)
        # can drive spurious results. Winsorizing caps outliers at the 1st and
        # 99th percentile values.
        car_cols = [c for c in df.columns if c.startswith("car_") and "_adj" not in c and "_bhar" not in c]
        car_adj_cols = [c for c in df.columns if c.endswith("_adj")]
        car_bhar_cols = [c for c in df.columns if c.endswith("_bhar")]
        all_car_cols = car_cols + car_adj_cols + car_bhar_cols

        for col in all_car_cols:
            if col in df.columns:
                vals = df[col].dropna().values
                if len(vals) > 100:
                    p1, p99 = np.percentile(vals, [1, 99])
                    df[col] = df[col].clip(lower=p1, upper=p99)

        # D2: Create interaction terms for regressions
        # connected x same_industry captures whether the information channel
        # effect is stronger for same-industry pairs (as theory predicts).
        df["conn_x_same2"] = df["connected"] * df["same_ind_sic2"]
        df["conn_x_same3"] = df["connected"] * df["same_ind_sic3"]

        # D3: Save the control group dataset (connected + sampled non-connected)
        outpath = os.path.join(out_dir, f"control_group_{net_name}_{grp_name}.csv")
        df.to_csv(outpath, index=False)
        n_conn = df["connected"].sum()
        n_same = df["conn_x_same2"].sum()
        elapsed = time.time() - t0
        print(f"    SAVED: {os.path.basename(outpath)}")
        print(
            f"    N={len(df):,} | connected={n_conn:,} | conn_x_same_ind={n_same:,} | "
            f"{elapsed:.0f}s"
        )

        # D4: Save connected-only dataset (for regulatory shock tests)
        # These datasets are used in event studies around NVCA/Clayton Act
        # regulatory changes where we only need connected stocks.
        conn_only = df[df["connected"] == 1].copy()
        conn_path = os.path.join(out_dir, f"connected_{net_name}_{grp_name}.csv")
        conn_only.to_csv(conn_path, index=False)
        print(f"    Connected-only: {len(conn_only):,} -> {os.path.basename(conn_path)}")


# =============================================================================
# SECTION E: FORM 4 TRADING DATASET
# =============================================================================
#
# This section matches SEC Form 4 insider trades to private company events
# through the observer network. The logic is:
#
#   1. An observer sits on a private company's board (CIQ records)
#   2. That same person trades stock at a public company (Form 4 filings)
#   3. We check: did the trade happen in the 30 days BEFORE a material event
#      at the observed private company?
#
# This directly tests information permeability: if observers trade before
# events at companies they monitor, that suggests information leakage.
#
# Key: we do NOT query WRDS here. The SIC code for traded stocks is looked
# up from the industry crosswalk via PERMNO->CIK->SIC, which avoids the
# CUSIP-based WRDS lookup the old script used.
# =============================================================================

print("\n" + "=" * 100)
print("SECTION E: FORM 4 TRADING DATASET")
print("=" * 100)

# E1: Clean trades data
print("\n  [E1] Cleaning Form 4 trades...")
trades = trades_raw.copy()
trades["trandate"] = pd.to_datetime(trades["trandate"], errors="coerce")
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
trades = trades.dropna(subset=["trandate"])

# Keep only open-market purchases (P) and sales (S).
# Exclude derivative transactions, gifts, exercises, etc.
trades = trades[trades["trancode"].isin(["P", "S"])].copy()
trades["is_buy"] = (trades["trancode"] == "P").astype(int)
print(f"       {len(trades):,} trades after filtering to P/S only")

# E2: Link TR person IDs to CIQ person IDs
# Thomson Reuters (now Refinitiv) Form 4 data uses different person IDs than
# CIQ. The crosswalk maps between them using name + CUSIP matching.
print("  [E2] Linking TR person IDs to CIQ person IDs...")
tr_xwalk_clean = tr_xwalk.copy()
tr_xwalk_clean["tr_personid"] = (
    tr_xwalk_clean["tr_personid"].astype(str).str.replace(".0", "", regex=False)
)
tr_xwalk_clean["ciq_personid"] = (
    tr_xwalk_clean["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
)
tr_to_ciq = dict(zip(tr_xwalk_clean["tr_personid"], tr_xwalk_clean["ciq_personid"]))
trades["ciq_personid"] = trades["personid"].map(tr_to_ciq)
trades = trades.dropna(subset=["ciq_personid"])
print(f"       {len(trades):,} trades matched to CIQ person IDs")

# E3: Build SIC lookup for traded stocks (no WRDS needed)
# We look up SIC codes through the CUSIP -> PERMNO -> CIK -> SIC chain,
# but since we do not have a direct CUSIP->PERMNO map loaded, we use the
# CIK chain from the portfolio crosswalk. The traded stock's CIK is not
# directly available in Form 4 data, so we map the CIQ person's portfolio
# companies to SIC codes.
print("  [E3] Building observer -> company -> SIC lookup for trades...")
observer_to_companies = (
    obs_records.groupby("personid")["companyid"].apply(set).to_dict()
)
print(f"       {len(observer_to_companies):,} observers with company links")

# E4: Build event lookup by company
# For fast matching, index events by companyid.
print("  [E4] Indexing events by company for trade matching...")
company_events = {}
for _, ev in events.iterrows():
    cid = ev["companyid"]
    if cid not in company_events:
        company_events[cid] = []
    company_events[cid].append(ev["announcedate"])
print(f"       {len(company_events):,} companies with events")

# E5: Match each trade to events
# For each trade by an observer, check whether any of the companies that
# observer monitors had a material event in the 30 days AFTER the trade.
# If so, flag the trade as "pre_event" (the trade preceded a material event).
#
# Also flag same-industry: does the traded stock's industry match the
# industry of the observed company where the event occurred?
print("  [E5] Matching trades to events (this may take a while)...")

# Build CIQ personid -> SIC codes for traded stocks via portfolio positions
# For same-industry flagging, we need the SIC of the stock being traded.
# We approximate this from the person's portfolio companies' SIC codes.
# The traded stock's CUSIP8 can also be used if we had a CUSIP->SIC map.
# Without WRDS, we use the portfolio CIK -> SIC chain.

trade_rows = []
for ti, (_, tr) in enumerate(trades.iterrows()):
    obs_pid = tr["ciq_personid"]
    trade_date = tr["trandate"]

    # Get the set of observed companies for this person
    observed_companies = observer_to_companies.get(obs_pid, set())

    # Try to determine the SIC of the traded stock from the person's
    # portfolio companies. This is an approximation: we assume the traded
    # stock SIC can be inferred from the portfolio positions.
    # For a more precise match, the CUSIP->SIC lookup from CRSP would be needed.
    trade_sic2 = ""
    # We can try to match the CUSIP to our portfolio stocks
    cusip6 = str(tr.get("cusip6", "")).strip()
    cusip2 = str(tr.get("cusip2", "")).strip()
    cusip8 = cusip6 + cusip2 if cusip6 and cusip2 else ""

    # Look through portfolio CIKs for this observer to find the traded stock's SIC
    # The most reliable approach without WRDS: match the traded ticker/CUSIP
    # to one of the person's portfolio companies.
    # Since we have the person's portfolio companies, and those have CIK->SIC,
    # we can use that for same-industry flagging relative to the event company.

    best_match = None

    for ocid in observed_companies:
        ev_dates = company_events.get(ocid, [])
        ecik = cid_to_cik.get(ocid)
        esic2 = cik_to_sic2.get(ecik, "") if ecik else ""

        for edate in ev_dates:
            days_gap = (trade_date - edate).days

            # Pre-event window: trade occurred 1-30 days BEFORE the event
            if -30 <= days_gap <= -1:
                # For same-industry, check if any of the person's portfolio
                # companies share the event company's SIC
                si = 0
                if esic2:
                    for pc_cid in observed_companies:
                        pc_cik = cid_to_cik.get(pc_cid)
                        pc_sic2 = cik_to_sic2.get(pc_cik, "") if pc_cik else ""
                        if pc_sic2 and pc_sic2 == esic2 and pc_cid != ocid:
                            si = 1
                            break
                best_match = {"pre_event": 1, "same_industry": si}
                break
        if best_match:
            break

    # If no pre-event match found, mark as non-pre-event trade
    if best_match is None:
        any_same = 0
        for ocid in observed_companies:
            ecik = cid_to_cik.get(ocid)
            esic2 = cik_to_sic2.get(ecik, "") if ecik else ""
            if esic2:
                for pc_cid in observed_companies:
                    pc_cik = cid_to_cik.get(pc_cid)
                    pc_sic2 = cik_to_sic2.get(pc_cik, "") if pc_cik else ""
                    if pc_sic2 and pc_sic2 == esic2 and pc_cid != ocid:
                        any_same = 1
                        break
            if any_same:
                break
        best_match = {"pre_event": 0, "same_industry": any_same}

    trade_rows.append(
        {
            "ciq_personid": obs_pid,
            "is_buy": tr["is_buy"],
            "pre_event": best_match["pre_event"],
            "same_industry": best_match["same_industry"],
            "trade_year": trade_date.year,
        }
    )

    # Progress reporting
    if (ti + 1) % 10000 == 0:
        print(f"    Processed {ti + 1:,}/{len(trades):,} trades...")

# Build output DataFrame
f4_df = pd.DataFrame(trade_rows)

# Interaction term: pre-event AND same-industry
# This is the key variable: trades that happen before events at companies
# in the same industry as the traded stock.
f4_df["pre_x_same"] = f4_df["pre_event"] * f4_df["same_industry"]

# Save
f4_path = os.path.join(out_dir, "form4_trades.csv")
f4_df.to_csv(f4_path, index=False)
print(f"\n  SAVED: {os.path.basename(f4_path)}")
print(
    f"  {len(f4_df):,} trades | "
    f"pre_event={f4_df['pre_event'].sum():,} | "
    f"same_industry={f4_df['same_industry'].sum():,} | "
    f"pre_x_same={f4_df['pre_x_same'].sum():,}"
)


# =============================================================================
# SECTION F: SUMMARY
# =============================================================================
#
# Print a summary of all output files, their sizes, and key counts.
# =============================================================================

print("\n" + "=" * 100)
print("SECTION F: SUMMARY")
print("=" * 100)

print(f"\nOutput directory: {out_dir}\n")
print(f"  {'File':<60} {'Size (MB)':>10} ")
print(f"  {'-' * 60} {'-' * 10}")

total_size = 0
file_count = 0
for f in sorted(os.listdir(out_dir)):
    fpath = os.path.join(out_dir, f)
    if os.path.isfile(fpath):
        size_mb = os.path.getsize(fpath) / (1024 * 1024)
        total_size += size_mb
        file_count += 1
        print(f"  {f:<60} {size_mb:>8.1f} MB")

print(f"  {'-' * 60} {'-' * 10}")
print(f"  {'TOTAL':<60} {total_size:>8.1f} MB")
print(f"\n  {file_count} output files generated.")
print(f"\n{'=' * 100}")
print("03_prepare_analysis.py COMPLETE")
print(f"{'=' * 100}")
