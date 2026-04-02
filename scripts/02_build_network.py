"""
02_build_network.py — Build the observer network from all raw data sources.

=== PURPOSE ===

This is the SECOND script in the consolidated pipeline. It reads CSV files
created by 01_pull_all_wrds_data.py (raw CIQ, EDGAR, BoardEx, and Form 4
extracts) and builds the complete observer network linking private observed
companies to public portfolio companies through shared board observers.

This script consolidates three earlier scripts:
  - build_unified_dataset.py        (Sections A, B)
  - pull_panel_b_and_c.py           (Section B network portion)
  - build_supplemented_network.py   (Sections C, D, E)

It does NOT query WRDS for bulk data pulls — all raw data comes from CSVs.
However, it DOES require a WRDS connection for three small identifier lookups:
  1. CIQ company ID -> CIK for public portfolio companies (ciq_common.wrds_cik)
  2. CUSIP -> PERMNO mapping for Form 4 edges (crsp.stocknames)
  3. PERMNO -> CIK mapping for Form 4 edges (crsp_a_ccm.ccmxpf_lnkhist + comp.company)
  4. CIK -> GVKEY -> PERMNO crosswalk for all portfolio CIKs (crsp_a_ccm.ccmxpf_lnkhist)
  5. SIC/NAICS industry codes for all network companies (comp.company)

=== DATA FLOW ===

    CIQ_Extract/01-07*.csv   EDGAR_Extract/*.csv   BoardEx/*.csv   Form4/*.csv
              |                      |                   |              |
              +---------- THIS SCRIPT (02_build_network.py) -----------+
              |                                                        |
              v                                                        v
    table_a_company_master.csv                     Panel_C_Network/
    table_b_observer_network.csv                     01_public_portfolio_companies.csv
                                                     02_observer_public_portfolio_edges.csv
                                                     02b_supplemented_network_edges.csv
                                                     03_portfolio_permno_crosswalk.csv
                                                     05_industry_codes.csv
                                                     new_ciks_for_crsp.csv

=== SECTIONS ===

  A. COMPANY MASTER (Table A)        — company-level aggregates with board
                                       composition, EDGAR fiduciary flags,
                                       NVCA treatment, and event counts
  B. BASE CIQ NETWORK (Table B)      — observer -> observed company -> VC firm
                                       -> public portfolio company edges
  C. BOARDEX SUPPLEMENTATION         — additional public company connections
                                       from BoardEx board positions
  D. FORM 4 SUPPLEMENTATION          — additional public company connections
                                       from SEC Form 4 insider filings
  E. COMBINE AND DEDUPLICATE         — stack CIQ + BoardEx + Form 4 edges,
                                       dedup, tag source, flag same-industry
  F. IDENTIFIER CROSSWALKS           — CIK -> GVKEY -> PERMNO for all portfolio
                                       companies, plus SIC/NAICS industry codes
  G. SUMMARY                         — comparison of original vs supplemented
                                       network statistics
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import os, csv, time
from collections import defaultdict, Counter
import pandas as pd
import numpy as np
import psycopg2

# =====================================================================
# CONFIGURATION
# =====================================================================

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir  = os.path.join(data_dir, "CIQ_Extract")
edgar_dir = os.path.join(data_dir, "EDGAR_Extract")
boardex_dir = os.path.join(data_dir, "BoardEx")
form4_dir = os.path.join(data_dir, "Form4")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

# Create output directories if they don't exist
os.makedirs(panel_c_dir, exist_ok=True)

# WRDS connection parameters — used only for small identifier lookups
WRDS_PARAMS = dict(
    host="wrds-pgdata.wharton.upenn.edu",
    port=9737,
    dbname="wrds",
    user="harperjung",
    password="Wwjksnm9087yu!"
)


# =====================================================================
# HELPER FUNCTIONS
# =====================================================================

def normalize_id(val):
    """
    Clean up IDs that pandas may have stored as floats.

    When pandas reads an integer column that has missing values, it converts
    the entire column to float64. When written to CSV, IDs like 12345 become
    '12345.0'. This function strips the trailing '.0' so IDs match correctly
    across datasets.

    Examples:
        normalize_id('12345.0') -> '12345'
        normalize_id('12345')   -> '12345'
        normalize_id('')        -> ''
        normalize_id(None)      -> ''
        normalize_id(12345)     -> '12345'
        normalize_id(12345.0)   -> '12345'
    """
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    val = str(val).strip()
    if val.endswith(".0"):
        val = val[:-2]
    return val


def load_csv(filepath):
    """
    Load a CSV file into a list of dictionaries (one dict per row).

    Uses the csv module rather than pandas for consistency with the original
    build_unified_dataset.py script. The csv.DictReader automatically uses
    the first row as column names.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_csv_from_dicts(rows, filepath, fieldnames=None):
    """
    Save a list of dictionaries to a CSV file.

    If fieldnames is not provided, uses the keys from the first row.
    """
    if not rows:
        print(f"  WARNING: No rows to save -> {filepath}")
        return
    if fieldnames is None:
        fieldnames = list(rows[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def get_wrds_connection():
    """
    Open a connection to WRDS PostgreSQL database.

    Used only for small identifier lookups (CUSIP->PERMNO, PERMNO->CIK, etc.)
    All bulk data was already pulled by 01_pull_all_wrds_data.py.
    """
    print("  Connecting to WRDS...")
    conn = psycopg2.connect(**WRDS_PARAMS)
    print("  Connected.")
    return conn


# =====================================================================
# =====================================================================
#
#   SECTION A: COMPANY MASTER (Table A)
#
#   Reads: CIQ_Extract/01-07, EDGAR_Extract/exhibit*, s1_body*, efts*
#   Writes: Data/table_a_company_master.csv
#
#   This section builds a company-level master table with one row per
#   observed company. Each row contains:
#     - Company attributes (name, type, status, location)
#     - Board composition (n_directors, n_observers, n_advisory, ratios)
#     - CIK mapping from the CIQ crosswalk
#     - EDGAR fiduciary language flags (from S-1 exhibits and body)
#     - NVCA 2020 treatment coding (pre/post template change)
#     - Event counts by type (M&A, lawsuits, earnings, etc.)
#
# =====================================================================
# =====================================================================

print("=" * 80)
print("SECTION A: COMPANY MASTER (Table A)")
print("=" * 80)

# ---------------------------------------------------------------------
# A1: Load all source files
# ---------------------------------------------------------------------
# Each file is loaded into a list of dicts for row-by-row processing.
# This matches the original build_unified_dataset.py approach.
print("\nA1: Loading source files...")

# CIQ files — extracted from S&P Capital IQ via WRDS
observers = load_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
#   Columns: proid, personid, companyid, title, boardflag, currentproflag,
#            currentboardflag, firstname, lastname
#   Each row = one person-company-title record where the title contains "observer"

advisory = load_csv(os.path.join(ciq_dir, "02_advisory_board_records.csv"))
#   Columns: proid, personid, companyid, title, boardflag, currentproflag,
#            currentboardflag, firstname, lastname
#   People with "advisory" board roles at companies that also have observers

directors = load_csv(os.path.join(ciq_dir, "03_directors_at_observer_companies.csv"))
#   Columns: proid, personid, companyid, title, boardflag, proflag,
#            currentproflag, currentboardflag, firstname, lastname
#   All directors/officers at companies with at least one observer

companies = load_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
#   Columns: companyid, companyname, companytypename, companystatustypename,
#            yearfounded, city, zipcode, webpage, country
#   Company-level attributes for all companies with observers

all_positions = load_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
#   Columns: proid, personid, companyid, companyname, companytypename, title,
#            boardflag, currentproflag, currentboardflag, firstname, lastname
#   ALL positions held by each observer person across ALL companies

events = load_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
#   Columns: keydevid, companyid, announcedate, headline,
#            keydeveventtypeid, keydeveventtypename
#   Material events at observed companies from CIQ Key Developments

# EDGAR files — extracted from SEC EDGAR filings
exhibits = load_csv(os.path.join(edgar_dir, "exhibit_analysis_results.csv"))
#   Columns: cik, company, adsh, file_type, file_date, file_description, url,
#            fetch_status, file_size, observer_mentions, has_fiduciary_manner,
#            has_no_fiduciary_duty
#   Parsed IRA exhibits from S-1 filings with fiduciary language flags

s1_body = load_csv(os.path.join(edgar_dir, "s1_body_observer_analysis.csv"))
#   Columns: cik, company, file_date, fetch_status, observer_mentions,
#            has_fiduciary_manner, has_no_fiduciary_duty, observer_passages
#   Same analysis on S-1 body text (for IRAs embedded in the body)

efts_hits = load_csv(os.path.join(edgar_dir, "efts_board_observer_s1_hits.csv"))
#   Columns: file_id, ciks, display_names, file_date, file_type, file_description,
#            adsh, form
#   EDGAR full-text search results for "board observer" in S-1 filings

print(f"  Observers:         {len(observers):>7,} records")
print(f"  Advisory:          {len(advisory):>7,} records")
print(f"  Directors:         {len(directors):>7,} records")
print(f"  Companies:         {len(companies):>7,} records")
print(f"  All positions:     {len(all_positions):>7,} records")
print(f"  Events:            {len(events):>7,} records")
print(f"  EDGAR exhibits:    {len(exhibits):>7,} records")
print(f"  EDGAR S-1 body:    {len(s1_body):>7,} records")
print(f"  EDGAR EFTS hits:   {len(efts_hits):>7,} records")

# ---------------------------------------------------------------------
# A2: Normalize all IDs across datasets
# ---------------------------------------------------------------------
# Pandas stores integer columns with NaN as float64, so IDs like 12345
# become '12345.0' in CSV. We strip the '.0' suffix so IDs match correctly
# when merging across datasets.
print("\nA2: Normalizing IDs across all datasets...")

for dataset in [observers, advisory, directors, companies, all_positions, events]:
    for r in dataset:
        if "companyid" in r:
            r["companyid"] = normalize_id(r["companyid"])
        if "personid" in r:
            r["personid"] = normalize_id(r["personid"])

print("  Done. All companyid/personid values cleaned (removed .0 suffix)")

# ---------------------------------------------------------------------
# A2b: Filter to US-only companies
# ---------------------------------------------------------------------
# The paper's institutional framework (Reg FD, NVCA, Clayton Act) is
# US-specific. Non-US companies have different observer roles (e.g.,
# Norwegian employee representative observers, French supervisory board
# observers) that are institutionally distinct from VC-appointed observers.
# We restrict to US-only observed companies here at the top of the
# pipeline so all downstream tables and network edges are US-only.
print("\nA2b: Filtering to US-only companies...")

us_companyids = set()
for co in companies:
    if co.get("country", "") == "United States":
        us_companyids.add(co["companyid"])

before_obs = len(observers)
observers = [r for r in observers if r["companyid"] in us_companyids]
before_adv = len(advisory)
advisory = [r for r in advisory if r["companyid"] in us_companyids]
before_dir = len(directors)
directors = [r for r in directors if r["companyid"] in us_companyids]
before_co = len(companies)
companies = [r for r in companies if r["companyid"] in us_companyids]
before_evt = len(events)
events = [r for r in events if r["companyid"] in us_companyids]

# Keep all positions for US observers (they may hold positions at non-US companies,
# which is fine — we want ALL their positions to find public company connections)
us_personids = set(r["personid"] for r in observers)
before_pos = len(all_positions)
all_positions = [r for r in all_positions if r["personid"] in us_personids]

print(f"  US companies: {len(us_companyids):,}")
print(f"  Observers:    {before_obs:,} -> {len(observers):,}")
print(f"  Advisory:     {before_adv:,} -> {len(advisory):,}")
print(f"  Directors:    {before_dir:,} -> {len(directors):,}")
print(f"  Companies:    {before_co:,} -> {len(companies):,}")
print(f"  Events:       {before_evt:,} -> {len(events):,}")
print(f"  Positions:    {before_pos:,} -> {len(all_positions):,}")

# ---------------------------------------------------------------------
# A3: Build company-level aggregates from CIQ
# ---------------------------------------------------------------------
# Group observers, directors, advisory members, and events by company
# so we can compute per-company statistics.
print("\nA3: Building company-level aggregates...")

# Observer counts per company
obs_by_company = defaultdict(list)
for r in observers:
    obs_by_company[r["companyid"]].append(r)

# Director counts per company.
# IMPORTANT: We exclude observers and advisory board members from the
# director count. CIQ sometimes classifies observers under "directors"
# because they attend board meetings. We want pure directors only.
# We check the title field for keywords "observer" and "advisory".
dir_by_company = defaultdict(list)
for r in directors:
    title_lower = r["title"].lower()
    is_observer = "observer" in title_lower
    is_advisory = "advisory" in title_lower or "board advisor" in title_lower
    if not is_observer and not is_advisory:
        dir_by_company[r["companyid"]].append(r)

# Advisory board counts per company (only at companies that also have observers).
# We restrict to observer companies because the advisory board data was pulled
# specifically for companies in our observer sample.
adv_by_company = defaultdict(list)
observer_companyids = set(r["companyid"] for r in observers)
for r in advisory:
    if r["companyid"] in observer_companyids:
        adv_by_company[r["companyid"]].append(r)

# Event counts per company, broken down by event type.
# This gives us variables like n_exec_board_changes, n_lawsuits, etc.
event_by_company = defaultdict(lambda: defaultdict(int))
for r in events:
    event_by_company[r["companyid"]][r["keydeveventtypename"]] += 1

print(f"  Companies with observers:  {len(obs_by_company):,}")
print(f"  Companies with directors:  {len(dir_by_company):,}")
print(f"  Companies with advisory:   {len(adv_by_company):,}")
print(f"  Companies with events:     {len(event_by_company):,}")

# ---------------------------------------------------------------------
# A4: Build CIK crosswalk
# ---------------------------------------------------------------------
# Load the CIQ company ID -> SEC CIK mapping.
# This was pulled from WRDS (ciq_common.wrds_cik) by 01_pull_all_wrds_data.py.
# Not all CIQ companies have CIKs — many are private and never filed with SEC.
print("\nA4: Loading CIQ-CIK crosswalk...")

cik_crosswalk_file = os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv")
cik_map = {}  # companyid -> cik

if os.path.exists(cik_crosswalk_file):
    for r in load_csv(cik_crosswalk_file):
        # Only use the primary CIK when a company has multiple (e.g., after a merger)
        if r.get("primaryflag", "1") == "1":
            cik_map[normalize_id(r["companyid"])] = r["cik"]
    print(f"  Loaded {len(cik_map):,} CIK mappings (primary flag = 1)")
else:
    print(f"  WARNING: CIK crosswalk not found at {cik_crosswalk_file}")

# Also build a reverse map: CIQ companyid -> CIK as integer (for industry matching later)
# We strip leading zeros from CIK and convert to int for consistent matching.
cid_to_cik_int = {}
for cid, cik_str in cik_map.items():
    try:
        cid_to_cik_int[cid] = int(cik_str.strip().lstrip("0") or "0")
    except (ValueError, AttributeError):
        pass

# ---------------------------------------------------------------------
# A5: Build EDGAR fiduciary language lookup
# ---------------------------------------------------------------------
# Parse S-1 filing exhibits and body text to determine whether each
# company's IRA (Investors' Rights Agreement) contains fiduciary language
# for observers.
#
# Two key flags:
#   has_fiduciary_manner: True if the IRA says observers should
#       "act in a fiduciary manner" (pre-2020 NVCA language)
#   has_no_fiduciary_duty: True if the IRA explicitly disclaims
#       fiduciary duty for observers (post-2020 NVCA language)
#
# These flags code the NVCA 2020 treatment variable used in the paper.
print("\nA5: Building EDGAR fiduciary language lookup...")

edgar_fiduciary = {}  # cik -> dict of fiduciary flags

# Process exhibit files first (these are the cleanest source —
# the IRA is a standalone exhibit attached to the S-1)
for r in exhibits:
    cik = r.get("cik", "")
    if not cik:
        continue
    date = r.get("file_date", "")
    has_fid = r.get("has_fiduciary_manner", "") == "True"
    has_no_fid = r.get("has_no_fiduciary_duty", "") == "True"
    obs_mentions = int(r.get("observer_mentions", 0) or 0)

    # Keep the record with the most observer mentions (most informative filing)
    if cik not in edgar_fiduciary or obs_mentions > edgar_fiduciary[cik].get("observer_mentions", 0):
        edgar_fiduciary[cik] = {
            "edgar_filing_date": date,
            "edgar_has_fiduciary_manner": has_fid,
            "edgar_has_no_fiduciary_duty": has_no_fid,
            "edgar_observer_mentions": obs_mentions,
            "edgar_source": "exhibit",
        }

# Supplement from S-1 body text for companies not covered by exhibits.
# Some S-1 filings include the IRA in the body text rather than as a
# separate exhibit. We only use this if we don't already have exhibit data.
for r in s1_body:
    cik = r.get("cik", "")
    if not cik or cik in edgar_fiduciary:
        continue
    if r.get("fetch_status", "") != "ok":
        continue
    has_fid = r.get("has_fiduciary_manner", "") == "True"
    has_no_fid = r.get("has_no_fiduciary_duty", "") == "True"
    obs_mentions = int(r.get("observer_mentions", 0) or 0)
    edgar_fiduciary[cik] = {
        "edgar_filing_date": r.get("file_date", ""),
        "edgar_has_fiduciary_manner": has_fid,
        "edgar_has_no_fiduciary_duty": has_no_fid,
        "edgar_observer_mentions": obs_mentions,
        "edgar_source": "s1_body",
    }

# EFTS hit CIKs: companies with any "board observer" mention in S-1
# (from EDGAR full-text search). This is a broader flag than the
# fiduciary coding above — it just indicates the S-1 mentions observers.
efts_ciks = set()
for h in efts_hits:
    if h.get("ciks"):
        for cik in h["ciks"].split("|"):
            efts_ciks.add(cik.strip())

print(f"  EDGAR fiduciary coding:      {len(edgar_fiduciary):,} companies")
print(f"  EFTS S-1 observer mentions:  {len(efts_ciks):,} unique CIKs")

# ---------------------------------------------------------------------
# A6: Assemble company-level master (Table A)
# ---------------------------------------------------------------------
# Combine all the above into a single company-level dataset.
# Each row = one observed company with board composition, EDGAR flags,
# NVCA treatment coding, and event counts.
print("\nA6: Assembling company-level master (Table A)...")

company_master = []
for co in companies:
    cid = co["companyid"]

    # --- Observer aggregates ---
    obs_list = obs_by_company.get(cid, [])
    n_observers = len(obs_list)
    n_current_observers = sum(1 for r in obs_list if r.get("currentboardflag") == "1.0")
    observer_names = "; ".join(sorted(set(
        f"{r['firstname']} {r['lastname']}" for r in obs_list
    )))

    # --- Director aggregates (excluding observers and advisory) ---
    dir_list = dir_by_company.get(cid, [])
    n_directors = len(dir_list)
    n_current_directors = sum(1 for r in dir_list if r.get("currentboardflag") == "1.0")

    # --- Advisory board aggregates ---
    adv_list = adv_by_company.get(cid, [])
    n_advisory = len(adv_list)

    # --- Board composition ratios ---
    # Total board = directors + observers + advisory members
    # Observer ratio = fraction of total board that are observers
    total_board = n_directors + n_observers + n_advisory
    observer_ratio = n_observers / total_board if total_board > 0 else 0

    # --- CIK mapping ---
    cik = cik_map.get(cid, "")

    # --- EDGAR fiduciary language (requires CIK) ---
    edgar_data = edgar_fiduciary.get(cik, {})
    has_s1_observer = cik in efts_ciks if cik else False

    # --- NVCA 2020 treatment coding ---
    # The NVCA (National Venture Capital Association) changed its model IRA
    # template in 2020: the old version said observers should "act in a
    # fiduciary manner"; the new version explicitly disclaims fiduciary duty.
    # We code companies based on when their S-1 was filed:
    #   - Filings before 2020: likely used the old template (fiduciary manner)
    #   - Filings from 2021+: likely used the new template (no fiduciary duty)
    #   - Filings in 2020: transition year (coded separately)
    filing_date = edgar_data.get("edgar_filing_date", "")
    if filing_date:
        filing_year = int(filing_date[:4]) if filing_date[:4].isdigit() else 0
        nvca_post2020 = 1 if filing_year >= 2021 else 0
        nvca_transition = 1 if filing_year == 2020 else 0
    else:
        nvca_post2020 = ""
        nvca_transition = ""

    # --- Event counts by type ---
    # These come from CIQ Key Developments (announcements of material events)
    evt = event_by_company.get(cid, {})

    row = {
        "companyid": cid,
        "companyname": co["companyname"],
        "companytypename": co["companytypename"],
        "companystatustypename": co["companystatustypename"],
        "yearfounded": co.get("yearfounded", ""),
        "city": co.get("city", ""),
        "country": co.get("country", ""),
        # Board composition
        "n_directors": n_directors,
        "n_current_directors": n_current_directors,
        "n_observers": n_observers,
        "n_current_observers": n_current_observers,
        "n_advisory": n_advisory,
        "total_board": total_board,
        "observer_ratio": round(observer_ratio, 4),
        "observer_names": observer_names,
        # CIK and EDGAR
        "cik": cik,
        "has_s1_observer_mention": has_s1_observer,
        "edgar_filing_date": edgar_data.get("edgar_filing_date", ""),
        "edgar_has_fiduciary_manner": edgar_data.get("edgar_has_fiduciary_manner", ""),
        "edgar_has_no_fiduciary_duty": edgar_data.get("edgar_has_no_fiduciary_duty", ""),
        "edgar_observer_mentions": edgar_data.get("edgar_observer_mentions", ""),
        "edgar_source": edgar_data.get("edgar_source", ""),
        # NVCA treatment
        "nvca_post2020": nvca_post2020,
        "nvca_transition_2020": nvca_transition,
        # Events (from CIQ Key Developments)
        "n_exec_board_changes": evt.get("Executive/Board Changes - Other", 0),
        "n_lawsuits": evt.get("Lawsuits & Legal Issues", 0),
        "n_restatements": evt.get("Restatements of Operating Results", 0),
        "n_earnings_announcements": evt.get("Announcements of Earnings", 0),
        "n_financing_events": evt.get("Seeking Financing/Partners", 0),
        "n_bankruptcy": evt.get("Bankruptcy - Other", 0),
    }
    company_master.append(row)

# Save Table A
outfile_a = os.path.join(data_dir, "table_a_company_master.csv")
save_csv_from_dicts(company_master, outfile_a)
print(f"  Table A saved: {len(company_master):,} rows -> {outfile_a}")


# =====================================================================
# =====================================================================
#
#   SECTION B: BASE CIQ NETWORK (Table B + Panel C base edges)
#
#   Reads: CIQ_Extract/01_observer_records.csv
#          CIQ_Extract/05_observer_person_all_positions.csv
#   Queries: ciq_common.wrds_cik (for public portfolio company CIKs)
#   Writes: Data/table_b_observer_network.csv
#           Panel_C_Network/01_public_portfolio_companies.csv
#           Panel_C_Network/02_observer_public_portfolio_edges.csv
#
#   This section does two things:
#
#   1. Table B: For each observer, identify their VC/PE firm affiliation
#      and build the observer -> observed company -> VC firm network.
#
#   2. Panel C base: For each observer, find their positions at PUBLIC
#      companies and create network edges linking observed private
#      companies to public portfolio companies through shared observers.
#      This is the core data structure for the paper.
#
# =====================================================================
# =====================================================================

print(f"\n\n{'=' * 80}")
print("SECTION B: BASE CIQ NETWORK (Table B + Panel C base edges)")
print("=" * 80)

# ---------------------------------------------------------------------
# B1: Classify each observer's positions
# ---------------------------------------------------------------------
# Using the all-positions file (05_observer_person_all_positions.csv),
# classify each position into:
#   - VC/PE firm: companytypename in {"Private Investment Firm",
#     "Public Investment Firm", "Private Fund"}
#   - Public Company: companytypename = "Public Company"
#   - Other: everything else (universities, nonprofits, etc.)
print("\nB1: Classifying observer positions...")

# Set of all person IDs that appear in the observer records
observer_persons = set(r["personid"] for r in observers)

# Build lookup structures for each observer's connections
person_vc_firms = defaultdict(set)    # personid -> set of (vc_companyid, vc_name)
person_portfolio = defaultdict(set)   # personid -> set of (portfolio_companyid, portfolio_name)
person_observed = defaultdict(set)    # personid -> set of (observed_companyid)

# Map each observer to their observed companies (from observer records)
for r in observers:
    person_observed[r["personid"]].add(r["companyid"])

# Company types that indicate a VC/PE firm
vc_pe_types = {"Private Investment Firm", "Public Investment Firm", "Private Fund"}

# Classify each position in the all-positions file.
# For each observer:
#   - If the company is a VC/PE firm -> record as the observer's VC affiliation
#   - If the observer holds a director/board title at a non-VC company ->
#     record as a portfolio company (potential network edge)
for r in all_positions:
    pid = r["personid"]
    ctype = r.get("companytypename", "")
    cid = r["companyid"]
    cname = r.get("companyname", "")
    title = r.get("title", "")

    if ctype in vc_pe_types:
        # This is a VC/PE firm where the observer works
        person_vc_firms[pid].add((cid, cname))
    elif "Director" in title or "Board" in title:
        # This is a company where the observer holds a board-level position
        # (could be public or private)
        person_portfolio[pid].add((cid, cname))

print(f"  Observers with VC/PE affiliation:  {len(person_vc_firms):,}")
print(f"  Observers with portfolio companies: {len(person_portfolio):,}")

# Build a quick lookup: company name by companyid (for Table B output)
company_name_map = {co["companyid"]: co["companyname"] for co in companies}

# ---------------------------------------------------------------------
# B2: Build Table B (observer -> observed company -> VC firm)
# ---------------------------------------------------------------------
# For each observer, create one row per (observed company, VC firm) pair.
# This captures the chain: Observer X observes at Private Co A via VC Firm Y.
print("\nB2: Building Table B (observer -> observed co -> VC firm)...")

network_rows = []
for pid in observer_persons:
    observed = person_observed.get(pid, set())
    vc_firms = person_vc_firms.get(pid, set())

    # Get observer name from the observer records
    obs_records = [r for r in observers if r["personid"] == pid]
    if not obs_records:
        continue
    obs_name = f"{obs_records[0]['firstname']} {obs_records[0]['lastname']}"

    # Create one row for each (observed company, VC firm) combination
    for obs_cid in observed:
        obs_company_name = company_name_map.get(obs_cid, "")

        for vc_cid, vc_name in vc_firms:
            row = {
                "observer_personid": pid,
                "observer_name": obs_name,
                "observed_companyid": obs_cid,
                "observed_companyname": obs_company_name,
                "vc_firm_companyid": vc_cid,
                "vc_firm_name": vc_name,
                "n_portfolio_companies": len(person_portfolio.get(pid, set())),
                "observer_title": obs_records[0].get("title", ""),
                "is_current_observer": obs_records[0].get("currentboardflag", "") == "1.0",
            }
            network_rows.append(row)

# Save Table B
outfile_b = os.path.join(data_dir, "table_b_observer_network.csv")
save_csv_from_dicts(network_rows, outfile_b)
print(f"  Table B saved: {len(network_rows):,} rows -> {outfile_b}")

# ---------------------------------------------------------------------
# B3: Get CIKs for public portfolio companies (WRDS query)
# ---------------------------------------------------------------------
# We need CIKs for the public companies in the observer network so we
# can link to CRSP for stock returns. This is one of the small WRDS
# lookups required by this script.
#
# Query: ciq_common.wrds_cik WHERE companyid IN (public_companyids)
print("\nB3: Getting CIKs for public portfolio companies...")

# Identify all public company positions held by observers
public_positions = [r for r in all_positions if r.get("companytypename") == "Public Company"]
public_companyids = set(normalize_id(r["companyid"]) for r in public_positions)

print(f"  Public company positions held by observers: {len(public_positions):,}")
print(f"  Unique public companies in observer network: {len(public_companyids):,}")

# Query WRDS for CIK mappings
pub_cik_map = {}  # CIQ companyid -> {cik, name}

if public_companyids:
    conn = get_wrds_connection()
    cur = conn.cursor()

    # Filter out any empty IDs
    pub_ids_clean = sorted(pid for pid in public_companyids if pid)
    pub_id_str = ", ".join(pub_ids_clean)

    time.sleep(3)  # Rate limit
    cur.execute(f"""
        SELECT companyid, cik, companyname, primaryflag
        FROM ciq_common.wrds_cik
        WHERE companyid IN ({pub_id_str})
        AND primaryflag = 1
    """)
    pub_cik_rows = cur.fetchall()

    for r in pub_cik_rows:
        cid = str(int(r[0]))
        pub_cik_map[cid] = {"cik": r[1], "name": r[2]}

    cur.close()
    conn.close()

print(f"  Public portfolio companies with CIK: {len(pub_cik_map):,} of {len(public_companyids):,}")

# Save the public portfolio company list
# This file lists all public companies connected to observers with their CIKs.
outfile_pub = os.path.join(panel_c_dir, "01_public_portfolio_companies.csv")
pub_rows = [{"companyid": cid, "cik": info["cik"], "companyname": info["name"]}
            for cid, info in sorted(pub_cik_map.items())]
save_csv_from_dicts(pub_rows, outfile_pub, fieldnames=["companyid", "cik", "companyname"])
print(f"  Saved: {len(pub_cik_map):,} companies -> {outfile_pub}")

# ---------------------------------------------------------------------
# B4: Build network edges (observed private co <-> public portfolio co)
# ---------------------------------------------------------------------
# THIS IS THE KEY STEP for the paper.
#
# For each observer in the network (Table B), we look at all their positions.
# If they hold a position at a PUBLIC company (that is not the same as the
# observed private company), we create a network edge:
#
#   (observed private company) --[observer]--> (public portfolio company)
#
# The resulting dataset is the person-level network described in the paper:
# "the same individual sits as a nonvoting observer at a private company
# and as a voting director at a public company, personally bridging the
# two boardrooms."
#
# Each edge records:
#   - Who the observer is (personid, name)
#   - Which private company they observe at (observed_companyid)
#   - Which VC firm they work for (vc_firm_companyid)
#   - Which public company they are connected to (portfolio_companyid)
#   - What role they hold at the public company (portfolio_title)
#   - Whether the position is current (is_current)
print("\nB4: Building network edges (observed co <-> public portfolio co)...")

ciq_edges = []

for r in network_rows:
    obs_pid = r["observer_personid"]      # CIQ person ID of the observer
    obs_name = r["observer_name"]
    obs_cid = r["observed_companyid"]     # CIQ company ID of the private firm they observe
    obs_cname = r["observed_companyname"]
    vc_cid = r["vc_firm_companyid"]       # CIQ company ID of the VC firm
    vc_name = r["vc_firm_name"]

    # Search all positions to find this observer's public company connections
    for pos in all_positions:
        pid_norm = normalize_id(pos["personid"])

        # Skip if this position belongs to a different person
        if pid_norm != obs_pid:
            continue
        # Skip if the position is not at a public company
        if pos.get("companytypename") != "Public Company":
            continue

        port_cid = normalize_id(pos["companyid"])

        # Skip self-links: the public company IS the observed company.
        # This can happen if a private company later went public.
        if port_cid == obs_cid:
            continue

        port_cik_info = pub_cik_map.get(port_cid, {})

        # Record this network edge
        ciq_edges.append({
            "observer_personid": obs_pid,
            "observer_name": obs_name,
            "observed_companyid": obs_cid,
            "observed_companyname": obs_cname,
            "vc_firm_companyid": vc_cid,
            "vc_firm_name": vc_name,
            "portfolio_companyid": port_cid,
            "portfolio_companyname": pos.get("companyname", ""),
            "portfolio_cik": port_cik_info.get("cik", ""),
            "portfolio_title": pos.get("title", ""),
            "is_current": pos.get("currentproflag", ""),
        })

# Save base CIQ network edges
outfile_edges = os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv")
save_csv_from_dicts(ciq_edges, outfile_edges)

unique_obs = len(set(r["observer_personid"] for r in ciq_edges))
unique_port = len(set(r["portfolio_companyid"] for r in ciq_edges))
unique_port_cik = len(set(r["portfolio_cik"] for r in ciq_edges if r["portfolio_cik"]))
print(f"  CIQ network edges:                     {len(ciq_edges):,}")
print(f"  Unique observers with public links:     {unique_obs:,}")
print(f"  Unique public portfolio companies:      {unique_port:,}")
print(f"  Portfolio companies with CIK:           {unique_port_cik:,}")
print(f"  Saved -> {outfile_edges}")


# =====================================================================
# =====================================================================
#
#   SECTION C: BOARDEX SUPPLEMENTATION
#
#   Reads: BoardEx/observer_boardex_crosswalk.csv
#          BoardEx/observer_boardex_positions.csv
#          BoardEx/observer_boardex_companies.csv
#   Writes: (edges stored in memory, combined in Section E)
#
#   For each observer matched in BoardEx, find their public company board
#   positions and create new network edges. This catches connections that
#   CIQ missed because CIQ's Professionals database may not have complete
#   coverage of all board positions.
#
#   Logic:
#     Observer X (CIQ personid) -> matched to BoardEx directorid
#     BoardEx shows directorid holds board seat at Public Firm B
#     Observer X also observes at Private Firms {A1, A2, ...}
#     Create edges: (A1, B), (A2, B), ... for each observed company
#
# =====================================================================
# =====================================================================

print(f"\n\n{'=' * 80}")
print("SECTION C: BOARDEX SUPPLEMENTATION")
print("=" * 80)

# ---------------------------------------------------------------------
# C1: Load BoardEx crosswalk and positions
# ---------------------------------------------------------------------
# The crosswalk links CIQ personid to BoardEx directorid (built by
# pull_boardex_supplement.py, which matched on name/company overlap).
print("\nC1: Loading BoardEx data...")

bd_xwalk = pd.read_csv(os.path.join(boardex_dir, "observer_boardex_crosswalk.csv"))
#   Columns: directorid, directorname, forename1, surname, ciq_personid,
#            firstname, lastname, score, matchstyle
#   Maps BoardEx director IDs to CIQ person IDs

bd_pos = pd.read_csv(os.path.join(boardex_dir, "observer_boardex_positions.csv"))
#   Columns: directorid, directorname, companyname, companyid, rolename,
#            datestartrole, dateendrole, brdposition, ned, orgtype, isin,
#            sector, hocountryname, rowtype
#   All positions held by matched directors in BoardEx

bd_co = pd.read_csv(os.path.join(boardex_dir, "observer_boardex_companies.csv"))
#   Columns: boardid, companyid, boardname, ticker, isin, cikcode, hocountryname
#   BoardEx company identifiers including CIK codes

print(f"  BoardEx crosswalk:  {len(bd_xwalk):,} matched persons")
print(f"  BoardEx positions:  {len(bd_pos):,} total positions")
print(f"  BoardEx companies:  {len(bd_co):,} companies")

# Normalize the CIQ person IDs in the crosswalk (remove .0 suffix)
bd_xwalk["ciq_personid"] = bd_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
# Build the BoardEx directorid -> CIQ personid mapping
bd_did_to_ciq = dict(zip(bd_xwalk["directorid"], bd_xwalk["ciq_personid"]))

# ---------------------------------------------------------------------
# C2: Filter to board positions at public companies
# ---------------------------------------------------------------------
# We only want edges to publicly traded companies (where we can measure
# stock returns). In BoardEx:
#   - brdposition="Yes" means a formal board seat
#   - rolename containing Director/Board/Chairman/CEO/CFO/Officer catches
#     senior roles even if brdposition is not flagged
#   - orgtype in {"Quoted", "Listed"} identifies public companies
print("\nC2: Filtering to board positions at public companies...")

bd_board = bd_pos[
    # Formal board position flag
    (bd_pos["brdposition"] == "Yes") |
    # OR senior role name (catches positions BoardEx doesn't flag as board)
    (bd_pos["rolename"].str.contains(
        "Director|Board|Chairman|CEO|CFO|Officer",
        case=False, na=False
    ))
].copy()

# Restrict to public companies only
bd_board = bd_board[bd_board["orgtype"].isin(["Quoted", "Listed"])]

# Map BoardEx directorid -> CIQ personid via the crosswalk
bd_board["ciq_personid"] = bd_board["directorid"].map(bd_did_to_ciq)
# Drop rows where we don't have a CIQ match
bd_board = bd_board.dropna(subset=["ciq_personid"])

print(f"  Board positions at public companies (matched to CIQ): {len(bd_board):,}")

# ---------------------------------------------------------------------
# C3: Map BoardEx company IDs to CIK codes
# ---------------------------------------------------------------------
# The BoardEx companies file contains CIK codes (cikcode column).
# We need CIKs to match BoardEx companies to CRSP for stock returns.
print("\nC3: Mapping BoardEx companies to CIK...")

bd_co_cik = dict(zip(bd_co["companyid"], bd_co["cikcode"]))
bd_board["cik"] = bd_board["companyid"].map(bd_co_cik)
bd_board["cik"] = pd.to_numeric(bd_board["cik"], errors="coerce")
# Drop companies without a CIK (can't link to CRSP)
bd_board = bd_board.dropna(subset=["cik"])

print(f"  BoardEx positions with CIK: {len(bd_board):,}")

# ---------------------------------------------------------------------
# C4: Create network edges from BoardEx
# ---------------------------------------------------------------------
# For each (observer, public company) pair from BoardEx, create an edge
# to each private company the observer observes at.
#
# We use the obs_to_companies mapping built from the observer records:
# for each CIQ personid, which private companies do they observe at?
print("\nC4: Creating BoardEx network edges...")

# Build observer -> observed companies mapping using CIQ observer records
# (same data from Section B, reformatted for this section)
obs_to_companies = defaultdict(set)
for r in observers:
    obs_to_companies[r["personid"]].add(r["companyid"])

bd_edges = []
for _, row in bd_board.iterrows():
    obs_pid = row["ciq_personid"]
    cik = int(row["cik"])
    # Get all private companies this observer watches
    observed_companies = obs_to_companies.get(obs_pid, set())
    # Create one edge for each (observer, observed co, public co) triple
    for obs_cid in observed_companies:
        bd_edges.append({
            "observer_personid": obs_pid,
            "observed_companyid": obs_cid,
            "portfolio_cik": cik,
            "portfolio_companyname": row["companyname"],
            "portfolio_title": row["rolename"],
            "source": "BoardEx",
        })

bd_edges_df = pd.DataFrame(bd_edges) if bd_edges else pd.DataFrame(
    columns=["observer_personid", "observed_companyid", "portfolio_cik",
             "portfolio_companyname", "portfolio_title", "source"])

print(f"  BoardEx edges (before dedup): {len(bd_edges_df):,}")
print(f"  Unique observers in BoardEx edges: "
      f"{bd_edges_df['observer_personid'].nunique() if len(bd_edges_df) > 0 else 0:,}")


# =====================================================================
# =====================================================================
#
#   SECTION D: FORM 4 SUPPLEMENTATION
#
#   Reads: Form4/observer_form4_trades.csv
#          CIQ_Extract/08_observer_tr_insider_crosswalk.csv
#   Queries: crsp.stocknames (CUSIP -> PERMNO)
#            crsp_a_ccm.ccmxpf_lnkhist + comp.company (PERMNO -> CIK)
#   Writes: (edges stored in memory, combined in Section E)
#
#   If an observer filed a Form 4 (insider trading report) at a public
#   company, this CONFIRMS they have an insider relationship there.
#   Form 4 is the strongest evidence of a connection because SEC law
#   requires insiders to disclose their trades.
#
#   Pipeline:
#     Form 4 trades -> TR personid -> CIQ personid (via crosswalk)
#     Form 4 CUSIP -> CRSP PERMNO -> CIK (via CRSP + CCM)
#
# =====================================================================
# =====================================================================

print(f"\n\n{'=' * 80}")
print("SECTION D: FORM 4 SUPPLEMENTATION")
print("=" * 80)

# ---------------------------------------------------------------------
# D1: Load Form 4 trades and TR-CIQ crosswalk
# ---------------------------------------------------------------------
print("\nD1: Loading Form 4 trades and crosswalk...")

trades = pd.read_csv(os.path.join(form4_dir, "observer_form4_trades.csv"))
#   Columns: personid, owner, secid, ticker, cusip6, cusip2, cname, rolecode1,
#            rolecode2, formtype, trancode, acqdisp, trandate, tprice, shares,
#            sharesheld, ownership, cleanse, shares_adj, tprice_adj, sectitle,
#            fdate, sigdate
#   Form 4 insider trades filed by observers. The personid here is the
#   Thomson Reuters insider person ID (not CIQ).

tr_xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
#   Columns: tr_personid, owner, ciq_personid, firstname, middlename,
#            lastname, score, matchstyle
#   Maps TR insider person IDs to CIQ person IDs (built by fuzzy name matching)

print(f"  Form 4 trades:     {len(trades):,} records")
print(f"  TR-CIQ crosswalk:  {len(tr_xwalk):,} matched persons")

# Normalize IDs
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk["tr_personid"] = tr_xwalk["tr_personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk["ciq_personid"] = tr_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)

# Map TR personid -> CIQ personid
tr_to_ciq = dict(zip(tr_xwalk["tr_personid"], tr_xwalk["ciq_personid"]))
trades["ciq_personid"] = trades["personid"].map(tr_to_ciq)
# Drop trades where we can't match the person to CIQ
trades = trades.dropna(subset=["ciq_personid"])

print(f"  Trades matched to CIQ persons: {len(trades):,}")

# ---------------------------------------------------------------------
# D2: Map Form 4 CUSIPs to CIKs via CRSP (WRDS query)
# ---------------------------------------------------------------------
# Form 4 reports a CUSIP for the issuer. We need to map:
#   CUSIP -> PERMNO (via crsp.stocknames)
#   PERMNO -> CIK (via crsp_a_ccm.ccmxpf_lnkhist + comp.company)
#
# These are the WRDS lookups that require a database connection.
print("\nD2: Mapping CUSIPs to CIKs via WRDS (CUSIP -> PERMNO -> CIK)...")

conn = get_wrds_connection()
cur = conn.cursor()

# Query 1: CUSIP-to-PERMNO mapping from CRSP stock names
# CRSP stores the "historical CUSIP" as ncusip (8-character CUSIP without check digit)
time.sleep(3)
print("  Querying crsp.stocknames for CUSIP -> PERMNO...")
cur.execute("""
    SELECT DISTINCT ncusip, permno
    FROM crsp.stocknames
    WHERE ncusip IS NOT NULL
    AND permno IS NOT NULL
""")
cusip_to_permno = {r[0]: r[1] for r in cur.fetchall()}
print(f"  CUSIP -> PERMNO mappings: {len(cusip_to_permno):,}")

# Query 2: PERMNO-to-CIK mapping via CCM link table
# The CCM (CRSP-Compustat Merged) link table bridges CRSP PERMNOs
# to Compustat GVKEYs, and Compustat's company table has CIKs.
# Link types LU/LC = "link used/confirmed by CRSP"
# Link priority P/C = "primary/primary candidate"
time.sleep(3)
print("  Querying crsp_a_ccm for PERMNO -> CIK...")
cur.execute("""
    SELECT DISTINCT b.cik, a.lpermno as permno
    FROM crsp_a_ccm.ccmxpf_lnkhist a
    JOIN comp.company b ON a.gvkey = b.gvkey
    WHERE a.linktype IN ('LU', 'LC')
    AND a.linkprim IN ('P', 'C')
    AND b.cik IS NOT NULL
""")
permno_to_cik = {}
for r in cur.fetchall():
    try:
        permno_to_cik[int(r[1])] = int(r[0])
    except (TypeError, ValueError):
        pass
print(f"  PERMNO -> CIK mappings: {len(permno_to_cik):,}")

# Don't close connection yet — we need it for Section F
# cur.close() and conn.close() happen after Section F

# ---------------------------------------------------------------------
# D3: Build the CUSIP -> PERMNO -> CIK chain for Form 4 trades
# ---------------------------------------------------------------------
# Form 4 has cusip6 (first 6 characters) and cusip2 (check digits).
# Concatenate them to form the 8-character CUSIP used by CRSP.
print("\nD3: Mapping Form 4 trades to CIKs...")

trades["cusip8"] = (trades["cusip6"].astype(str).str.strip() +
                    trades["cusip2"].astype(str).str.strip())
trades["permno"] = trades["cusip8"].map(cusip_to_permno)
trades["cik"] = trades["permno"].map(permno_to_cik)

matched_trades = trades.dropna(subset=["cik"])
print(f"  Trades with CUSIP->PERMNO->CIK chain: {len(matched_trades):,} of {len(trades):,}")

# ---------------------------------------------------------------------
# D4: Create network edges from Form 4
# ---------------------------------------------------------------------
# Deduplicate to unique (observer, company) connections — we don't need
# individual trades, just the fact that the observer is an insider at
# the company.
print("\nD4: Creating Form 4 network edges...")

f4_unique = trades[["ciq_personid", "cname", "cik"]].dropna(subset=["cik"]).drop_duplicates()
f4_unique["cik"] = f4_unique["cik"].astype(int)

# For each (observer, public company) pair from Form 4, create an edge
# to each private company the observer observes at.
f4_edges = []
for _, row in f4_unique.iterrows():
    obs_pid = row["ciq_personid"]
    cik = row["cik"]
    observed_companies = obs_to_companies.get(obs_pid, set())
    for obs_cid in observed_companies:
        f4_edges.append({
            "observer_personid": obs_pid,
            "observed_companyid": obs_cid,
            "portfolio_cik": cik,
            "portfolio_companyname": row["cname"],
            "portfolio_title": "Form 4 Insider",
            "source": "Form4",
        })

f4_edges_df = pd.DataFrame(f4_edges) if f4_edges else pd.DataFrame(
    columns=["observer_personid", "observed_companyid", "portfolio_cik",
             "portfolio_companyname", "portfolio_title", "source"])

print(f"  Form 4 edges (before dedup): {len(f4_edges_df):,}")
print(f"  Unique observers in Form 4 edges: "
      f"{f4_edges_df['observer_personid'].nunique() if len(f4_edges_df) > 0 else 0:,}")


# =====================================================================
# =====================================================================
#
#   SECTION E: COMBINE AND DEDUPLICATE
#
#   Reads: CIQ edges (from Section B), BoardEx edges (Section C),
#          Form 4 edges (Section D), industry codes
#   Writes: Panel_C_Network/02b_supplemented_network_edges.csv
#
#   Stack all three edge sources, deduplicate on the triple
#   (observer_personid, observed_companyid, portfolio_cik), tag each
#   edge with its data source, and add same-industry flags.
#
# =====================================================================
# =====================================================================

print(f"\n\n{'=' * 80}")
print("SECTION E: COMBINE AND DEDUPLICATE")
print("=" * 80)

# ---------------------------------------------------------------------
# E1: Standardize CIQ edges to match BoardEx/Form4 format
# ---------------------------------------------------------------------
# The CIQ edges from Section B have more columns (vc_firm, observer_name, etc.)
# We need to extract just the columns used for deduplication and source tagging.
print("\nE1: Standardizing CIQ edges...")

ciq_edges_df = pd.DataFrame(ciq_edges) if ciq_edges else pd.DataFrame(
    columns=["observer_personid", "observed_companyid", "portfolio_cik",
             "portfolio_companyname", "portfolio_title"])

# Select columns needed for the unified edge format
orig_std = ciq_edges_df[["observer_personid", "observed_companyid", "portfolio_cik",
                          "portfolio_companyname", "portfolio_title"]].copy()
orig_std["source"] = "CIQ"

# Convert portfolio_cik to numeric for consistent matching
orig_std["portfolio_cik"] = pd.to_numeric(orig_std["portfolio_cik"], errors="coerce").astype("Int64")

print(f"  CIQ edges (standardized): {len(orig_std):,}")

# ---------------------------------------------------------------------
# E2: Stack all edges
# ---------------------------------------------------------------------
print("\nE2: Stacking CIQ + BoardEx + Form 4 edges...")

# Ensure BoardEx and Form 4 edges have consistent CIK types
if len(bd_edges_df) > 0:
    bd_edges_df["portfolio_cik"] = pd.to_numeric(bd_edges_df["portfolio_cik"], errors="coerce").astype("Int64")
if len(f4_edges_df) > 0:
    f4_edges_df["portfolio_cik"] = pd.to_numeric(f4_edges_df["portfolio_cik"], errors="coerce").astype("Int64")

all_edges = pd.concat([orig_std, bd_edges_df, f4_edges_df], ignore_index=True)
all_edges["portfolio_cik"] = pd.to_numeric(all_edges["portfolio_cik"], errors="coerce").astype("Int64")

print(f"  Total edges before dedup: {len(all_edges):,}")
print(f"    CIQ:     {len(orig_std):,}")
print(f"    BoardEx: {len(bd_edges_df):,}")
print(f"    Form 4:  {len(f4_edges_df):,}")

# ---------------------------------------------------------------------
# E3: Deduplicate on (observer, observed company, portfolio CIK)
# ---------------------------------------------------------------------
# The same (observer, observed company, portfolio company) triple may
# appear in multiple sources. For example, CIQ might list a director
# position that BoardEx also covers. We keep one copy and preserve
# the source from the first occurrence (CIQ > BoardEx > Form4 priority
# since CIQ is listed first in the concat).
print("\nE3: Deduplicating...")

before = len(all_edges)
all_edges = all_edges.drop_duplicates(
    subset=["observer_personid", "observed_companyid", "portfolio_cik"]
)
print(f"  Before dedup: {before:,}")
print(f"  After dedup:  {len(all_edges):,}")
print(f"  Duplicates removed: {before - len(all_edges):,}")

# ---------------------------------------------------------------------
# E4: Add same-industry flag
# ---------------------------------------------------------------------
# For each edge, check whether the observed private company and the
# connected public portfolio company share the same 2-digit SIC code.
# This is the key moderator in the paper: information spillover should
# be strongest for same-industry connections.
#
# Chain:
#   observed_companyid -> CIK (via cid_to_cik_int) -> SIC2 (via cik_to_sic2)
#   portfolio_cik -> SIC2 (via cik_to_sic2)
print("\nE4: Adding same-industry flags...")

# Load industry codes (SIC/NAICS from Compustat, saved in Panel_C_Network)
industry_file = os.path.join(panel_c_dir, "05_industry_codes.csv")
if os.path.exists(industry_file):
    industry = pd.read_csv(industry_file)
    industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
    industry["sic2"] = industry["sic"].astype(str).str[:2]
    cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))
    print(f"  Loaded {len(cik_to_sic2):,} CIK -> SIC2 mappings from existing file")

    # Map observed company -> CIK -> SIC2
    all_edges["obs_sic2"] = (all_edges["observed_companyid"]
                              .map(cid_to_cik_int)
                              .map(cik_to_sic2))
    # Map portfolio company -> SIC2
    all_edges["port_sic2"] = all_edges["portfolio_cik"].map(cik_to_sic2)
    # Flag same-industry edges
    all_edges["same_industry"] = (
        (all_edges["obs_sic2"] == all_edges["port_sic2"]) &
        all_edges["obs_sic2"].notna() &
        all_edges["port_sic2"].notna()
    ).astype(int)
    # Drop intermediate columns
    all_edges = all_edges.drop(columns=["obs_sic2", "port_sic2"])

    n_same = all_edges["same_industry"].sum()
    print(f"  Same-industry edges: {n_same:,} of {len(all_edges):,}")
else:
    # If industry codes file doesn't exist yet, we'll create it in Section F
    # and the same_industry flag will be added then.
    print(f"  Industry codes file not found — will be created in Section F")
    all_edges["same_industry"] = np.nan

# ---------------------------------------------------------------------
# E5: Save supplemented network edges
# ---------------------------------------------------------------------
outfile_supp = os.path.join(panel_c_dir, "02b_supplemented_network_edges.csv")
all_edges.to_csv(outfile_supp, index=False)
print(f"\n  Supplemented network saved: {len(all_edges):,} edges -> {outfile_supp}")


# =====================================================================
# =====================================================================
#
#   SECTION F: IDENTIFIER CROSSWALKS AND INDUSTRY CODES
#
#   Queries: crsp_a_ccm.ccmxpf_lnkhist + comp.company
#   Writes: Panel_C_Network/03_portfolio_permno_crosswalk.csv
#           Panel_C_Network/05_industry_codes.csv
#           Panel_C_Network/new_ciks_for_crsp.csv
#
#   Build the CIK -> GVKEY -> PERMNO crosswalk for all portfolio
#   companies in the supplemented network. Also pull SIC/NAICS codes
#   from Compustat for industry overlap analysis.
#
# =====================================================================
# =====================================================================

print(f"\n\n{'=' * 80}")
print("SECTION F: IDENTIFIER CROSSWALKS AND INDUSTRY CODES")
print("=" * 80)

# ---------------------------------------------------------------------
# F1: CIK -> GVKEY -> PERMNO crosswalk for all portfolio CIKs
# ---------------------------------------------------------------------
# We need PERMNOs to pull CRSP stock returns for the portfolio companies.
# The CCM link table bridges Compustat GVKEYs (which have CIKs) to
# CRSP PERMNOs (which have returns).
print("\nF1: Building CIK -> GVKEY -> PERMNO crosswalk for portfolio companies...")

# Collect all unique CIKs from the supplemented network
port_ciks = sorted(set(
    int(c) for c in all_edges["portfolio_cik"].dropna()
))
print(f"  Unique portfolio CIKs: {len(port_ciks):,}")

if port_ciks:
    port_cik_str = ", ".join(str(c) for c in port_ciks)

    time.sleep(3)
    cur.execute(f"""
        SELECT DISTINCT b.cik, a.gvkey, a.lpermno as permno
        FROM crsp_a_ccm.ccmxpf_lnkhist a
        JOIN comp.company b ON a.gvkey = b.gvkey
        WHERE CAST(b.cik AS BIGINT) IN ({port_cik_str})
        AND a.linktype IN ('LU', 'LC')
        AND a.linkprim IN ('P', 'C')
    """)
    port_links = cur.fetchall()
    port_permnos = sorted(set(int(r[2]) for r in port_links if r[2]))
    matched_ciks = len(set(str(r[0]).strip() for r in port_links if r[0]))

    print(f"  Matched: {matched_ciks:,} CIKs -> {len(port_permnos):,} PERMNOs")

    # Save the crosswalk
    outfile_xwalk = os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv")
    with open(outfile_xwalk, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["cik", "gvkey", "permno"])
        writer.writerows(port_links)
    print(f"  Saved -> {outfile_xwalk}")

# ---------------------------------------------------------------------
# F2: SIC/NAICS industry codes for all network companies
# ---------------------------------------------------------------------
# Pull industry codes for both sides of the network:
#   - Public portfolio companies (the ones we measure returns for)
#   - Observed private companies that have CIKs (for same-industry matching)
print("\nF2: Pulling SIC/NAICS industry codes from Compustat...")

# Collect CIKs from both sides of the network
all_network_ciks = set()

# Portfolio companies (public side)
for c in all_edges["portfolio_cik"].dropna():
    try:
        all_network_ciks.add(int(c))
    except (ValueError, TypeError):
        pass

# Observed companies (private side, those with CIKs)
for r in company_master:
    if r["cik"]:
        try:
            all_network_ciks.add(int(r["cik"]))
        except (ValueError, TypeError):
            pass

print(f"  Total unique CIKs in network (both sides): {len(all_network_ciks):,}")

if all_network_ciks:
    all_cik_str = ", ".join(str(c) for c in sorted(all_network_ciks))

    time.sleep(3)
    cur.execute(f"""
        SELECT DISTINCT gvkey, cik, sic, naics, conm
        FROM comp.company
        WHERE CAST(cik AS BIGINT) IN ({all_cik_str})
    """)
    ind_rows = cur.fetchall()
    ind_cols = [d[0] for d in cur.description]

    outfile_ind = os.path.join(panel_c_dir, "05_industry_codes.csv")
    with open(outfile_ind, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(ind_cols)
        writer.writerows(ind_rows)

    n_cos = len(set(r[0] for r in ind_rows))
    print(f"  Saved: {len(ind_rows):,} rows | {n_cos:,} companies -> {outfile_ind}")

    # -------------------------------------------------------------------
    # F2b: Re-compute same_industry flag now that we have fresh industry codes
    # -------------------------------------------------------------------
    # If we just created or updated the industry codes file, re-do the
    # same-industry flagging on the supplemented network.
    print("\n  Re-computing same_industry flag with fresh industry codes...")
    industry_fresh = pd.DataFrame(ind_rows, columns=ind_cols)
    industry_fresh["cik_int"] = pd.to_numeric(industry_fresh["cik"], errors="coerce")
    industry_fresh["sic2"] = industry_fresh["sic"].astype(str).str[:2]
    cik_to_sic2_fresh = dict(zip(industry_fresh["cik_int"], industry_fresh["sic2"]))

    all_edges["obs_sic2"] = (all_edges["observed_companyid"]
                              .map(cid_to_cik_int)
                              .map(cik_to_sic2_fresh))
    all_edges["port_sic2"] = all_edges["portfolio_cik"].map(cik_to_sic2_fresh)
    all_edges["same_industry"] = (
        (all_edges["obs_sic2"] == all_edges["port_sic2"]) &
        all_edges["obs_sic2"].notna() &
        all_edges["port_sic2"].notna()
    ).astype(int)
    all_edges = all_edges.drop(columns=["obs_sic2", "port_sic2"])

    # Re-save with updated same_industry flag
    all_edges.to_csv(outfile_supp, index=False)
    n_same = all_edges["same_industry"].sum()
    print(f"  Same-industry edges (updated): {n_same:,} of {len(all_edges):,}")
    print(f"  Re-saved -> {outfile_supp}")

# ---------------------------------------------------------------------
# F3: Identify new CIKs needing CRSP returns
# ---------------------------------------------------------------------
# The supplemented network may include public companies that were not in
# the original CIQ network. We need to pull CRSP returns for these
# new companies. This step identifies which CIKs are new.
print("\nF3: Identifying new CIKs needing CRSP returns...")

# Load existing CRSP crosswalk (from original CIQ network, built in Section F1
# or from a previous run of pull_panel_b_and_c.py)
existing_xwalk_file = os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv")
if os.path.exists(existing_xwalk_file):
    pxw = pd.read_csv(existing_xwalk_file)
    existing_ciks = set(pd.to_numeric(pxw["cik"], errors="coerce").dropna().astype(int))
else:
    existing_ciks = set()

# Find CIKs in the supplemented network not in existing CRSP data
all_supp_ciks = set(all_edges["portfolio_cik"].dropna().astype(int))
new_ciks = all_supp_ciks - existing_ciks

print(f"  Existing portfolio CIKs with CRSP crosswalk: {len(existing_ciks):,}")
print(f"  Total CIKs in supplemented network:          {len(all_supp_ciks):,}")
print(f"  New CIKs needing CRSP:                       {len(new_ciks):,}")

# Save new CIKs for the CRSP pull script
outfile_new = os.path.join(panel_c_dir, "new_ciks_for_crsp.csv")
pd.DataFrame({"cik": sorted(new_ciks)}).to_csv(outfile_new, index=False)
print(f"  Saved -> {outfile_new}")

# Close WRDS connection (opened in Section D2)
cur.close()
conn.close()
print("\n  WRDS connection closed.")


# =====================================================================
# =====================================================================
#
#   SECTION G: SUMMARY
#
#   Print comparison table: Original CIQ vs Supplemented network
#   (observers, companies, edges, same-industry edges, by source)
#
# =====================================================================
# =====================================================================

print(f"\n\n{'=' * 80}")
print("SECTION G: SUMMARY")
print("=" * 80)

# --- Table A summary ---
print(f"\n--- Table A: Company-Level Master ---")
print(f"  Total companies: {len(company_master):,}")
with_cik = sum(1 for r in company_master if r["cik"])
with_edgar = sum(1 for r in company_master if r["edgar_filing_date"])
with_fid = sum(1 for r in company_master if r["edgar_has_fiduciary_manner"] is True)
with_no_fid = sum(1 for r in company_master if r["edgar_has_no_fiduciary_duty"] is True)
print(f"  With CIK mapping:                {with_cik:,}")
print(f"  With EDGAR fiduciary coding:     {with_edgar:,}")
print(f"    Fiduciary manner (pre-2020):   {with_fid:,}")
print(f"    No fiduciary duty (post-2020): {with_no_fid:,}")

if company_master:
    avg_dirs = sum(r["n_directors"] for r in company_master) / len(company_master)
    avg_obs = sum(r["n_observers"] for r in company_master) / len(company_master)
    avg_adv = sum(r["n_advisory"] for r in company_master) / len(company_master)
    print(f"  Avg directors per company:  {avg_dirs:.1f}")
    print(f"  Avg observers per company:  {avg_obs:.1f}")
    print(f"  Avg advisory per company:   {avg_adv:.1f}")

# --- Table B summary ---
print(f"\n--- Table B: Observer Network ---")
if network_rows:
    unique_observers_b = len(set(r["observer_personid"] for r in network_rows))
    unique_vc_firms_b = len(set(r["vc_firm_companyid"] for r in network_rows))
    print(f"  Total network links:             {len(network_rows):,}")
    print(f"  Unique observers with VC affil:  {unique_observers_b:,}")
    print(f"  Unique VC/PE firms:              {unique_vc_firms_b:,}")

# --- Network comparison: Original CIQ vs Supplemented ---
print(f"\n--- Network Comparison: Original CIQ vs Supplemented ---")
print(f"\n  {'Metric':<40} {'Original CIQ':>14} {'Supplemented':>14} {'Change':>12}")
print(f"  {'-' * 80}")

o_obs = orig_std["observer_personid"].nunique()
s_obs = all_edges["observer_personid"].nunique()
print(f"  {'Unique observers':<40} {o_obs:>14,} {s_obs:>14,} {'+' + str(s_obs - o_obs):>12}")

o_observed = orig_std["observed_companyid"].nunique()
s_observed = all_edges["observed_companyid"].nunique()
print(f"  {'Unique observed companies':<40} {o_observed:>14,} {s_observed:>14,} {'+' + str(s_observed - o_observed):>12}")

o_port = orig_std["portfolio_cik"].nunique()
s_port = all_edges["portfolio_cik"].nunique()
print(f"  {'Unique portfolio CIKs':<40} {o_port:>14,} {s_port:>14,} {'+' + str(s_port - o_port):>12}")

o_edges = len(orig_std)
s_edges = len(all_edges)
print(f"  {'Total edges':<40} {o_edges:>14,} {s_edges:>14,} {'+' + str(s_edges - o_edges):>12}")

# Same-industry comparison
# For original CIQ edges, merge with the same_industry flag from all_edges
if "same_industry" in all_edges.columns:
    o_same_df = orig_std.merge(
        all_edges[["observer_personid", "observed_companyid",
                    "portfolio_cik", "same_industry"]].drop_duplicates(),
        on=["observer_personid", "observed_companyid", "portfolio_cik"],
        how="left"
    )
    o_same = int(o_same_df["same_industry"].sum())
    s_same = int(all_edges["same_industry"].sum())
    print(f"  {'Same-industry edges':<40} {o_same:>14,} {s_same:>14,} {'+' + str(s_same - o_same):>12}")

# --- Breakdown by data source ---
print(f"\n  By source:")
for src, n in all_edges["source"].value_counts().items():
    print(f"    {src:<20} {n:>8,} edges")

# --- Output files summary ---
print(f"\n--- Output Files ---")

output_files = [
    os.path.join(data_dir, "table_a_company_master.csv"),
    os.path.join(data_dir, "table_b_observer_network.csv"),
    os.path.join(panel_c_dir, "01_public_portfolio_companies.csv"),
    os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"),
    os.path.join(panel_c_dir, "02b_supplemented_network_edges.csv"),
    os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"),
    os.path.join(panel_c_dir, "05_industry_codes.csv"),
    os.path.join(panel_c_dir, "new_ciks_for_crsp.csv"),
]

for fp in output_files:
    if os.path.exists(fp):
        size_kb = os.path.getsize(fp) / 1024
        with open(fp, "r", encoding="utf-8") as fh:
            n_rows = sum(1 for _ in fh) - 1  # Subtract header
        fname = os.path.basename(fp)
        print(f"  {fname:<50} {n_rows:>8,} rows  ({size_kb:>8.0f} KB)")
    else:
        fname = os.path.basename(fp)
        print(f"  {fname:<50} {'NOT FOUND':>8}")

print(f"\n{'=' * 80}")
print("02_build_network.py COMPLETE")
print(f"{'=' * 80}")
