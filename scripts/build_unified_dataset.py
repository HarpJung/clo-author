"""
Build unified company-level and observer-level datasets.

=== PURPOSE ===

This is the FIRST assembly script in the pipeline. It takes raw data
extracted from CIQ (S&P Capital IQ) and EDGAR (SEC filings) and builds
two foundational tables used by all downstream scripts:

  Table A (table_a_company_master.csv):
    Company-level master with board composition, EDGAR fiduciary language,
    NVCA treatment coding, and event counts. One row per observed company.

  Table B (table_b_observer_network.csv):
    Observer-level network linking each observer to the private companies
    they observe at and the VC/PE firms they work for. This is the input
    for pull_panel_b_and_c.py, which extends it by finding each observer's
    public company connections.

=== DATA FLOW ===

Raw CIQ pulls:                    Raw EDGAR pulls:
  01_observer_records.csv           exhibit_analysis_results.csv
  02_advisory_board_records.csv     s1_body_observer_analysis.csv
  03_directors_at_observer_cos.csv  efts_board_observer_s1_hits.csv
  04_observer_company_details.csv
  05_observer_person_all_positions.csv
  06_observer_company_key_events.csv
  07_ciq_cik_crosswalk.csv
            |                               |
            +---------- THIS SCRIPT ---------+
            |                               |
            v                               v
  table_a_company_master.csv      table_b_observer_network.csv
            |                               |
            v                               v
   (used for sample description)   (input to pull_panel_b_and_c.py)

=== INPUT FILES ===

From CIQ (Data/CIQ_Extract/):
  01_observer_records.csv         -- People with "observer" in their CIQ title.
                                     Each row = one person-company-title record.
                                     Fields: personid, firstname, lastname, companyid,
                                     companyname, title, currentboardflag

  02_advisory_board_records.csv   -- People with "advisory" board roles at
                                     companies that also have observers.

  03_directors_at_observer_companies.csv -- All directors/officers at companies
                                     that have at least one observer. Used to
                                     compute board composition statistics.

  04_observer_company_details.csv -- Company-level attributes for all companies
                                     with observers (type, status, year founded,
                                     location).

  05_observer_person_all_positions.csv -- ALL positions held by each observer
                                     person across ALL companies (not just the
                                     observed company). This is how we find
                                     VC/PE affiliations and portfolio company
                                     connections. Fields: personid, companyid,
                                     companyname, companytypename, title,
                                     currentproflag.
                                     Originally pulled from CIQ Professionals
                                     via WRDS.

  06_observer_company_key_events.csv -- Material events (M&A, bankruptcy,
                                     exec changes, etc.) at observed companies.
                                     From CIQ Key Developments.

  07_ciq_cik_crosswalk.csv        -- Maps CIQ company IDs to SEC CIK numbers.
                                     From ciq_common.wrds_cik on WRDS.

From EDGAR (Data/EDGAR_Extract/):
  exhibit_analysis_results.csv    -- Parsed IRA exhibits from S-1 filings.
                                     Flags whether the exhibit contains
                                     "fiduciary manner" or "no fiduciary duty"
                                     language for observer provisions.

  s1_body_observer_analysis.csv   -- Same analysis on the S-1 body text
                                     (for companies whose IRA was in the body
                                     rather than a separate exhibit).

  efts_board_observer_s1_hits.csv -- EDGAR full-text search results for
                                     "board observer" in S-1 filings. Gives
                                     CIKs of companies that mention observers.

=== OUTPUT FILES ===

  Data/table_a_company_master.csv  -- Company-level master (one row per company)
  Data/table_b_observer_network.csv -- Observer network (one row per observer-VC link)
"""

import csv, os, re
from collections import defaultdict, Counter

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
edgar_dir = os.path.join(data_dir, "EDGAR_Extract")
out_dir = data_dir  # save unified datasets in the root Data folder


def load_csv(filepath):
    """Load a CSV file into a list of dictionaries (one dict per row)."""
    with open(filepath, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


print("Loading source files...")

# =====================================================================
# LOAD ALL SOURCE DATA
# =====================================================================
observers = load_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
advisory = load_csv(os.path.join(ciq_dir, "02_advisory_board_records.csv"))
directors = load_csv(os.path.join(ciq_dir, "03_directors_at_observer_companies.csv"))
companies = load_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
network = load_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
events = load_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))

exhibits = load_csv(os.path.join(edgar_dir, "exhibit_analysis_results.csv"))
s1_body = load_csv(os.path.join(edgar_dir, "s1_body_observer_analysis.csv"))
efts_hits = load_csv(os.path.join(edgar_dir, "efts_board_observer_s1_hits.csv"))

print(f"  Observers: {len(observers):,}")
print(f"  Advisory: {len(advisory):,}")
print(f"  Directors: {len(directors):,}")
print(f"  Companies: {len(companies):,}")
print(f"  Network positions: {len(network):,}")
print(f"  Events: {len(events):,}")
print(f"  Exhibits: {len(exhibits):,}")
print(f"  S-1 body: {len(s1_body):,}")


def normalize_id(val):
    """Clean up CIQ IDs that pandas may have stored as floats (e.g., '12345.0' -> '12345')."""
    if not val:
        return ""
    val = str(val).strip()
    if val.endswith(".0"):
        val = val[:-2]
    return val


# Normalize all person and company IDs across datasets
for dataset in [observers, advisory, directors, companies, network, events]:
    for r in dataset:
        if "companyid" in r:
            r["companyid"] = normalize_id(r["companyid"])
        if "personid" in r:
            r["personid"] = normalize_id(r["personid"])

print("\nNormalized all companyid/personid values (removed .0 suffix)")


# =====================================================================
# STEP 1: BUILD COMPANY-LEVEL AGGREGATES FROM CIQ
# =====================================================================
# Group observers, directors, advisory members, and events by company
# so we can create per-company statistics for Table A.
print("\nStep 1: Building company-level aggregates...")

# Observer counts per company
obs_by_company = defaultdict(list)
for r in observers:
    obs_by_company[r["companyid"]].append(r)

# Director counts per company.
# IMPORTANT: We exclude observers and advisory board members from the
# director count. CIQ sometimes classifies observers under "directors"
# because they attend board meetings. We want pure directors only.
dir_by_company = defaultdict(list)
for r in directors:
    title_lower = r["title"].lower()
    is_observer = "observer" in title_lower
    is_advisory = "advisory" in title_lower or "board advisor" in title_lower
    if not is_observer and not is_advisory:
        dir_by_company[r["companyid"]].append(r)

# Advisory board counts per company (only at companies that also have observers)
adv_by_company = defaultdict(list)
observer_companyids = set(r["companyid"] for r in observers)
for r in advisory:
    if r["companyid"] in observer_companyids:
        adv_by_company[r["companyid"]].append(r)

# Event counts per company, broken down by event type
event_by_company = defaultdict(lambda: defaultdict(int))
for r in events:
    event_by_company[r["companyid"]][r["keydeveventtypename"]] += 1


# =====================================================================
# STEP 2: BUILD CIK CROSSWALK
# =====================================================================
# Load the CIQ company ID -> SEC CIK mapping.
# This was pulled from WRDS (ciq_common.wrds_cik) in an earlier script.
# Not all CIQ companies have CIKs (many are private and never filed with SEC).
print("Step 2: Loading CIQ-CIK crosswalk...")

cik_crosswalk_file = os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv")
cik_map = {}  # companyid -> cik

if os.path.exists(cik_crosswalk_file):
    for r in load_csv(cik_crosswalk_file):
        if r.get("primaryflag", "1") == "1":  # Only use primary CIK
            cik_map[normalize_id(r["companyid"])] = r["cik"]
    print(f"  Loaded {len(cik_map):,} CIK mappings from cache")
else:
    print(f"  CIK crosswalk not yet downloaded - will create placeholder")


# =====================================================================
# STEP 3: BUILD EDGAR FIDUCIARY LANGUAGE LOOKUP
# =====================================================================
# Parse S-1 filing exhibits and body text to determine whether each
# company's IRA contains fiduciary language for observers.
#
# Two key flags:
#   has_fiduciary_manner: True if the IRA says observers should
#       "act in a fiduciary manner" (pre-2020 NVCA language)
#   has_no_fiduciary_duty: True if the IRA explicitly disclaims
#       fiduciary duty for observers (post-2020 NVCA language)
#
# These flags are used to code the NVCA 2020 treatment variable.
print("Step 3: Building EDGAR fiduciary language lookup...")

edgar_fiduciary = {}  # cik -> dict of fiduciary flags
for r in exhibits:
    cik = r.get("cik", "")
    if not cik:
        continue
    date = r.get("file_date", "")
    has_fid = r.get("has_fiduciary_manner", "") == "True"
    has_no_fid = r.get("has_no_fiduciary_duty", "") == "True"
    obs_mentions = int(r.get("observer_mentions", 0))

    # Keep the record with the most observer mentions (most informative)
    if cik not in edgar_fiduciary or obs_mentions > edgar_fiduciary[cik].get("observer_mentions", 0):
        edgar_fiduciary[cik] = {
            "edgar_filing_date": date,
            "edgar_has_fiduciary_manner": has_fid,
            "edgar_has_no_fiduciary_duty": has_no_fid,
            "edgar_observer_mentions": obs_mentions,
            "edgar_source": "exhibit",
        }

# Supplement from S-1 body for companies not covered by exhibits.
# Some S-1 filings include the IRA in the body text rather than as
# a separate exhibit.
for r in s1_body:
    cik = r.get("cik", "")
    if not cik or cik in edgar_fiduciary:
        continue
    if r.get("fetch_status", "") != "ok":
        continue
    has_fid = r.get("has_fiduciary_manner", "") == "True"
    has_no_fid = r.get("has_no_fiduciary_duty", "") == "True"
    obs_mentions = int(r.get("observer_mentions", 0))
    edgar_fiduciary[cik] = {
        "edgar_filing_date": r.get("file_date", ""),
        "edgar_has_fiduciary_manner": has_fid,
        "edgar_has_no_fiduciary_duty": has_no_fid,
        "edgar_observer_mentions": obs_mentions,
        "edgar_source": "s1_body",
    }

# EFTS hit CIKs: companies with any "board observer" mention in S-1
# (from EDGAR full-text search). This is a broader flag than the
# fiduciary coding above.
efts_ciks = set()
for h in efts_hits:
    if h.get("ciks"):
        for cik in h["ciks"].split("|"):
            efts_ciks.add(cik.strip())

print(f"  EDGAR fiduciary coding: {len(edgar_fiduciary):,} companies")
print(f"  EFTS S-1 observer mentions: {len(efts_ciks):,} unique CIKs")


# =====================================================================
# STEP 4: ASSEMBLE COMPANY-LEVEL MASTER (Table A)
# =====================================================================
# Combine all the above into a single company-level dataset.
# Each row = one observed company with:
#   - Company attributes (name, type, status, location)
#   - Board composition (n_directors, n_observers, n_advisory, ratios)
#   - CIK and EDGAR fiduciary language flags
#   - NVCA 2020 treatment coding (pre/post fiduciary language removal)
#   - Event counts by type
print("\nStep 4: Assembling company-level master...")

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
    total_board = n_directors + n_observers + n_advisory
    observer_ratio = n_observers / total_board if total_board > 0 else 0

    # --- CIK mapping ---
    cik = cik_map.get(cid, "")

    # --- EDGAR fiduciary language (requires CIK) ---
    edgar_data = edgar_fiduciary.get(cik, {})
    has_s1_observer = cik in efts_ciks if cik else False

    # --- NVCA 2020 treatment coding ---
    # If we have an EDGAR filing date, use it to determine whether the
    # company's IRA was drafted before or after the 2020 NVCA template change.
    # Filings from 2021+ are likely based on the post-2020 template.
    filing_date = edgar_data.get("edgar_filing_date", "")
    if filing_date:
        filing_year = int(filing_date[:4]) if filing_date[:4].isdigit() else 0
        nvca_post2020 = 1 if filing_year >= 2021 else 0
        nvca_transition = 1 if filing_year == 2020 else 0
    else:
        nvca_post2020 = ""
        nvca_transition = ""

    # --- Event counts by type ---
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
        # Events
        "n_exec_board_changes": evt.get("Executive/Board Changes - Other", 0),
        "n_lawsuits": evt.get("Lawsuits & Legal Issues", 0),
        "n_restatements": evt.get("Restatements of Operating Results", 0),
        "n_earnings_announcements": evt.get("Announcements of Earnings", 0),
        "n_financing_events": evt.get("Seeking Financing/Partners", 0),
        "n_bankruptcy": evt.get("Bankruptcy - Other", 0),
    }
    company_master.append(row)

# Save Table A
outfile_a = os.path.join(out_dir, "table_a_company_master.csv")
with open(outfile_a, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=company_master[0].keys())
    writer.writeheader()
    writer.writerows(company_master)

print(f"  Table A saved: {len(company_master):,} rows -> {outfile_a}")


# =====================================================================
# STEP 5: BUILD OBSERVER NETWORK (Table B)
# =====================================================================
# For each observer, identify their VC/PE firm affiliation and create
# network links.
#
# The logic uses the 05_observer_person_all_positions.csv file, which
# lists ALL positions held by each observer across ALL companies.
# We classify each company into:
#   - VC/PE firm: companytypename in {"Private Investment Firm",
#     "Public Investment Firm", "Private Fund"}
#   - Portfolio company: observer holds a "Director" or "Board" title there
#
# Then for each observer, we create rows linking:
#   observer -> observed private company -> VC firm
#
# This table is the input to pull_panel_b_and_c.py, which extends it
# by finding each observer's public company connections and pulling
# CRSP returns.
print("\nStep 5: Building observer network...")

observer_persons = set(r["personid"] for r in observers)

person_vc_firms = defaultdict(set)    # personid -> set of (vc_companyid, vc_name)
person_portfolio = defaultdict(set)   # personid -> set of (portfolio_companyid, portfolio_name)
person_observed = defaultdict(set)    # personid -> set of (observed_companyid, observed_name)

# Build observed companies per person (from observer records)
for r in observers:
    person_observed[r["personid"]].add((r["companyid"], ""))

# Classify each position in the all-positions file.
# For each observer's positions:
#   - If the company is a VC/PE firm -> record as the observer's VC affiliation
#   - If the observer holds a director/board title -> record as a portfolio company
vc_pe_types = {"Private Investment Firm", "Public Investment Firm", "Private Fund"}

for r in network:
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
        person_portfolio[pid].add((cid, cname))

# Build network table: one row per (observer, observed company, VC firm) triple.
# This captures the chain: Observer X observes at Private Co A via VC Firm Y.
network_rows = []
for pid in observer_persons:
    observed = person_observed.get(pid, set())
    vc_firms = person_vc_firms.get(pid, set())
    portfolio = person_portfolio.get(pid, set())

    # Get observer name from the observer records
    obs_records = [r for r in observers if r["personid"] == pid]
    if not obs_records:
        continue
    obs_name = f"{obs_records[0]['firstname']} {obs_records[0]['lastname']}"

    # Create one row for each (observed company, VC firm) combination
    for obs_cid, _ in observed:
        obs_company_name = next(
            (co["companyname"] for co in companies if co["companyid"] == obs_cid),
            ""
        )

        for vc_cid, vc_name in vc_firms:
            row = {
                "observer_personid": pid,
                "observer_name": obs_name,
                "observed_companyid": obs_cid,
                "observed_companyname": obs_company_name,
                "vc_firm_companyid": vc_cid,
                "vc_firm_name": vc_name,
                "n_portfolio_companies": len(portfolio),
                "observer_title": obs_records[0].get("title", ""),
                "is_current_observer": obs_records[0].get("currentboardflag", "") == "1.0",
            }
            network_rows.append(row)

# Save Table B
outfile_b = os.path.join(out_dir, "table_b_observer_network.csv")
if network_rows:
    with open(outfile_b, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=network_rows[0].keys())
        writer.writeheader()
        writer.writerows(network_rows)

print(f"  Table B saved: {len(network_rows):,} rows -> {outfile_b}")


# =====================================================================
# SUMMARY STATISTICS
# =====================================================================
print(f"\n{'='*60}")
print(f"UNIFIED DATASET SUMMARY")
print(f"{'='*60}")

print(f"\nTable A: Company-Level Master")
print(f"  Total companies: {len(company_master):,}")
with_cik = sum(1 for r in company_master if r["cik"])
with_edgar = sum(1 for r in company_master if r["edgar_filing_date"])
with_fid = sum(1 for r in company_master if r["edgar_has_fiduciary_manner"] == True)
with_no_fid = sum(1 for r in company_master if r["edgar_has_no_fiduciary_duty"] == True)
print(f"  With CIK mapping: {with_cik:,}")
print(f"  With EDGAR fiduciary coding: {with_edgar:,}")
print(f"    Fiduciary manner (pre-2020): {with_fid}")
print(f"    No fiduciary duty (post-2020): {with_no_fid}")

avg_dirs = sum(r["n_directors"] for r in company_master) / len(company_master)
avg_obs = sum(r["n_observers"] for r in company_master) / len(company_master)
avg_adv = sum(r["n_advisory"] for r in company_master) / len(company_master)
print(f"  Avg directors per company: {avg_dirs:.1f}")
print(f"  Avg observers per company: {avg_obs:.1f}")
print(f"  Avg advisory members per company: {avg_adv:.1f}")

print(f"\nTable B: Observer Network")
unique_observers = len(set(r["observer_personid"] for r in network_rows))
unique_vc_firms = len(set(r["vc_firm_companyid"] for r in network_rows))
print(f"  Total network links: {len(network_rows):,}")
print(f"  Unique observers with VC affiliation: {unique_observers:,}")
print(f"  Unique VC/PE firms: {unique_vc_firms:,}")
