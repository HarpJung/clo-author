"""
01_pull_all_wrds_data.py
========================

CONSOLIDATED WRDS DATA PULL -- Board Observer Research Project
--------------------------------------------------------------

This script replaces 7 separate data-pull scripts with a single,
sequential pull from the WRDS PostgreSQL database. It opens one
connection and executes all queries needed to reconstruct the
project's raw data files from scratch.

SCRIPTS REPLACED:
  1. pull_ciq_raw_data.py          --> Section A (CIQ Professionals)
  2. pull_boardex_supplement.py     --> Section B (BoardEx)
  3. pull_form4_observer_trades.py  --> Section C (Form 4 Insider Trades)
  4. pull_panel_b_and_c.py          --> Section D (CRSP Returns, Compustat, IBES)
  5. pull_crsp_new_ciks.py          --> Section D (supplemental CRSP)
  6. pull_crsp_2025_v2.py           --> Section D (2025 CRSP returns)
  7. pull_ciq_all_events_v2.py      --> Section E (Expanded Events)

OUTPUT DIRECTORY STRUCTURE:
  Data/
  |-- CIQ_Extract/
  |   |-- 01_observer_records.csv
  |   |-- 02_advisory_board_records.csv
  |   |-- 03_directors_at_observer_companies.csv
  |   |-- 04_observer_company_details.csv
  |   |-- 05_observer_person_all_positions.csv
  |   |-- 06_observer_company_key_events.csv
  |   |-- 06d_observer_all_events_full.csv
  |   |-- 07_ciq_cik_crosswalk.csv
  |   |-- 08_observer_tr_insider_crosswalk.csv
  |-- BoardEx/
  |   |-- observer_boardex_crosswalk.csv
  |   |-- observer_boardex_positions.csv
  |   |-- observer_boardex_companies.csv
  |-- Form4/
  |   |-- observer_form4_trades.csv
  |   |-- observer_form4_derivatives.csv
  |-- Panel_C_Network/
  |   |-- 06_portfolio_crsp_daily.csv        (Section D)
  |-- Panel_B_Outcomes/
  |   |-- 02_compustat_annual.csv            (Section D)
  |   |-- 03_crsp_monthly_returns.csv        (Section D)
  |   |-- 04_ibes_consensus.csv              (Section D)

DEPENDENCY NOTE -- READ THIS BEFORE RUNNING:
  Sections A, B, C, and E can all run immediately. They pull raw data
  from WRDS using only the CIQ Professionals database as a starting point.

  Section D CANNOT run until you have executed 02_build_network.py,
  which constructs the observer-to-public-company network and produces
  the file Panel_C_Network/03_portfolio_permno_crosswalk.csv.  That file
  provides the list of PERMNOs whose CRSP returns we need.

  Recommended execution order:
    1. Run this script (Sections A-C and E will execute; Section D will
       skip with a warning if the crosswalk file is not found).
    2. Run 02_build_network.py to build the network.
    3. Re-run this script (Section D will now execute).

  Alternatively, you can comment/uncomment sections as needed.

RATE LIMITING:
  All queries that operate on large ID lists are batched (300-500 IDs
  per batch) with a 3-second sleep between batches to avoid overloading
  the WRDS PostgreSQL server.

WRDS CONNECTION:
  Host: wrds-pgdata.wharton.upenn.edu
  Port: 9737
  Database: wrds
  User: harperjung
"""

# =====================================================================
# IMPORTS AND GLOBAL CONFIGURATION
# =====================================================================
import psycopg2
import csv
import os
import time
import sys

# Force UTF-8 output even on Windows terminals that default to cp1252.
# The "replace" error handler prevents crashes on unencodable characters
# (e.g., accented names in CIQ person data).
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# --- Directory paths ---
# All output files land under this root directory.
# Subdirectories are created automatically if they do not exist.
data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"

ciq_dir    = os.path.join(data_dir, "CIQ_Extract")
boardex_dir = os.path.join(data_dir, "BoardEx")
form4_dir  = os.path.join(data_dir, "Form4")
panel_b_dir = os.path.join(data_dir, "Panel_B_Outcomes")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

for d in [ciq_dir, boardex_dir, form4_dir, panel_b_dir, panel_c_dir]:
    os.makedirs(d, exist_ok=True)


# =====================================================================
# DATABASE CONNECTION
# =====================================================================
# WRDS provides a PostgreSQL interface to all subscribed databases.
# The connection stays open for the entire script so we can reuse the
# cursor across all five sections.
print("=" * 80)
print("CONNECTING TO WRDS...")
print("=" * 80)

conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu",
    port=9737,
    dbname="wrds",
    user="harperjung",
    password="Wwjksnm9087yu!"
)
cur = conn.cursor()
print("  Connected successfully.\n")


# =====================================================================
# HELPER FUNCTION: save_csv
# =====================================================================
def save_csv(rows, columns, filepath):
    """
    Write query results to a CSV file.

    Parameters
    ----------
    rows : list of tuples
        Each tuple is one row returned by cur.fetchall().
    columns : list of str
        Column names (typically from cur.description).
    filepath : str
        Full path to the output CSV file.

    Returns
    -------
    None.  Prints a summary line showing the row count and filename.
    """
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    print(f"  Saved: {len(rows):,} rows -> {os.path.basename(filepath)}")


# #####################################################################
#
#  SECTION A: CIQ PROFESSIONALS DATA
#  (replaces pull_ciq_raw_data.py)
#
#  The Capital IQ (CIQ) Professionals database is the foundation of
#  the entire project. It tracks people and their professional roles
#  at companies. We start by finding everyone whose title contains
#  "observer" and who is flagged as a board-level professional
#  (boardflag = 1). From that seed set we fan out to pull their
#  companies, their other positions, company details, events, and
#  the CIQ-to-SEC-CIK crosswalk.
#
# #####################################################################
print("\n" + "#" * 80)
print("#  SECTION A: CIQ PROFESSIONALS DATA")
print("#" * 80)


# =====================================================================
# A1: OBSERVER RECORDS
# =====================================================================
#
# TABLE: ciq_pplintel.ciqprofessional (23 columns)
#   Each row is one person-company-title record. A single person can
#   have many rows (one per position at each company). Key columns:
#     proid         -- unique row ID for this position record
#     personid      -- unique person ID (shared across all positions)
#     companyid     -- CIQ company ID
#     title         -- free-text job title (e.g., "Board Observer",
#                      "Observer to the Board of Directors")
#     boardflag     -- 1 if this is a board-level role, 0 otherwise
#     proflag       -- 1 if this is an executive/professional role
#     currentproflag -- 1 if this is their current role
#     currentboardflag -- 1 if they are currently on the board
#     educatedflag  -- 1 if education data is available
#     specialty     -- area of expertise
#     prorank       -- CIQ ranking among professionals
#     boardrank     -- CIQ ranking among board members
#
# TABLE: ciq_pplintel.ciqperson (10 columns)
#   One row per person. Contains name fields:
#     personid, firstname, middlename, lastname, prefix, suffix, yearborn
#
# WHY: This is the seed query. Every other query in the script depends
#   on the person IDs and company IDs extracted from this result.
#
# OUTPUT: 01_observer_records.csv
# =====================================================================
print("\n" + "=" * 80)
print("A1: Pull observer records (people with 'observer' in title)")
print("=" * 80)
time.sleep(3)

cur.execute("""
    SELECT p.proid,
           p.personid,
           p.companyid,
           p.title,
           p.boardflag,
           p.proflag,
           p.currentproflag,
           p.currentboardflag,
           p.educatedflag,
           p.specialty,
           p.prorank,
           p.boardrank,
           per.firstname,
           per.middlename,
           per.lastname,
           per.prefix,
           per.suffix,
           per.yearborn
    FROM ciq_pplintel.ciqprofessional p
    LEFT JOIN ciq_pplintel.ciqperson per
        ON p.personid = per.personid
    WHERE LOWER(p.title) LIKE '%observer%'
      AND p.boardflag = 1
    ORDER BY p.personid, p.companyid
""")
a1_rows = cur.fetchall()
a1_cols = [d[0] for d in cur.description]
save_csv(a1_rows, a1_cols, os.path.join(ciq_dir, "01_observer_records.csv"))

# ---- Extract the two key ID sets used by every subsequent query ----
# observer_personids: the set of unique people who hold at least one
#   board observer role. Used for Sections A5, B, C.
# observer_companyids: the set of unique companies that have at least
#   one board observer. Used for Sections A2-A4, A6-A7, E.
observer_personids = sorted(set(
    str(int(r[1])) for r in a1_rows if r[1] is not None
))
observer_companyids = sorted(set(
    str(int(r[2])) for r in a1_rows if r[2] is not None
))
print(f"  Unique observer persons:   {len(observer_personids):,}")
print(f"  Unique observer companies: {len(observer_companyids):,}")


# =====================================================================
# A2: ADVISORY BOARD RECORDS
# =====================================================================
#
# TABLE: ciq_pplintel.ciqprofessional + ciq_pplintel.ciqperson
#   Same tables as A1, but different filter.
#
# FILTER: title LIKE '%advisory%' AND boardflag = 1
#   This captures people whose title includes "advisory" (e.g.,
#   "Advisory Board Member", "Member of Advisory Board") at the same
#   companies that have board observers.
#
# WHY: The paper studies the three-tier board architecture: directors,
#   advisors, and observers. This query identifies the advisory tier
#   at companies where we already know observers exist.
#
# OUTPUT: 02_advisory_board_records.csv
# =====================================================================
print("\n" + "=" * 80)
print("A2: Pull advisory board records at observer companies")
print("=" * 80)

batch_size = 500
a2_rows = []

for i in range(0, len(observer_companyids), batch_size):
    batch = observer_companyids[i:i + batch_size]
    cid_str = ", ".join(batch)
    time.sleep(3)

    cur.execute(f"""
        SELECT p.proid,
               p.personid,
               p.companyid,
               p.title,
               p.boardflag,
               p.proflag,
               p.currentproflag,
               p.currentboardflag,
               per.firstname,
               per.lastname
        FROM ciq_pplintel.ciqprofessional p
        LEFT JOIN ciq_pplintel.ciqperson per
            ON p.personid = per.personid
        WHERE p.companyid IN ({cid_str})
          AND LOWER(p.title) LIKE '%advisory%'
          AND p.boardflag = 1
        ORDER BY p.personid, p.companyid
    """)
    a2_rows.extend(cur.fetchall())
    print(f"  Batch {i // batch_size + 1}: {len(a2_rows):,} total rows so far")

a2_cols = [d[0] for d in cur.description]
save_csv(a2_rows, a2_cols, os.path.join(ciq_dir, "02_advisory_board_records.csv"))


# =====================================================================
# A3: ALL BOARD MEMBERS AT OBSERVER COMPANIES
# =====================================================================
#
# TABLE: ciq_pplintel.ciqprofessional + ciq_pplintel.ciqperson
#   Same tables, filtered to boardflag = 1 only (no title filter).
#
# FILTER: companyid IN (observer company IDs) AND boardflag = 1
#   This pulls every person with any board-level role (directors,
#   committee members, observers, advisory) at observer companies.
#
# WHY: We need the full board composition to:
#   a) Count total board size (for board-size controls)
#   b) Separate observers from directors in the three-tier architecture
#   c) Identify the formal directors whose voting power contrasts with
#      the observer's advisory-only role
#
# OUTPUT: 03_directors_at_observer_companies.csv
# =====================================================================
print("\n" + "=" * 80)
print("A3: Pull ALL board members at observer companies")
print("=" * 80)

batch_size = 500
a3_rows = []

for i in range(0, len(observer_companyids), batch_size):
    batch = observer_companyids[i:i + batch_size]
    cid_str = ", ".join(batch)
    time.sleep(3)

    cur.execute(f"""
        SELECT p.proid,
               p.personid,
               p.companyid,
               p.title,
               p.boardflag,
               p.proflag,
               p.currentproflag,
               p.currentboardflag,
               per.firstname,
               per.lastname
        FROM ciq_pplintel.ciqprofessional p
        LEFT JOIN ciq_pplintel.ciqperson per
            ON p.personid = per.personid
        WHERE p.companyid IN ({cid_str})
          AND p.boardflag = 1
        ORDER BY p.companyid, p.personid
    """)
    a3_rows.extend(cur.fetchall())
    print(f"  Batch {i // batch_size + 1}: {len(a3_rows):,} total rows so far")

a3_cols = [d[0] for d in cur.description]
save_csv(a3_rows, a3_cols, os.path.join(ciq_dir, "03_directors_at_observer_companies.csv"))


# =====================================================================
# A4: OBSERVER COMPANY DETAILS
# =====================================================================
#
# TABLES:
#   ciq_common.ciqcompany (23 columns)
#     Core company attributes. Key columns:
#       companyid            -- unique company identifier
#       companyname          -- legal name
#       companytypeid        -- FK to ciqcompanytype (Public, Private, etc.)
#       companystatustypeid  -- FK to ciqcompanystatustype (Operating, Defunct, etc.)
#       simpleindustryid     -- CIQ's simplified industry classification
#       yearfounded, monthfounded, dayfounded -- founding date components
#       city, zipcode, webpage, countryid, stateid
#
#   ciq_common.ciqcompanytype
#     Lookup table: companytypeid -> companytypename
#     Values include: "Public Company", "Private Company",
#       "Private Investment Firm", "Private Fund", etc.
#
#   ciq_common.ciqcompanystatustype
#     Lookup table: companystatustypeid -> companystatustypename
#     Values include: "Operating", "Operating Subsidiary",
#       "Target", "Defunct", etc.
#
#   ciq_common.ciqcountrygeo
#     Lookup table: countryid -> country, region, isocountry2
#     Provides human-readable country names and ISO codes.
#
# WHY: We need company-level attributes for sample characterization,
#   subsample splits (by company type, status, region, industry), and
#   to distinguish public from private companies in the network.
#
# OUTPUT: 04_observer_company_details.csv
# =====================================================================
print("\n" + "=" * 80)
print("A4: Pull observer company details")
print("=" * 80)

batch_size = 500
a4_rows = []

for i in range(0, len(observer_companyids), batch_size):
    batch = observer_companyids[i:i + batch_size]
    cid_str = ", ".join(batch)
    time.sleep(3)

    cur.execute(f"""
        SELECT c.companyid,
               c.companyname,
               ct.companytypename,
               cst.companystatustypename,
               c.yearfounded,
               c.monthfounded,
               c.dayfounded,
               c.city,
               c.zipcode,
               c.webpage,
               cg.country,
               cg.region,
               cg.isocountry2,
               c.simpleindustryid,
               c.companytypeid,
               c.companystatustypeid,
               c.countryid,
               c.stateid
        FROM ciq_common.ciqcompany c
        LEFT JOIN ciq_common.ciqcompanytype ct
            ON c.companytypeid = ct.companytypeid
        LEFT JOIN ciq_common.ciqcompanystatustype cst
            ON c.companystatustypeid = cst.companystatustypeid
        LEFT JOIN ciq_common.ciqcountrygeo cg
            ON c.countryid = cg.countryid
        WHERE c.companyid IN ({cid_str})
        ORDER BY c.companyid
    """)
    a4_rows.extend(cur.fetchall())
    print(f"  Batch {i // batch_size + 1}: {len(a4_rows):,} total rows so far")

a4_cols = [d[0] for d in cur.description]
save_csv(a4_rows, a4_cols, os.path.join(ciq_dir, "04_observer_company_details.csv"))


# =====================================================================
# A4b: FILTER TO US-ONLY COMPANIES
# =====================================================================
#
# The paper's institutional framework is entirely US-based (Reg FD, NVCA,
# Clayton Act, Obasi case). Non-US companies have different disclosure
# regimes and different observer roles (e.g., Norwegian "Employee
# Representative Observers" are mandatory labor governance roles, not
# VC-appointed observers). We restrict to US-only observed companies.
#
# This filter is applied to observer_companyids (the set of private
# companies with observers) and observer_personids (the set of observer
# persons). All subsequent queries use the filtered sets.
#
# Impact: drops ~33% of companies and ~17% of network edges.
#
print("\n--- A4b: Filtering to US-only companies ---")

# Build a set of US company IDs from the A4 results.
# The country column is at index position determined by the query.
# Find the country column index.
country_col_idx = a4_cols.index("country")
us_companyids_set = set()
for row in a4_rows:
    if row[country_col_idx] == "United States":
        cid = str(int(row[0]))  # companyid is first column
        us_companyids_set.add(cid)

before_cos = len(observer_companyids)
observer_companyids = sorted(us_companyids_set)
print(f"  Companies: {before_cos:,} -> {len(observer_companyids):,} (US only)")

# Also filter observer person IDs to those who observe at US companies
us_observer_pids = set()
for row in a1_rows:
    cid = str(int(row[2]))  # companyid
    if cid in us_companyids_set:
        us_observer_pids.add(str(int(row[1])))  # personid

before_pids = len(observer_personids)
observer_personids = sorted(us_observer_pids)
print(f"  Observer persons: {before_pids:,} -> {len(observer_personids):,} (US only)")


# =====================================================================
# A5: ALL POSITIONS HELD BY EACH OBSERVER PERSON
# =====================================================================
#
# TABLES:
#   ciq_pplintel.ciqprofessional -- the position-level table
#   ciq_common.ciqcompany        -- for the company name
#   ciq_common.ciqcompanytype    -- for the company type name
#   ciq_pplintel.ciqperson       -- for the person's name
#
# FILTER: personid IN (observer person IDs) -- no title filter, no
#   boardflag filter, no company filter. We want EVERY position this
#   person holds at ANY company, regardless of role.
#
# WHY: This is the CRITICAL file for building the observer network.
#   The research design relies on the fact that the same person who
#   observes at a private company also holds positions at public
#   companies (typically as a director or officer). By pulling ALL
#   positions we can identify:
#
#   a) Public Company positions -- the information bridge to public firms.
#      The observer attends private board meetings and also sits on
#      public boards, creating a channel for information flow.
#
#   b) VC/PE firm positions -- the observer's employer (typically
#      "Private Investment Firm", "Public Investment Firm", or
#      "Private Fund"). This identifies which VC/PE fund placed the
#      observer on the private company's board.
#
#   c) Other private company positions -- reveals the observer's full
#      network across the portfolio of the VC/PE fund.
#
#   The companytypename field is essential for distinguishing these
#   categories in downstream scripts (02_build_network.py,
#   build_unified_dataset.py).
#
# BATCH SIZE: 300 (smaller than other queries because each person can
#   have many positions, producing a large result set per batch).
#
# OUTPUT: 05_observer_person_all_positions.csv
# =====================================================================
print("\n" + "=" * 80)
print("A5: Pull ALL positions for each observer person (across all companies)")
print("=" * 80)

batch_size = 300
a5_rows = []
n_total_batches = (len(observer_personids) + batch_size - 1) // batch_size

for i in range(0, len(observer_personids), batch_size):
    batch = observer_personids[i:i + batch_size]
    pid_str = ", ".join(batch)
    time.sleep(3)

    cur.execute(f"""
        SELECT p.proid,
               p.personid,
               p.companyid,
               c.companyname,
               ct.companytypename,
               p.title,
               p.boardflag,
               p.proflag,
               p.currentproflag,
               p.currentboardflag,
               per.firstname,
               per.lastname
        FROM ciq_pplintel.ciqprofessional p
        LEFT JOIN ciq_common.ciqcompany c
            ON p.companyid = c.companyid
        LEFT JOIN ciq_common.ciqcompanytype ct
            ON c.companytypeid = ct.companytypeid
        LEFT JOIN ciq_pplintel.ciqperson per
            ON p.personid = per.personid
        WHERE p.personid IN ({pid_str})
        ORDER BY p.personid, p.companyid
    """)
    a5_rows.extend(cur.fetchall())
    n_batch = i // batch_size + 1
    print(f"  Batch {n_batch}/{n_total_batches}: {len(a5_rows):,} total rows so far")

a5_cols = [d[0] for d in cur.description]
save_csv(a5_rows, a5_cols, os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))


# =====================================================================
# A6: KEY EVENTS AT OBSERVER COMPANIES
# =====================================================================
#
# TABLE: ciq_keydev.wrds_keydev (23 columns)
#   This is a WRDS convenience view that pre-joins several CIQ Key
#   Developments tables:
#     - ciq_keydev.ciqkeydev          (the base event table)
#     - ciq_keydev.ciqkeydeveventtype (event type names)
#     - ciq_keydev.ciqkeydevtoobjectroletype (role names: Target, Buyer, etc.)
#     - ciq_keydev.ciqkeydevobjtocomp (links events to companies)
#   The result provides human-readable names for event types and
#   object roles, plus the Compustat GVKEY if available.
#
#   Key columns:
#     keydevid              -- unique event ID
#     companyid             -- CIQ company involved
#     companyname           -- company's name
#     headline              -- one-line event summary
#     keydeveventtypeid     -- numeric event type code
#     eventtype             -- human-readable event type (e.g., "M&A Transaction",
#                              "Earnings", "Product/Service", "Client Win")
#     keydevtoobjectroletypeid -- numeric role code
#     objectroletype        -- role the company plays (e.g., "Target", "Buyer")
#     announcedate          -- date the event was announced
#     announceddateutc      -- announcement timestamp in UTC
#     enterdate             -- when CIQ entered the event
#     mostimportantdateutc  -- the date CIQ considers most relevant
#     speffectivedate       -- S&P effective date (if applicable)
#     sptodate              -- S&P end date
#     gvkey                 -- Compustat GVKEY (if the company is in Compustat)
#     sourcetypename        -- data source (e.g., "Press Release", "SEC Filing")
#
# WHY: These events are the treatment in the information-permeability
#   tests. When a material event (M&A, earnings, financing) occurs at
#   the observed private company, we test whether the connected public
#   company's stock price responds. The event details (type, date,
#   company role) are used to construct event windows and to split
#   the sample by event importance.
#
# OUTPUT: 06_observer_company_key_events.csv
# =====================================================================
print("\n" + "=" * 80)
print("A6: Pull key events at observer companies (wrds_keydev)")
print("=" * 80)

batch_size = 500
a6_rows = []

for i in range(0, len(observer_companyids), batch_size):
    batch = observer_companyids[i:i + batch_size]
    cid_str = ", ".join(batch)
    time.sleep(3)

    try:
        cur.execute(f"""
            SELECT keydevid,
                   companyid,
                   companyname,
                   headline,
                   keydeveventtypeid,
                   eventtype,
                   keydevtoobjectroletypeid,
                   objectroletype,
                   announcedate,
                   announceddateutc,
                   enterdate,
                   mostimportantdateutc,
                   speffectivedate,
                   sptodate,
                   gvkey,
                   sourcetypename
            FROM ciq_keydev.wrds_keydev
            WHERE companyid IN ({cid_str})
            ORDER BY companyid, announcedate
        """)
        a6_rows.extend(cur.fetchall())
    except Exception as e:
        # Some batches may hit encoding or data issues. Log and continue
        # rather than aborting the entire pull.
        print(f"  ERROR in batch: {str(e)[:100]}")
        conn.rollback()

    print(f"  Batch {i // batch_size + 1}: {len(a6_rows):,} total rows so far")

a6_cols = [d[0] for d in cur.description]
save_csv(a6_rows, a6_cols, os.path.join(ciq_dir, "06_observer_company_key_events.csv"))


# =====================================================================
# A7: CIQ COMPANY ID -> SEC CIK CROSSWALK
# =====================================================================
#
# TABLE: ciq_common.wrds_cik
#   Maps CIQ company IDs to SEC Central Index Key (CIK) numbers.
#   Key columns:
#     companyid    -- CIQ company ID
#     cik          -- SEC CIK number
#     companyname  -- company name in the CIQ database
#     startdate    -- start of the CIK assignment period
#     enddate      -- end of the CIK assignment period (null if current)
#     primaryflag  -- 1 if this is the primary CIK for the company
#
# WHY: The CIK is the bridge between CIQ (which identifies companies)
#   and SEC-based databases (CRSP, Compustat, EDGAR). We need CIKs to:
#   a) Pull stock returns from CRSP (via CIK -> PERMNO mapping)
#   b) Pull financial data from Compustat (via CIK -> GVKEY mapping)
#   c) Match to Form 4 insider filings
#   d) Match to EDGAR filings (S-1, Form D, proxy statements)
#
# OUTPUT: 07_ciq_cik_crosswalk.csv
# =====================================================================
print("\n" + "=" * 80)
print("A7: Pull CIQ -> SEC CIK crosswalk")
print("=" * 80)

batch_size = 500
a7_rows = []

for i in range(0, len(observer_companyids), batch_size):
    batch = observer_companyids[i:i + batch_size]
    cid_str = ", ".join(batch)
    time.sleep(3)

    cur.execute(f"""
        SELECT companyid,
               cik,
               companyname,
               startdate,
               enddate,
               primaryflag
        FROM ciq_common.wrds_cik
        WHERE companyid IN ({cid_str})
        ORDER BY companyid
    """)
    a7_rows.extend(cur.fetchall())

a7_cols = [d[0] for d in cur.description]
save_csv(a7_rows, a7_cols, os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))

# Count how many observer companies have a primary CIK (i.e., filed with SEC)
primary_ciks = len(set(
    str(r[1]) for r in a7_rows if r[5] == 1 and r[1] is not None
))
print(f"  Companies with primary CIK: {primary_ciks:,} of {len(observer_companyids):,}")

# --- Section A summary ---
print(f"\n{'=' * 80}")
print("SECTION A COMPLETE: CIQ Professionals Data")
print(f"{'=' * 80}")
for fname in sorted(os.listdir(ciq_dir)):
    if fname.startswith("0") and fname.endswith(".csv"):
        fp = os.path.join(ciq_dir, fname)
        size_kb = os.path.getsize(fp) / 1024
        with open(fp, "r", encoding="utf-8") as fh:
            n_rows = sum(1 for _ in fh) - 1
        print(f"  {fname}: {n_rows:,} rows ({size_kb:.0f} KB)")


# #####################################################################
#
#  SECTION B: BOARDEX SUPPLEMENT
#  (replaces pull_boardex_supplement.py)
#
#  CIQ Professionals misses roughly half of all observer-to-public-firm
#  connections because CIQ does not always record every board seat a
#  person holds.  BoardEx is a comprehensive database of board members
#  and executives at public companies.  By matching our CIQ observers
#  to their BoardEx equivalents, we recover additional public-company
#  board seats that CIQ missed.
#
#  Pipeline:
#    B1. Match CIQ person IDs to BoardEx director IDs using WRDS crosswalk
#    B2. Pull ALL employment/board positions from BoardEx for matched people
#    B3. Pull company identifiers (CIK, ticker, ISIN) from BoardEx
#
# #####################################################################
print("\n\n" + "#" * 80)
print("#  SECTION B: BOARDEX SUPPLEMENT")
print("#" * 80)


# =====================================================================
# B1: CIQ -> BOARDEX PERSON CROSSWALK
# =====================================================================
#
# TABLE: wrdsapps_plink_boardex_ciq.boardex_ciq_link
#   WRDS-maintained crosswalk that matches CIQ person IDs to BoardEx
#   director IDs using name matching and security-level matching.
#   Key columns:
#     directorid   -- BoardEx's unique person identifier
#     directorname -- full name as it appears in BoardEx
#     forename1    -- first name in BoardEx
#     surname      -- last name in BoardEx
#     personid     -- CIQ person ID (our identifier)
#     firstname    -- first name in CIQ
#     lastname     -- last name in CIQ
#     score        -- match quality score (lower = better)
#     matchstyle   -- how the match was made ("name", "security", etc.)
#
# WHY: We need to translate our CIQ-based observer identifiers into
#   BoardEx identifiers so we can query BoardEx for their positions.
#   The crosswalk is probabilistic (name-based), so the score field
#   lets us filter on match quality in downstream analysis.
#
# NOTE: The personid column in this table is typed as numeric in
#   PostgreSQL, so we cast to text for the IN clause comparison.
#
# OUTPUT: BoardEx/observer_boardex_crosswalk.csv
# =====================================================================
print("\n" + "=" * 80)
print("B1: CIQ -> BoardEx person crosswalk")
print("=" * 80)

batch_size = 500
b1_rows = []

for i in range(0, len(observer_personids), batch_size):
    batch = observer_personids[i:i + batch_size]
    # Wrap each ID in quotes because personid is numeric in the table
    # and we are comparing via ::text cast.
    pid_str = ", ".join(f"'{p}'" for p in batch)
    time.sleep(3)

    try:
        cur.execute(f"""
            SELECT directorid,
                   directorname,
                   forename1,
                   surname,
                   personid,
                   firstname,
                   lastname,
                   score,
                   matchstyle
            FROM wrdsapps_plink_boardex_ciq.boardex_ciq_link
            WHERE personid::text IN ({pid_str})
        """)
        b1_rows.extend(cur.fetchall())
    except Exception as e:
        print(f"  ERROR in batch: {str(e)[:80]}")
        conn.rollback()

    print(f"  Batch {i // batch_size + 1}: {len(b1_rows):,} total matches so far")

b1_cols = ["directorid", "directorname", "forename1", "surname",
           "ciq_personid", "firstname", "lastname", "score", "matchstyle"]
save_csv(b1_rows, b1_cols, os.path.join(boardex_dir, "observer_boardex_crosswalk.csv"))

# Extract the set of matched BoardEx director IDs for the next query.
# Convert to int then to string to normalize any float formatting.
boardex_directorids = sorted(set(
    str(int(r[0])) for r in b1_rows if r[0] is not None
))
matched_ciq_persons = len(set(
    str(int(r[4])) for r in b1_rows if r[4] is not None
))
print(f"  Matched CIQ persons:     {matched_ciq_persons:,} of {len(observer_personids):,}")
print(f"  Unique BoardEx directors: {len(boardex_directorids):,}")


# =====================================================================
# B2: BOARDEX EMPLOYMENT/BOARD POSITIONS FOR MATCHED OBSERVERS
# =====================================================================
#
# TABLE: boardex.na_wrds_dir_profile_emp
#   Contains all employment and board positions for North American
#   directors tracked by BoardEx. Each row is one role at one company.
#   Total table size is ~8.5 million rows; we pull only for our matched
#   observer subset.
#
#   Key columns:
#     directorid    -- BoardEx person ID
#     directorname  -- person's full name
#     companyname   -- company name
#     companyid     -- BoardEx company ID (NOT the same as CIQ companyid)
#     rolename      -- position title (e.g., "Non-Executive Director",
#                      "Chairman", "CEO", "Board Observer")
#     datestartrole -- start date of this role
#     dateendrole   -- end date (null if current)
#     brdposition   -- "Yes" if this is a board-level position
#     ned           -- "Yes" if non-executive director
#     orgtype       -- organization type:
#                        "Quoted" or "Listed" = public company
#                        "Private" = private company
#                        "Government", "Charity", etc.
#     isin          -- ISIN identifier for the company
#     sector        -- industry sector
#     hocountryname -- country of headquarters
#     rowtype       -- "Board" or "Employment"
#
# WHY: This reveals public-company connections that CIQ missed. If a
#   person is in our observer set (from CIQ) and BoardEx shows them
#   serving on the board of a public company, that is an information
#   bridge we would otherwise not know about.
#
# BATCH SIZE: 300 (smaller because each director can have 50+ positions,
#   producing a large result set per batch).
#
# OUTPUT: BoardEx/observer_boardex_positions.csv
# =====================================================================
print("\n" + "=" * 80)
print("B2: Pull BoardEx positions for matched observers")
print("=" * 80)

batch_size = 300
b2_rows = []
n_total_batches = (len(boardex_directorids) + batch_size - 1) // batch_size

for i in range(0, len(boardex_directorids), batch_size):
    batch = boardex_directorids[i:i + batch_size]
    did_str = ", ".join(batch)
    time.sleep(3)

    try:
        cur.execute(f"""
            SELECT directorid,
                   directorname,
                   companyname,
                   companyid,
                   rolename,
                   datestartrole,
                   dateendrole,
                   brdposition,
                   ned,
                   orgtype,
                   isin,
                   sector,
                   hocountryname,
                   rowtype
            FROM boardex.na_wrds_dir_profile_emp
            WHERE directorid IN ({did_str})
            ORDER BY directorid, datestartrole
        """)
        b2_rows.extend(cur.fetchall())
    except Exception as e:
        print(f"  ERROR in batch: {str(e)[:80]}")
        conn.rollback()

    n_batch = i // batch_size + 1
    print(f"  Batch {n_batch}/{n_total_batches}: {len(b2_rows):,} total positions so far")

b2_cols = ["directorid", "directorname", "companyname", "companyid",
           "rolename", "datestartrole", "dateendrole",
           "brdposition", "ned", "orgtype", "isin", "sector",
           "hocountryname", "rowtype"]
save_csv(b2_rows, b2_cols, os.path.join(boardex_dir, "observer_boardex_positions.csv"))

# Extract BoardEx company IDs for the next query.
boardex_companyids = sorted(set(
    str(int(r[3])) for r in b2_rows if r[3] is not None
))
print(f"  Unique BoardEx companies in positions: {len(boardex_companyids):,}")


# =====================================================================
# B3: BOARDEX COMPANY IDENTIFIERS (CIK, TICKER, ISIN)
# =====================================================================
#
# TABLE: boardex.na_wrds_company_names
#   Company-level identifiers for BoardEx's North American companies.
#   Key columns:
#     boardid       -- BoardEx board-level ID
#     companyid     -- BoardEx company ID
#     boardname     -- company name
#     ticker        -- stock ticker symbol
#     isin          -- International Securities Identification Number
#     cikcode       -- SEC CIK number (the key we need for CRSP linking)
#     hocountryname -- country of headquarters
#
# WHY: BoardEx uses its own company ID system. To link BoardEx companies
#   to CRSP (for returns) and Compustat (for financials), we need the
#   SEC CIK code. This table provides CIK, ticker, and ISIN for
#   BoardEx companies.
#
# OUTPUT: BoardEx/observer_boardex_companies.csv
# =====================================================================
print("\n" + "=" * 80)
print("B3: Pull BoardEx company identifiers (CIK, ticker, ISIN)")
print("=" * 80)

batch_size = 300
b3_rows = []

for i in range(0, len(boardex_companyids), batch_size):
    batch = boardex_companyids[i:i + batch_size]
    cid_str = ", ".join(batch)
    time.sleep(3)

    try:
        cur.execute(f"""
            SELECT DISTINCT
                   boardid,
                   companyid,
                   boardname,
                   ticker,
                   isin,
                   cikcode,
                   hocountryname
            FROM boardex.na_wrds_company_names
            WHERE companyid IN ({cid_str})
        """)
        b3_rows.extend(cur.fetchall())
    except Exception as e:
        print(f"  ERROR in batch: {str(e)[:80]}")
        conn.rollback()

    print(f"  Batch {i // batch_size + 1}: {len(b3_rows):,} total rows so far")

b3_cols = ["boardid", "companyid", "boardname", "ticker",
           "isin", "cikcode", "hocountryname"]
save_csv(b3_rows, b3_cols, os.path.join(boardex_dir, "observer_boardex_companies.csv"))

# Count identifier coverage
n_with_cik = len(set(
    str(r[1]) for r in b3_rows if r[5] is not None and str(r[5]).strip()
))
print(f"  BoardEx companies with CIK:    {n_with_cik:,}")
print(f"  BoardEx companies total:       {len(boardex_companyids):,}")

# --- Section B summary ---
print(f"\n{'=' * 80}")
print("SECTION B COMPLETE: BoardEx Supplement")
print(f"{'=' * 80}")
for fname in sorted(os.listdir(boardex_dir)):
    if fname.endswith(".csv"):
        fp = os.path.join(boardex_dir, fname)
        size_kb = os.path.getsize(fp) / 1024
        with open(fp, "r", encoding="utf-8") as fh:
            n_rows = sum(1 for _ in fh) - 1
        print(f"  {fname}: {n_rows:,} rows ({size_kb:.0f} KB)")


# #####################################################################
#
#  SECTION C: FORM 4 INSIDER TRADES
#  (replaces pull_form4_observer_trades.py)
#
#  SEC Form 4 reports insider transactions (purchases, sales, grants)
#  by officers and directors at public companies. If an observer filed
#  a Form 4, it confirms they have a material connection to that public
#  company.  We use the WRDS TR-CIQ crosswalk to match our CIQ observer
#  person IDs to Thomson Reuters (TR) insider filing person IDs, then
#  pull their transactions.
#
#  Pipeline:
#    C1. Match CIQ person IDs to TR person IDs via WRDS crosswalk
#    C2. Pull non-derivative trades from tfn.v4_table1
#    C3. Pull derivative trades from tfn.v4_table2
#
# #####################################################################
print("\n\n" + "#" * 80)
print("#  SECTION C: FORM 4 INSIDER TRADES")
print("#" * 80)


# =====================================================================
# C1: CIQ -> THOMSON REUTERS INSIDER CROSSWALK
# =====================================================================
#
# TABLE: wrdsapps_plink_trinsider_ciq.trinsider_ciq_link
#   WRDS-maintained crosswalk matching CIQ person IDs to Thomson Reuters
#   insider filing person IDs.
#   Key columns:
#     tr_personid   -- TR's person identifier (used in tfn.table1/table2)
#     owner         -- insider owner code in TR system
#     ciq_personid  -- CIQ person ID (our identifier)
#     firstname     -- first name
#     middlename    -- middle name
#     lastname      -- last name
#     score         -- match quality score (lower = better match)
#     matchstyle    -- method used to match ("name", "security", etc.)
#
# WHY: The TR insider filing database uses its own person identifiers.
#   To look up our observers' trades, we first need to translate CIQ
#   person IDs into TR person IDs. This crosswalk provides that bridge.
#
# OUTPUT: CIQ_Extract/08_observer_tr_insider_crosswalk.csv
# =====================================================================
print("\n" + "=" * 80)
print("C1: CIQ -> Thomson Reuters insider crosswalk")
print("=" * 80)

batch_size = 500
c1_rows = []

for i in range(0, len(observer_personids), batch_size):
    batch = observer_personids[i:i + batch_size]
    pid_str = ", ".join(f"'{p}'" for p in batch)
    time.sleep(3)

    try:
        cur.execute(f"""
            SELECT tr_personid,
                   owner,
                   ciq_personid,
                   firstname,
                   middlename,
                   lastname,
                   score,
                   matchstyle
            FROM wrdsapps_plink_trinsider_ciq.trinsider_ciq_link
            WHERE ciq_personid::text IN ({pid_str})
        """)
        c1_rows.extend(cur.fetchall())
    except Exception as e:
        print(f"  ERROR in batch: {str(e)[:80]}")
        conn.rollback()

    print(f"  Batch {i // batch_size + 1}: {len(c1_rows):,} total matches so far")

c1_cols = ["tr_personid", "owner", "ciq_personid", "firstname",
           "middlename", "lastname", "score", "matchstyle"]
save_csv(c1_rows, c1_cols, os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))

# Extract TR person IDs for the trade queries.
tr_personids = sorted(set(
    str(int(r[0])) for r in c1_rows if r[0] is not None
))
matched_ciq_c1 = len(set(
    str(int(r[2])) for r in c1_rows if r[2] is not None
))
print(f"  Matched CIQ persons:  {matched_ciq_c1:,} of {len(observer_personids):,}")
print(f"  Unique TR person IDs: {len(tr_personids):,}")


# =====================================================================
# C2: NON-DERIVATIVE INSIDER TRADES (FORM 4 TABLE 1)
# =====================================================================
#
# TABLE: tfn.v4_table1 (also accessible as tfn.table1)
#   Non-derivative transactions reported on Form 4. Each row is one
#   transaction line from a Form 4 filing.
#
#   Key columns:
#     personid     -- TR person identifier
#     owner        -- insider owner code
#     secid        -- TR security identifier
#     ticker       -- stock ticker
#     cusip6       -- 6-digit CUSIP (issuer)
#     cusip2       -- 2-digit CUSIP extension (issue)
#     cname        -- company name
#     rolecode1    -- insider role (e.g., "D" = director, "O" = officer)
#     rolecode2    -- secondary role code
#     formtype     -- filing type ("4" = Form 4)
#     trancode     -- transaction code:
#                       P = Open-market purchase
#                       S = Open-market sale
#                       A = Grant/Award
#                       D = Sale back to issuer
#                       F = Payment of exercise price (tax)
#                       M = Exercise of derivative
#     acqdisp      -- "A" (acquisition) or "D" (disposition)
#     trandate     -- transaction date
#     tprice       -- transaction price per share
#     shares       -- number of shares transacted
#     sharesheld   -- shares held after transaction
#     ownership    -- "D" (direct) or "I" (indirect)
#     cleanse      -- data quality indicator
#     shares_adj   -- split-adjusted shares
#     tprice_adj   -- split-adjusted price
#     sectitle     -- security title
#     fdate        -- filing date
#     sigdate      -- signature date
#
# FILTER: transactioncode IN ('P','S','A','D','F','M')
#   These are the standard non-derivative transaction types. We exclude
#   codes like 'G' (gift) and 'J' (other) which are less informative.
#
# WHY: Form 4 trades provide direct evidence of an observer's economic
#   exposure to a public company. If the observer buys or sells stock,
#   they have skin in the game beyond their advisory role, and the
#   timing of trades relative to private-company events may reveal
#   information flows.
#
# OUTPUT: Form4/observer_form4_trades.csv
# =====================================================================
print("\n" + "=" * 80)
print("C2: Pull Form 4 non-derivative trades (tfn.v4_table1)")
print("=" * 80)

batch_size = 500
c2_rows = []
n_total_batches = (len(tr_personids) + batch_size - 1) // batch_size

for i in range(0, len(tr_personids), batch_size):
    batch = tr_personids[i:i + batch_size]
    pid_batch = ", ".join(batch)
    time.sleep(3)

    try:
        cur.execute(f"""
            SELECT personid, owner, secid, ticker, cusip6, cusip2, cname,
                   rolecode1, rolecode2, formtype, trancode, acqdisp,
                   trandate, tprice, shares, sharesheld, ownership,
                   cleanse, shares_adj, tprice_adj, sectitle,
                   fdate, sigdate
            FROM tfn.table1
            WHERE personid IN ({pid_batch})
              AND formtype = '4'
              AND trancode IN ('P', 'S', 'A', 'D', 'F', 'M')
            ORDER BY personid, trandate
        """)
        c2_rows.extend(cur.fetchall())
    except Exception as e:
        print(f"  ERROR in batch: {str(e)[:80]}")
        conn.rollback()

    n_batch = i // batch_size + 1
    print(f"  Batch {n_batch}/{n_total_batches}: {len(c2_rows):,} total trades so far")

c2_cols = ["personid", "owner", "secid", "ticker", "cusip6", "cusip2", "cname",
           "rolecode1", "rolecode2", "formtype", "trancode", "acqdisp",
           "trandate", "tprice", "shares", "sharesheld", "ownership",
           "cleanse", "shares_adj", "tprice_adj", "sectitle",
           "fdate", "sigdate"]
save_csv(c2_rows, c2_cols, os.path.join(form4_dir, "observer_form4_trades.csv"))


# =====================================================================
# C3: DERIVATIVE INSIDER TRADES (FORM 4 TABLE 2)
# =====================================================================
#
# TABLE: tfn.v4_table2 (also accessible as tfn.table2)
#   Derivative transactions reported on Form 4. These include stock
#   options, warrants, restricted stock units, and other derivative
#   securities.
#
#   Key columns (in addition to those shared with table1):
#     derivative  -- description of the derivative security
#     xdate       -- exercise/conversion date
#     xprice      -- exercise/conversion price
#     sprice      -- underlying security price at transaction
#     derivheld   -- number of derivatives held after transaction
#
# WHY: Derivative transactions complement non-derivative trades in
#   showing the observer's total economic exposure. Options exercises
#   and RSU vestings are particularly informative because they involve
#   active decisions about timing.
#
# OUTPUT: Form4/observer_form4_derivatives.csv
# =====================================================================
print("\n" + "=" * 80)
print("C3: Pull Form 4 derivative trades (tfn.v4_table2)")
print("=" * 80)

batch_size = 500
c3_rows = []
n_total_batches = (len(tr_personids) + batch_size - 1) // batch_size

for i in range(0, len(tr_personids), batch_size):
    batch = tr_personids[i:i + batch_size]
    pid_batch = ", ".join(batch)
    time.sleep(3)

    try:
        cur.execute(f"""
            SELECT personid, owner, secid, ticker, cusip6, cusip2, cname,
                   rolecode1, rolecode2, formtype, trancode, acqdisp,
                   trandate, derivative, xdate, shares, xprice, sprice,
                   derivheld, ownership, cleanse, shares_adj,
                   fdate, sigdate
            FROM tfn.table2
            WHERE personid IN ({pid_batch})
              AND formtype = '4'
            ORDER BY personid, trandate
        """)
        c3_rows.extend(cur.fetchall())
    except Exception as e:
        print(f"  ERROR in batch: {str(e)[:80]}")
        conn.rollback()

    n_batch = i // batch_size + 1
    print(f"  Batch {n_batch}/{n_total_batches}: {len(c3_rows):,} total derivative trades so far")

c3_cols = ["personid", "owner", "secid", "ticker", "cusip6", "cusip2", "cname",
           "rolecode1", "rolecode2", "formtype", "trancode", "acqdisp",
           "trandate", "derivative", "xdate", "shares", "xprice", "sprice",
           "derivheld", "ownership", "cleanse", "shares_adj",
           "fdate", "sigdate"]
save_csv(c3_rows, c3_cols, os.path.join(form4_dir, "observer_form4_derivatives.csv"))

# --- Section C summary ---
print(f"\n{'=' * 80}")
print("SECTION C COMPLETE: Form 4 Insider Trades")
print(f"{'=' * 80}")
for fname in sorted(os.listdir(form4_dir)):
    if fname.endswith(".csv"):
        fp = os.path.join(form4_dir, fname)
        size_kb = os.path.getsize(fp) / 1024
        with open(fp, "r", encoding="utf-8") as fh:
            n_rows = sum(1 for _ in fh) - 1
        print(f"  {fname}: {n_rows:,} rows ({size_kb:.0f} KB)")


# #####################################################################
#
#  SECTION D: CRSP RETURNS, COMPUSTAT, AND IBES
#  (replaces pull_panel_b_and_c.py Panel B + pull_crsp_new_ciks.py
#   + pull_crsp_2025_v2.py)
#
#  +------------------------------------------------------------------+
#  | DEPENDENCY: This section requires 02_build_network.py to have    |
#  | already been run. That script builds the observer-to-public-     |
#  | company network and produces:                                    |
#  |   Panel_C_Network/03_portfolio_permno_crosswalk.csv              |
#  | which contains the list of PERMNOs whose CRSP returns we need.   |
#  |                                                                  |
#  | If that file is not found, Section D will SKIP with a warning.   |
#  | Run 02_build_network.py first, then re-run this script to        |
#  | execute Section D.                                               |
#  +------------------------------------------------------------------+
#
#  What Section D does:
#    D1. Read the PERMNO crosswalk produced by 02_build_network.py
#    D2. Pull CRSP daily returns (crsp_a_stock.dsf) for 2015-2024
#    D3. Pull CRSP daily returns (crsp.dsf_v2) for 2025 and append
#    D4. Pull Compustat annual (comp.funda), CRSP monthly (crsp_a_stock.msf),
#        and IBES consensus for Panel B companies (observer companies with CIKs)
#
# #####################################################################
print("\n\n" + "#" * 80)
print("#  SECTION D: CRSP RETURNS, COMPUSTAT, AND IBES")
print("#" * 80)

# --- Check for the dependency file ---
permno_crosswalk_path = os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv")
section_d_ready = os.path.exists(permno_crosswalk_path)

if not section_d_ready:
    print("\n  *** SKIPPING SECTION D ***")
    print(f"  Required file not found: {permno_crosswalk_path}")
    print("  Run 02_build_network.py first to identify portfolio companies,")
    print("  then come back and run this script again to execute Section D.")
    print("  Sections A, B, C, and E do not depend on 02_build_network.py")
    print("  and have already completed above.")
else:
    # =================================================================
    # D1: READ PERMNO CROSSWALK FROM 02_build_network.py OUTPUT
    # =================================================================
    #
    # The crosswalk file maps:  CIK -> GVKEY -> PERMNO
    # for every public company connected to an observer through the
    # network.  PERMNO is CRSP's unique security identifier.
    #
    print("\n" + "=" * 80)
    print("D1: Read portfolio PERMNO crosswalk")
    print("=" * 80)

    portfolio_permnos = set()
    portfolio_gvkeys = set()
    portfolio_ciks = set()

    with open(permno_crosswalk_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            p = row.get("permno", "").strip()
            g = row.get("gvkey", "").strip()
            c = row.get("cik", "").strip()
            if p:
                portfolio_permnos.add(int(float(p)))
            if g:
                portfolio_gvkeys.add(g)
            if c:
                portfolio_ciks.add(int(float(c)))

    portfolio_permnos = sorted(portfolio_permnos)
    portfolio_gvkeys = sorted(portfolio_gvkeys)
    portfolio_ciks = sorted(portfolio_ciks)

    print(f"  Portfolio PERMNOs: {len(portfolio_permnos):,}")
    print(f"  Portfolio GVKEYs:  {len(portfolio_gvkeys):,}")
    print(f"  Portfolio CIKs:    {len(portfolio_ciks):,}")

    # =================================================================
    # D2: CRSP DAILY RETURNS 2015-2024 (crsp_a_stock.dsf)
    # =================================================================
    #
    # TABLE: crsp_a_stock.dsf (CRSP Daily Stock File)
    #   Contains one row per security per trading day.
    #   Key columns:
    #     permno -- CRSP permanent security identifier
    #     date   -- trading date
    #     ret    -- holding-period return (includes dividends)
    #     prc    -- closing price (negative if bid/ask average)
    #     vol    -- trading volume (shares)
    #
    # WHY: Daily returns are used to compute cumulative abnormal returns
    #   (CARs) in event windows around private-company events. The
    #   market-model residual over a [-1, +3] or [-5, +5] window
    #   around an event date measures the stock price reaction at the
    #   connected public company.
    #
    # DATE RANGE: 2015-01-01 to 2024-12-31 (end of the legacy dsf table).
    #   2025 data uses the new dsf_v2 schema (see D3).
    #
    # OUTPUT: Panel_C_Network/06_portfolio_crsp_daily.csv
    # =================================================================
    print("\n" + "=" * 80)
    print("D2: Pull CRSP daily returns 2015-2024 (crsp_a_stock.dsf)")
    print("=" * 80)

    batch_size = 300
    d2_rows = []
    n_total_batches = (len(portfolio_permnos) + batch_size - 1) // batch_size

    for i in range(0, len(portfolio_permnos), batch_size):
        batch = portfolio_permnos[i:i + batch_size]
        p_str = ", ".join(str(p) for p in batch)
        time.sleep(3)

        try:
            cur.execute(f"""
                SELECT permno, date, ret, prc, vol
                FROM crsp_a_stock.dsf
                WHERE permno IN ({p_str})
                  AND date >= '2015-01-01'
                  AND date <= '2024-12-31'
                ORDER BY permno, date
            """)
            d2_rows.extend(cur.fetchall())
        except Exception as e:
            print(f"  ERROR in batch: {str(e)[:80]}")
            conn.rollback()

        n_batch = i // batch_size + 1
        print(f"  Batch {n_batch}/{n_total_batches}: {len(d2_rows):,} total rows so far")

    d2_cols = ["permno", "date", "ret", "prc", "vol"]
    crsp_daily_path = os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv")
    save_csv(d2_rows, d2_cols, crsp_daily_path)

    # =================================================================
    # D3: CRSP DAILY RETURNS 2025+ (crsp.dsf_v2)
    # =================================================================
    #
    # TABLE: crsp.dsf_v2 (CRSP Daily Stock File v2)
    #   CRSP migrated to a new schema starting in 2025. The column names
    #   are different from the legacy dsf table:
    #     dlycaldt -- calendar date (was "date")
    #     dlyret   -- daily return (was "ret")
    #     dlyprc   -- daily price (was "prc")
    #     dlyvol   -- daily volume (was "vol")
    #
    #   We alias these columns back to the legacy names so the output
    #   CSV has consistent column names.
    #
    # WHY: Some events in our sample may extend into 2025, so we need
    #   returns through the most recent available date.
    #
    # APPEND: We append 2025 rows to the same output file started in D2,
    #   so the final file covers the full 2015-2025 date range.
    #
    # OUTPUT: Appended to Panel_C_Network/06_portfolio_crsp_daily.csv
    # =================================================================
    print("\n" + "=" * 80)
    print("D3: Pull CRSP daily returns 2025+ (crsp.dsf_v2, new schema)")
    print("=" * 80)

    batch_size = 300
    d3_rows = []
    n_total_batches = (len(portfolio_permnos) + batch_size - 1) // batch_size

    for i in range(0, len(portfolio_permnos), batch_size):
        batch = portfolio_permnos[i:i + batch_size]
        p_str = ", ".join(str(p) for p in batch)
        time.sleep(3)

        try:
            # Alias the v2 column names to match the legacy schema
            # so the output CSV has consistent headers.
            cur.execute(f"""
                SELECT permno,
                       dlycaldt AS date,
                       dlyret   AS ret,
                       dlyprc   AS prc,
                       dlyvol   AS vol
                FROM crsp.dsf_v2
                WHERE permno IN ({p_str})
                  AND dlycaldt >= '2025-01-01'
                ORDER BY permno, dlycaldt
            """)
            d3_rows.extend(cur.fetchall())
        except Exception as e:
            print(f"  ERROR in batch: {str(e)[:80]}")
            conn.rollback()

        n_batch = i // batch_size + 1
        print(f"  Batch {n_batch}/{n_total_batches}: {len(d3_rows):,} total 2025 rows so far")

    # Append 2025 rows to the existing daily returns file (no header row).
    if d3_rows:
        with open(crsp_daily_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(d3_rows)
        print(f"  Appended {len(d3_rows):,} rows (2025) to 06_portfolio_crsp_daily.csv")
        print(f"  Combined total: {len(d2_rows) + len(d3_rows):,} daily return observations")
    else:
        print("  No 2025 data found (may not yet be available).")

    # =================================================================
    # D4: PANEL B DATA -- COMPUSTAT, CRSP MONTHLY, AND IBES
    # =================================================================
    #
    # Panel B covers the observer companies themselves (not the connected
    # public portfolio companies). Some observer companies have SEC CIKs
    # because they filed Form D, S-1, or other documents. For those
    # companies we can pull financial data.
    #
    # Sub-steps:
    #   D4a. Build CIK -> GVKEY -> PERMNO crosswalk for observer companies
    #   D4b. Pull Compustat annual fundamentals
    #   D4c. Pull CRSP monthly returns
    #   D4d. Pull IBES analyst consensus forecasts
    #
    # The CIKs come from Section A7 (07_ciq_cik_crosswalk.csv), which
    # was already saved to disk.
    # =================================================================
    print("\n" + "=" * 80)
    print("D4: Panel B data -- Compustat, CRSP monthly, IBES for observer companies")
    print("=" * 80)

    # Load the CIK crosswalk saved in Section A7.
    panel_b_ciks = set()
    a7_path = os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv")
    with open(a7_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cik_val = row.get("cik", "").strip()
            if cik_val:
                try:
                    panel_b_ciks.add(int(float(cik_val)))
                except ValueError:
                    pass

    panel_b_ciks = sorted(panel_b_ciks)
    print(f"  Observer companies with CIK: {len(panel_b_ciks):,}")

    if panel_b_ciks:
        cik_str_b = ", ".join(str(c) for c in panel_b_ciks)

        # --- D4a: CIK -> GVKEY -> PERMNO crosswalk ---
        #
        # TABLE: crsp_a_ccm.ccmxpf_lnkhist (CRSP-Compustat Merged link history)
        #   Links Compustat GVKEYs to CRSP PERMNOs.
        #     gvkey    -- Compustat company identifier
        #     lpermno  -- CRSP PERMNO (linked permanent security number)
        #     linktype -- link classification:
        #                   LU = "link used by CRSP"
        #                   LC = "link confirmed by CRSP"
        #     linkprim -- link priority: P = primary, C = primary candidate
        #     linkdt   -- start date of the link
        #     linkenddt -- end date of the link (null if current)
        #
        # TABLE: comp.company
        #   Compustat company header. Join on gvkey, filter on CIK.
        #
        print("\n  --- D4a: Building CIK -> PERMNO -> GVKEY crosswalk ---")
        time.sleep(3)

        cur.execute(f"""
            SELECT DISTINCT b.cik, a.gvkey, a.lpermno AS permno,
                   a.linkdt, a.linkenddt, a.linktype, a.linkprim
            FROM crsp_a_ccm.ccmxpf_lnkhist a
            JOIN comp.company b ON a.gvkey = b.gvkey
            WHERE CAST(b.cik AS BIGINT) IN ({cik_str_b})
              AND a.linktype IN ('LU', 'LC')
              AND a.linkprim IN ('P', 'C')
            ORDER BY b.cik, a.gvkey
        """)
        d4a_rows = cur.fetchall()
        d4a_cols = [d[0] for d in cur.description]
        save_csv(d4a_rows, d4a_cols, os.path.join(panel_b_dir, "01_identifier_crosswalk.csv"))

        # Extract PERMNO and GVKEY lists for subsequent queries.
        panelb_permnos = sorted(set(int(r[2]) for r in d4a_rows if r[2]))
        panelb_gvkeys = sorted(set(r[1] for r in d4a_rows if r[1]))
        print(f"    Matched PERMNOs: {len(panelb_permnos):,}")
        print(f"    Matched GVKEYs:  {len(panelb_gvkeys):,}")

        # --- D4b: Compustat annual fundamentals ---
        #
        # TABLE: comp.funda (Compustat Fundamentals Annual)
        #   Standard Compustat annual data. Filters:
        #     indfmt = 'INDL'  -- industrial format
        #     datafmt = 'STD'  -- standardized data
        #     popsrc = 'D'     -- domestic
        #     consol = 'C'     -- consolidated statements
        #
        #   Variables:
        #     at     -- total assets
        #     lt     -- total liabilities
        #     seq    -- stockholders' equity (total)
        #     ceq    -- common equity
        #     csho   -- common shares outstanding
        #     prcc_f -- price close (fiscal year-end)
        #     sale   -- net sales/revenue
        #     revt   -- total revenue
        #     ni     -- net income (loss)
        #     ib     -- income before extraordinary items
        #     act    -- current assets
        #     lct    -- current liabilities
        #     che    -- cash and short-term investments
        #     dltt   -- long-term debt
        #     dlc    -- debt in current liabilities
        #     oancf  -- operating cash flow
        #     capx   -- capital expenditures
        #     dp     -- depreciation
        #     xrd    -- R&D expense
        #
        if panelb_gvkeys:
            print("\n  --- D4b: Compustat annual fundamentals ---")
            time.sleep(3)

            gvkey_str_b = ", ".join(f"'{g}'" for g in panelb_gvkeys)
            cur.execute(f"""
                SELECT gvkey, datadate, fyear, tic, conm, cik,
                       at, lt, seq, ceq, csho, prcc_f,
                       sale, revt, ni, ib,
                       act, lct, che, dltt, dlc,
                       oancf, capx, dp, xrd
                FROM comp.funda
                WHERE gvkey IN ({gvkey_str_b})
                  AND indfmt = 'INDL'
                  AND datafmt = 'STD'
                  AND popsrc = 'D'
                  AND consol = 'C'
                  AND datadate >= '2015-01-01'
                ORDER BY gvkey, datadate
            """)
            d4b_rows = cur.fetchall()
            d4b_cols = [d[0] for d in cur.description]
            save_csv(d4b_rows, d4b_cols, os.path.join(panel_b_dir, "02_compustat_annual.csv"))

        # --- D4c: CRSP monthly returns ---
        #
        # TABLE: crsp_a_stock.msf (CRSP Monthly Stock File)
        #   Monthly returns for securities identified by PERMNO.
        #   Key columns:
        #     permno -- security identifier
        #     date   -- month-end date
        #     ret    -- monthly holding-period return
        #     retx   -- return excluding dividends
        #     shrout -- shares outstanding (thousands)
        #     prc    -- month-end price
        #     vol    -- monthly volume
        #
        if panelb_permnos:
            print("\n  --- D4c: CRSP monthly returns ---")
            time.sleep(3)

            permno_str_b = ", ".join(str(p) for p in panelb_permnos)
            cur.execute(f"""
                SELECT permno, date, ret, retx, shrout, prc, vol
                FROM crsp_a_stock.msf
                WHERE permno IN ({permno_str_b})
                  AND date >= '2015-01-01'
                ORDER BY permno, date
            """)
            d4c_rows = cur.fetchall()
            d4c_cols = [d[0] for d in cur.description]
            save_csv(d4c_rows, d4c_cols, os.path.join(panel_b_dir, "03_crsp_monthly_returns.csv"))

        # --- D4d: IBES analyst consensus forecasts ---
        #
        # TABLES:
        #   ibes.idsum       -- maps IBES tickers to CUSIPs
        #   comp.security    -- Compustat security table (has CUSIP)
        #   ibes.statsumu_epsus -- consensus EPS estimates
        #
        # The linkage is: GVKEY -> Compustat CUSIP -> IBES CUSIP -> IBES ticker
        # Then we pull consensus EPS estimates using the IBES ticker.
        #
        # Key columns in statsumu_epsus:
        #   ticker   -- IBES ticker
        #   statpers -- statistics period (date of the consensus)
        #   fpedats  -- fiscal period end date
        #   measure  -- forecast measure ("EPS")
        #   fpi      -- fiscal period indicator: "1" = current FY, "2" = next FY
        #   numest   -- number of analysts contributing
        #   medest   -- median estimate
        #   meanest  -- mean estimate
        #   stdev    -- standard deviation of estimates
        #
        if panelb_gvkeys:
            print("\n  --- D4d: IBES analyst consensus ---")
            time.sleep(3)

            # Step 1: Map Compustat GVKEYs to IBES tickers via CUSIP matching.
            cur.execute(f"""
                SELECT DISTINCT a.ticker, b.gvkey
                FROM ibes.idsum a
                JOIN comp.security c ON a.cusip = SUBSTRING(c.cusip, 1, 8)
                JOIN comp.company b ON c.gvkey = b.gvkey
                WHERE b.gvkey IN ({gvkey_str_b})
            """)
            ticker_links = cur.fetchall()
            ibes_tickers = sorted(set(r[0] for r in ticker_links if r[0]))
            print(f"    IBES tickers matched: {len(ibes_tickers):,}")

            if ibes_tickers:
                ticker_str = ", ".join(f"'{t}'" for t in ibes_tickers)
                time.sleep(3)

                # Step 2: Pull consensus EPS estimates.
                cur.execute(f"""
                    SELECT ticker, statpers, fpedats, measure, fpi,
                           numest, medest, meanest, stdev
                    FROM ibes.statsumu_epsus
                    WHERE ticker IN ({ticker_str})
                      AND statpers >= '2015-01-01'
                      AND fpi IN ('1', '2')
                      AND measure = 'EPS'
                    ORDER BY ticker, statpers
                """)
                d4d_rows = cur.fetchall()
                d4d_cols = [d[0] for d in cur.description]
                save_csv(d4d_rows, d4d_cols, os.path.join(panel_b_dir, "04_ibes_consensus.csv"))

    # --- Section D summary ---
    print(f"\n{'=' * 80}")
    print("SECTION D COMPLETE: CRSP Returns, Compustat, IBES")
    print(f"{'=' * 80}")
    for subdir_name, subdir_path in [("Panel_B_Outcomes", panel_b_dir),
                                      ("Panel_C_Network", panel_c_dir)]:
        if os.path.isdir(subdir_path):
            for fname in sorted(os.listdir(subdir_path)):
                if fname.endswith(".csv"):
                    fp = os.path.join(subdir_path, fname)
                    size_kb = os.path.getsize(fp) / 1024
                    with open(fp, "r", encoding="utf-8") as fh:
                        n_rows = sum(1 for _ in fh) - 1
                    print(f"  {subdir_name}/{fname}: {n_rows:,} rows ({size_kb:.0f} KB)")


# #####################################################################
#
#  SECTION E: EXPANDED EVENTS (ALL EVENT TYPES)
#  (replaces pull_ciq_all_events_v2.py)
#
#  Section A6 pulled events from wrds_keydev for observer companies,
#  but that was a standard set of columns. This section pulls ALL
#  events with NO event type filter, producing the full 400K+ event
#  dataset used for the expanded event-study analysis.
#
#  The difference from A6:
#    - A6 uses the full set of default columns from wrds_keydev
#    - E1 uses a targeted column set optimized for the event study,
#      including the "announcetime" field (intraday timing)
#    - E1 has NO event type filter -- it pulls every event at every
#      observer company, regardless of type
#
# #####################################################################
print("\n\n" + "#" * 80)
print("#  SECTION E: EXPANDED EVENTS (ALL EVENT TYPES)")
print("#" * 80)


# =====================================================================
# E1: ALL EVENTS FROM wrds_keydev FOR OBSERVER COMPANIES
# =====================================================================
#
# TABLE: ciq_keydev.wrds_keydev
#   Same WRDS convenience view used in A6.
#
# FILTER: companyid IN (observer company IDs) -- NO event type filter.
#   This pulls every event type: M&A, earnings, financing, product,
#   legal, regulatory, personnel changes, client wins, and more.
#
# COLUMNS:
#   keydevid              -- unique event ID
#   companyid             -- CIQ company involved
#   companyname           -- company name
#   headline              -- one-line event summary
#   keydeveventtypeid     -- numeric event type code
#   eventtype             -- human-readable event type name
#   keydevtoobjectroletypeid -- numeric role code
#   objectroletype        -- company's role (Target, Buyer, Seller, etc.)
#   announcedate          -- announcement date
#   announcetime          -- intraday announcement time (for timing analysis)
#   gvkey                 -- Compustat GVKEY (for firms in Compustat)
#   sourcetypename        -- data source
#
# BATCH SIZE: 300 (smaller than other queries because the unfiltered
#   event pull returns many more rows per company -- some companies
#   have 500+ events spanning their full history).
#
# EXPECTED SIZE: ~400,000+ events total across all observer companies.
#
# WHY: The expanded event set is used for robustness tests that examine
#   information flow across ALL event types, not just the high-profile
#   ones (M&A, earnings). This tests whether even minor events at the
#   observed private company leak to the connected public company.
#
# OUTPUT: CIQ_Extract/06d_observer_all_events_full.csv
# =====================================================================
print("\n" + "=" * 80)
print("E1: Pull ALL events from wrds_keydev (no type filter)")
print("=" * 80)

batch_size = 300
e1_rows = []
n_total_batches = (len(observer_companyids) + batch_size - 1) // batch_size

for i in range(0, len(observer_companyids), batch_size):
    batch = observer_companyids[i:i + batch_size]
    cid_str = ", ".join(batch)
    time.sleep(3)

    try:
        cur.execute(f"""
            SELECT keydevid,
                   companyid,
                   companyname,
                   headline,
                   keydeveventtypeid,
                   eventtype,
                   keydevtoobjectroletypeid,
                   objectroletype,
                   announcedate,
                   announcetime,
                   gvkey,
                   sourcetypename
            FROM ciq_keydev.wrds_keydev
            WHERE companyid IN ({cid_str})
            ORDER BY companyid, announcedate
        """)
        e1_rows.extend(cur.fetchall())
    except Exception as e:
        print(f"  ERROR in batch: {str(e)[:80]}")
        conn.rollback()

    n_batch = i // batch_size + 1
    print(f"  Batch {n_batch}/{n_total_batches}: {len(e1_rows):,} total events so far")

e1_cols = ["keydevid", "companyid", "companyname", "headline",
           "keydeveventtypeid", "eventtype",
           "keydevtoobjectroletypeid", "objectroletype",
           "announcedate", "announcetime",
           "gvkey", "sourcetypename"]
save_csv(e1_rows, e1_cols, os.path.join(ciq_dir, "06d_observer_all_events_full.csv"))

# --- Section E summary ---
print(f"\n{'=' * 80}")
print("SECTION E COMPLETE: Expanded Events")
print(f"{'=' * 80}")
print(f"  Total events: {len(e1_rows):,}")
n_unique_events = len(set(r[0] for r in e1_rows if r[0] is not None))
n_unique_companies = len(set(r[1] for r in e1_rows if r[1] is not None))
print(f"  Unique event IDs: {n_unique_events:,}")
print(f"  Unique companies: {n_unique_companies:,}")


# #####################################################################
#
#  FINAL SUMMARY
#
# #####################################################################
print(f"\n\n{'#' * 80}")
print("#  ALL SECTIONS COMPLETE")
print(f"{'#' * 80}")

print("\nOutput files by directory:\n")

for label, dirpath in [("CIQ_Extract", ciq_dir),
                        ("BoardEx", boardex_dir),
                        ("Form4", form4_dir),
                        ("Panel_B_Outcomes", panel_b_dir),
                        ("Panel_C_Network", panel_c_dir)]:
    if os.path.isdir(dirpath):
        csv_files = [f for f in sorted(os.listdir(dirpath)) if f.endswith(".csv")]
        if csv_files:
            print(f"  {label}/")
            for fname in csv_files:
                fp = os.path.join(dirpath, fname)
                size_kb = os.path.getsize(fp) / 1024
                try:
                    with open(fp, "r", encoding="utf-8") as fh:
                        n_rows = sum(1 for _ in fh) - 1
                except Exception:
                    n_rows = -1
                print(f"    {fname}: {n_rows:,} rows ({size_kb:.0f} KB)")

if not section_d_ready:
    print("\n  NOTE: Section D was skipped. Run 02_build_network.py, then re-run this script.")

# =====================================================================
# CLOSE CONNECTION
# =====================================================================
cur.close()
conn.close()
print("\nWRDS connection closed. Done.")
