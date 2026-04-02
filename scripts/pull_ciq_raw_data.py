"""
Pull all raw CIQ data from WRDS to regenerate files 01-07.

=== PURPOSE ===

This script reconstructs ALL raw input files for the board observer
research project from scratch using WRDS SQL queries. It replaces the
original interactive pulls that were done without a saved script.

Run this to reproduce the full data pipeline from the source.

=== OUTPUT FILES (Data/CIQ_Extract/) ===

  01_observer_records.csv              -- People with "observer" in their CIQ title
  02_advisory_board_records.csv        -- Advisory board members at observer companies
  03_directors_at_observer_companies.csv -- All board members at observer companies
  04_observer_company_details.csv      -- Company attributes for observer companies
  05_observer_person_all_positions.csv -- ALL positions held by each observer person
  06_observer_company_key_events.csv   -- Material events at observer companies
  07_ciq_cik_crosswalk.csv             -- CIQ company ID -> SEC CIK mapping

=== WRDS TABLES USED ===

  ciq_pplintel.ciqprofessional (23 cols)
    proid, personid, companyid, title, boardflag, proflag,
    currentproflag, currentboardflag, educatedflag, specialty,
    prorank, boardrank, city, stateid, countryid, zipcode, ...

  ciq_pplintel.ciqperson (10 cols)
    personid, firstname, middlename, lastname, prefix, suffix,
    salutation, emailaddress, yearborn, phonevalue

  ciq_common.ciqcompany (23 cols)
    companyid, companyname, companytypeid, companystatustypeid,
    simpleindustryid, yearfounded, monthfounded, dayfounded,
    city, zipcode, webpage, countryid, stateid, ...

  ciq_common.ciqcompanytype         -- companytypeid -> companytypename
  ciq_common.ciqcompanystatustype   -- companystatustypeid -> companystatustypename
  ciq_common.ciqcountrygeo          -- countryid -> country, region, isocountry2
  ciq_common.wrds_cik               -- companyid -> cik, primaryflag

  ciq_keydev.wrds_keydev (23 cols)  -- WRDS convenience view joining events with types
    keydevid, companyid, companyname, headline, situation,
    keydeveventtypeid, eventtype, keydevtoobjectroletypeid, objectroletype,
    announcedate, announcedatetimezone, announceddateutc,
    enterdate, lastmodifieddate, mostimportantdateutc,
    speffectivedate, sptodate, gvkey, sourcetypename
"""

import psycopg2, csv, os, time, sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# =====================================================================
# CONFIGURATION
# =====================================================================
data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
os.makedirs(ciq_dir, exist_ok=True)

conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu",
    port=9737,
    dbname="wrds",
    user="harperjung",
    password="Wwjksnm9087yu!"
)
cur = conn.cursor()


def save_csv(rows, columns, filepath):
    """Save query results to CSV."""
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    print(f"  Saved: {len(rows):,} rows -> {os.path.basename(filepath)}")


# =====================================================================
# STEP 1: Pull observer records (01_observer_records.csv)
# =====================================================================
# Find all people in CIQ Professionals whose title contains "observer"
# and who have boardflag = 1 (indicating a board-level role).
#
# Join to ciqperson for name details (firstname, lastname, etc.).
# The original file had: proid, personid, companyid, title, boardflag,
#   currentproflag, currentboardflag, firstname, lastname
#
# NEW columns added: proflag, specialty, prorank, boardrank,
#   middlename, prefix, suffix, yearborn, educatedflag
print("=" * 80)
print("STEP 1: Pull observer records")
print("=" * 80)
time.sleep(3)

cur.execute("""
    SELECT p.proid, p.personid, p.companyid,
           p.title, p.boardflag, p.proflag,
           p.currentproflag, p.currentboardflag,
           p.educatedflag,
           p.specialty, p.prorank, p.boardrank,
           per.firstname, per.middlename, per.lastname,
           per.prefix, per.suffix, per.yearborn
    FROM ciq_pplintel.ciqprofessional p
    LEFT JOIN ciq_pplintel.ciqperson per ON p.personid = per.personid
    WHERE LOWER(p.title) LIKE '%observer%'
    AND p.boardflag = 1
    ORDER BY p.personid, p.companyid
""")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
save_csv(rows, cols, os.path.join(ciq_dir, "01_observer_records.csv"))

# Extract unique observer person IDs and company IDs for subsequent queries
observer_personids = sorted(set(str(int(r[1])) for r in rows if r[1]))
observer_companyids = sorted(set(str(int(r[2])) for r in rows if r[2]))
print(f"  Unique observer persons: {len(observer_personids):,}")
print(f"  Unique observer companies: {len(observer_companyids):,}")


# =====================================================================
# STEP 2: Pull advisory board records (02_advisory_board_records.csv)
# =====================================================================
# Find all people with "advisory" in their title at companies that
# also have board observers. This captures the advisory tier of the
# three-tier board architecture.
print("\n" + "=" * 80)
print("STEP 2: Pull advisory board records")
print("=" * 80)

batch_size = 500
adv_rows = []
for i in range(0, len(observer_companyids), batch_size):
    batch = observer_companyids[i:i+batch_size]
    cid_str = ", ".join(batch)
    time.sleep(2)
    cur.execute(f"""
        SELECT p.proid, p.personid, p.companyid,
               p.title, p.boardflag, p.proflag,
               p.currentproflag, p.currentboardflag,
               per.firstname, per.lastname
        FROM ciq_pplintel.ciqprofessional p
        LEFT JOIN ciq_pplintel.ciqperson per ON p.personid = per.personid
        WHERE p.companyid IN ({cid_str})
        AND LOWER(p.title) LIKE '%advisory%'
        AND p.boardflag = 1
        ORDER BY p.personid, p.companyid
    """)
    adv_rows.extend(cur.fetchall())
    print(f"  Batch {i//batch_size + 1}: {len(adv_rows):,} total")

cols = [d[0] for d in cur.description]
save_csv(adv_rows, cols, os.path.join(ciq_dir, "02_advisory_board_records.csv"))


# =====================================================================
# STEP 3: Pull directors at observer companies
#         (03_directors_at_observer_companies.csv)
# =====================================================================
# Pull ALL board-level professional records at companies that have
# at least one observer. Includes directors, officers, observers,
# and advisory members. The build_unified_dataset.py script later
# separates observers and advisory from pure directors.
print("\n" + "=" * 80)
print("STEP 3: Pull all board members at observer companies")
print("=" * 80)

dir_rows = []
for i in range(0, len(observer_companyids), batch_size):
    batch = observer_companyids[i:i+batch_size]
    cid_str = ", ".join(batch)
    time.sleep(3)
    cur.execute(f"""
        SELECT p.proid, p.personid, p.companyid,
               p.title, p.boardflag, p.proflag,
               p.currentproflag, p.currentboardflag,
               per.firstname, per.lastname
        FROM ciq_pplintel.ciqprofessional p
        LEFT JOIN ciq_pplintel.ciqperson per ON p.personid = per.personid
        WHERE p.companyid IN ({cid_str})
        AND p.boardflag = 1
        ORDER BY p.companyid, p.personid
    """)
    dir_rows.extend(cur.fetchall())
    print(f"  Batch {i//batch_size + 1}: {len(dir_rows):,} total")

cols = [d[0] for d in cur.description]
save_csv(dir_rows, cols, os.path.join(ciq_dir, "03_directors_at_observer_companies.csv"))


# =====================================================================
# STEP 4: Pull observer company details
#         (04_observer_company_details.csv)
# =====================================================================
# Get company-level attributes for every company with a board observer.
# Join to lookup tables for human-readable names.
#
# NEW columns vs original: simpleindustryid (CIQ industry code),
#   monthfounded, dayfounded, countryid, stateid, region, isocountry2
print("\n" + "=" * 80)
print("STEP 4: Pull observer company details")
print("=" * 80)

comp_rows = []
for i in range(0, len(observer_companyids), batch_size):
    batch = observer_companyids[i:i+batch_size]
    cid_str = ", ".join(batch)
    time.sleep(3)
    cur.execute(f"""
        SELECT c.companyid, c.companyname,
               ct.companytypename,
               cst.companystatustypename,
               c.yearfounded, c.monthfounded, c.dayfounded,
               c.city, c.zipcode, c.webpage,
               cg.country, cg.region, cg.isocountry2,
               c.simpleindustryid,
               c.companytypeid, c.companystatustypeid,
               c.countryid, c.stateid
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
    comp_rows.extend(cur.fetchall())
    print(f"  Batch {i//batch_size + 1}: {len(comp_rows):,} total")

cols = [d[0] for d in cur.description]
save_csv(comp_rows, cols, os.path.join(ciq_dir, "04_observer_company_details.csv"))


# =====================================================================
# STEP 5: Pull ALL positions held by each observer person
#         (05_observer_person_all_positions.csv)
# =====================================================================
# For each observer, get EVERY position they hold at ANY company.
# This is the critical file for building the network in
# build_unified_dataset.py and pull_panel_b_and_c.py.
#
# Join to ciqcompany for company type so we can identify:
#   - Public Company positions (the information bridge to public firms)
#   - Private Investment Firm / Public Investment Firm / Private Fund
#     positions (the observer's VC/PE affiliation)
print("\n" + "=" * 80)
print("STEP 5: Pull ALL positions for each observer person")
print("=" * 80)

pos_rows = []
batch_size = 300
for i in range(0, len(observer_personids), batch_size):
    batch = observer_personids[i:i+batch_size]
    pid_str = ", ".join(batch)
    time.sleep(3)
    cur.execute(f"""
        SELECT p.proid, p.personid, p.companyid,
               c.companyname,
               ct.companytypename,
               p.title, p.boardflag, p.proflag,
               p.currentproflag, p.currentboardflag,
               per.firstname, per.lastname
        FROM ciq_pplintel.ciqprofessional p
        LEFT JOIN ciq_common.ciqcompany c ON p.companyid = c.companyid
        LEFT JOIN ciq_common.ciqcompanytype ct
            ON c.companytypeid = ct.companytypeid
        LEFT JOIN ciq_pplintel.ciqperson per ON p.personid = per.personid
        WHERE p.personid IN ({pid_str})
        ORDER BY p.personid, p.companyid
    """)
    pos_rows.extend(cur.fetchall())
    n_batch = i // batch_size + 1
    n_total = (len(observer_personids) + batch_size - 1) // batch_size
    print(f"  Batch {n_batch}/{n_total}: {len(pos_rows):,} total")

cols = [d[0] for d in cur.description]
save_csv(pos_rows, cols, os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))


# =====================================================================
# STEP 6: Pull key events at observer companies
#         (06_observer_company_key_events.csv)
# =====================================================================
# Pull material events from the WRDS convenience view wrds_keydev.
# This view already joins ciqkeydev with event type names, company
# names, object role types (Target/Buyer/Seller), and gvkey.
#
# NEW columns vs original: situation (narrative text), objectroletype,
#   announcedate (clean date), gvkey, sourcetypename,
#   speffectivedate, sptodate
print("\n" + "=" * 80)
print("STEP 6: Pull key events at observer companies")
print("=" * 80)

evt_rows = []
batch_size = 500
for i in range(0, len(observer_companyids), batch_size):
    batch = observer_companyids[i:i+batch_size]
    cid_str = ", ".join(batch)
    time.sleep(3)
    try:
        cur.execute(f"""
            SELECT keydevid, companyid, companyname,
                   headline,
                   keydeveventtypeid, eventtype,
                   keydevtoobjectroletypeid, objectroletype,
                   announcedate,
                   announceddateutc,
                   enterdate,
                   mostimportantdateutc,
                   speffectivedate, sptodate,
                   gvkey, sourcetypename
            FROM ciq_keydev.wrds_keydev
            WHERE companyid IN ({cid_str})
            ORDER BY companyid, announcedate
        """)
        evt_rows.extend(cur.fetchall())
    except Exception as e:
        print(f"  Error: {str(e)[:100]}")
        conn.rollback()
    print(f"  Batch {i//batch_size + 1}: {len(evt_rows):,} total")

cols = [d[0] for d in cur.description]
save_csv(evt_rows, cols, os.path.join(ciq_dir, "06_observer_company_key_events.csv"))


# =====================================================================
# STEP 7: Pull CIQ -> CIK crosswalk (07_ciq_cik_crosswalk.csv)
# =====================================================================
# Map CIQ company IDs to SEC CIK numbers.
print("\n" + "=" * 80)
print("STEP 7: Pull CIQ -> CIK crosswalk")
print("=" * 80)
time.sleep(3)

cik_rows = []
for i in range(0, len(observer_companyids), batch_size):
    batch = observer_companyids[i:i+batch_size]
    cid_str = ", ".join(batch)
    time.sleep(2)
    cur.execute(f"""
        SELECT companyid, cik, companyname, startdate, enddate, primaryflag
        FROM ciq_common.wrds_cik
        WHERE companyid IN ({cid_str})
        ORDER BY companyid
    """)
    cik_rows.extend(cur.fetchall())

cols = [d[0] for d in cur.description]
save_csv(cik_rows, cols, os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))

primary_ciks = len(set(str(r[1]) for r in cik_rows if r[5] == 1 and r[1]))
print(f"  Companies with primary CIK: {primary_ciks:,} of {len(observer_companyids):,}")


# =====================================================================
# SUMMARY
# =====================================================================
print(f"\n\n{'='*80}")
print("ALL FILES REGENERATED")
print(f"{'='*80}")

for fname in sorted(os.listdir(ciq_dir)):
    if fname.startswith("0") and fname.endswith(".csv"):
        fp = os.path.join(ciq_dir, fname)
        size = os.path.getsize(fp) / 1024
        with open(fp, "r", encoding="utf-8") as fh:
            n = sum(1 for _ in fh) - 1
        print(f"  {fname}: {n:,} rows ({size:.0f} KB)")

cur.close()
conn.close()
print("\nDone.")
