"""Detailed sample construction and attrition for all tests."""

import csv, os
from collections import Counter

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
edgar_dir = os.path.join(data_dir, "EDGAR_Extract")

def load(fp):
    with open(fp, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def norm(val):
    val = str(val).strip()
    return val[:-2] if val.endswith(".0") else val

print("=" * 80)
print("DETAILED SAMPLE CONSTRUCTION AND ATTRITION")
print("=" * 80)

# =====================================================================
# SOURCE 1: CIQ
# =====================================================================
print("\n" + "=" * 80)
print("SOURCE 1: S&P CAPITAL IQ (WRDS PostgreSQL)")
print("=" * 80)

print("""
HOW COLLECTED:
  Connected to WRDS PostgreSQL (wrds-pgdata.wharton.upenn.edu:9737).
  Queried ciq_pplintel.ciqprofessional for all records where the
  free-text 'title' field contains the word 'observer'.

WHY THIS WORKS:
  CIQ analysts manually code professional titles from company disclosures,
  press releases, and public sources. When someone is appointed as a
  'Board Observer' at a company, CIQ creates a record with that title.
  This is NOT a standardized function code -- it is in the free-text
  title field, so coverage depends on CIQ analyst attention.

QUERY:
  SELECT p.proid, p.personid, p.companyid, p.title, p.boardflag,
         p.currentproflag, p.currentboardflag, per.firstname, per.lastname
  FROM ciq_pplintel.ciqprofessional p
  JOIN ciq_pplintel.ciqperson per ON p.personid = per.personid
  WHERE p.title ILIKE '%observer%'
""")

obs = load(os.path.join(ciq_dir, "01_observer_records.csv"))
obs_cos = set(norm(r["companyid"]) for r in obs)
obs_persons = set(norm(r["personid"]) for r in obs)

print(f"  Records returned:          {len(obs):>8,}")
print(f"  Unique companies:          {len(obs_cos):>8,}")
print(f"  Unique persons:            {len(obs_persons):>8,}")

cos = load(os.path.join(ciq_dir, "04_observer_company_details.csv"))
types = Counter(r["companytypename"] for r in cos)
countries = Counter(r["country"] for r in cos)
statuses = Counter(r["companystatustypename"] for r in cos)

print(f"\n  Company type breakdown:")
for t, n in types.most_common():
    print(f"    {t:35} {n:>5} ({100*n/len(cos):.1f}%)")

print(f"\n  Company status breakdown (top 5):")
for s, n in statuses.most_common(5):
    print(f"    {s:35} {n:>5} ({100*n/len(cos):.1f}%)")

print(f"\n  Top 5 countries:")
for c, n in countries.most_common(5):
    print(f"    {c:35} {n:>5} ({100*n/len(cos):.1f}%)")

# Additional CIQ data
dirs = load(os.path.join(ciq_dir, "03_directors_at_observer_companies.csv"))
dir_cos = set(norm(r["companyid"]) for r in dirs)
net = load(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
vc_pe = [r for r in net if r.get("companytypename") in ("Private Investment Firm", "Public Investment Firm", "Private Fund")]
vc_persons = set(norm(r["personid"]) for r in vc_pe)
xwalk = load(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
xwalk_cos = set(norm(r["companyid"]) for r in xwalk)
deals = load(os.path.join(ciq_dir, "08_company_deal_amounts.csv"))
deal_cos = set(norm(r["companyid"]) for r in deals)
events = load(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
event_cos = set(norm(r["companyid"]) for r in events)

print(f"\n  Additional CIQ data coverage (of {len(obs_cos):,} observer companies):")
print(f"    Directors data:          {len(dir_cos):>5,} ({100*len(dir_cos)/len(obs_cos):.0f}%)")
print(f"    Observer network positions: {len(net):>5,} across {len(set(norm(r['companyid']) for r in net)):,} connected cos")
print(f"    VC/PE-affiliated observers: {len(vc_persons):>5,} of {len(obs_persons):,} ({100*len(vc_persons)/len(obs_persons):.0f}%)")
print(f"    CIQ -> CIK crosswalk:    {len(xwalk_cos):>5,} ({100*len(xwalk_cos)/len(obs_cos):.0f}%)")
print(f"    Deal/transaction data:   {len(deal_cos):>5,} ({100*len(deal_cos)/len(obs_cos):.0f}%)")
print(f"    Key Dev events:          {len(event_cos):>5,} ({100*len(event_cos)/len(obs_cos):.0f}%)")


# =====================================================================
# SOURCE 2: EDGAR
# =====================================================================
print("\n\n" + "=" * 80)
print("SOURCE 2: SEC EDGAR")
print("=" * 80)

print("""
HOW COLLECTED (three steps):

  Step A - Quarterly Index Files:
    Downloaded 37 quarterly company.idx files from EDGAR full-index
    (sec.gov/Archives/edgar/full-index/{year}/QTR{q}/company.idx)
    covering 2017 Q1 through 2026 Q1. These are static files listing
    every SEC filing in that quarter. Parsed for form_type = 'S-1' or 'S-1/A'.
    Rate: 5-sec delay between downloads.

  Step B - Full-Text Search (EFTS):
    Used EDGAR Full-Text Search System API to find S-1 filings
    mentioning 'board observer'.
    API: efts.sec.gov/LATEST/search-index?q="board observer"&forms=S-1
    Paginated at 100 results/page with 10-sec delays.
    Required User-Agent header per SEC fair access policy.

    Expanded search with 8 additional terms: 'nonvoting observer',
    'non-voting observer', 'observer rights', 'nonvoting capacity',
    'non-voting capacity', 'observer to the board'.

  Step C - Exhibit Fetching:
    For each EFTS hit that was an exhibit file (EX-4.x, EX-10.x),
    fetched the actual HTML document from EDGAR archives.
    URL: sec.gov/Archives/edgar/data/{CIK}/{accession}/{filename}
    Rate: 10-sec delay between fetches, 60-sec pause on 403.
    Stripped HTML tags with regex, then searched for:
    - 'fiduciary manner' (pre-2020 NVCA language)
    - 'shall not|no.*fiduciary dut|obligation' (post-2020 language)
""")

s1_all = load(os.path.join(edgar_dir, "all_s1_filings_2017_2026.csv"))
s1_unique = set(str(int(r["cik"])) for r in s1_all if r["cik"] and r["cik"].strip().isdigit())

print(f"  Step A results:")
print(f"    Total S-1 + S-1/A filings:   {len(s1_all):>8,}")
print(f"    Unique companies (by CIK):    {len(s1_unique):>8,}")

efts = load(os.path.join(edgar_dir, "efts_board_observer_s1_hits.csv"))
efts_ciks = set()
for h in efts:
    if h.get("ciks"):
        for c in h["ciks"].split("|"):
            if c.strip():
                try:
                    efts_ciks.add(str(int(c.strip())))
                except:
                    pass

print(f"\n  Step B results:")
print(f"    EFTS file hits:               {len(efts):>8,}")
print(f"    Unique companies:             {len(efts_ciks):>8,}")
print(f"    After expanded search:        {'975':>8}")

exh = load(os.path.join(edgar_dir, "exhibit_analysis_results.csv"))
exh_ok = [r for r in exh if r.get("fetch_status") in ("ok", "cached")]
exh_ciks = set(r["cik"] for r in exh_ok if r["cik"])
fid_yes = len([r for r in exh_ok if r.get("has_fiduciary_manner") == "True"])
fid_no = len([r for r in exh_ok if r.get("has_no_fiduciary_duty") == "True"])

print(f"\n  Step C results:")
print(f"    Exhibits fetched:             {len(exh_ok):>8,}")
print(f"    Unique companies:             {len(exh_ciks):>8,}")
print(f"    Fiduciary manner (pre-2020):  {fid_yes:>8,}")
print(f"    No fiduciary duty (post-2020):{fid_no:>8,}")
print(f"    Neither pattern:              {len(exh_ok)-fid_yes-fid_no:>8,}")


# =====================================================================
# TEST-BY-TEST ATTRITION
# =====================================================================
print("\n\n" + "=" * 80)
print("SAMPLE ATTRITION BY TEST")
print("=" * 80)

print("""
TEST 1: OBSERVER PRESENCE (observer vs non-observer S-1 filers)
================================================================

  S-1 filers 2017-2026 (from EDGAR quarterly indexes)          5,566
    |
    | Classify: does the S-1 mention 'board observer' (EFTS search)?
    |
    +-- Treatment (yes):                                          632
    +-- Control (no):                                           4,934
    |   (expanded treatment with broader terms:                   975)
    |
    | Match ALL to CRSP via CIK -> GVKEY -> PERMNO
    | (using crsp_a_ccm.ccmxpf_lnkhist + comp.company)
    |
  Matched to CRSP:                                             3,129
    LOST: 2,437 (44%) -- no GVKEY/PERMNO (foreign, never traded,
          recent IPO not yet in CRSP, or CIK doesn't link)
    |
    | Compute return volatility (std dev of daily returns, year 1)
    | Require >= 100 trading days
    |
  Have volatility:                                             2,728
    Treatment: 420  |  Control: 2,308
    LOST: 401 (13%) -- insufficient trading history
    |
    | Require non-null Compustat controls (log_assets, leverage)
    | Cap leverage at 99th percentile, remove infinities
    |
  MAIN REGRESSION SAMPLE:                                      1,520
    Treatment: 239  |  Control: 1,281
    LOST: 1,208 (44%) -- no Compustat financial data

  Outcome-specific further attrition (from 1,520):
    Return volatility:    1,520 (no further loss)
    IPO underpricing:       271 (most firms lack exact 1st-day return data)
    6-month BHAR:         1,517 (minimal loss)
    Analyst coverage:       605 (requires IBES ticker link via CUSIP match)
    Forecast dispersion:    559 (requires IBES with non-null stdev)
""")

print("""
TEST 2: OBSERVER INTENSITY (continuous, within observer firms)
==============================================================

  POST-IPO PATH (requires CRSP/Compustat):

    CIQ observer companies                                     3,058
      |
      | Require CIK via ciq_common.wrds_cik crosswalk
      |
    Have CIK:                                                  1,355
      LOST: 1,703 (56%) -- private firms with no SEC filings
      |
      | Match to CRSP via CIK -> GVKEY -> PERMNO
      |
    Matched to CRSP/Compustat:                                   310
      LOST: 1,045 (77% of CIK sample) -- CIK exists but no
            GVKEY/PERMNO link (subsidiaries, non-traded entities)
      |
      | Require >= 12 months return data + Compustat controls
      |
    REGRESSION SAMPLE:                                           184
      LOST: 2,874 total (94% of starting 3,058)

  PRIVATE FIRM PATH (CIQ only, no CRSP needed):

    CIQ observer companies                                     3,058
      |
      | Filter to companytypename = 'Private Company'
      |
    Private firms:                                             2,602
      |
      | Require non-null: observer_ratio, log_board_size,
      | has_advisory, is_us, log_capital_raised, firm_age
      |
    REGRESSION SAMPLE:                                         2,537
      LOST: 65 (2.5%) -- minimal attrition
""")

print("""
TEST 3 DAILY: INFORMATION SPILLOVER (cross-portfolio CARs)
===========================================================

  CIQ observer persons                                         4,915
    |
    | For each person, look up ALL their other positions
    | (from ciq_pplintel.ciqprofessional, same personid)
    |
  All positions held by observer persons:                     42,857
    Across connected companies:                               26,079
    |
    | Classify positions:
    | - At 'Private Investment Firm' / 'Public Investment Firm' /
    |   'Private Fund' -> VC/PE AFFILIATION
    | - At other companies with 'Director'/'Chairman' in title
    |   -> assumed PORTFOLIO COMPANY of that VC
    |
  Observers with VC/PE affiliation:                            2,749 (56%)
    LOST: 2,166 (44%) -- no identifiable VC/PE employer in CIQ
    |
    | Build edges: observer -> VC firm -> portfolio company
    | (one edge per observer-VC-portfolio triple)
    |
  Network edges:                                              16,670
    |
    | Filter: portfolio company must have CRSP PERMNO
    | (via ciq_common.wrds_cik -> crsp_a_ccm.ccmxpf_lnkhist)
    |
  Edges with PERMNO:                                          12,885
    LOST: 3,785 (23%) -- portfolio company is private (no CRSP)
    |
    | Add industry codes: SIC2 from Compustat via GVKEY
    | (for observed co: CIQ companyid -> CIK -> GVKEY -> SIC)
    | (for portfolio co: CIK -> GVKEY -> SIC)
    |
  Same-industry pairs (SIC2 match):                            1,186
    |
    | Identify material events at OBSERVED companies
    | Source: CIQ Key Dev (ciq_keydev.wrds_keydev)
    | Event types: 28 (Earnings) + 16 (Exec/Board Changes)
    |
  Events at observed companies:                               52,705
    Earnings announcements:   30,765
    Exec/board changes:       21,940
    |
    | Cross: events x edges (every event paired with every
    | connected portfolio company)
    |
  Event-edge pairs:                                          186,638
    |
    | For each pair: look up portfolio company daily returns
    | in [-5, +5] trading days around event date.
    | Source: CRSP daily (crsp_a_stock.dsf), pulled for all
    | portfolio PERMNOs in batches of 200 with 5-sec delays.
    | Require >= 5 trading days in window.
    | CAR = sum of (firm return - equal-weighted market return)
    |
  CARs computed:                                              70,218
    LOST: 116,420 (62%) -- no daily return match for permno/date

  FINAL SAMPLE:
    Total CARs:                70,218
    Unique observers:             509
    Unique VC firms:            1,353 (used for clustering)
    Unique portfolio companies:   765
    Same-industry CARs:        24,222
    Director-connected CARs:   50,001
    Non-director CARs:         20,217
""")

print("""
TEST 4: PRE-ANNOUNCEMENT DRIFT
================================

  CRSP-matched S-1 filers (from Test 1 crosswalk)             3,129
    |
    | Identify which are CIQ observer companies
    | (match CIK between CIQ master and CRSP crosswalk)
    |
  Observer firms in CRSP:                                         95
  Non-observer firms in CRSP:                                  3,034
    |
    | Pull CIQ Key Dev events for ALL these firms
    | (via WRDS: join wrds_keydev with comp.company on GVKEY)
    | Event types: 28 (Earnings) + 16 (Exec changes)
    |
  Total events:                                              144,496
    At observer firms:        6,391
    At non-observer firms:  138,105
    |
    | Match to CRSP daily returns
    | (from Test 1 daily file: 2.9M rows, 2,821 securities)
    |
  Events with CRSP coverage:                                134,269
    |
    | Sample 50,000 for computational speed
    | (random sample, seed=42)
    |
  Sampled events:                                             50,000
    |
    | Compute CAR[-10, -1] for each event
    | (10 trading days before announcement)
    | Require >= 3 pre-announcement trading days
    |
  CARs computed:                                              37,710
    Observer-firm events:    1,626
    Non-observer events:    36,084
""")

print("""
TEST 5: FULL CIQ PRIVATE FIRMS (with all controls)
====================================================

  CIQ observer companies                                       3,058
    |
    | Filter to companytypename = 'Private Company'
    |
  Private firms:                                               2,602
    |
    | Merge with deal amounts (from ciq_transactions.wrds_offerings)
    | 618 companies have deal data; rest get log_capital_raised = 0
    |
    | Require non-null: observer_ratio, log_board_size,
    | has_advisory, is_us, log_capital_raised, firm_age
    |
  REGRESSION SAMPLE:                                           2,537
    |
  Outcome event counts within this sample:
    Lawsuits:       148 firms (5.8%)
    Restatements:    14 firms (0.6%)
    Bankruptcies:    42 firms (1.7%)
    Acquired:       172 firms (6.8%)
    Failed:          66 firms (2.6%)
    Exec changes: 2,537 firms (count variable, mean=4.7)
""")

print("=" * 80)
print("KEY TAKEAWAY")
print("=" * 80)
print("""
  The CIQ -> CRSP path loses 94% of our sample (3,058 -> 184).
  The CIQ private firm path loses only 2.5% (3,058 -> 2,537).

  Test 5 (private, N=2,537) is 14x larger than Test 2 (CRSP, N=184).
  This is why Test 5 produces the strongest and most robust results.

  Test 3 (spillover, N=70,218) retains power despite attrition because
  the network multiplication (events x edges) creates many observations.
  But only 509 of 4,915 observers (10%) contribute to the final sample
  after requiring VC affiliation + public portfolio companies + CRSP match.
""")
