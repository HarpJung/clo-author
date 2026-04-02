"""
Pull outcome data for Panel B (CIQ companies with CIK) and
build Panel C (observer network with public portfolio company returns).

=== OVERVIEW ===

This script does two things:

PANEL B: For private companies that have board observers AND a CIK
(i.e., they filed with the SEC at some point), pull financial data
from Compustat, CRSP, and IBES. This gives us outcome variables
for the observed private companies themselves.

PANEL C: Build the observer network that connects private firms
to public firms through shared personnel. The key idea:

    Private Firm A  --(has observer)-->  Person X  --(also serves at)-->  Public Firm B

Person X sits as a board observer at Private Firm A and simultaneously
holds a position (typically director/chairman) at Public Firm B.
This creates an information bridge between the two firms.

We then pull CRSP returns for all these connected public firms (Firm B's)
so we can test whether their stock prices move around events at Firm A.

=== INPUT FILES ===
- Data/CIQ_Extract/07_ciq_cik_crosswalk.csv    -- CIQ company ID to SEC CIK mapping
- Data/table_b_observer_network.csv             -- Observer-level network (who observes where, via which VC)
- Data/CIQ_Extract/05_observer_person_all_positions.csv -- ALL positions held by each observer (public + private)
- Data/table_a_company_master.csv               -- Master list of observed companies

=== OUTPUT FILES ===
Panel B (Data/Panel_B_Outcomes/):
  01_identifier_crosswalk.csv   -- CIK -> PERMNO -> GVKEY mapping
  02_compustat_annual.csv       -- Annual financials for observer companies with CIK
  03_crsp_monthly_returns.csv   -- Monthly stock returns (for companies that were ever public)
  04_ibes_consensus.csv         -- Analyst EPS forecasts

Panel C (Data/Panel_C_Network/):
  01_public_portfolio_companies.csv      -- List of public firms connected through observers
  02_observer_public_portfolio_edges.csv -- The network edges (observer, observed co, VC, public co)
  03_portfolio_permno_crosswalk.csv      -- CIK -> GVKEY -> PERMNO for portfolio companies
  04_portfolio_crsp_monthly.csv          -- Monthly returns for all connected public firms
  05_industry_codes.csv                  -- SIC/NAICS codes for industry overlap analysis
"""

import psycopg2, csv, os, time

# =====================================================================
# DATABASE CONNECTION
# =====================================================================
conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu",
    port=9737,
    dbname="wrds",
    user="harperjung",
    password="Wwjksnm9087yu!"
)
cur = conn.cursor()

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")

# =====================================================================
# PANEL B: OUTCOMES FOR CIQ COMPANIES WITH CIK
# =====================================================================
#
# Purpose: Some of our private observed companies have SEC CIKs because
# they filed Form D, S-1, or other documents. For these companies we can
# pull financial data to characterize the sample.
#
print("=" * 60)
print("PANEL B: CIQ Observer Companies with CIK -> Outcome Data")
print("=" * 60)

# Load CIK crosswalk -- maps CIQ company IDs to SEC CIK numbers.
# This was built earlier from ciq_common.wrds_cik.
with open(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"), "r", encoding="utf-8") as f:
    xwalk = list(csv.DictReader(f))

# Extract unique CIK values (some CIQ companies share a CIK)
ciks_b = sorted(set(r["cik"].strip() for r in xwalk if r["cik"]))
cik_ints_b = [int(c) for c in ciks_b]
cik_str_b = ", ".join(str(c) for c in cik_ints_b)

print(f"\nPanel B: {len(ciks_b)} unique CIKs from CIQ observer companies")

out_dir_b = os.path.join(data_dir, "Panel_B_Outcomes")
os.makedirs(out_dir_b, exist_ok=True)

# --- Step B1: Build identifier crosswalk (CIK -> PERMNO -> GVKEY) ---
#
# We need to link SEC CIKs to CRSP PERMNOs (for returns) and
# Compustat GVKEYs (for financials). The CCM linking table
# (crsp_a_ccm.ccmxpf_lnkhist) provides this bridge.
#
# Link types: LU = "link used by CRSP", LC = "link confirmed by CRSP"
# Link priority: P = primary, C = primary candidate
#
print("\n--- B1: Building CIK -> PERMNO -> GVKEY crosswalk ---")
time.sleep(3)

cur.execute(f"""
    SELECT DISTINCT b.cik, a.gvkey, a.lpermno as permno,
           a.linkdt, a.linkenddt, a.linktype, a.linkprim
    FROM crsp_a_ccm.ccmxpf_lnkhist a
    JOIN comp.company b ON a.gvkey = b.gvkey
    WHERE CAST(b.cik AS BIGINT) IN ({cik_str_b})
    AND a.linktype IN ('LU', 'LC')
    AND a.linkprim IN ('P', 'C')
    ORDER BY b.cik, a.gvkey
""")
links = cur.fetchall()
cols = [d[0] for d in cur.description]

outfile = os.path.join(out_dir_b, "01_identifier_crosswalk.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(links)

permnos_b = sorted(set(int(r[2]) for r in links if r[2]))
gvkeys_b = sorted(set(r[1] for r in links if r[1]))
matched_b = len(set(str(int(r[0])) for r in links if r[0]))
print(f"  Matched: {matched_b} CIKs -> {len(permnos_b)} PERMNOs, {len(gvkeys_b)} GVKEYs")

# --- Step B2: Pull Compustat annual fundamentals ---
#
# Standard Compustat filters:
#   indfmt='INDL' (industrial format), datafmt='STD' (standardized),
#   popsrc='D' (domestic), consol='C' (consolidated)
#
# Variables pulled: total assets (at), total liabilities (lt),
# stockholders equity (seq, ceq), revenue (sale, revt), net income (ni, ib),
# cash (che), debt (dltt, dlc), capex (capx), R&D (xrd), etc.
#
print("\n--- B2: Compustat annual fundamentals ---")
time.sleep(5)

if gvkeys_b:
    gvkey_str_b = ", ".join(f"'{g}'" for g in gvkeys_b)
    cur.execute(f"""
        SELECT
            gvkey, datadate, fyear, tic, conm, cik,
            at, lt, seq, ceq, csho, prcc_f,
            sale, revt, ni, ib,
            act, lct, che, dltt, dlc,
            oancf, capx, dp, xrd
        FROM comp.funda
        WHERE gvkey IN ({gvkey_str_b})
        AND indfmt = 'INDL' AND datafmt = 'STD' AND popsrc = 'D' AND consol = 'C'
        AND datadate >= '2015-01-01'
        ORDER BY gvkey, datadate
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    outfile = os.path.join(out_dir_b, "02_compustat_annual.csv")
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)

    n_cos = len(set(r[0] for r in rows))
    print(f"  Saved: {len(rows):,} rows | {n_cos} companies -> 02_compustat_annual.csv")

# --- Step B3: Pull CRSP monthly returns ---
#
# Monthly stock returns for observer companies that have PERMNOs
# (i.e., they were publicly traded at some point).
# Most observer companies are private, so only a subset will match.
#
print("\n--- B3: CRSP monthly returns ---")
time.sleep(5)

if permnos_b:
    permno_str_b = ", ".join(str(p) for p in permnos_b)
    cur.execute(f"""
        SELECT permno, date, ret, retx, shrout, prc, vol
        FROM crsp_a_stock.msf
        WHERE permno IN ({permno_str_b})
        AND date >= '2015-01-01'
        ORDER BY permno, date
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    outfile = os.path.join(out_dir_b, "03_crsp_monthly_returns.csv")
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)

    n_permnos = len(set(r[0] for r in rows))
    print(f"  Saved: {len(rows):,} rows | {n_permnos} securities -> 03_crsp_monthly_returns.csv")

# --- Step B4: Pull IBES analyst consensus forecasts ---
#
# Match GVKEYs to IBES tickers via CUSIP, then pull consensus
# EPS estimates. This lets us study analyst coverage and
# earnings surprise patterns for observer companies.
#
# fpi='1' = current fiscal year, fpi='2' = next fiscal year
#
print("\n--- B4: IBES analyst consensus ---")
time.sleep(5)

if gvkeys_b:
    # First, map Compustat GVKEYs to IBES tickers via CUSIP matching
    cur.execute(f"""
        SELECT DISTINCT a.ticker, b.gvkey
        FROM ibes.idsum a
        JOIN comp.security c ON a.cusip = SUBSTRING(c.cusip, 1, 8)
        JOIN comp.company b ON c.gvkey = b.gvkey
        WHERE b.gvkey IN ({gvkey_str_b})
    """)
    ticker_links = cur.fetchall()
    tickers = sorted(set(r[0] for r in ticker_links if r[0]))
    print(f"  IBES ticker links: {len(tickers)} tickers")

    if tickers:
        ticker_str = ", ".join(f"'{t}'" for t in tickers)
        time.sleep(5)

        # Pull consensus EPS estimates (mean, median, std dev, number of analysts)
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
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

        outfile = os.path.join(out_dir_b, "04_ibes_consensus.csv")
        with open(outfile, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            writer.writerows(rows)

        n_tickers = len(set(r[0] for r in rows))
        print(f"  Saved: {len(rows):,} rows | {n_tickers} tickers -> 04_ibes_consensus.csv")

print(f"\n{'='*60}")
print(f"PANEL B COMPLETE")
print(f"{'='*60}")

for f_name in sorted(os.listdir(out_dir_b)):
    fp = os.path.join(out_dir_b, f_name)
    size = os.path.getsize(fp) / 1024
    with open(fp, "r", encoding="utf-8") as fh:
        n_rows = sum(1 for _ in fh) - 1
    print(f"  {f_name}: {n_rows:,} rows ({size:.0f} KB)")


# =====================================================================
# PANEL C: OBSERVER NETWORK + PUBLIC PORTFOLIO COMPANY RETURNS
# =====================================================================
#
# This is the core data construction for the paper. We build the
# person-level network that connects private observed firms to
# public portfolio companies through shared board observers.
#
# The logic:
#   1. Load the observer network (who observes at which private company,
#      through which VC firm)
#   2. For each observer, find ALL their positions at PUBLIC companies
#      (from CIQ Professionals data)
#   3. Each (observed private firm, public portfolio firm) pair linked
#      through the same observer becomes a "network edge"
#   4. Pull CRSP returns for all the public portfolio companies
#   5. Pull SIC/NAICS codes for industry overlap analysis
#
print(f"\n\n{'='*60}")
print("PANEL C: Observer Network -> Public Portfolio Company Returns")
print("=" * 60)

out_dir_c = os.path.join(data_dir, "Panel_C_Network")
os.makedirs(out_dir_c, exist_ok=True)

# --- Load the observer network ---
# This file was built from CIQ Professionals data (table_b).
# Each row = one observer link: person X observes at company Y via VC firm Z.
with open(os.path.join(data_dir, "table_b_observer_network.csv"), "r", encoding="utf-8") as f:
    network = list(csv.DictReader(f))

print(f"\nNetwork: {len(network):,} links | "
      f"{len(set(r['observer_personid'] for r in network)):,} observers | "
      f"{len(set(r['vc_firm_companyid'] for r in network)):,} VC firms")

# --- Load ALL positions held by each observer ---
# This file contains every position (current and past) that each observer
# holds at any company, not just the observed company. This is how we
# find their PUBLIC company connections (typically director/chairman roles).
with open(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"), "r", encoding="utf-8") as f:
    all_positions = list(csv.DictReader(f))

# Filter to positions at public companies only.
# companytypename = 'Public Company' identifies firms listed on a stock exchange.
public_positions = [r for r in all_positions if r.get("companytypename") == "Public Company"]
public_companyids = set(r["companyid"] for r in public_positions)

print(f"Public company positions held by observers: {len(public_positions):,}")
print(f"Unique public companies in observer network: {len(public_companyids):,}")

# --- C1: Get CIKs for these public portfolio companies ---
# We need CIKs to link to CRSP (via the CCM crosswalk) for returns data.
print("\n--- C1: Getting CIKs for public portfolio companies ---")
time.sleep(5)

# CIQ company IDs sometimes have ".0" suffixes from pandas float conversion
pub_ids_normalized = set()
for pid in public_companyids:
    pid = str(pid).strip()
    if pid.endswith(".0"):
        pid = pid[:-2]
    pub_ids_normalized.add(pid)

pub_id_str = ", ".join(pub_ids_normalized)

# Query the CIQ-to-CIK mapping table.
# primaryflag = 1 ensures we get the primary CIK (some companies have multiple).
cur.execute(f"""
    SELECT companyid, cik, companyname, primaryflag
    FROM ciq_common.wrds_cik
    WHERE companyid IN ({pub_id_str})
    AND primaryflag = 1
""")
pub_cik_rows = cur.fetchall()
pub_cik_map = {}
for r in pub_cik_rows:
    cid = str(int(r[0]))
    pub_cik_map[cid] = {"cik": r[1], "name": r[2]}

print(f"  Public portfolio companies with CIK: {len(pub_cik_map):,} of {len(public_companyids):,}")

# Save the public portfolio company list
outfile = os.path.join(out_dir_c, "01_public_portfolio_companies.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["companyid", "cik", "companyname"])
    for cid, info in sorted(pub_cik_map.items()):
        writer.writerow([cid, info["cik"], info["name"]])
print(f"  Saved: {len(pub_cik_map):,} companies -> 01_public_portfolio_companies.csv")

# --- C2: Build the network edges ---
#
# THIS IS THE KEY STEP.
#
# For each observer in the network, we look at all their positions.
# If they hold a position at a PUBLIC company (that is not the same
# as the observed private company), we create a network edge:
#
#   (observed private company) --[observer]--> (public portfolio company)
#
# Each edge records:
#   - Who the observer is (personid, name)
#   - Which private company they observe at (observed_companyid)
#   - Which VC firm they work for (vc_firm_companyid)
#   - Which public company they are connected to (portfolio_companyid)
#   - What role they hold at the public company (portfolio_title)
#   - Whether the position is current (is_current)
#
# This produces the person-level network described in the paper:
# "the same individual sits as a nonvoting observer at a private
# company and as a voting director at a public company, personally
# bridging the two boardrooms."
#
print("\n--- C2: Building network edges for public portfolio companies ---")

network_edges = []
for r in network:
    obs_pid = r["observer_personid"]     # CIQ person ID of the observer
    obs_name = r["observer_name"]
    obs_cid = r["observed_companyid"]    # CIQ company ID of the private firm they observe
    obs_cname = r["observed_companyname"]
    vc_cid = r["vc_firm_companyid"]      # CIQ company ID of the VC firm
    vc_name = r["vc_firm_name"]

    # Search all positions to find this observer's public company connections
    for pos in all_positions:
        # Normalize person ID for matching (handle float formatting)
        pid_norm = str(pos["personid"]).strip()
        if pid_norm.endswith(".0"):
            pid_norm = pid_norm[:-2]

        # Skip if this position belongs to a different person
        if pid_norm != obs_pid:
            continue
        # Skip if the position is not at a public company
        if pos.get("companytypename") != "Public Company":
            continue

        port_cid = str(pos["companyid"]).strip()
        if port_cid.endswith(".0"):
            port_cid = port_cid[:-2]

        # Skip self-links (the public company IS the observed company)
        if port_cid == obs_cid:
            continue

        port_cik_info = pub_cik_map.get(port_cid, {})

        # Record this network edge
        network_edges.append({
            "observer_personid": obs_pid,
            "observer_name": obs_name,
            "observed_companyid": obs_cid,        # Private firm with the event
            "observed_companyname": obs_cname,
            "vc_firm_companyid": vc_cid,
            "vc_firm_name": vc_name,
            "portfolio_companyid": port_cid,       # Public firm we measure returns for
            "portfolio_companyname": pos.get("companyname", ""),
            "portfolio_cik": port_cik_info.get("cik", ""),
            "portfolio_title": pos.get("title", ""),  # e.g., "Director", "Chairman"
            "is_current": pos.get("currentproflag", ""),
        })

# Save all network edges
outfile = os.path.join(out_dir_c, "02_observer_public_portfolio_edges.csv")
if network_edges:
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=network_edges[0].keys())
        writer.writeheader()
        writer.writerows(network_edges)

unique_obs = len(set(r["observer_personid"] for r in network_edges))
unique_port = len(set(r["portfolio_companyid"] for r in network_edges))
unique_port_cik = len(set(r["portfolio_cik"] for r in network_edges if r["portfolio_cik"]))
print(f"  Network edges: {len(network_edges):,}")
print(f"  Unique observers with public portfolio links: {unique_obs:,}")
print(f"  Unique public portfolio companies: {unique_port:,}")
print(f"  Unique portfolio companies with CIK: {unique_port_cik:,}")

# --- C3: Pull CRSP monthly returns for public portfolio companies ---
#
# These are the returns we use to compute CARs around events at the
# observed private companies. First we map CIKs to PERMNOs via the
# CCM linking table, then pull monthly returns from CRSP.
#
# Note: Daily returns are pulled separately in a later script
# (pull_crsp_daily.py) because the daily file is much larger.
# This monthly pull is for initial exploration and monthly-level tests.
#
print("\n--- C3: CRSP returns for public portfolio companies ---")
time.sleep(5)

port_ciks = sorted(set(int(r["portfolio_cik"]) for r in network_edges if r["portfolio_cik"]))
if port_ciks:
    port_cik_str = ", ".join(str(c) for c in port_ciks)

    # Map CIKs to PERMNOs via Compustat-CRSP merged (CCM) link table
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
    print(f"  Portfolio company PERMNOs: {len(port_permnos):,}")

    # Save the CIK-GVKEY-PERMNO crosswalk for later use
    outfile = os.path.join(out_dir_c, "03_portfolio_permno_crosswalk.csv")
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["cik", "gvkey", "permno"])
        writer.writerows(port_links)

    if port_permnos:
        port_permno_str = ", ".join(str(p) for p in port_permnos)
        time.sleep(5)

        # Pull monthly returns, price, volume, and shares outstanding
        cur.execute(f"""
            SELECT permno, date, ret, prc, vol, shrout
            FROM crsp_a_stock.msf
            WHERE permno IN ({port_permno_str})
            AND date >= '2015-01-01'
            ORDER BY permno, date
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

        outfile = os.path.join(out_dir_c, "04_portfolio_crsp_monthly.csv")
        with open(outfile, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            writer.writerows(rows)

        n_permnos = len(set(r[0] for r in rows))
        print(f"  Saved: {len(rows):,} rows | {n_permnos} securities -> 04_portfolio_crsp_monthly.csv")

# --- C4: Pull industry codes for competitive overlap analysis ---
#
# We need SIC and NAICS codes to determine whether the observed
# private company and the connected public company are in the same
# industry. The "same industry" interaction is the key test in the paper.
#
# We pull industry codes for ALL companies in the network (both
# observed private companies with CIKs and public portfolio companies).
#
print("\n--- C4: Industry codes for observed + portfolio companies ---")
time.sleep(5)

# Collect CIKs from both sides of the network
all_network_ciks = set()

# Public portfolio companies
for r in network_edges:
    if r["portfolio_cik"]:
        all_network_ciks.add(int(r["portfolio_cik"]))

# Observed private companies (those with CIKs from the company master)
with open(os.path.join(data_dir, "table_a_company_master.csv"), "r", encoding="utf-8") as f:
    master = list(csv.DictReader(f))
for r in master:
    if r["cik"]:
        try:
            all_network_ciks.add(int(r["cik"]))
        except ValueError:
            pass

if all_network_ciks:
    all_cik_str = ", ".join(str(c) for c in sorted(all_network_ciks))

    # Pull SIC and NAICS from Compustat company header
    cur.execute(f"""
        SELECT DISTINCT gvkey, cik, sic, naics, conm
        FROM comp.company
        WHERE CAST(cik AS BIGINT) IN ({all_cik_str})
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    outfile = os.path.join(out_dir_c, "05_industry_codes.csv")
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)

    n_cos = len(set(r[0] for r in rows))
    print(f"  Saved: {len(rows):,} rows | {n_cos} companies -> 05_industry_codes.csv")

# =====================================================================
# SUMMARY
# =====================================================================
print(f"\n\n{'='*60}")
print(f"PANEL C COMPLETE")
print(f"{'='*60}")

for f_name in sorted(os.listdir(out_dir_c)):
    fp = os.path.join(out_dir_c, f_name)
    size = os.path.getsize(fp) / 1024
    with open(fp, "r", encoding="utf-8") as fh:
        n_rows = sum(1 for _ in fh) - 1
    print(f"  {f_name}: {n_rows:,} rows ({size:.0f} KB)")

cur.close()
conn.close()
