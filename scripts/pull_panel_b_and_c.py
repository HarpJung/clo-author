"""Pull outcome data for Panel B (CIQ companies with CIK) and
build Panel C (observer network with public portfolio company returns)."""

import psycopg2, csv, os, time

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
print("=" * 60)
print("PANEL B: CIQ Observer Companies with CIK -> Outcome Data")
print("=" * 60)

# Load CIK crosswalk
with open(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"), "r", encoding="utf-8") as f:
    xwalk = list(csv.DictReader(f))

ciks_b = sorted(set(r["cik"].strip() for r in xwalk if r["cik"]))
cik_ints_b = [int(c) for c in ciks_b]
cik_str_b = ", ".join(str(c) for c in cik_ints_b)

print(f"\nPanel B: {len(ciks_b)} unique CIKs from CIQ observer companies")

out_dir_b = os.path.join(data_dir, "Panel_B_Outcomes")
os.makedirs(out_dir_b, exist_ok=True)

# --- Step 1: Identifier crosswalk ---
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

# --- Step 2: Compustat annual ---
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

# --- Step 3: CRSP monthly returns ---
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

# --- Step 4: IBES consensus ---
print("\n--- B4: IBES analyst consensus ---")
time.sleep(5)

if gvkeys_b:
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
print(f"\n\n{'='*60}")
print("PANEL C: Observer Network -> Public Portfolio Company Returns")
print("=" * 60)

out_dir_c = os.path.join(data_dir, "Panel_C_Network")
os.makedirs(out_dir_c, exist_ok=True)

# Load the observer network (Table B)
with open(os.path.join(data_dir, "table_b_observer_network.csv"), "r", encoding="utf-8") as f:
    network = list(csv.DictReader(f))

print(f"\nNetwork: {len(network):,} links | "
      f"{len(set(r['observer_personid'] for r in network)):,} observers | "
      f"{len(set(r['vc_firm_companyid'] for r in network)):,} VC firms")

# Load ALL positions held by observers to find public portfolio companies
with open(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"), "r", encoding="utf-8") as f:
    all_positions = list(csv.DictReader(f))

# Find public company positions (companytypename = 'Public Company')
public_positions = [r for r in all_positions if r.get("companytypename") == "Public Company"]
public_companyids = set(r["companyid"] for r in public_positions)

print(f"Public company positions held by observers: {len(public_positions):,}")
print(f"Unique public companies in observer network: {len(public_companyids):,}")

# Get CIKs for these public companies from CIQ
print("\n--- C1: Getting CIKs for public portfolio companies ---")
time.sleep(5)

# Normalize IDs
pub_ids_normalized = set()
for pid in public_companyids:
    pid = str(pid).strip()
    if pid.endswith(".0"):
        pid = pid[:-2]
    pub_ids_normalized.add(pid)

pub_id_str = ", ".join(pub_ids_normalized)

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

# --- C2: Build the network edges (observer -> observed -> VC -> public portfolio co) ---
print("\n--- C2: Building network edges for public portfolio companies ---")

# For each observer, map: observed company -> VC firm -> other public companies
network_edges = []
for r in network:
    obs_pid = r["observer_personid"]
    obs_name = r["observer_name"]
    obs_cid = r["observed_companyid"]
    obs_cname = r["observed_companyname"]
    vc_cid = r["vc_firm_companyid"]
    vc_name = r["vc_firm_name"]

    # Find this observer's positions at public companies
    for pos in all_positions:
        pid_norm = str(pos["personid"]).strip()
        if pid_norm.endswith(".0"):
            pid_norm = pid_norm[:-2]

        if pid_norm != obs_pid:
            continue
        if pos.get("companytypename") != "Public Company":
            continue

        port_cid = str(pos["companyid"]).strip()
        if port_cid.endswith(".0"):
            port_cid = port_cid[:-2]

        # Skip if the public company IS the observed company
        if port_cid == obs_cid:
            continue

        port_cik_info = pub_cik_map.get(port_cid, {})

        network_edges.append({
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
print("\n--- C3: CRSP returns for public portfolio companies ---")
time.sleep(5)

port_ciks = sorted(set(int(r["portfolio_cik"]) for r in network_edges if r["portfolio_cik"]))
if port_ciks:
    port_cik_str = ", ".join(str(c) for c in port_ciks)

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

    # Save crosswalk
    outfile = os.path.join(out_dir_c, "03_portfolio_permno_crosswalk.csv")
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["cik", "gvkey", "permno"])
        writer.writerows(port_links)

    if port_permnos:
        port_permno_str = ", ".join(str(p) for p in port_permnos)
        time.sleep(5)

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

# --- C4: Industry codes for competitive overlap ---
print("\n--- C4: Industry codes for observed + portfolio companies ---")
time.sleep(5)

# Get SIC codes from Compustat for all companies in the network
all_network_ciks = set()
for r in network_edges:
    if r["portfolio_cik"]:
        all_network_ciks.add(int(r["portfolio_cik"]))

# Also add observed companies that have CIKs
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
