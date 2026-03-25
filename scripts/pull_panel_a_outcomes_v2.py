"""Pull Panel A outcome data using available WRDS databases.
Available: CRSP, Compustat, ExecuComp, IBES, CIQ KeyDev
NOT available: Audit Analytics (no subscription)
"""

import psycopg2, csv, os, time

conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu",
    port=9737,
    dbname="wrds",
    user="harperjung",
    password="Wwjksnm9087yu!"
)
cur = conn.cursor()

edgar_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data/EDGAR_Extract"
out_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data/Panel_A_Outcomes"
os.makedirs(out_dir, exist_ok=True)

# Load EDGAR exhibit CIKs
with open(os.path.join(edgar_dir, "exhibit_analysis_results.csv"), "r", encoding="utf-8") as f:
    exh = list(csv.DictReader(f))

ciks = sorted(set(
    r["cik"].strip() for r in exh
    if r.get("fetch_status") in ("ok", "cached") and r.get("cik")
))
cik_ints = [int(c) for c in ciks]
cik_str_int = ", ".join(str(c) for c in cik_ints)

print(f"Panel A: {len(ciks)} unique CIKs")

# =====================================================================
# 1. CIK-to-PERMNO-to-GVKEY Linkage
# =====================================================================
print("\n--- Step 1: Building identifier crosswalk (CIK -> PERMNO -> GVKEY) ---")
time.sleep(3)

cur.execute(f"""
    SELECT DISTINCT b.cik, a.gvkey, a.lpermno as permno,
           a.linkdt, a.linkenddt, a.linktype, a.linkprim
    FROM crsp_a_ccm.ccmxpf_lnkhist a
    JOIN comp.company b ON a.gvkey = b.gvkey
    WHERE CAST(b.cik AS BIGINT) IN ({cik_str_int})
    AND a.linktype IN ('LU', 'LC')
    AND a.linkprim IN ('P', 'C')
    ORDER BY b.cik, a.gvkey
""")
links = cur.fetchall()
cols = [d[0] for d in cur.description]

outfile = os.path.join(out_dir, "01_identifier_crosswalk.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(links)

permnos = sorted(set(int(r[2]) for r in links if r[2]))
gvkeys = sorted(set(r[1] for r in links if r[1]))
matched_ciks = len(set(str(int(r[0])) for r in links if r[0]))
print(f"  Matched: {matched_ciks} CIKs -> {len(permnos)} PERMNOs, {len(gvkeys)} GVKEYs")

permno_str = ", ".join(str(p) for p in permnos)
gvkey_str = ", ".join(f"'{g}'" for g in gvkeys)

# =====================================================================
# 2. CRSP Monthly Stock Returns
# =====================================================================
print("\n--- Step 2: CRSP monthly returns ---")
time.sleep(5)

if permnos:
    cur.execute(f"""
        SELECT permno, date, ret, retx, shrout, prc, vol, cfacpr, cfacshr
        FROM crsp_a_stock.msf
        WHERE permno IN ({permno_str})
        AND date >= '2015-01-01'
        ORDER BY permno, date
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    outfile = os.path.join(out_dir, "02_crsp_monthly_returns.csv")
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)

    n_permnos = len(set(r[0] for r in rows))
    print(f"  Saved: {len(rows):,} rows | {n_permnos} securities -> 02_crsp_monthly_returns.csv")

# =====================================================================
# 3. CRSP Daily Returns (for event study / IPO underpricing)
# =====================================================================
print("\n--- Step 3: CRSP daily returns (first 60 trading days post-listing) ---")
time.sleep(5)

if permnos:
    # Get listing dates first
    cur.execute(f"""
        SELECT permno, MIN(date) as first_date
        FROM crsp_a_stock.dsf
        WHERE permno IN ({permno_str})
        AND date >= '2015-01-01'
        GROUP BY permno
    """)
    first_dates = {r[0]: r[1] for r in cur.fetchall()}
    print(f"  Found listing dates for {len(first_dates)} securities")

    time.sleep(5)

    # Pull first 60 trading days for IPO underpricing analysis
    cur.execute(f"""
        SELECT permno, date, ret, prc, vol, shrout
        FROM crsp_a_stock.dsf
        WHERE permno IN ({permno_str})
        AND date >= '2015-01-01'
        ORDER BY permno, date
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    outfile = os.path.join(out_dir, "03_crsp_daily_returns.csv")
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)

    n_permnos = len(set(r[0] for r in rows))
    print(f"  Saved: {len(rows):,} rows | {n_permnos} securities -> 03_crsp_daily_returns.csv")

# =====================================================================
# 4. Compustat Annual Fundamentals
# =====================================================================
print("\n--- Step 4: Compustat annual fundamentals ---")
time.sleep(5)

if gvkeys:
    cur.execute(f"""
        SELECT
            gvkey, datadate, fyear, tic, conm, cik,
            at, lt, seq, ceq, csho, prcc_f,
            sale, revt, cogs, xsga, oibdp, ni, ib,
            act, lct, che, dltt, dlc,
            oancf, capx, dp,
            ppent, intan, gdwl,
            rect, invt, ap,
            xrd, xad
        FROM comp.funda
        WHERE gvkey IN ({gvkey_str})
        AND indfmt = 'INDL' AND datafmt = 'STD' AND popsrc = 'D' AND consol = 'C'
        AND datadate >= '2015-01-01'
        ORDER BY gvkey, datadate
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    outfile = os.path.join(out_dir, "04_compustat_annual.csv")
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)

    n_cos = len(set(r[0] for r in rows))
    print(f"  Saved: {len(rows):,} rows | {n_cos} companies -> 04_compustat_annual.csv")

# =====================================================================
# 5. ExecuComp (CEO compensation and turnover)
# =====================================================================
print("\n--- Step 5: ExecuComp ---")
time.sleep(5)

if gvkeys:
    cur.execute(f"""
        SELECT
            gvkey, year, execid, exec_fullname,
            ceoann, pceo, titleann,
            tdc1, tdc2, salary, bonus, stock_awards, option_awards,
            shrown_excl_opts, shrown_tot
        FROM execcomp.anncomp
        WHERE gvkey IN ({gvkey_str})
        AND year >= 2015
        ORDER BY gvkey, year, execid
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    outfile = os.path.join(out_dir, "05_execucomp.csv")
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)

    n_cos = len(set(r[0] for r in rows))
    n_ceos = len(set((r[0], r[1]) for r in rows if r[4] == "CEO"))
    print(f"  Saved: {len(rows):,} rows | {n_cos} companies | {n_ceos} CEO-years -> 05_execucomp.csv")

# =====================================================================
# 6. IBES Analyst Forecasts
# =====================================================================
print("\n--- Step 6: IBES analyst consensus ---")
time.sleep(5)

# Need ticker mapping first
if gvkeys:
    cur.execute(f"""
        SELECT DISTINCT a.ticker, b.gvkey
        FROM ibes.idsum a
        JOIN comp.security c ON a.cusip = SUBSTRING(c.cusip, 1, 8)
        JOIN comp.company b ON c.gvkey = b.gvkey
        WHERE b.gvkey IN ({gvkey_str})
    """)
    ticker_links = cur.fetchall()
    tickers = sorted(set(r[0] for r in ticker_links if r[0]))
    print(f"  IBES ticker links: {len(tickers)} tickers")

    if tickers:
        ticker_str = ", ".join(f"'{t}'" for t in tickers)
        time.sleep(5)

        cur.execute(f"""
            SELECT ticker, statpers, fpedats, measure, fpi,
                   numest, medest, meanest, stdev, highest, lowest
            FROM ibes.statsumu_epsus
            WHERE ticker IN ({ticker_str})
            AND statpers >= '2015-01-01'
            AND fpi IN ('1', '2')
            AND measure = 'EPS'
            ORDER BY ticker, statpers
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

        outfile = os.path.join(out_dir, "06_ibes_consensus.csv")
        with open(outfile, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            writer.writerows(rows)

        n_tickers = len(set(r[0] for r in rows))
        print(f"  Saved: {len(rows):,} rows | {n_tickers} tickers -> 06_ibes_consensus.csv")

# =====================================================================
# SUMMARY
# =====================================================================
print(f"\n{'='*60}")
print(f"PANEL A OUTCOME DATA COMPLETE")
print(f"{'='*60}")

for f_name in sorted(os.listdir(out_dir)):
    fp = os.path.join(out_dir, f_name)
    size = os.path.getsize(fp) / 1024
    with open(fp, "r", encoding="utf-8") as fh:
        n_rows = sum(1 for _ in fh) - 1
    print(f"  {f_name}: {n_rows:,} rows ({size:.0f} KB)")

print(f"\nNote: Audit Analytics NOT available on this WRDS account.")
print(f"  Missing: audit fees, restatements, SOX 404, going concern")
print(f"  Alternatives: CIQ Key Dev (restatements), ExecuComp (CEO turnover)")
print(f"  Action needed: Request Audit Analytics access from Harvard library")

cur.close()
conn.close()
