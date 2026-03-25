"""Pull outcome data for Test 1 control group: S-1 filers WITHOUT board observer mentions.
Treatment: 596 S-1 filers with "board observer" mention
Control: ~4,970 S-1 filers WITHOUT "board observer" mention
"""

import csv, os, time
import psycopg2

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
edgar_dir = os.path.join(data_dir, "EDGAR_Extract")
out_dir = os.path.join(data_dir, "Test1_Observer_vs_NoObserver")
os.makedirs(out_dir, exist_ok=True)

# =====================================================================
# STEP 1: Identify treatment and control CIKs
# =====================================================================
print("=" * 60)
print("TEST 1: Observer vs No-Observer S-1 Filers")
print("=" * 60)

# All S-1 filers
with open(os.path.join(edgar_dir, "all_s1_filings_2017_2026.csv"), "r", encoding="utf-8") as f:
    all_s1 = list(csv.DictReader(f))

all_ciks = set(r["cik"].strip() for r in all_s1 if r["cik"])

# Observer S-1 filers (from EFTS search)
with open(os.path.join(edgar_dir, "efts_board_observer_s1_hits.csv"), "r", encoding="utf-8") as f:
    efts = list(csv.DictReader(f))

observer_ciks = set()
for h in efts:
    if h.get("ciks"):
        for cik in h["ciks"].split("|"):
            cik = cik.strip()
            if cik:
                observer_ciks.add(cik)

# Control = S-1 filers NOT in observer set
control_ciks = all_ciks - observer_ciks
treatment_ciks = all_ciks & observer_ciks

print(f"\n  All S-1 filers (2017-2026):        {len(all_ciks):,}")
print(f"  Treatment (observer mention):       {len(treatment_ciks):,}")
print(f"  Control (no observer mention):      {len(control_ciks):,}")

# Save treatment/control assignment
outfile = os.path.join(out_dir, "00_treatment_control_assignment.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["cik", "group", "has_observer_mention"])
    for cik in sorted(treatment_ciks):
        writer.writerow([cik, "treatment", 1])
    for cik in sorted(control_ciks):
        writer.writerow([cik, "control", 0])
print(f"  Saved assignment -> 00_treatment_control_assignment.csv")

# =====================================================================
# STEP 2: Connect to WRDS and pull data
# =====================================================================
print("\n--- Connecting to WRDS ---")

conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu",
    port=9737,
    dbname="wrds",
    user="harperjung",
    password="Wwjksnm9087yu!"
)
cur = conn.cursor()

# Combine all CIKs for a single set of queries
all_cik_ints = sorted(set(int(c) for c in all_ciks if c.isdigit()))
print(f"  Total CIKs for WRDS lookup: {len(all_cik_ints):,}")

# =====================================================================
# STEP 3: CIK -> GVKEY -> PERMNO crosswalk
# =====================================================================
print("\n--- Step 3: Building identifier crosswalk ---")

# Process in batches of 1000 to avoid SQL query limits
batch_size = 1000
all_links = []

for i in range(0, len(all_cik_ints), batch_size):
    batch = all_cik_ints[i:i + batch_size]
    batch_str = ", ".join(str(c) for c in batch)

    cur.execute(f"""
        SELECT DISTINCT b.cik, a.gvkey, a.lpermno as permno,
               a.linkdt, a.linkenddt
        FROM crsp_a_ccm.ccmxpf_lnkhist a
        JOIN comp.company b ON a.gvkey = b.gvkey
        WHERE CAST(b.cik AS BIGINT) IN ({batch_str})
        AND a.linktype IN ('LU', 'LC')
        AND a.linkprim IN ('P', 'C')
    """)
    all_links.extend(cur.fetchall())

    batch_num = i // batch_size + 1
    total_batches = (len(all_cik_ints) + batch_size - 1) // batch_size
    print(f"    Batch {batch_num}/{total_batches}: {len(all_links):,} links so far")
    time.sleep(2)

cols = ["cik", "gvkey", "permno", "linkdt", "linkenddt"]
outfile = os.path.join(out_dir, "01_identifier_crosswalk.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(all_links)

# Deduplicate: one permno per CIK
cik_to_permno = {}
cik_to_gvkey = {}
for r in all_links:
    cik_val = str(int(r[0]))
    if cik_val not in cik_to_permno and r[2]:
        cik_to_permno[cik_val] = int(r[2])
        cik_to_gvkey[cik_val] = r[1]

matched_treatment = len([c for c in treatment_ciks if str(int(c)) in cik_to_permno])
matched_control = len([c for c in control_ciks if str(int(c)) in cik_to_permno])
print(f"\n  Total links: {len(all_links):,}")
print(f"  Unique CIK-PERMNO pairs: {len(cik_to_permno):,}")
print(f"  Treatment matched: {matched_treatment:,} / {len(treatment_ciks):,}")
print(f"  Control matched:   {matched_control:,} / {len(control_ciks):,}")

# =====================================================================
# STEP 4: CRSP monthly returns (all matched firms)
# =====================================================================
print("\n--- Step 4: Pulling CRSP monthly returns ---")

all_permnos = sorted(set(cik_to_permno.values()))
print(f"  PERMNOs to pull: {len(all_permnos):,}")

all_crsp = []
for i in range(0, len(all_permnos), batch_size):
    batch = all_permnos[i:i + batch_size]
    batch_str = ", ".join(str(p) for p in batch)

    cur.execute(f"""
        SELECT permno, date, ret, retx, shrout, prc, vol
        FROM crsp_a_stock.msf
        WHERE permno IN ({batch_str})
        AND date >= '2015-01-01'
        ORDER BY permno, date
    """)
    all_crsp.extend(cur.fetchall())

    batch_num = i // batch_size + 1
    total_batches = (len(all_permnos) + batch_size - 1) // batch_size
    print(f"    Batch {batch_num}/{total_batches}: {len(all_crsp):,} rows so far")
    time.sleep(3)

cols = ["permno", "date", "ret", "retx", "shrout", "prc", "vol"]
outfile = os.path.join(out_dir, "02_crsp_monthly_returns.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(all_crsp)

n_permnos = len(set(r[0] for r in all_crsp))
print(f"  Saved: {len(all_crsp):,} rows | {n_permnos:,} securities")

# =====================================================================
# STEP 5: CRSP daily returns (for IPO underpricing + volatility)
# =====================================================================
print("\n--- Step 5: Pulling CRSP daily returns ---")
print("  (This is the largest pull - may take a few minutes)")

all_daily = []
for i in range(0, len(all_permnos), batch_size):
    batch = all_permnos[i:i + batch_size]
    batch_str = ", ".join(str(p) for p in batch)

    cur.execute(f"""
        SELECT permno, date, ret, prc, vol, shrout
        FROM crsp_a_stock.dsf
        WHERE permno IN ({batch_str})
        AND date >= '2015-01-01'
        ORDER BY permno, date
    """)
    all_daily.extend(cur.fetchall())

    batch_num = i // batch_size + 1
    total_batches = (len(all_permnos) + batch_size - 1) // batch_size
    print(f"    Batch {batch_num}/{total_batches}: {len(all_daily):,} rows so far")
    time.sleep(5)  # longer pause for daily data

cols = ["permno", "date", "ret", "prc", "vol", "shrout"]
outfile = os.path.join(out_dir, "03_crsp_daily_returns.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(all_daily)

n_permnos = len(set(r[0] for r in all_daily))
print(f"  Saved: {len(all_daily):,} rows | {n_permnos:,} securities")

# =====================================================================
# STEP 6: Compustat annual fundamentals
# =====================================================================
print("\n--- Step 6: Pulling Compustat annual fundamentals ---")

all_gvkeys = sorted(set(cik_to_gvkey.values()))
all_comp = []

for i in range(0, len(all_gvkeys), batch_size):
    batch = all_gvkeys[i:i + batch_size]
    batch_str = ", ".join(f"'{g}'" for g in batch)

    cur.execute(f"""
        SELECT
            gvkey, datadate, fyear, tic, conm, cik,
            at, lt, seq, ceq, csho, prcc_f,
            sale, revt, ni, ib,
            act, lct, che, dltt, dlc,
            oancf, capx, dp, xrd
        FROM comp.funda
        WHERE gvkey IN ({batch_str})
        AND indfmt = 'INDL' AND datafmt = 'STD' AND popsrc = 'D' AND consol = 'C'
        AND datadate >= '2015-01-01'
        ORDER BY gvkey, datadate
    """)
    all_comp.extend(cur.fetchall())

    batch_num = i // batch_size + 1
    total_batches = (len(all_gvkeys) + batch_size - 1) // batch_size
    print(f"    Batch {batch_num}/{total_batches}: {len(all_comp):,} rows so far")
    time.sleep(3)

cols = ["gvkey", "datadate", "fyear", "tic", "conm", "cik",
        "at", "lt", "seq", "ceq", "csho", "prcc_f",
        "sale", "revt", "ni", "ib",
        "act", "lct", "che", "dltt", "dlc",
        "oancf", "capx", "dp", "xrd"]
outfile = os.path.join(out_dir, "04_compustat_annual.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(all_comp)

n_cos = len(set(r[0] for r in all_comp))
print(f"  Saved: {len(all_comp):,} rows | {n_cos:,} companies")

# =====================================================================
# STEP 7: Industry codes (SIC from Compustat company table)
# =====================================================================
print("\n--- Step 7: Pulling industry codes ---")

all_industry = []
for i in range(0, len(all_gvkeys), batch_size):
    batch = all_gvkeys[i:i + batch_size]
    batch_str = ", ".join(f"'{g}'" for g in batch)

    cur.execute(f"""
        SELECT DISTINCT gvkey, cik, sic, naics, conm
        FROM comp.company
        WHERE gvkey IN ({batch_str})
    """)
    all_industry.extend(cur.fetchall())
    time.sleep(2)

cols = ["gvkey", "cik", "sic", "naics", "conm"]
outfile = os.path.join(out_dir, "05_industry_codes.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(all_industry)

print(f"  Saved: {len(all_industry):,} companies")

# =====================================================================
# STEP 8: IBES analyst consensus
# =====================================================================
print("\n--- Step 8: Pulling IBES analyst consensus ---")

# First get ticker links
all_ibes_links = []
for i in range(0, len(all_gvkeys), batch_size):
    batch = all_gvkeys[i:i + batch_size]
    batch_str = ", ".join(f"'{g}'" for g in batch)

    cur.execute(f"""
        SELECT DISTINCT a.ticker, b.gvkey
        FROM ibes.idsum a
        JOIN comp.security c ON a.cusip = SUBSTRING(c.cusip, 1, 8)
        JOIN comp.company b ON c.gvkey = b.gvkey
        WHERE b.gvkey IN ({batch_str})
    """)
    all_ibes_links.extend(cur.fetchall())
    time.sleep(2)

tickers = sorted(set(r[0] for r in all_ibes_links if r[0]))
print(f"  IBES ticker links: {len(tickers):,}")

# Pull consensus data
all_ibes = []
for i in range(0, len(tickers), 500):
    batch = tickers[i:i + 500]
    batch_str = ", ".join(f"'{t}'" for t in batch)

    cur.execute(f"""
        SELECT ticker, statpers, fpedats, measure, fpi,
               numest, medest, meanest, stdev
        FROM ibes.statsumu_epsus
        WHERE ticker IN ({batch_str})
        AND statpers >= '2015-01-01'
        AND fpi IN ('1', '2')
        AND measure = 'EPS'
        ORDER BY ticker, statpers
    """)
    all_ibes.extend(cur.fetchall())

    batch_num = i // 500 + 1
    total_batches = (len(tickers) + 499) // 500
    print(f"    Batch {batch_num}/{total_batches}: {len(all_ibes):,} rows so far")
    time.sleep(3)

cols = ["ticker", "statpers", "fpedats", "measure", "fpi",
        "numest", "medest", "meanest", "stdev"]
outfile = os.path.join(out_dir, "06_ibes_consensus.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(all_ibes)

n_tickers = len(set(r[0] for r in all_ibes))
print(f"  Saved: {len(all_ibes):,} rows | {n_tickers:,} tickers")

# =====================================================================
# SUMMARY
# =====================================================================
print(f"\n{'='*60}")
print(f"TEST 1 DATA PULL COMPLETE")
print(f"{'='*60}")

for f_name in sorted(os.listdir(out_dir)):
    fp = os.path.join(out_dir, f_name)
    size = os.path.getsize(fp) / 1024
    with open(fp, "r", encoding="utf-8") as fh:
        n_rows = sum(1 for _ in fh) - 1
    print(f"  {f_name}: {n_rows:,} rows ({size:.0f} KB)")

print(f"\n  Treatment (observer S-1 filers) matched to CRSP: {matched_treatment:,}")
print(f"  Control (non-observer S-1 filers) matched to CRSP: {matched_control:,}")

cur.close()
conn.close()
