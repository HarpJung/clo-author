"""Pull outcome data for Panel A (EDGAR exhibit companies) from WRDS."""

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
print(f"Panel A: {len(ciks)} unique CIKs")

# Convert CIKs to integers for matching (WRDS stores as numeric)
cik_ints = [int(c) for c in ciks]
cik_str_int = ", ".join(str(c) for c in cik_ints)
cik_str = ", ".join(f"'{c}'" for c in ciks)  # varchar format for Audit Analytics

# =====================================================================
# 1. AUDIT FEES
# =====================================================================
print("\n--- Pulling audit fees (Audit Analytics feed03) ---")
time.sleep(3)

cur.execute(f"""
    SELECT
        company_fkey,
        fiscal_year,
        audit_fees,
        non_audit_fees,
        audit_related_fees,
        tax_fees,
        other_fees,
        total_fees,
        auditor_fkey,
        auditor_name,
        restatement
    FROM audit.feed03_audit_fees
    WHERE CAST(company_fkey AS BIGINT) IN ({cik_str_int})
    ORDER BY company_fkey, fiscal_year
""")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]

outfile = os.path.join(out_dir, "01_audit_fees.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(rows)

n_cos = len(set(r[0] for r in rows))
print(f"  Saved: {len(rows):,} rows | {n_cos} companies | -> 01_audit_fees.csv")

# =====================================================================
# 2. RESTATEMENTS
# =====================================================================
print("\n--- Pulling restatements (Audit Analytics feed39) ---")
time.sleep(5)

cur.execute(f"""
    SELECT
        r.company_fkey,
        r.file_date,
        r.res_begin_date,
        r.res_end_date,
        r.res_accounting,
        r.res_fraud,
        r.res_cler_err,
        r.res_sec_invest,
        r.res_adverse,
        r.res_other
    FROM audit.feed39_financial_restatements r
    WHERE r.company_fkey IN ({cik_str})
    ORDER BY r.company_fkey, r.file_date
""")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]

outfile = os.path.join(out_dir, "02_restatements.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(rows)

n_cos = len(set(r[0] for r in rows))
print(f"  Saved: {len(rows):,} rows | {n_cos} companies | -> 02_restatements.csv")

# =====================================================================
# 3. SOX 404 INTERNAL CONTROLS
# =====================================================================
print("\n--- Pulling SOX 404 internal controls (Audit Analytics feed11) ---")
time.sleep(5)

cur.execute(f"""
    SELECT
        company_fkey,
        fiscal_year,
        ic_op_fkey,
        count_weak,
        count_weak_acc_specific,
        count_weak_company_level
    FROM audit.feed11_sox_404_internal_controls
    WHERE CAST(company_fkey AS BIGINT) IN ({cik_str_int})
    ORDER BY company_fkey, fiscal_year
""")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]

outfile = os.path.join(out_dir, "03_sox404_internal_controls.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(rows)

n_cos = len(set(r[0] for r in rows))
print(f"  Saved: {len(rows):,} rows | {n_cos} companies | -> 03_sox404_internal_controls.csv")

# =====================================================================
# 4. CEO/CFO CHANGES (Audit Analytics f74)
# =====================================================================
print("\n--- Pulling CEO changes ---")
time.sleep(5)

cur.execute(f"""
    SELECT
        company_fkey,
        fiscal_year,
        ceo_change_flag,
        ceo_change_date
    FROM audit.f74_feed_support74_ceo_change
    WHERE CAST(company_fkey AS BIGINT) IN ({cik_str_int})
    ORDER BY company_fkey, fiscal_year
""")
rows_ceo = cur.fetchall()
cols_ceo = [d[0] for d in cur.description]

print(f"\n--- Pulling CFO changes ---")
time.sleep(5)

cur.execute(f"""
    SELECT
        company_fkey,
        fiscal_year,
        cfo_change_flag,
        cfo_change_date
    FROM audit.f74_feed_support74_cfo_change
    WHERE CAST(company_fkey AS BIGINT) IN ({cik_str_int})
    ORDER BY company_fkey, fiscal_year
""")
rows_cfo = cur.fetchall()
cols_cfo = [d[0] for d in cur.description]

outfile = os.path.join(out_dir, "04_ceo_changes.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols_ceo)
    writer.writerows(rows_ceo)

outfile = os.path.join(out_dir, "04b_cfo_changes.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols_cfo)
    writer.writerows(rows_cfo)

n_ceo = len(set(r[0] for r in rows_ceo))
n_cfo = len(set(r[0] for r in rows_cfo))
print(f"  CEO changes: {len(rows_ceo):,} rows | {n_ceo} companies")
print(f"  CFO changes: {len(rows_cfo):,} rows | {n_cfo} companies")

# =====================================================================
# 5. IPO DATA (Audit Analytics feed19)
# =====================================================================
print("\n--- Pulling IPO data ---")
time.sleep(5)

cur.execute(f"""
    SELECT
        company_fkey,
        ipo_date,
        offer_price,
        first_close_price,
        shares_offered
    FROM audit.feed19_ipo
    WHERE CAST(company_fkey AS BIGINT) IN ({cik_str_int})
    ORDER BY company_fkey
""")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]

outfile = os.path.join(out_dir, "05_ipo_data.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(rows)

n_cos = len(set(r[0] for r in rows))
print(f"  Saved: {len(rows):,} rows | {n_cos} companies | -> 05_ipo_data.csv")

# =====================================================================
# 6. GOING CONCERN OPINIONS
# =====================================================================
print("\n--- Pulling going concern opinions ---")
time.sleep(5)

cur.execute(f"""
    SELECT
        company_fkey,
        fiscal_year,
        going_concern
    FROM audit.f74_feed_support74_going_concern
    WHERE CAST(company_fkey AS BIGINT) IN ({cik_str_int})
    ORDER BY company_fkey, fiscal_year
""")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]

outfile = os.path.join(out_dir, "06_going_concern.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(rows)

n_cos = len(set(r[0] for r in rows))
print(f"  Saved: {len(rows):,} rows | {n_cos} companies | -> 06_going_concern.csv")

# =====================================================================
# 7. CRSP STOCK DATA (daily returns for post-IPO period)
# =====================================================================
print("\n--- Pulling CRSP stock data ---")
time.sleep(5)

# First need to link CIK to CRSP permno
# Use the CRSP-Compustat merged link table
cur.execute(f"""
    SELECT DISTINCT a.gvkey, a.lpermno as permno, b.cik
    FROM crsp_a_ccm.ccmxpf_lnkhist a
    JOIN comp.company b ON a.gvkey = b.gvkey
    WHERE CAST(b.cik AS BIGINT) IN ({cik_str_int})
    AND a.linktype IN ('LU', 'LC')
    AND a.linkprim IN ('P', 'C')
""")
links = cur.fetchall()
link_cols = [d[0] for d in cur.description]

outfile = os.path.join(out_dir, "07a_cik_permno_link.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(link_cols)
    writer.writerows(links)

permnos = sorted(set(int(r[1]) for r in links if r[1]))
cik_to_permno = {str(int(r[2])): int(r[1]) for r in links if r[1] and r[2]}
print(f"  CIK-PERMNO links: {len(links)} | Unique permnos: {len(permnos)}")

if permnos:
    permno_str = ", ".join(str(p) for p in permnos)

    time.sleep(5)

    # Pull monthly stock data (smaller than daily)
    cur.execute(f"""
        SELECT permno, date, ret, shrout, prc, vol
        FROM crsp_a_stock.msf
        WHERE permno IN ({permno_str})
        AND date >= '2015-01-01'
        ORDER BY permno, date
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    outfile = os.path.join(out_dir, "07b_crsp_monthly_returns.csv")
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)

    n_permnos = len(set(r[0] for r in rows))
    print(f"  CRSP monthly returns: {len(rows):,} rows | {n_permnos} securities | -> 07b_crsp_monthly_returns.csv")
else:
    print("  No PERMNO links found, skipping CRSP pull")

# =====================================================================
# 8. COMPUSTAT ANNUAL FUNDAMENTALS
# =====================================================================
print("\n--- Pulling Compustat annual fundamentals ---")
time.sleep(5)

cur.execute(f"""
    SELECT
        gvkey, datadate, fyear,
        at, lt, seq, ceq, csho, prcc_f,
        sale, revt, cogs, xsga, oibdp, ni, ib,
        act, lct, ch, dltt, dlc,
        oancf, capx, dp
    FROM comp.funda
    WHERE CAST(cik AS BIGINT) IN ({cik_str_int})
    AND indfmt = 'INDL' AND datafmt = 'STD' AND popsrc = 'D' AND consol = 'C'
    AND datadate >= '2015-01-01'
    ORDER BY gvkey, datadate
""")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]

outfile = os.path.join(out_dir, "08_compustat_annual.csv")
with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(rows)

n_cos = len(set(r[0] for r in rows))
print(f"  Saved: {len(rows):,} rows | {n_cos} companies | -> 08_compustat_annual.csv")

# =====================================================================
# SUMMARY
# =====================================================================
print(f"\n{'='*60}")
print(f"PANEL A OUTCOME DATA PULL COMPLETE")
print(f"{'='*60}")

files = os.listdir(out_dir)
for f in sorted(files):
    fp = os.path.join(out_dir, f)
    size = os.path.getsize(fp) / 1024
    with open(fp, "r", encoding="utf-8") as fh:
        n_rows = sum(1 for _ in fh) - 1
    print(f"  {f}: {n_rows:,} rows ({size:.0f} KB)")

cur.close()
conn.close()
