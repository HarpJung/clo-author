"""Comprehensive check of BoardEx, ISS, and Form 4 data on WRDS.
Inventory all relevant tables, columns, row counts, and sample data
before pulling anything.
"""
import psycopg2, time, sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu", port=9737, dbname="wrds",
    user="harperjung", password="Wwjksnm9087yu!")
cur = conn.cursor()


def describe_table(schema, table, sample=True):
    """Print full column info and optional sample rows."""
    time.sleep(1)
    try:
        cur.execute(f"""SELECT column_name, data_type FROM information_schema.columns
                       WHERE table_schema = '{schema}' AND table_name = '{table}'
                       ORDER BY ordinal_position""")
        cols = cur.fetchall()
        cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        n = cur.fetchone()[0]
        print(f"\n  {schema}.{table} ({n:,} rows)")
        for c in cols:
            print(f"    {c[0]:<40} {c[1]}")
        if sample and n > 0:
            time.sleep(1)
            cur.execute(f"SELECT * FROM {schema}.{table} LIMIT 2")
            scols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            print(f"  Sample:")
            for r in rows:
                for c, v in zip(scols, r):
                    print(f"    {c:<40} {v}")
                print()
    except Exception as e:
        print(f"  ERROR: {str(e)[:80]}")
        conn.rollback()


# =====================================================================
# 1. BOARDEX — Board composition database
# =====================================================================
print("=" * 100)
print("1. BOARDEX — COMPREHENSIVE BOARD COMPOSITION")
print("=" * 100)

# Find the key BoardEx tables for US companies
time.sleep(2)
cur.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'boardex'
               AND (table_name LIKE 'na_%' OR table_name LIKE 'eur_%')
               AND (table_name LIKE '%dir%' OR table_name LIKE '%board%'
                    OR table_name LIKE '%company%' OR table_name LIKE '%char%')
               ORDER BY table_name""")
bd_tables = [r[0] for r in cur.fetchall()]
print(f"\nBoardEx board/director tables: {len(bd_tables)}")
for t in bd_tables:
    print(f"  {t}")

# Key tables to examine in detail
key_bd = [
    "na_dir_profile_details",      # Director profiles
    "na_board_characteristics",    # Board-level data
    "na_dir_profile_employment",   # Director employment history
    "na_board_dir_committees",     # Committee memberships
    "na_wrds_company_profile",     # Company identifiers
    "na_wrds_dir_profile",         # WRDS-enhanced director profiles
    "na_wrds_company_boards",      # WRDS-enhanced board composition
]

# Check which of these exist
time.sleep(1)
cur.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'boardex' ORDER BY table_name""")
all_bd = set(r[0] for r in cur.fetchall())

print(f"\n--- Key BoardEx Tables ---")
for t in key_bd:
    if t in all_bd:
        describe_table("boardex", t, sample=True)
    else:
        # Try similar names
        matches = [x for x in all_bd if any(k in x for k in t.split('_')[1:3])]
        if matches:
            print(f"\n  {t}: NOT FOUND. Similar: {matches[:5]}")

# Check for WRDS-enhanced BoardEx tables
time.sleep(1)
cur.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'boardex'
               AND table_name LIKE 'na_wrds%'
               ORDER BY table_name""")
wrds_bd = [r[0] for r in cur.fetchall()]
print(f"\nWRDS-enhanced BoardEx tables:")
for t in wrds_bd:
    describe_table("boardex", t, sample=False)

# Check crosswalk tables
print(f"\n--- BoardEx Crosswalks ---")
time.sleep(1)
cur.execute("""SELECT table_schema, table_name FROM information_schema.tables
               WHERE table_schema LIKE '%boardex%' AND table_schema LIKE '%link%'
               ORDER BY table_schema, table_name""")
link_tables = cur.fetchall()
for s, t in link_tables:
    describe_table(s, t, sample=True)

# =====================================================================
# 2. ISS — Director and governance data
# =====================================================================
print("\n\n" + "=" * 100)
print("2. ISS — DIRECTOR AND GOVERNANCE DATA")
print("=" * 100)

time.sleep(2)
cur.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'iss'
               AND (table_name LIKE '%director%' OR table_name LIKE '%board%'
                    OR table_name LIKE '%governance%' OR table_name LIKE '%company%')
               ORDER BY table_name""")
iss_tables = [r[0] for r in cur.fetchall()]
print(f"\nISS director/board/governance tables: {len(iss_tables)}")
for t in iss_tables:
    describe_table("iss", t, sample=False)

# Check for director-level data
time.sleep(1)
cur.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'iss' ORDER BY table_name""")
all_iss = [r[0] for r in cur.fetchall()]
print(f"\nAll ISS tables ({len(all_iss)}):")
for t in all_iss[:30]:
    print(f"  {t}")
if len(all_iss) > 30:
    print(f"  ... and {len(all_iss)-30} more")

# =====================================================================
# 3. FORM 4 (Thomson Reuters) — Already explored, check for additions
# =====================================================================
print("\n\n" + "=" * 100)
print("3. FORM 4 — ADDITIONAL FIELDS TO CHECK")
print("=" * 100)

# Check header table for full person info
describe_table("tfn", "header", sample=True)

# Check idfhist for historical identifier mapping
describe_table("tfn", "idfhist", sample=True)

# =====================================================================
# 4. WRDS CROSSWALK TABLES — Critical for matching
# =====================================================================
print("\n\n" + "=" * 100)
print("4. ALL WRDS CROSSWALK/LINK TABLES")
print("=" * 100)

time.sleep(2)
cur.execute("""SELECT table_schema, table_name FROM information_schema.tables
               WHERE (table_schema LIKE 'wrdsapps_plink%'
                      OR table_schema LIKE 'wrdsapps_link%')
               ORDER BY table_schema, table_name""")
link_tables = cur.fetchall()
print(f"\nWRDS person-linking tables: {len(link_tables)}")
for s, t in link_tables:
    time.sleep(0.5)
    try:
        cur.execute(f"SELECT COUNT(*) FROM {s}.{t}")
        n = cur.fetchone()[0]
        cur.execute(f"""SELECT column_name FROM information_schema.columns
                       WHERE table_schema = '{s}' AND table_name = '{t}'
                       ORDER BY ordinal_position""")
        cols = [r[0] for r in cur.fetchall()]
        print(f"  {s}.{t} ({n:,} rows): {cols[:10]}")
    except:
        conn.rollback()
        print(f"  {s}.{t}: ERROR")

# =====================================================================
# 5. EXECUCOMP — Executive compensation with board data
# =====================================================================
print("\n\n" + "=" * 100)
print("5. EXECUCOMP")
print("=" * 100)

time.sleep(2)
cur.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'comp' AND table_name LIKE '%exec%'
               ORDER BY table_name""")
exec_tables = [r[0] for r in cur.fetchall()]
print(f"ExecuComp tables: {exec_tables}")

for t in exec_tables[:3]:
    describe_table("comp", t, sample=False)

conn.close()
print("\n\nDone.")
