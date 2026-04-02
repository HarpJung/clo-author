"""
Supplement observer network using ISS/RiskMetrics director data.

=== PURPOSE ===

The observer network already includes CIQ and BoardEx connections. This script
adds a third source: ISS/RiskMetrics director data (risk_directors.rmdirectors).
ISS covers 2007-2025 with 268,451 rows of public company board membership data.

Unlike BoardEx, there is no WRDS crosswalk linking CIQ person IDs to ISS director
IDs. Instead, we match on normalized (first_name, last_name) pairs. Because name
matching is inherently noisy, we flag match quality and track potential false
positives from common names.

=== PIPELINE ===

Step 1: Load our CIQ observer persons from 01_observer_records.csv
Step 2: Pull ALL rows from risk_directors.rmdirectors (268K rows, batched)
Step 3: Name-match CIQ observers to ISS directors (exact on normalized names)
Step 4: Flag match quality: exact_unique vs exact_multiple
Step 5: Extract public company directorships with date info for matched observers
Step 6: Map CUSIPs to CIK via CRSP stocknames + CCM crosswalk
Step 7: Save crosswalk and positions files
Step 8: Print summary and comparison with BoardEx coverage

=== OUTPUT FILES ===
- Data/ISS/observer_iss_crosswalk.csv   -- CIQ personid <-> ISS director_detail_id, match quality
- Data/ISS/observer_iss_positions.csv   -- personid, cusip, ticker, company, dirsince, year_term_ends, meetingdate

=== NEXT STEP ===
After this script, integrate ISS positions into build_supplemented_network.py
alongside CIQ + BoardEx + Form 4 edges.
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import psycopg2, csv, os, time, re
import pandas as pd
import numpy as np

# =====================================================================
# DATABASE CONNECTION
# =====================================================================
conn = psycopg2.connect(
    host="wrds-pgdata.wharton.upenn.edu", port=9737, dbname="wrds",
    user="harperjung", password="Wwjksnm9087yu!")
cur = conn.cursor()

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
iss_dir = os.path.join(data_dir, "ISS")
os.makedirs(iss_dir, exist_ok=True)

print("=" * 100)
print("SUPPLEMENT OBSERVER NETWORK: ISS/RiskMetrics Directors")
print("=" * 100)


# =====================================================================
# Helper: normalize names for matching
# =====================================================================
def normalize_name(s):
    """Lowercase, strip whitespace, remove periods/commas/hyphens for matching."""
    if pd.isna(s) or s is None:
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"[.,\-'\"()]", "", s)       # remove punctuation
    s = re.sub(r"\s+", " ", s).strip()       # collapse whitespace
    return s


# =====================================================================
# STEP 1: Load our CIQ observer person IDs
# =====================================================================
print("\n--- Step 1: Load CIQ observers ---")
obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
obs["companyid"] = obs["companyid"].astype(str).str.replace(".0", "", regex=False)

# Extract unique (personid, firstname, lastname) tuples
obs_persons = obs[["personid", "firstname", "lastname"]].drop_duplicates(subset=["personid"])
# Use first token of first name only (matches ISS normalization below)
def normalize_first(s):
    """Normalize and take first token only (drop middle initial)."""
    n = normalize_name(s)
    parts = n.split()
    return parts[0] if parts else ""

obs_persons["fn_clean"] = obs_persons["firstname"].apply(normalize_first)
obs_persons["ln_clean"] = obs_persons["lastname"].apply(normalize_name)

ciq_personids = sorted(set(obs_persons["personid"]))
print(f"  CIQ observer personids: {len(ciq_personids):,}")
print(f"  Unique (first, last) name pairs: {obs_persons[['fn_clean','ln_clean']].drop_duplicates().shape[0]:,}")

# Build lookup: (fn_clean, ln_clean) -> list of personids
from collections import defaultdict
name_to_ciq = defaultdict(list)
for _, row in obs_persons.iterrows():
    key = (row["fn_clean"], row["ln_clean"])
    if key[0] and key[1]:  # skip blanks
        name_to_ciq[key].append(row["personid"])

print(f"  Unique normalized name keys: {len(name_to_ciq):,}")


# =====================================================================
# STEP 2: Pull ISS/RiskMetrics directors
# =====================================================================
# risk_directors.rmdirectors has ~268K rows spanning 2007-2025.
# We pull ALL rows in batches by year to stay within memory/timeout limits.
print("\n--- Step 2: Pull ISS/RiskMetrics rmdirectors ---")

# First, check the table schema
time.sleep(3)
try:
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'risk_directors' AND table_name = 'rmdirectors'
        ORDER BY ordinal_position
    """)
    schema_rows = cur.fetchall()
    print(f"  Table schema ({len(schema_rows)} columns):")
    for col_name, col_type in schema_rows[:20]:
        print(f"    {col_name:<35} {col_type}")
    if len(schema_rows) > 20:
        print(f"    ... and {len(schema_rows) - 20} more columns")
except Exception as e:
    print(f"  Schema query error: {str(e)[:120]}")
    conn.rollback()

# Get row count and year range
time.sleep(3)
try:
    cur.execute("""
        SELECT COUNT(*), MIN(EXTRACT(YEAR FROM meetingdate)),
               MAX(EXTRACT(YEAR FROM meetingdate))
        FROM risk_directors.rmdirectors
    """)
    total_rows, min_yr, max_yr = cur.fetchone()
    print(f"\n  Total rows: {total_rows:,}")
    print(f"  Year range: {int(min_yr)} - {int(max_yr)}")
except Exception as e:
    print(f"  Count query error: {str(e)[:120]}")
    conn.rollback()
    total_rows, min_yr, max_yr = 268451, 2007, 2025

# Pull in yearly batches
iss_rows = []
columns = [
    "director_detail_id", "first_name", "last_name", "fullname",
    "cusip", "ticker", "meetingdate", "dirsince", "year_term_ends",
    "primary_employer", "classification", "company_name"
]

# Check if company_name column exists; if not, adjust
col_query = """
    SELECT column_name FROM information_schema.columns
    WHERE table_schema = 'risk_directors' AND table_name = 'rmdirectors'
    AND column_name IN ('company_name', 'companyname', 'cname', 'issuer_name')
"""
time.sleep(2)
try:
    cur.execute(col_query)
    co_name_cols = [r[0] for r in cur.fetchall()]
    print(f"\n  Company name columns found: {co_name_cols}")
except Exception as e:
    co_name_cols = []
    conn.rollback()

# Also check for classification and company name columns
time.sleep(2)
try:
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'risk_directors' AND table_name = 'rmdirectors'
        AND column_name IN ('classification', 'director_type', 'dirtype',
                            'primary_employer', 'employer', 'name')
    """)
    class_cols = [r[0] for r in cur.fetchall()]
    print(f"  Classification/employer/name columns found: {class_cols}")
except Exception as e:
    class_cols = []
    conn.rollback()

# Build the SELECT column list from what actually exists
# We need: director_detail_id, first_name, last_name, fullname, cusip, ticker,
#           meetingdate, dirsince, year_term_ends, plus whatever name/class cols exist
time.sleep(3)
try:
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'risk_directors' AND table_name = 'rmdirectors'
        ORDER BY ordinal_position
    """)
    all_cols = [r[0] for r in cur.fetchall()]
    print(f"\n  All {len(all_cols)} columns: {all_cols}")
except Exception as e:
    all_cols = []
    conn.rollback()

# Build select list from available columns
wanted = [
    "director_detail_id", "first_name", "last_name", "fullname",
    "cusip", "ticker", "meetingdate", "dirsince", "year_term_ends"
]
# Add optional columns if they exist
# Note: ISS uses "name" for the company name (distinct from fullname for director)
for c in ["primary_employer", "classification", "company_name", "companyname", "name"]:
    if c in all_cols and c not in wanted:
        wanted.append(c)

# Filter to only columns that actually exist
select_cols = [c for c in wanted if c in all_cols]
select_str = ", ".join(select_cols)
print(f"\n  SELECT columns: {select_cols}")

# Pull year by year
years = list(range(int(min_yr), int(max_yr) + 1))
print(f"\n  Pulling {len(years)} year-batches...")

for yr in years:
    time.sleep(3)
    try:
        cur.execute(f"""
            SELECT {select_str}
            FROM risk_directors.rmdirectors
            WHERE EXTRACT(YEAR FROM meetingdate) = {yr}
        """)
        rows = cur.fetchall()
        iss_rows.extend(rows)
        print(f"    {yr}: {len(rows):>8,} rows  (cumulative: {len(iss_rows):>9,})")
    except Exception as e:
        print(f"    {yr}: ERROR - {str(e)[:100]}")
        conn.rollback()

iss_df = pd.DataFrame(iss_rows, columns=select_cols)
print(f"\n  ISS data loaded: {len(iss_df):,} rows")
print(f"  Unique director_detail_id: {iss_df['director_detail_id'].nunique():,}")
print(f"  Unique (first_name, last_name) pairs: {iss_df[['first_name','last_name']].drop_duplicates().shape[0]:,}")

# Show sample
print(f"\n  Sample row:")
if len(iss_df) > 0:
    sample = iss_df.iloc[0]
    for c in select_cols:
        print(f"    {c:<30} {sample[c]}")


# =====================================================================
# STEP 3: Name-match CIQ observers to ISS directors
# =====================================================================
print("\n--- Step 3: Name-match CIQ observers to ISS directors ---")

# Normalize ISS names.
# ISS first_name often includes middle initial (e.g., "DAVID M.").
# We take only the first token of the normalized first_name to match CIQ
# which typically stores just the given name.
# normalize_first() is defined in Step 1 above.
iss_df["fn_clean"] = iss_df["first_name"].apply(normalize_first)
iss_df["ln_clean"] = iss_df["last_name"].apply(normalize_name)

# Build ISS name -> list of director_detail_ids
iss_name_to_ids = defaultdict(set)
for _, row in iss_df[["director_detail_id", "fn_clean", "ln_clean"]].drop_duplicates().iterrows():
    key = (row["fn_clean"], row["ln_clean"])
    if key[0] and key[1]:
        iss_name_to_ids[key].add(row["director_detail_id"])

print(f"  ISS unique normalized name keys: {len(iss_name_to_ids):,}")

# Find overlapping name keys
common_keys = set(name_to_ciq.keys()) & set(iss_name_to_ids.keys())
print(f"  Name keys in common: {len(common_keys):,}")

# Build the crosswalk
crosswalk_rows = []
for key in common_keys:
    fn, ln = key
    ciq_pids = name_to_ciq[key]
    iss_dids = iss_name_to_ids[key]

    # Determine match quality
    n_ciq = len(ciq_pids)
    n_iss = len(iss_dids)

    if n_ciq == 1 and n_iss == 1:
        quality = "exact_unique"
    elif n_ciq == 1 and n_iss > 1:
        quality = "exact_multiple_iss"
    elif n_ciq > 1 and n_iss == 1:
        quality = "exact_multiple_ciq"
    else:
        quality = "exact_multiple_both"

    for pid in ciq_pids:
        for did in iss_dids:
            crosswalk_rows.append({
                "ciq_personid": pid,
                "iss_director_detail_id": did,
                "fn_clean": fn,
                "ln_clean": ln,
                "match_quality": quality,
                "n_ciq_persons": n_ciq,
                "n_iss_persons": n_iss
            })

xwalk = pd.DataFrame(crosswalk_rows)
print(f"\n  Crosswalk rows: {len(xwalk):,}")
print(f"  Unique CIQ personids matched: {xwalk['ciq_personid'].nunique():,}")
print(f"  Unique ISS director_detail_ids matched: {xwalk['iss_director_detail_id'].nunique():,}")

print(f"\n  Match quality distribution:")
for q, n in xwalk["match_quality"].value_counts().items():
    n_persons = xwalk[xwalk["match_quality"] == q]["ciq_personid"].nunique()
    print(f"    {q:<25} {n:>6,} rows  ({n_persons:>5,} unique CIQ persons)")


# =====================================================================
# STEP 4: Plausibility checks for ambiguous matches
# =====================================================================
print("\n--- Step 4: Plausibility checks ---")

# For exact_unique matches, we have high confidence.
# For exact_multiple, we flag but keep all — downstream analysis can filter.

# Check how many CIQ observers have employer info we could use for disambiguation
# Look at ISS primary_employer column if it exists
employer_col = None
for c in ["primary_employer", "employer"]:
    if c in iss_df.columns:
        employer_col = c
        break

if employer_col:
    matched_iss_ids = set(xwalk["iss_director_detail_id"])
    matched_iss = iss_df[iss_df["director_detail_id"].isin(matched_iss_ids)]
    n_with_employer = matched_iss[employer_col].notna().sum()
    print(f"  ISS rows with {employer_col}: {n_with_employer:,} / {len(matched_iss):,}")

    # Show common employers for matched observers
    if n_with_employer > 0:
        print(f"\n  Top 20 primary employers of matched ISS directors:")
        for emp, n in matched_iss[employer_col].value_counts().head(20).items():
            print(f"    {str(emp)[:60]:<60} {n:>5,}")

# Show common names that matched to multiple ISS persons
multi = xwalk[xwalk["match_quality"].str.contains("multiple")]
if len(multi) > 0:
    name_counts = multi.groupby(["fn_clean", "ln_clean"])["iss_director_detail_id"].nunique()
    name_counts = name_counts.sort_values(ascending=False)
    print(f"\n  Most ambiguous names (multiple ISS matches):")
    for (fn, ln), n in name_counts.head(15).items():
        print(f"    {fn} {ln}: {n} ISS director IDs")


# =====================================================================
# STEP 5: Extract ISS public company positions for matched observers
# =====================================================================
print("\n--- Step 5: Extract ISS public company positions ---")

# Merge crosswalk with ISS data to get positions
matched_iss_ids = set(xwalk["iss_director_detail_id"])
positions = iss_df[iss_df["director_detail_id"].isin(matched_iss_ids)].copy()

# Map ISS director_detail_id -> CIQ personid(s)
did_to_ciq = defaultdict(set)
for _, row in xwalk.iterrows():
    did_to_ciq[row["iss_director_detail_id"]].add(row["ciq_personid"])

# Expand positions to have one row per CIQ personid
pos_rows = []
for _, row in positions.iterrows():
    did = row["director_detail_id"]
    for pid in did_to_ciq.get(did, []):
        r = row.to_dict()
        r["ciq_personid"] = pid
        pos_rows.append(r)

pos_df = pd.DataFrame(pos_rows)
# Rename to avoid ambiguity with ISS source column name
if "director_detail_id" in pos_df.columns:
    pos_df.rename(columns={"director_detail_id": "iss_director_detail_id"}, inplace=True)
print(f"  Total position rows (after expanding to CIQ personids): {len(pos_df):,}")
print(f"  Unique CIQ personids with positions: {pos_df['ciq_personid'].nunique():,}")

# Company name column (ISS uses "name" for company name)
co_name_col = None
for c in ["company_name", "companyname", "name"]:
    if c in pos_df.columns:
        co_name_col = c
        break
if co_name_col:
    print(f"  Unique companies ({co_name_col}): {pos_df[co_name_col].nunique():,}")

# Date info
if "dirsince" in pos_df.columns:
    n_dirsince = pos_df["dirsince"].notna().sum()
    print(f"  Rows with dirsince: {n_dirsince:,} ({100*n_dirsince/max(1,len(pos_df)):.1f}%)")
if "year_term_ends" in pos_df.columns:
    n_yte = pos_df["year_term_ends"].notna().sum()
    print(f"  Rows with year_term_ends: {n_yte:,} ({100*n_yte/max(1,len(pos_df)):.1f}%)")
if "meetingdate" in pos_df.columns:
    n_md = pos_df["meetingdate"].notna().sum()
    print(f"  Rows with meetingdate: {n_md:,} ({100*n_md/max(1,len(pos_df)):.1f}%)")

# Classification distribution
class_col = None
for c in ["classification", "director_type"]:
    if c in pos_df.columns:
        class_col = c
        break
if class_col:
    print(f"\n  Director classification distribution:")
    for cls, n in pos_df[class_col].value_counts().head(10).items():
        print(f"    {str(cls):<40} {n:>6,}")


# =====================================================================
# STEP 6: Map CUSIPs to CIK via CRSP stocknames + CCM
# =====================================================================
print("\n--- Step 6: Map CUSIPs to CIK ---")

# Extract unique CUSIPs from ISS positions
if "cusip" in pos_df.columns:
    iss_cusips = sorted(set(pos_df["cusip"].dropna().astype(str).str.strip()))
    # ISS CUSIPs are 9-char (6 issuer + 2 issue + 1 check digit).
    # CRSP ncusip is 8-char (6 issuer + 2 issue, no check digit).
    # Match on first 8 characters.
    iss_cusip8 = sorted(set(c[:8] for c in iss_cusips if len(c) >= 8))
    iss_cusip6 = sorted(set(c[:6] for c in iss_cusips if len(c) >= 6))
    print(f"  Unique CUSIPs in ISS positions: {len(iss_cusips):,}")
    print(f"  Unique 8-char CUSIP headers: {len(iss_cusip8):,}")
    print(f"  Unique 6-char CUSIP issuers: {len(iss_cusip6):,}")
else:
    iss_cusips = []
    iss_cusip8 = []
    iss_cusip6 = []
    print("  WARNING: No cusip column found in ISS data")

# Query CRSP stocknames for CUSIP -> PERMNO -> CIK mapping
# Then use CCM for PERMNO -> GVKEY -> CIK
cusip_to_cik = {}

if len(iss_cusip8) > 0:
    # Batch query CRSP stocknames using 8-char CUSIP
    batch_size = 500
    crsp_rows = []

    for batch_start in range(0, len(iss_cusip8), batch_size):
        batch = iss_cusip8[batch_start:batch_start + batch_size]
        cusip_str = ", ".join(f"'{c}'" for c in batch)
        time.sleep(3)
        try:
            cur.execute(f"""
                SELECT DISTINCT ncusip, permno, comnam, ticker
                FROM crsp.stocknames
                WHERE ncusip IN ({cusip_str})
            """)
            rows = cur.fetchall()
            crsp_rows.extend(rows)
            print(f"    CRSP batch {batch_start//batch_size + 1}: {len(rows):,} matches (total: {len(crsp_rows):,})")
        except Exception as e:
            print(f"    CRSP batch error: {str(e)[:100]}")
            conn.rollback()

    crsp_match = pd.DataFrame(crsp_rows, columns=["ncusip", "permno", "comnam", "ticker_crsp"])
    print(f"\n  CRSP matches: {len(crsp_match):,} rows, {crsp_match['ncusip'].nunique():,} unique CUSIPs")

    # Now get PERMNO -> CIK via CCM linktable
    if len(crsp_match) > 0:
        permnos = sorted(set(str(int(p)) for p in crsp_match["permno"].dropna()))
        print(f"  Unique PERMNOs to look up: {len(permnos):,}")

        ccm_rows = []
        for batch_start in range(0, len(permnos), batch_size):
            batch = permnos[batch_start:batch_start + batch_size]
            pno_str = ", ".join(batch)
            time.sleep(3)
            try:
                cur.execute(f"""
                    SELECT DISTINCT lpermno, gvkey
                    FROM crsp.ccmxpf_lnkhist
                    WHERE lpermno IN ({pno_str})
                    AND linktype IN ('LC', 'LU', 'LS')
                    AND linkprim IN ('P', 'C')
                """)
                rows = cur.fetchall()
                ccm_rows.extend(rows)
            except Exception as e:
                print(f"    CCM batch error: {str(e)[:100]}")
                conn.rollback()

        ccm_df = pd.DataFrame(ccm_rows, columns=["permno", "gvkey"])
        print(f"  CCM matches: {len(ccm_df):,} rows, {ccm_df['permno'].nunique():,} unique PERMNOs")

        # GVKEY -> CIK via Compustat company table
        if len(ccm_df) > 0:
            gvkeys = sorted(set(str(g) for g in ccm_df["gvkey"].dropna()))
            gvk_str = ", ".join(f"'{g}'" for g in gvkeys[:2000])
            time.sleep(3)
            try:
                cur.execute(f"""
                    SELECT DISTINCT gvkey, cik
                    FROM comp.company
                    WHERE gvkey IN ({gvk_str})
                    AND cik IS NOT NULL
                """)
                cik_rows = cur.fetchall()
                gvkey_to_cik = {str(r[0]): str(r[1]) for r in cik_rows}
                print(f"  Compustat GVKEY->CIK: {len(gvkey_to_cik):,} mappings")
            except Exception as e:
                print(f"  Compustat CIK error: {str(e)[:100]}")
                gvkey_to_cik = {}
                conn.rollback()

            # If we need more than 2000, batch the rest
            if len(gvkeys) > 2000:
                for batch_start in range(2000, len(gvkeys), 2000):
                    batch = gvkeys[batch_start:batch_start + 2000]
                    gvk_str = ", ".join(f"'{g}'" for g in batch)
                    time.sleep(3)
                    try:
                        cur.execute(f"""
                            SELECT DISTINCT gvkey, cik
                            FROM comp.company
                            WHERE gvkey IN ({gvk_str})
                            AND cik IS NOT NULL
                        """)
                        for r in cur.fetchall():
                            gvkey_to_cik[str(r[0])] = str(r[1])
                    except Exception as e:
                        conn.rollback()

            # Chain: CUSIP8 -> PERMNO -> GVKEY -> CIK
            # Then map back to raw CUSIP for position-level linking
            cusip_permno = dict(zip(
                crsp_match["ncusip"].astype(str).str.strip(),
                crsp_match["permno"]
            ))
            # CCM returns lpermno as numeric; normalize to int-string for lookup
            permno_gvkey = {}
            for _, r in ccm_df.iterrows():
                pkey = str(int(r["permno"])) if pd.notna(r["permno"]) else None
                gval = str(r["gvkey"]).strip() if pd.notna(r["gvkey"]) else None
                if pkey and gval:
                    permno_gvkey[pkey] = gval

            # Build cusip8 -> CIK
            cusip8_to_cik = {}
            for cusip8 in iss_cusip8:
                permno_val = cusip_permno.get(cusip8)
                if permno_val is None:
                    continue
                pkey = str(int(permno_val)) if pd.notna(permno_val) else None
                gvkey = permno_gvkey.get(pkey) if pkey else None
                if gvkey is None:
                    continue
                cik = gvkey_to_cik.get(gvkey)
                if cik is None:
                    continue
                cusip8_to_cik[cusip8] = cik

            print(f"\n  CUSIP8 -> CIK mappings: {len(cusip8_to_cik):,} (of {len(iss_cusip8):,} unique CUSIP8s)")

            # Also build raw_cusip -> CIK so we can map from ISS position CUSIPs
            for raw_cusip in iss_cusips:
                c8 = raw_cusip[:8]
                if c8 in cusip8_to_cik:
                    cusip_to_cik[raw_cusip] = cusip8_to_cik[c8]

    print(f"  Raw CUSIP -> CIK mappings: {len(cusip_to_cik):,} (of {len(iss_cusips):,} unique raw CUSIPs)")

conn.close()
print("\n  WRDS connection closed.")


# =====================================================================
# STEP 7: Save crosswalk and positions
# =====================================================================
print("\n--- Step 7: Save results ---")

# Add CIK to positions (map from raw CUSIP string)
if "cusip" in pos_df.columns:
    pos_df["cusip_raw"] = pos_df["cusip"].astype(str).str.strip()
    pos_df["cusip6"] = pos_df["cusip_raw"].str[:6]
    pos_df["cik"] = pos_df["cusip_raw"].map(cusip_to_cik)

# Save crosswalk
xwalk.to_csv(os.path.join(iss_dir, "observer_iss_crosswalk.csv"), index=False)
print(f"  Saved: {os.path.join(iss_dir, 'observer_iss_crosswalk.csv')}")
print(f"    Rows: {len(xwalk):,}")

# Build positions output
pos_out_cols = ["ciq_personid", "iss_director_detail_id"]
for c in ["cusip", "cusip6", "ticker", "meetingdate", "dirsince", "year_term_ends", "cik"]:
    if c in pos_df.columns:
        pos_out_cols.append(c)
if co_name_col and co_name_col in pos_df.columns:
    pos_out_cols.append(co_name_col)
if employer_col and employer_col in pos_df.columns:
    pos_out_cols.append(employer_col)
if class_col and class_col in pos_df.columns:
    pos_out_cols.append(class_col)

# Deduplicate positions (same person at same company in same meeting year)
dedup_keys = ["ciq_personid"]
if "iss_director_detail_id" in pos_df.columns:
    dedup_keys.append("iss_director_detail_id")
if "cusip" in pos_df.columns:
    dedup_keys.append("cusip")
if "meetingdate" in pos_df.columns:
    dedup_keys.append("meetingdate")

pos_out = pos_df[pos_out_cols].drop_duplicates(subset=dedup_keys)
pos_out.to_csv(os.path.join(iss_dir, "observer_iss_positions.csv"), index=False)
print(f"  Saved: {os.path.join(iss_dir, 'observer_iss_positions.csv')}")
print(f"    Rows: {len(pos_out):,}")
print(f"    Unique CIQ personids: {pos_out['ciq_personid'].nunique():,}")
if "cusip" in pos_out.columns:
    print(f"    Unique CUSIPs: {pos_out['cusip'].nunique():,}")
if "cik" in pos_out.columns:
    n_with_cik = pos_out["cik"].notna().sum()
    print(f"    Rows with CIK: {n_with_cik:,} ({100*n_with_cik/max(1,len(pos_out)):.1f}%)")


# =====================================================================
# STEP 8: Summary and comparison with BoardEx
# =====================================================================
print(f"\n\n{'='*100}")
print("SUMMARY: ISS/RiskMetrics Supplement")
print(f"{'='*100}")

n_ciq_total = len(ciq_personids)
n_matched = xwalk["ciq_personid"].nunique()
n_unique_match = xwalk[xwalk["match_quality"] == "exact_unique"]["ciq_personid"].nunique()
n_multi_match = n_matched - n_unique_match

print(f"\n  CIQ observers total:             {n_ciq_total:>6,}")
print(f"  Matched to ISS:                   {n_matched:>6,}  ({100*n_matched/n_ciq_total:.1f}%)")
print(f"    exact_unique matches:            {n_unique_match:>6,}")
print(f"    ambiguous (multiple) matches:    {n_multi_match:>6,}")

print(f"\n  ISS positions for matched observers:")
print(f"    Total position-year rows:        {len(pos_out):>8,}")
if "cusip" in pos_out.columns:
    print(f"    Unique public companies (CUSIP): {pos_out['cusip'].nunique():>8,}")
if "cik" in pos_out.columns:
    print(f"    With CIK mapping:                {pos_out['cik'].notna().sum():>8,}")
if "dirsince" in pos_out.columns:
    print(f"    With dirsince (start year):      {pos_out['dirsince'].notna().sum():>8,}")
if "year_term_ends" in pos_out.columns:
    print(f"    With year_term_ends:             {pos_out['year_term_ends'].notna().sum():>8,}")

# Compare with BoardEx
bd_xwalk_path = os.path.join(data_dir, "BoardEx", "observer_boardex_crosswalk.csv")
if os.path.exists(bd_xwalk_path):
    print(f"\n  --- Comparison with BoardEx ---")
    bd_xwalk = pd.read_csv(bd_xwalk_path)
    bd_xwalk["ciq_personid"] = bd_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)

    bd_matched = set(bd_xwalk["ciq_personid"])
    iss_matched = set(xwalk["ciq_personid"])

    both = bd_matched & iss_matched
    bd_only = bd_matched - iss_matched
    iss_only = iss_matched - bd_matched
    neither = set(ciq_personids) - bd_matched - iss_matched

    print(f"    BoardEx only:    {len(bd_only):>6,}")
    print(f"    ISS only:        {len(iss_only):>6,}")
    print(f"    Both:            {len(both):>6,}")
    print(f"    Neither:         {len(neither):>6,}")

    # How many NEW companies does ISS add beyond BoardEx?
    bd_pos_path = os.path.join(data_dir, "BoardEx", "observer_boardex_positions.csv")
    if os.path.exists(bd_pos_path) and "cusip" in pos_out.columns:
        bd_pos = pd.read_csv(bd_pos_path)
        if "isin" in bd_pos.columns:
            # ISINs start with country code + CUSIP; extract for comparison
            bd_isins = set(bd_pos["isin"].dropna().astype(str))
            iss_cusips_set = set(pos_out["cusip"].dropna().astype(str))
            print(f"\n    ISS unique CUSIPs: {len(iss_cusips_set):,}")
            print(f"    BoardEx unique ISINs: {len(bd_isins):,}")
else:
    print(f"\n  (BoardEx crosswalk not found at {bd_xwalk_path} -- skipping comparison)")

# Year distribution of ISS positions
if "meetingdate" in pos_out.columns:
    pos_out["meeting_year"] = pd.to_datetime(pos_out["meetingdate"], errors="coerce").dt.year
    print(f"\n  ISS positions by meeting year:")
    for yr, n in pos_out["meeting_year"].value_counts().sort_index().items():
        if pd.notna(yr):
            print(f"    {int(yr)}: {n:>6,}")

print(f"\n\n{'='*100}")
print("Done. Output files:")
print(f"  1. {os.path.join(iss_dir, 'observer_iss_crosswalk.csv')}")
print(f"  2. {os.path.join(iss_dir, 'observer_iss_positions.csv')}")
print(f"{'='*100}")
