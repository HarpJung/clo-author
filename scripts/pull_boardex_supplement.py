"""
Supplement observer network using BoardEx + Form 4.

=== PURPOSE ===

The original observer network (from pull_panel_b_and_c.py) only captures
connections that appear in CIQ Professionals data. CIQ misses roughly
half of all observer-to-public-firm connections. This script supplements
the network with two additional data sources:

1. BoardEx — a comprehensive database of board members and executives at
   public companies. We match our CIQ observers to BoardEx director IDs
   using the WRDS person-matching crosswalk, then pull ALL their positions
   to find public company connections CIQ missed.

2. Form 4 — SEC insider trading filings. If an observer filed a Form 4
   at a public company, they definitively have a connection there. We use
   the WRDS TR-CIQ crosswalk to match Form 4 filers back to our CIQ observers.

=== PIPELINE ===

Step 1: Load our CIQ observer person IDs
Step 2: Match CIQ person IDs to BoardEx director IDs via WRDS crosswalk
        (wrdsapps_plink_boardex_ciq.boardex_ciq_link)
Step 3: Pull ALL employment/board positions from BoardEx for matched observers
        (boardex.na_wrds_dir_profile_emp — ~8.5M total rows, we pull for our subset)
Step 4: Pull company identifiers (CIK, ticker, ISIN) from BoardEx
        (boardex.na_wrds_company_names)
Step 5: Build preliminary supplemented network by combining new connections
Step 6: Print comparison statistics (old vs. supplemented)

=== OUTPUT FILES ===
- Data/BoardEx/observer_boardex_crosswalk.csv  — CIQ personid <-> BoardEx directorid mapping
- Data/BoardEx/observer_boardex_positions.csv  — All positions held by matched observers
- Data/BoardEx/observer_boardex_companies.csv  — Company identifiers (CIK, ticker, ISIN)

=== NEXT STEP ===
After this script, run build_supplemented_network.py to combine CIQ + BoardEx + Form 4
edges into a unified network and identify new CIKs needing CRSP returns.
"""

import psycopg2, csv, os, time, sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
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
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 100)
print("SUPPLEMENT OBSERVER NETWORK: BoardEx + Form 4")
print("=" * 100)

# =====================================================================
# STEP 1: Load our CIQ observer person IDs
# =====================================================================
# These are the ~4,915 unique individuals with "observer" in their CIQ
# position title. We need to find their BoardEx equivalents.
print("\n--- Step 1: Load CIQ observers ---")
obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
obs["companyid"] = obs["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_personids = sorted(set(obs["personid"]))
print(f"  CIQ observer personids: {len(ciq_personids):,}")

# Build a mapping: each observer -> set of private companies they observe at.
# We need this later to create (observed private co, public portfolio co) edges.
obs_to_companies = obs.groupby("personid")["companyid"].apply(set).to_dict()

# =====================================================================
# STEP 2: CIQ -> BoardEx person crosswalk
# =====================================================================
# The WRDS crosswalk table (wrdsapps_plink_boardex_ciq.boardex_ciq_link)
# matches CIQ person IDs to BoardEx director IDs using name matching
# and security-level matching. Each match has a quality score.
#
# We query in batches of 1,000 because the full list of ~4,915 person IDs
# can be too large for a single IN clause.
print("\n--- Step 2: CIQ -> BoardEx crosswalk ---")
time.sleep(3)

batch_size = 1000
bd_xwalk_rows = []

for batch_start in range(0, len(ciq_personids), batch_size):
    batch = ciq_personids[batch_start:batch_start + batch_size]
    pid_str = ", ".join(f"'{p}'" for p in batch)
    time.sleep(2)
    try:
        cur.execute(f"""
            SELECT directorid, directorname, forename1, surname, personid,
                   firstname, lastname, score, matchstyle
            FROM wrdsapps_plink_boardex_ciq.boardex_ciq_link
            WHERE personid::text IN ({pid_str})
        """)
        rows = cur.fetchall()
        bd_xwalk_rows.extend(rows)
        print(f"  Batch {batch_start//batch_size + 1}: {len(rows):,} matches (total: {len(bd_xwalk_rows):,})")
    except Exception as e:
        print(f"  Batch error: {str(e)[:80]}")
        conn.rollback()

bd_xwalk = pd.DataFrame(bd_xwalk_rows, columns=[
    "directorid",     # BoardEx's unique person identifier
    "directorname",   # Full name in BoardEx
    "forename1",      # First name in BoardEx
    "surname",        # Last name in BoardEx
    "ciq_personid",   # CIQ person ID (our identifier)
    "firstname",      # First name in CIQ
    "lastname",       # Last name in CIQ
    "score",          # Match quality score (lower = better match)
    "matchstyle"      # How the match was made (name, security, etc.)
])
bd_xwalk["ciq_personid"] = bd_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)

print(f"\n  Total matches: {len(bd_xwalk):,}")
print(f"  Unique CIQ personids matched: {bd_xwalk['ciq_personid'].nunique():,} (of {len(ciq_personids):,})")
print(f"  Unique BoardEx directorids: {bd_xwalk['directorid'].nunique():,}")
print(f"  Match score distribution:")
for s, n in bd_xwalk["score"].value_counts().sort_index().head(10).items():
    print(f"    score={s}: {n:,}")

# Save crosswalk for use by build_supplemented_network.py
os.makedirs(os.path.join(data_dir, "BoardEx"), exist_ok=True)
bd_xwalk.to_csv(os.path.join(data_dir, "BoardEx", "observer_boardex_crosswalk.csv"), index=False)

# =====================================================================
# STEP 3: Pull ALL BoardEx positions for matched observers
# =====================================================================
# Now that we know which BoardEx director IDs correspond to our observers,
# pull every position they have ever held. The key table is
# boardex.na_wrds_dir_profile_emp, which contains employment and board
# positions for North American directors.
#
# Key columns:
#   - brdposition: "Yes" if this is a board seat (vs. executive role)
#   - ned: "Yes" if non-executive director
#   - orgtype: "Quoted"/"Listed" for public companies, "Private" for private
#   - rolename: e.g., "Non-Executive Director", "Chairman", "CEO"
#   - datestartrole / dateendrole: tenure dates
#
# We query in batches of 200 because each director can have 50+ positions,
# so the result set gets large quickly.
print("\n--- Step 3: Pull BoardEx employment/board positions ---")

directorids = sorted(set(str(int(d)) for d in bd_xwalk["directorid"].dropna()))
print(f"  BoardEx directorids to query: {len(directorids):,}")

bd_positions = []
batch_size = 200

for batch_start in range(0, len(directorids), batch_size):
    batch = directorids[batch_start:batch_start + batch_size]
    did_str = ", ".join(batch)
    time.sleep(3)

    try:
        cur.execute(f"""
            SELECT directorid, directorname, companyname, companyid,
                   rolename, datestartrole, dateendrole,
                   brdposition, ned, orgtype, isin, sector,
                   hocountryname, rowtype
            FROM boardex.na_wrds_dir_profile_emp
            WHERE directorid IN ({did_str})
            ORDER BY directorid, datestartrole
        """)
        rows = cur.fetchall()
        bd_positions.extend(rows)
        n_batch = batch_start // batch_size + 1
        n_total = (len(directorids) + batch_size - 1) // batch_size
        print(f"  Batch {n_batch}/{n_total}: {len(rows):,} positions (total: {len(bd_positions):,})")
    except Exception as e:
        print(f"  Batch error: {str(e)[:80]}")
        conn.rollback()

pos_cols = [
    "directorid",      # BoardEx person ID
    "directorname",    # Person's name
    "companyname",     # Company where they hold this position
    "companyid",       # BoardEx company ID
    "rolename",        # Position title (e.g., "Non-Executive Director")
    "datestartrole",   # Start date of this role
    "dateendrole",     # End date (null if current)
    "brdposition",     # "Yes" if this is a board-level position
    "ned",             # "Yes" if non-executive director
    "orgtype",         # "Quoted", "Listed", "Private", "Government", etc.
    "isin",            # ISIN identifier for the company
    "sector",          # Industry sector
    "hocountryname",   # Country of headquarters
    "rowtype"          # "Board" or "Employment"
]
bd_pos_df = pd.DataFrame(bd_positions, columns=pos_cols)
print(f"\n  Total positions: {len(bd_pos_df):,}")
print(f"  Unique companies: {bd_pos_df['companyname'].nunique():,}")

# Save all positions for use by build_supplemented_network.py
bd_pos_df.to_csv(os.path.join(data_dir, "BoardEx", "observer_boardex_positions.csv"), index=False)

# =====================================================================
# STEP 4: Pull company identifiers (CIK, ticker) from BoardEx
# =====================================================================
# BoardEx uses its own company IDs. To link to CRSP/Compustat, we need
# CIK codes. The na_wrds_company_names table provides CIK, ticker, and
# ISIN for BoardEx companies.
print("\n--- Step 4: Pull BoardEx company identifiers ---")

bd_companyids = sorted(set(str(int(c)) for c in bd_pos_df["companyid"].dropna()))
print(f"  Unique BoardEx companies: {len(bd_companyids):,}")

time.sleep(3)
bd_company_ids = []
for batch_start in range(0, len(bd_companyids), batch_size):
    batch = bd_companyids[batch_start:batch_start + batch_size]
    cid_str = ", ".join(batch)
    time.sleep(2)
    try:
        cur.execute(f"""
            SELECT DISTINCT boardid, companyid, boardname, ticker, isin, cikcode, hocountryname
            FROM boardex.na_wrds_company_names
            WHERE companyid IN ({cid_str})
        """)
        rows = cur.fetchall()
        bd_company_ids.extend(rows)
    except Exception as e:
        conn.rollback()

co_cols = [
    "boardid",        # BoardEx board-level ID
    "companyid",      # BoardEx company ID
    "boardname",      # Company name
    "ticker",         # Stock ticker
    "isin",           # International Securities Identification Number
    "cikcode",        # SEC CIK number (what we need for CRSP linking)
    "hocountryname"   # Country of headquarters
]
bd_co_df = pd.DataFrame(bd_company_ids, columns=co_cols)
bd_co_df = bd_co_df.drop_duplicates(subset=["companyid"])
print(f"  Companies with identifiers: {len(bd_co_df):,}")
print(f"  With CIK: {bd_co_df['cikcode'].notna().sum():,}")
print(f"  With ticker: {bd_co_df['ticker'].notna().sum():,}")
print(f"  With ISIN: {bd_co_df['isin'].notna().sum():,}")

bd_co_df.to_csv(os.path.join(data_dir, "BoardEx", "observer_boardex_companies.csv"), index=False)

conn.close()

# =====================================================================
# STEP 5: Build preliminary supplemented network
# =====================================================================
# Combine the original CIQ edges with the new BoardEx connections.
# A "new" connection is one where a CIQ observer also holds a board
# position at a public company in BoardEx that was NOT already in the
# CIQ network.
#
# We also count Form 4 connections for comparison, though the full
# Form 4 integration happens in build_supplemented_network.py.
print("\n--- Step 5: Build supplemented network ---")

# Original CIQ network (from pull_panel_b_and_c.py)
orig_edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
orig_edges["observer_personid"] = orig_edges["observer_personid"].astype(str).str.replace(".0", "", regex=False)
print(f"  Original CIQ network: {len(orig_edges):,} edges, {orig_edges['observer_personid'].nunique():,} observers")

# Map BoardEx directorid -> CIQ personid so we can merge
bd_did_to_ciq = dict(zip(bd_xwalk["directorid"], bd_xwalk["ciq_personid"]))

# Filter BoardEx positions to board-level roles at public companies.
# We want directors, chairmen, and senior officers at listed/quoted firms.
# These are the positions that create the information bridge to public firms.
bd_board = bd_pos_df[
    (bd_pos_df["brdposition"] == "Yes") |  # Formal board position
    (bd_pos_df["rolename"].str.contains("Director|Board|Chairman|CEO|CFO|Officer", case=False, na=False))
].copy()
bd_board = bd_board[bd_board["orgtype"].isin(["Quoted", "Listed"])]  # Public companies only
bd_board["ciq_personid"] = bd_board["directorid"].map(bd_did_to_ciq)
bd_board = bd_board.dropna(subset=["ciq_personid"])  # Drop if no CIQ match

# Map BoardEx company IDs to CIK codes for CRSP linking
bd_co_cik = dict(zip(bd_co_df["companyid"], bd_co_df["cikcode"]))
bd_board["cik"] = bd_board["companyid"].map(bd_co_cik)

print(f"  BoardEx board positions at public companies: {len(bd_board):,}")
print(f"  With CIK: {bd_board['cik'].notna().sum():,}")
print(f"  Unique observers: {bd_board['ciq_personid'].nunique():,}")
print(f"  Unique companies: {bd_board['companyname'].nunique():,}")

# Load Form 4 trades for comparison statistics.
# The TR-CIQ crosswalk maps Thomson Reuters insider person IDs to CIQ person IDs.
trades = pd.read_csv(os.path.join(data_dir, "Form4", "observer_form4_trades.csv"))
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
tr_xwalk["tr_personid"] = tr_xwalk["tr_personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk["ciq_personid"] = tr_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
tr_to_ciq = dict(zip(tr_xwalk["tr_personid"], tr_xwalk["ciq_personid"]))
trades["ciq_personid"] = trades["personid"].map(tr_to_ciq)
trades = trades.dropna(subset=["ciq_personid"])

# Unique Form 4 connections (one row per observer-company pair)
f4_connections = trades[["ciq_personid", "cname", "cusip6", "cusip2", "ticker"]].drop_duplicates()
print(f"\n  Form 4 unique connections: {len(f4_connections):,}")
print(f"  Unique observers: {f4_connections['ciq_personid'].nunique():,}")
print(f"  Unique companies: {f4_connections['cname'].nunique():,}")

# =====================================================================
# STEP 6: Summary comparison (old vs. supplemented)
# =====================================================================
# Compare the original CIQ-only network with the supplemented network
# to understand how much coverage each source adds.
print(f"\n\n{'='*100}")
print("NETWORK COMPARISON: Old vs Supplemented")
print(f"{'='*100}")

old_observers = set(orig_edges["observer_personid"])
old_companies = set(orig_edges["portfolio_companyname"].dropna())
old_edges_n = len(orig_edges)

# How many NEW observers and companies does BoardEx add?
bd_new_observers = set(bd_board["ciq_personid"]) - old_observers
bd_new_companies = set(bd_board["companyname"].dropna()) - old_companies
bd_total_positions = len(bd_board)

# How many NEW observers and companies does Form 4 add?
f4_new_observers = set(f4_connections["ciq_personid"]) - old_observers
f4_new_companies = set(f4_connections["cname"].dropna()) - old_companies

print(f"\n  {'Source':<25} {'Observers':>12} {'Companies':>12} {'Positions':>12}")
print(f"  {'-'*61}")
print(f"  {'Original CIQ':<25} {len(old_observers):>12,} {len(old_companies):>12,} {old_edges_n:>12,}")
print(f"  {'+ BoardEx':<25} {'+' + str(len(bd_new_observers)):>12} {'+' + str(len(bd_new_companies)):>12} {'+' + str(bd_total_positions):>12}")
print(f"  {'+ Form 4':<25} {'+' + str(len(f4_new_observers)):>12} {'+' + str(len(f4_new_companies)):>12} {'+' + str(len(f4_connections)):>12}")

all_observers = old_observers | set(bd_board["ciq_personid"]) | set(f4_connections["ciq_personid"])
all_companies = old_companies | set(bd_board["companyname"].dropna()) | set(f4_connections["cname"].dropna())
print(f"  {'TOTAL':<25} {len(all_observers):>12,} {len(all_companies):>12,}")

# What roles do observers hold at public companies in BoardEx?
print(f"\n  BoardEx role distribution:")
for role, n in bd_board["rolename"].value_counts().head(15).items():
    print(f"    {role:<50} {n:>6,}")

# What types of organizations appear in the full BoardEx position data?
print(f"\n  BoardEx org type:")
for ot, n in bd_pos_df["orgtype"].value_counts().items():
    print(f"    {ot:<30} {n:>8,}")

print("\n\nDone.")
