"""Build unified company-level and observer-level datasets."""

import csv, os, re
from collections import defaultdict, Counter

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
edgar_dir = os.path.join(data_dir, "EDGAR_Extract")
out_dir = data_dir  # save unified datasets in the root Data folder


def load_csv(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


print("Loading source files...")

# =====================================================================
# LOAD ALL SOURCE DATA
# =====================================================================
observers = load_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
advisory = load_csv(os.path.join(ciq_dir, "02_advisory_board_records.csv"))
directors = load_csv(os.path.join(ciq_dir, "03_directors_at_observer_companies.csv"))
companies = load_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
network = load_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
events = load_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))

exhibits = load_csv(os.path.join(edgar_dir, "exhibit_analysis_results.csv"))
s1_body = load_csv(os.path.join(edgar_dir, "s1_body_observer_analysis.csv"))
efts_hits = load_csv(os.path.join(edgar_dir, "efts_board_observer_s1_hits.csv"))

print(f"  Observers: {len(observers):,}")
print(f"  Advisory: {len(advisory):,}")
print(f"  Directors: {len(directors):,}")
print(f"  Companies: {len(companies):,}")
print(f"  Network positions: {len(network):,}")
print(f"  Events: {len(events):,}")
print(f"  Exhibits: {len(exhibits):,}")
print(f"  S-1 body: {len(s1_body):,}")


# Normalize companyid across all sources (some have '.0' suffix from float storage)
def normalize_id(val):
    if not val:
        return ""
    val = str(val).strip()
    if val.endswith(".0"):
        val = val[:-2]
    return val


for dataset in [observers, advisory, directors, companies, network, events]:
    for r in dataset:
        if "companyid" in r:
            r["companyid"] = normalize_id(r["companyid"])
        if "personid" in r:
            r["personid"] = normalize_id(r["personid"])

print("\nNormalized all companyid/personid values (removed .0 suffix)")


# =====================================================================
# STEP 1: BUILD COMPANY-LEVEL AGGREGATES FROM CIQ
# =====================================================================
print("\nStep 1: Building company-level aggregates...")

# Observer counts per company
obs_by_company = defaultdict(list)
for r in observers:
    obs_by_company[r["companyid"]].append(r)

# Director counts per company (exclude observers and advisory from director count)
dir_by_company = defaultdict(list)
for r in directors:
    title_lower = r["title"].lower()
    is_observer = "observer" in title_lower
    is_advisory = "advisory" in title_lower or "board advisor" in title_lower
    if not is_observer and not is_advisory:
        dir_by_company[r["companyid"]].append(r)

# Advisory board counts per company (only for observer companies)
adv_by_company = defaultdict(list)
observer_companyids = set(r["companyid"] for r in observers)
for r in advisory:
    if r["companyid"] in observer_companyids:
        adv_by_company[r["companyid"]].append(r)

# Event counts per company
event_by_company = defaultdict(lambda: defaultdict(int))
for r in events:
    event_by_company[r["companyid"]][r["keydeveventtypename"]] += 1


# =====================================================================
# STEP 2: BUILD CIK CROSSWALK (from WRDS - need to load separately)
# =====================================================================
print("Step 2: Loading CIQ-CIK crosswalk...")

# We need to load this from the WRDS query we already ran
# For now, we'll create a placeholder and fill it from a separate query
# The crosswalk file will be created by the WRDS query below
cik_crosswalk_file = os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv")
cik_map = {}  # companyid -> cik

if os.path.exists(cik_crosswalk_file):
    for r in load_csv(cik_crosswalk_file):
        if r.get("primaryflag", "1") == "1":
            cik_map[normalize_id(r["companyid"])] = r["cik"]
    print(f"  Loaded {len(cik_map):,} CIK mappings from cache")
else:
    print(f"  CIK crosswalk not yet downloaded - will create placeholder")


# =====================================================================
# STEP 3: BUILD EDGAR FIDUCIARY LANGUAGE LOOKUP
# =====================================================================
print("Step 3: Building EDGAR fiduciary language lookup...")

# From exhibits: CIK -> fiduciary language
edgar_fiduciary = {}  # cik -> dict
for r in exhibits:
    cik = r.get("cik", "")
    if not cik:
        continue
    date = r.get("file_date", "")
    has_fid = r.get("has_fiduciary_manner", "") == "True"
    has_no_fid = r.get("has_no_fiduciary_duty", "") == "True"
    obs_mentions = int(r.get("observer_mentions", 0))

    # Keep the most informative record per CIK
    if cik not in edgar_fiduciary or obs_mentions > edgar_fiduciary[cik].get("observer_mentions", 0):
        edgar_fiduciary[cik] = {
            "edgar_filing_date": date,
            "edgar_has_fiduciary_manner": has_fid,
            "edgar_has_no_fiduciary_duty": has_no_fid,
            "edgar_observer_mentions": obs_mentions,
            "edgar_source": "exhibit",
        }

# Supplement from S-1 body for companies not in exhibits
for r in s1_body:
    cik = r.get("cik", "")
    if not cik or cik in edgar_fiduciary:
        continue
    if r.get("fetch_status", "") != "ok":
        continue
    has_fid = r.get("has_fiduciary_manner", "") == "True"
    has_no_fid = r.get("has_no_fiduciary_duty", "") == "True"
    obs_mentions = int(r.get("observer_mentions", 0))
    edgar_fiduciary[cik] = {
        "edgar_filing_date": r.get("file_date", ""),
        "edgar_has_fiduciary_manner": has_fid,
        "edgar_has_no_fiduciary_duty": has_no_fid,
        "edgar_observer_mentions": obs_mentions,
        "edgar_source": "s1_body",
    }

# EFTS hit CIKs (companies with any "board observer" mention in S-1)
efts_ciks = set()
for h in efts_hits:
    if h.get("ciks"):
        for cik in h["ciks"].split("|"):
            efts_ciks.add(cik.strip())

print(f"  EDGAR fiduciary coding: {len(edgar_fiduciary):,} companies")
print(f"  EFTS S-1 observer mentions: {len(efts_ciks):,} unique CIKs")


# =====================================================================
# STEP 4: ASSEMBLE COMPANY-LEVEL MASTER (Table A)
# =====================================================================
print("\nStep 4: Assembling company-level master...")

company_master = []
for co in companies:
    cid = co["companyid"]

    # Observer aggregates
    obs_list = obs_by_company.get(cid, [])
    n_observers = len(obs_list)
    n_current_observers = sum(1 for r in obs_list if r.get("currentboardflag") == "1.0")
    observer_names = "; ".join(sorted(set(
        f"{r['firstname']} {r['lastname']}" for r in obs_list
    )))

    # Director aggregates (excluding observers and advisory)
    dir_list = dir_by_company.get(cid, [])
    n_directors = len(dir_list)
    n_current_directors = sum(1 for r in dir_list if r.get("currentboardflag") == "1.0")

    # Advisory board aggregates
    adv_list = adv_by_company.get(cid, [])
    n_advisory = len(adv_list)

    # Board composition
    total_board = n_directors + n_observers + n_advisory
    observer_ratio = n_observers / total_board if total_board > 0 else 0

    # CIK mapping
    cik = cik_map.get(cid, "")

    # EDGAR fiduciary language (requires CIK)
    edgar_data = edgar_fiduciary.get(cik, {})
    has_s1_observer = cik in efts_ciks if cik else False

    # NVCA treatment proxy: based on EDGAR filing date if available
    filing_date = edgar_data.get("edgar_filing_date", "")
    if filing_date:
        filing_year = int(filing_date[:4]) if filing_date[:4].isdigit() else 0
        nvca_post2020 = 1 if filing_year >= 2021 else 0
        nvca_transition = 1 if filing_year == 2020 else 0
    else:
        # Proxy from founding year (crude)
        yr = co.get("yearfounded", "")
        nvca_post2020 = ""
        nvca_transition = ""

    # Event counts
    evt = event_by_company.get(cid, {})

    row = {
        "companyid": cid,
        "companyname": co["companyname"],
        "companytypename": co["companytypename"],
        "companystatustypename": co["companystatustypename"],
        "yearfounded": co.get("yearfounded", ""),
        "city": co.get("city", ""),
        "country": co.get("country", ""),
        # Board composition
        "n_directors": n_directors,
        "n_current_directors": n_current_directors,
        "n_observers": n_observers,
        "n_current_observers": n_current_observers,
        "n_advisory": n_advisory,
        "total_board": total_board,
        "observer_ratio": round(observer_ratio, 4),
        "observer_names": observer_names,
        # CIK and EDGAR
        "cik": cik,
        "has_s1_observer_mention": has_s1_observer,
        "edgar_filing_date": edgar_data.get("edgar_filing_date", ""),
        "edgar_has_fiduciary_manner": edgar_data.get("edgar_has_fiduciary_manner", ""),
        "edgar_has_no_fiduciary_duty": edgar_data.get("edgar_has_no_fiduciary_duty", ""),
        "edgar_observer_mentions": edgar_data.get("edgar_observer_mentions", ""),
        "edgar_source": edgar_data.get("edgar_source", ""),
        # NVCA treatment
        "nvca_post2020": nvca_post2020,
        "nvca_transition_2020": nvca_transition,
        # Events
        "n_exec_board_changes": evt.get("Executive/Board Changes - Other", 0),
        "n_lawsuits": evt.get("Lawsuits & Legal Issues", 0),
        "n_restatements": evt.get("Restatements of Operating Results", 0),
        "n_earnings_announcements": evt.get("Announcements of Earnings", 0),
        "n_financing_events": evt.get("Seeking Financing/Partners", 0),
        "n_bankruptcy": evt.get("Bankruptcy - Other", 0),
    }
    company_master.append(row)

# Save Table A
outfile_a = os.path.join(out_dir, "table_a_company_master.csv")
with open(outfile_a, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=company_master[0].keys())
    writer.writeheader()
    writer.writerows(company_master)

print(f"  Table A saved: {len(company_master):,} rows -> {outfile_a}")


# =====================================================================
# STEP 5: BUILD OBSERVER NETWORK (Table B)
# =====================================================================
print("\nStep 5: Building observer network...")

# For each observer, find their VC/PE affiliation and other portfolio companies
# Step 5a: Identify each observer's VC/PE firm(s)
observer_persons = set(r["personid"] for r in observers)

person_vc_firms = defaultdict(set)  # personid -> set of (vc_companyid, vc_name)
person_portfolio = defaultdict(set)  # personid -> set of (portfolio_companyid, portfolio_name)
person_observed = defaultdict(set)  # personid -> set of (observed_companyid, observed_name)

# Build observed companies per person
for r in observers:
    person_observed[r["personid"]].add((r["companyid"], ""))

# Build VC affiliations and portfolio companies from network data
vc_pe_types = {"Private Investment Firm", "Public Investment Firm", "Private Fund"}

for r in network:
    pid = r["personid"]
    ctype = r.get("companytypename", "")
    cid = r["companyid"]
    cname = r.get("companyname", "")
    title = r.get("title", "")

    if ctype in vc_pe_types:
        person_vc_firms[pid].add((cid, cname))
    elif "Director" in title or "Board" in title:
        person_portfolio[pid].add((cid, cname))

# Build network table: observer -> VC -> portfolio companies
network_rows = []
for pid in observer_persons:
    observed = person_observed.get(pid, set())
    vc_firms = person_vc_firms.get(pid, set())
    portfolio = person_portfolio.get(pid, set())

    # Get observer name
    obs_records = [r for r in observers if r["personid"] == pid]
    if not obs_records:
        continue
    obs_name = f"{obs_records[0]['firstname']} {obs_records[0]['lastname']}"

    for obs_cid, _ in observed:
        # Find the company name from the observer record
        obs_company = next((r["companyid"] for r in obs_records if r["companyid"] == obs_cid), obs_cid)
        obs_company_name = next(
            (co["companyname"] for co in companies if co["companyid"] == obs_cid),
            ""
        )

        for vc_cid, vc_name in vc_firms:
            row = {
                "observer_personid": pid,
                "observer_name": obs_name,
                "observed_companyid": obs_cid,
                "observed_companyname": obs_company_name,
                "vc_firm_companyid": vc_cid,
                "vc_firm_name": vc_name,
                "n_portfolio_companies": len(portfolio),
                "observer_title": obs_records[0].get("title", ""),
                "is_current_observer": obs_records[0].get("currentboardflag", "") == "1.0",
            }
            network_rows.append(row)

# Save Table B
outfile_b = os.path.join(out_dir, "table_b_observer_network.csv")
if network_rows:
    with open(outfile_b, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=network_rows[0].keys())
        writer.writeheader()
        writer.writerows(network_rows)

print(f"  Table B saved: {len(network_rows):,} rows -> {outfile_b}")


# =====================================================================
# SUMMARY STATISTICS
# =====================================================================
print(f"\n{'='*60}")
print(f"UNIFIED DATASET SUMMARY")
print(f"{'='*60}")

print(f"\nTable A: Company-Level Master")
print(f"  Total companies: {len(company_master):,}")
with_cik = sum(1 for r in company_master if r["cik"])
with_edgar = sum(1 for r in company_master if r["edgar_filing_date"])
with_fid = sum(1 for r in company_master if r["edgar_has_fiduciary_manner"] == True)
with_no_fid = sum(1 for r in company_master if r["edgar_has_no_fiduciary_duty"] == True)
print(f"  With CIK mapping: {with_cik:,}")
print(f"  With EDGAR fiduciary coding: {with_edgar:,}")
print(f"    Fiduciary manner (pre-2020): {with_fid}")
print(f"    No fiduciary duty (post-2020): {with_no_fid}")

avg_dirs = sum(r["n_directors"] for r in company_master) / len(company_master)
avg_obs = sum(r["n_observers"] for r in company_master) / len(company_master)
avg_adv = sum(r["n_advisory"] for r in company_master) / len(company_master)
print(f"  Avg directors per company: {avg_dirs:.1f}")
print(f"  Avg observers per company: {avg_obs:.1f}")
print(f"  Avg advisory members per company: {avg_adv:.1f}")

print(f"\nTable B: Observer Network")
unique_observers = len(set(r["observer_personid"] for r in network_rows))
unique_vc_firms = len(set(r["vc_firm_companyid"] for r in network_rows))
print(f"  Total network links: {len(network_rows):,}")
print(f"  Unique observers with VC affiliation: {unique_observers:,}")
print(f"  Unique VC/PE firms: {unique_vc_firms:,}")
