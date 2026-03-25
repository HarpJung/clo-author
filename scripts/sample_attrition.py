"""Generate sample attrition table across all data sources and merge steps."""

import csv, os
from collections import Counter

ciq_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data/CIQ_Extract"
edgar_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data/EDGAR_Extract"
data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"


def load_csv(fp):
    with open(fp, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def norm(val):
    val = str(val).strip()
    return val[:-2] if val.endswith(".0") else val


print("=" * 70)
print("SAMPLE ATTRITION TABLE")
print("=" * 70)

# =====================================================================
# STAGE 1: RAW SOURCE DATA
# =====================================================================
print("\n### STAGE 1: RAW SOURCE DATA (pre-merge) ###\n")

obs = load_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
adv = load_csv(os.path.join(ciq_dir, "02_advisory_board_records.csv"))
dirs = load_csv(os.path.join(ciq_dir, "03_directors_at_observer_companies.csv"))
cos = load_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
net = load_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
events = load_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
crosswalk = load_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))

obs_companies = set(norm(r["companyid"]) for r in obs)
obs_persons = set(norm(r["personid"]) for r in obs)
dir_companies = set(norm(r["companyid"]) for r in dirs)
dir_persons = set(norm(r["personid"]) for r in dirs)
adv_cos_in_obs = set(norm(r["companyid"]) for r in adv if norm(r["companyid"]) in obs_companies)
net_persons = set(norm(r["personid"]) for r in net)
net_companies = set(norm(r["companyid"]) for r in net)
event_companies = set(norm(r["companyid"]) for r in events)
xwalk_companies = set(norm(r["companyid"]) for r in crosswalk)

print("  CIQ Data:")
print(f"    Observer records:        {len(obs):>8,} records | {len(obs_companies):>6,} companies | {len(obs_persons):>6,} persons")
print(f"    Advisory board records:  {len(adv):>8,} records | {len(adv_cos_in_obs):>6,} cos overlapping w/ observer cos")
print(f"    Director records:        {len(dirs):>8,} records | {len(dir_companies):>6,} companies | {len(dir_persons):>6,} persons")
print(f"    Company details:                            {len(cos):>6,} companies")
print(f"    Network positions:       {len(net):>8,} records | {len(net_companies):>6,} connected cos | {len(net_persons):>6,} persons")
print(f"    Key dev events:          {len(events):>8,} records | {len(event_companies):>6,} companies")
print(f"    CIQ-CIK crosswalk:       {len(crosswalk):>8,} records | {len(xwalk_companies):>6,} companies")

# EDGAR
efts = load_csv(os.path.join(edgar_dir, "efts_board_observer_s1_hits.csv"))
exh = load_csv(os.path.join(edgar_dir, "exhibit_analysis_results.csv"))
s1b = load_csv(os.path.join(edgar_dir, "s1_body_observer_analysis.csv"))
s1_all = load_csv(os.path.join(edgar_dir, "all_s1_filings_2017_2026.csv"))

efts_ciks = set(h["ciks"].split("|")[0] for h in efts if h["ciks"])
exh_ok = [r for r in exh if r.get("fetch_status") in ("ok", "cached")]
exh_ciks = set(r["cik"] for r in exh_ok if r["cik"])
s1b_ok = [r for r in s1b if r.get("fetch_status") == "ok"]
s1b_ciks = set(r["cik"] for r in s1b_ok if r["cik"])
s1_all_ciks = set(r["cik"] for r in s1_all)

exh_fid = set(r["cik"] for r in exh_ok if r.get("has_fiduciary_manner") == "True")
exh_nofid = set(r["cik"] for r in exh_ok if r.get("has_no_fiduciary_duty") == "True")
exh_neither = exh_ciks - exh_fid - exh_nofid

print(f"\n  EDGAR Data:")
print(f"    S-1 filing universe:     {len(s1_all):>8,} filings | {len(s1_all_ciks):>6,} companies")
print(f"    EFTS 'board observer':   {len(efts):>8,} files   | {len(efts_ciks):>6,} companies")
print(f"    Exhibits fetched (R1):   {len(exh_ok):>8,} exhibits| {len(exh_ciks):>6,} companies")
print(f"      Fiduciary manner:                         {len(exh_fid):>6,} companies")
print(f"      No fiduciary duty:                        {len(exh_nofid):>6,} companies")
print(f"      Neither pattern:                          {len(exh_neither):>6,} companies")
print(f"    S-1 body fetched:        {len(s1b_ok):>8,} bodies  | {len(s1b_ciks):>6,} companies")

# =====================================================================
# STAGE 2: MERGE ATTRITION
# =====================================================================
print(f"\n\n### STAGE 2: MERGE ATTRITION (Table A) ###\n")

master = load_csv(os.path.join(data_dir, "table_a_company_master.csv"))

m_all = set(r["companyid"] for r in master)
m_dirs = set(r["companyid"] for r in master if int(r["n_directors"]) > 0)
m_obs = set(r["companyid"] for r in master if int(r["n_observers"]) > 0)
m_adv = set(r["companyid"] for r in master if int(r["n_advisory"]) > 0)
m_cik = set(r["companyid"] for r in master if r["cik"])
m_s1 = set(r["companyid"] for r in master if r["has_s1_observer_mention"] == "True")
m_edgar = set(r["companyid"] for r in master if r["edgar_filing_date"])
m_fid = set(r["companyid"] for r in master if r["edgar_has_fiduciary_manner"] == "True")
m_nofid = set(r["companyid"] for r in master if r["edgar_has_no_fiduciary_duty"] == "True")
m_events = set(r["companyid"] for r in master if int(r.get("n_exec_board_changes", 0)) > 0)

rows = [
    ("Start: All CIQ observer companies", len(m_all), ""),
    ("  with directors matched", len(m_dirs), f"-{len(m_all)-len(m_dirs):,}"),
    ("  with observers matched", len(m_obs), f"-{len(m_all)-len(m_obs):,}"),
    ("  with advisory board matched", len(m_adv), f"-{len(m_all)-len(m_adv):,}"),
    ("  with CIK (linkable to EDGAR/CRSP)", len(m_cik), f"-{len(m_all)-len(m_cik):,}"),
    ("    with S-1 'board observer' mention", len(m_s1), f"-{len(m_cik)-len(m_s1):,} from CIK"),
    ("    with EDGAR fiduciary language coded", len(m_edgar), f"-{len(m_s1)-len(m_edgar):,} from S-1"),
    ("      fiduciary manner (pre-2020)", len(m_fid), ""),
    ("      no fiduciary duty (post-2020)", len(m_nofid), ""),
    ("  with key dev events", len(m_events), f"-{len(m_all)-len(m_events):,}"),
]

print(f"  {'Step':<50} {'N':>8} {'Attrition':>12}")
print(f"  {'-'*70}")
for label, n, attr in rows:
    print(f"  {label:<50} {n:>8,} {attr:>12}")

# =====================================================================
# STAGE 3: OBSERVER NETWORK ATTRITION (Table B)
# =====================================================================
print(f"\n\n### STAGE 3: OBSERVER NETWORK ATTRITION (Table B) ###\n")

network_tb = load_csv(os.path.join(data_dir, "table_b_observer_network.csv"))
net_obs_persons = set(r["observer_personid"] for r in network_tb)
net_vc = set(r["vc_firm_companyid"] for r in network_tb)
net_obs_cos = set(r["observed_companyid"] for r in network_tb)

print(f"  {'Step':<50} {'N':>8}")
print(f"  {'-'*58}")
print(f"  {'All observer persons (CIQ)':<50} {len(obs_persons):>8,}")
print(f"  {'  with VC/PE affiliation identified':<50} {len(net_obs_persons):>8,}")
print(f"  {'  lost (no VC/PE link found)':<50} {len(obs_persons)-len(net_obs_persons):>8,}")
print(f"  {'Unique VC/PE firms in network':<50} {len(net_vc):>8,}")
print(f"  {'Unique observed companies in network':<50} {len(net_obs_cos):>8,}")
print(f"  {'Total observer-VC links':<50} {len(network_tb):>8,}")

# =====================================================================
# STAGE 4: CIQ-EDGAR OVERLAP
# =====================================================================
print(f"\n\n### STAGE 4: CIQ-EDGAR OVERLAP ANALYSIS ###\n")

ciq_ciks = set(r["cik"] for r in master if r["cik"])
overlap = ciq_ciks & exh_ciks
edgar_only = exh_ciks - ciq_ciks
ciq_only = ciq_ciks - exh_ciks

print(f"  CIQ companies with CIK:          {len(ciq_ciks):>6,}")
print(f"  EDGAR exhibit companies (by CIK): {len(exh_ciks):>6,}")
print(f"  Overlap (in BOTH):                {len(overlap):>6,}")
print(f"  In EDGAR only (not in CIQ):       {len(edgar_only):>6,}")
print(f"  In CIQ only (not in EDGAR):       {len(ciq_only):>6,}")

# =====================================================================
# STAGE 5: FINAL ANALYSIS-READY SUBSAMPLES
# =====================================================================
print(f"\n\n### STAGE 5: ANALYSIS-READY SUBSAMPLES ###\n")

private = [r for r in master if r["companytypename"] == "Private Company"]
public = [r for r in master if r["companytypename"] == "Public Company"]
us = [r for r in master if r["country"] == "United States"]
us_priv = [r for r in private if r["country"] == "United States"]
operating = [r for r in master if r["companystatustypename"] == "Operating"]

print(f"  {'Subsample':<55} {'Companies':>10}")
print(f"  {'-'*65}")
print(f"  {'All observer companies (Table A)':<55} {len(master):>10,}")
print(f"  {'  Private only':<55} {len(private):>10,}")
print(f"  {'  Public only':<55} {len(public):>10,}")
print(f"  {'  US only':<55} {len(us):>10,}")
print(f"  {'  US Private only':<55} {len(us_priv):>10,}")
print(f"  {'  Operating only':<55} {len(operating):>10,}")
print(f"  {'With CIK (for CRSP/Compustat/AuditAnalytics)':<55} {len(m_cik):>10,}")
print(f"  {'NVCA test - direct (fiduciary coded from exhibit)':<55} {len(m_edgar):>10,}")
print(f"  {'NVCA test - proxy (has CIK, use filing date)':<55} {len(m_cik):>10,}")
print(f"  {'Info leakage test (observers w/ VC network)':<55} {len(net_obs_persons):>10,}")
