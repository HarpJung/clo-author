"""Complete data inventory: every dataset, its characteristics, variables, and coverage."""

import os
import csv
import pandas as pd
import numpy as np

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
edgar_dir = os.path.join(data_dir, "EDGAR_Extract")
fd_dir = os.path.join(data_dir, "FormD")

def load(fp):
    with open(fp, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def norm(val):
    val = str(val).strip()
    return val[:-2] if val.endswith(".0") else val

print("=" * 80)
print("COMPLETE DATA INVENTORY")
print("=" * 80)


# =====================================================================
# DATASET 1: CIQ Observer Records
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 1: CIQ Observer Records")
print("  File: CIQ_Extract/01_observer_records.csv")
print("=" * 80)

obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
print(f"\n  Rows: {len(obs):,}")
print(f"  Unique companies: {obs['companyid'].nunique():,}")
print(f"  Unique persons: {obs['personid'].nunique():,}")
print(f"\n  Variables:")
for col in obs.columns:
    non_null = obs[col].notna().sum()
    print(f"    {col:30} non-null={non_null:>6,}  dtype={obs[col].dtype}")
print(f"\n  Title distribution (top 5):")
for val, cnt in obs["title"].value_counts().head(5).items():
    print(f"    {val:40} {cnt:>5,}")


# =====================================================================
# DATASET 2: CIQ Advisory Board Records
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 2: CIQ Advisory Board Records")
print("  File: CIQ_Extract/02_advisory_board_records.csv")
print("=" * 80)

adv = pd.read_csv(os.path.join(ciq_dir, "02_advisory_board_records.csv"))
print(f"\n  Rows: {len(adv):,}")
print(f"  Unique companies: {adv['companyid'].nunique():,}")
print(f"  Unique persons: {adv['personid'].nunique():,}")
print(f"\n  Variables: {list(adv.columns)}")


# =====================================================================
# DATASET 3: CIQ Directors at Observer Companies
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 3: CIQ Directors at Observer Companies")
print("  File: CIQ_Extract/03_directors_at_observer_companies.csv")
print("=" * 80)

dirs = pd.read_csv(os.path.join(ciq_dir, "03_directors_at_observer_companies.csv"))
print(f"\n  Rows: {len(dirs):,}")
print(f"  Unique companies: {dirs['companyid'].nunique():,}")
print(f"  Unique persons: {dirs['personid'].nunique():,}")
print(f"\n  Variables: {list(dirs.columns)}")
print(f"\n  Top roles (top 10):")
for val, cnt in dirs["title"].value_counts().head(10).items():
    print(f"    {val:40} {cnt:>5,}")


# =====================================================================
# DATASET 4: CIQ Observer Company Details
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 4: CIQ Observer Company Details")
print("  File: CIQ_Extract/04_observer_company_details.csv")
print("=" * 80)

cos = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
print(f"\n  Rows: {len(cos):,}")
print(f"\n  Variables and coverage:")
for col in cos.columns:
    non_null = cos[col].notna().sum()
    n_unique = cos[col].nunique()
    print(f"    {col:30} non-null={non_null:>5,} ({100*non_null/len(cos):.0f}%)  unique={n_unique:>5,}")
print(f"\n  Company types:")
for val, cnt in cos["companytypename"].value_counts().items():
    print(f"    {val:35} {cnt:>5,} ({100*cnt/len(cos):.1f}%)")
print(f"\n  Status:")
for val, cnt in cos["companystatustypename"].value_counts().head(5).items():
    print(f"    {val:35} {cnt:>5,} ({100*cnt/len(cos):.1f}%)")
print(f"\n  Top countries:")
for val, cnt in cos["country"].value_counts().head(5).items():
    print(f"    {val:35} {cnt:>5,} ({100*cnt/len(cos):.1f}%)")
yrs = pd.to_numeric(cos["yearfounded"], errors="coerce").dropna()
print(f"\n  Year founded: {yrs.min():.0f} - {yrs.max():.0f}, median={yrs.median():.0f}")


# =====================================================================
# DATASET 5: CIQ Observer Network (All Positions)
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 5: CIQ Observer Person All Positions")
print("  File: CIQ_Extract/05_observer_person_all_positions.csv")
print("=" * 80)

net = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
print(f"\n  Rows: {len(net):,}")
print(f"  Unique persons: {net['personid'].nunique():,}")
print(f"  Unique companies: {net['companyid'].nunique():,}")
print(f"\n  Variables: {list(net.columns)}")
print(f"\n  Company types of positions:")
for val, cnt in net["companytypename"].value_counts().head(8).items():
    print(f"    {val:35} {cnt:>6,}")


# =====================================================================
# DATASET 6: CIQ Key Dev Events
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 6: CIQ Key Development Events")
print("  File: CIQ_Extract/06_observer_company_key_events.csv")
print("=" * 80)

events = pd.read_csv(os.path.join(ciq_dir, "06_observer_company_key_events.csv"))
print(f"\n  Rows: {len(events):,}")
print(f"  Unique companies: {events['companyid'].nunique():,}")
print(f"\n  Variables: {list(events.columns)}")
print(f"\n  Event types:")
for val, cnt in events["keydeveventtypename"].value_counts().items():
    print(f"    {val:45} {cnt:>6,}")


# =====================================================================
# DATASET 7: CIQ-CIK Crosswalk
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 7: CIQ-CIK Crosswalk")
print("  File: CIQ_Extract/07_ciq_cik_crosswalk.csv")
print("=" * 80)

xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
print(f"\n  Rows: {len(xwalk):,}")
print(f"  Unique CIQ companyids: {xwalk['companyid'].nunique():,}")
print(f"  Unique CIKs: {xwalk['cik'].nunique():,}")
print(f"\n  Variables: {list(xwalk.columns)}")


# =====================================================================
# DATASET 8: CIQ Deal Amounts
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 8: CIQ Transaction Deal Amounts")
print("  File: CIQ_Extract/08_company_deal_amounts.csv")
print("=" * 80)

deals = pd.read_csv(os.path.join(ciq_dir, "08_company_deal_amounts.csv"))
print(f"\n  Rows: {len(deals):,}")
print(f"\n  Variables and coverage:")
for col in deals.columns:
    non_null = deals[col].notna().sum()
    print(f"    {col:30} non-null={non_null:>5,}")
total_sold = pd.to_numeric(deals["total_size_usd"], errors="coerce")
print(f"\n  Total size USD: median=${total_sold.median():,.0f}, mean=${total_sold.mean():,.0f}")


# =====================================================================
# DATASET 9: EDGAR S-1 Filing Universe
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 9: EDGAR S-1 Filing Universe")
print("  File: EDGAR_Extract/all_s1_filings_2017_2026.csv")
print("=" * 80)

s1 = pd.read_csv(os.path.join(edgar_dir, "all_s1_filings_2017_2026.csv"))
print(f"\n  Rows: {len(s1):,}")
print(f"  Unique companies: {s1['cik'].nunique():,}")
print(f"\n  Variables: {list(s1.columns)}")
print(f"\n  Form type: {dict(s1['form_type'].value_counts())}")
print(f"  Year range: {s1['year'].min()} - {s1['year'].max()}")


# =====================================================================
# DATASET 10: EDGAR EFTS Observer Hits
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 10: EDGAR EFTS 'Board Observer' Hits")
print("  File: EDGAR_Extract/efts_board_observer_s1_hits.csv")
print("=" * 80)

efts = pd.read_csv(os.path.join(edgar_dir, "efts_board_observer_s1_hits.csv"))
print(f"\n  Rows: {len(efts):,}")
efts_ciks = set()
for _, r in efts.iterrows():
    if pd.notna(r.get("ciks")):
        for c in str(r["ciks"]).split("|"):
            if c.strip():
                efts_ciks.add(c.strip())
print(f"  Unique CIKs: {len(efts_ciks):,}")
print(f"\n  Variables: {list(efts.columns)}")
print(f"\n  File types:")
for val, cnt in efts["file_type"].value_counts().head(8).items():
    print(f"    {val:20} {cnt:>5,}")


# =====================================================================
# DATASET 11: EDGAR Exhibit Fiduciary Analysis
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 11: EDGAR Exhibit Fiduciary Language Analysis")
print("  File: EDGAR_Extract/exhibit_analysis_results.csv")
print("=" * 80)

exh = pd.read_csv(os.path.join(edgar_dir, "exhibit_analysis_results.csv"))
exh_ok = exh[exh["fetch_status"].isin(["ok", "cached"])]
print(f"\n  Total rows: {len(exh):,}")
print(f"  Successfully fetched: {len(exh_ok):,}")
print(f"  Unique companies (CIK): {exh_ok['cik'].nunique():,}")
print(f"\n  Variables: {list(exh.columns)}")
print(f"\n  Fiduciary language:")
print(f"    has_fiduciary_manner = True:    {(exh_ok['has_fiduciary_manner']=='True').sum():>5}")
print(f"    has_no_fiduciary_duty = True:   {(exh_ok['has_no_fiduciary_duty']=='True').sum():>5}")
print(f"    Neither:                        {len(exh_ok) - (exh_ok['has_fiduciary_manner']=='True').sum() - (exh_ok['has_no_fiduciary_duty']=='True').sum():>5}")


# =====================================================================
# DATASET 12: Form D Company Capital
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 12: Form D Company Capital (aggregated)")
print("  File: FormD/formd_company_capital.csv")
print("=" * 80)

formd = pd.read_csv(os.path.join(fd_dir, "formd_company_capital.csv"))
print(f"\n  Rows (unique companies): {len(formd):,}")
print(f"\n  Variables:")
for col in formd.columns:
    non_null = formd[col].notna().sum()
    print(f"    {col:30} non-null={non_null:>8,}")
ts = pd.to_numeric(formd["formd_total_sold"], errors="coerce")
print(f"\n  Total sold: median=${ts.median():,.0f}, mean=${ts.mean():,.0f}")
print(f"  N filings: median={formd['formd_n_filings'].median():.0f}, mean={formd['formd_n_filings'].mean():.1f}")


# =====================================================================
# DATASET 13: Form D Raw Quarterly Data
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 13: Form D Raw Quarterly Data")
print("  Directory: FormD/ (36 quarterly folders)")
print("=" * 80)

total_rp = 0
total_off = 0
total_sub = 0
for qtr_dir in sorted(os.listdir(fd_dir)):
    qtr_path = os.path.join(fd_dir, qtr_dir)
    if not os.path.isdir(qtr_path):
        continue
    for root, dirs, files in os.walk(qtr_path):
        for f in files:
            fp = os.path.join(root, f)
            try:
                with open(fp, "r", encoding="utf-8", errors="replace") as fh:
                    n = sum(1 for _ in fh) - 1
                if "RELATEDPERSONS" in f.upper():
                    total_rp += n
                elif f.upper() == "OFFERING.TSV":
                    total_off += n
                elif f.upper() == "FORMDSUBMISSION.TSV":
                    total_sub += n
            except:
                pass

print(f"\n  Quarters: 36 (2017 Q1 - 2025 Q4)")
print(f"  Total submissions: {total_sub:,}")
print(f"  Total offerings: {total_off:,}")
print(f"  Total related persons: {total_rp:,}")
print(f"\n  RELATEDPERSONS variables:")
print(f"    ACCESSIONNUMBER, RELATEDPERSON_SEQ_KEY")
print(f"    FIRSTNAME, MIDDLENAME, LASTNAME")
print(f"    STREET1, STREET2, CITY, STATEORCOUNTRY, ZIPCODE")
print(f"    RELATIONSHIP_1 (Director/Executive Officer/Promoter)")
print(f"    RELATIONSHIP_2, RELATIONSHIP_3, RELATIONSHIPCLARIFICATION")
print(f"\n  OFFERING variables include:")
print(f"    TOTALOFFERINGAMOUNT, TOTALAMOUNTSOLD, SALE_DATE")
print(f"    INDUSTRYGROUPTYPE, REVENUERANGE (88% blank/decline)")
print(f"    FEDERALEXEMPTIONS_ITEMS_LIST")


# =====================================================================
# DATASET 14: Ewens-Malenko Board Composition
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 14: Ewens-Malenko VC Board Composition")
print("  File: Ewens_Malenko/board_composition.csv")
print("=" * 80)

em = pd.read_csv(os.path.join(data_dir, "Ewens_Malenko", "board_composition.csv"))
print(f"\n  Rows: {len(em):,}")
print(f"  Unique startups (cik1): {em['cik1'].nunique():,}")
print(f"  Year range: {em['year'].min()} - {em['year'].max()}")
print(f"\n  Variables:")
for col in em.columns:
    non_null = em[col].notna().sum()
    print(f"    {col:30} non-null={non_null:>6,}  mean={em[col].mean():.2f}" if em[col].dtype in ['int64','float64'] else f"    {col:30} non-null={non_null:>6,}")


# =====================================================================
# DATASET 15: NVCA Model IRA Documents
# =====================================================================
print("\n" + "=" * 80)
print("DATASET 15: NVCA Model IRA Documents")
print("=" * 80)

for fname in ["NVCA_IRA_Oct2023.docx", "NVCA_IRA_Oct2025.docx"]:
    fp = os.path.join(data_dir, fname)
    if os.path.exists(fp):
        size = os.path.getsize(fp)
        print(f"\n  {fname}: {size:,} bytes")


# =====================================================================
# UNIFIED TABLES
# =====================================================================
print("\n" + "=" * 80)
print("UNIFIED ANALYSIS TABLES")
print("=" * 80)

master = pd.read_csv(os.path.join(data_dir, "table_a_company_master.csv"))
print(f"\n  table_a_company_master.csv (ORIGINAL)")
print(f"    Rows: {len(master):,}")
print(f"    Variables ({len(master.columns)}):")
for col in master.columns:
    non_null = master[col].notna().sum()
    pct = 100 * non_null / len(master)
    print(f"      {col:40} {non_null:>5,} ({pct:.0f}%)")

enhanced = pd.read_csv(os.path.join(data_dir, "table_a_company_master_enhanced.csv"))
print(f"\n  table_a_company_master_enhanced.csv")
print(f"    Rows: {len(enhanced):,}")
new_cols = [c for c in enhanced.columns if c not in master.columns]
print(f"    New variables added ({len(new_cols)}):")
for col in new_cols:
    non_null = enhanced[col].notna().sum()
    pct = 100 * non_null / len(enhanced)
    print(f"      {col:40} {non_null:>5,} ({pct:.0f}%)")

network = pd.read_csv(os.path.join(data_dir, "table_b_observer_network.csv"))
print(f"\n  table_b_observer_network.csv")
print(f"    Rows: {len(network):,}")
print(f"    Variables: {list(network.columns)}")
print(f"    Unique observers: {network['observer_personid'].nunique():,}")
print(f"    Unique VC firms: {network['vc_firm_companyid'].nunique():,}")


# =====================================================================
# OUTCOME DATA (by panel)
# =====================================================================
print("\n" + "=" * 80)
print("OUTCOME DATA BY PANEL")
print("=" * 80)

for panel, panel_dir in [("Panel A", "Panel_A_Outcomes"),
                          ("Panel B", "Panel_B_Outcomes"),
                          ("Panel C", "Panel_C_Network"),
                          ("Test 1", "Test1_Observer_vs_NoObserver")]:
    full_path = os.path.join(data_dir, panel_dir)
    if not os.path.exists(full_path):
        continue
    print(f"\n  {panel} ({panel_dir}/):")
    for f in sorted(os.listdir(full_path)):
        fp = os.path.join(full_path, f)
        if os.path.isfile(fp) and f.endswith(".csv"):
            size = os.path.getsize(fp) / 1024
            with open(fp, "r", encoding="utf-8") as fh:
                n = sum(1 for _ in fh) - 1
            print(f"    {f:50} {n:>10,} rows  ({size:>8,.0f} KB)")


# =====================================================================
# TOTAL DATA FOOTPRINT
# =====================================================================
print("\n" + "=" * 80)
print("TOTAL DATA FOOTPRINT")
print("=" * 80)

total_size = 0
total_files = 0
for root, dirs, files in os.walk(data_dir):
    for f in files:
        fp = os.path.join(root, f)
        total_size += os.path.getsize(fp)
        total_files += 1

print(f"\n  Total files: {total_files:,}")
print(f"  Total size: {total_size / (1024*1024):,.1f} MB")
print(f"  Git repo: github.com/HarpJung/BoardGov (private)")
