"""Merge Form D TOTALAMOUNTSOLD into our CIQ observer sample as firm size control.
Does NOT modify any original files — creates new merged files only."""

import os
import csv
import pandas as pd
import numpy as np

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
fd_dir = os.path.join(data_dir, "FormD")
ciq_dir = os.path.join(data_dir, "CIQ_Extract")

print("=" * 70)
print("MERGE: Form D TOTALAMOUNTSOLD -> CIQ Observer Companies")
print("=" * 70)

# =====================================================================
# STEP 1: Load and aggregate Form D offerings data
# =====================================================================
print("\n--- Step 1: Loading Form D offering data across all quarters ---")

all_offerings = []
all_issuers = []

for qtr_dir in sorted(os.listdir(fd_dir)):
    qtr_path = os.path.join(fd_dir, qtr_dir)
    if not os.path.isdir(qtr_path):
        continue

    for root, dirs, files in os.walk(qtr_path):
        for f in files:
            fp = os.path.join(root, f)
            if f.upper() == "OFFERING.TSV":
                try:
                    df = pd.read_csv(fp, sep="\t", encoding="utf-8",
                                     on_bad_lines="skip", low_memory=False)
                    all_offerings.append(df)
                except Exception as e:
                    print(f"  Warning: {qtr_dir}/{f}: {str(e)[:50]}")

            elif f.upper() == "ISSUERS.TSV":
                try:
                    df = pd.read_csv(fp, sep="\t", encoding="utf-8",
                                     on_bad_lines="skip", low_memory=False)
                    all_issuers.append(df)
                except Exception as e:
                    print(f"  Warning: {qtr_dir}/{f}: {str(e)[:50]}")

offerings = pd.concat(all_offerings, ignore_index=True)
issuers = pd.concat(all_issuers, ignore_index=True)

print(f"  Total offerings: {len(offerings):,}")
print(f"  Total issuers: {len(issuers):,}")
print(f"  Offering columns: {list(offerings.columns)[:10]}...")

# =====================================================================
# STEP 2: Extract TOTALAMOUNTSOLD and link to issuers (for CIK)
# =====================================================================
print("\n--- Step 2: Extracting TOTALAMOUNTSOLD and CIK ---")

# Check what identifier columns the issuers table has
print(f"  Issuer columns: {list(issuers.columns)}")

# The issuers table should have CIK and ACCESSIONNUMBER
# Link offerings to issuers via ACCESSIONNUMBER
offerings["TOTALAMOUNTSOLD"] = pd.to_numeric(offerings["TOTALAMOUNTSOLD"], errors="coerce")
offerings["TOTALOFFERINGAMOUNT"] = pd.to_numeric(offerings["TOTALOFFERINGAMOUNT"], errors="coerce")

# Check REVENUERANGE distribution (confirming the agent's finding)
if "REVENUERANGE" in offerings.columns:
    print(f"\n  REVENUERANGE distribution:")
    rev_counts = offerings["REVENUERANGE"].value_counts(dropna=False)
    for val, cnt in rev_counts.head(10).items():
        print(f"    {str(val):40} {cnt:>8,} ({100*cnt/len(offerings):.1f}%)")

# Check TOTALAMOUNTSOLD coverage
has_amount = offerings["TOTALAMOUNTSOLD"].notna() & (offerings["TOTALAMOUNTSOLD"] > 0)
print(f"\n  TOTALAMOUNTSOLD coverage: {has_amount.sum():,} of {len(offerings):,} ({100*has_amount.mean():.1f}%)")
print(f"  TOTALAMOUNTSOLD stats (non-zero):")
print(f"    Mean:   ${offerings.loc[has_amount, 'TOTALAMOUNTSOLD'].mean():,.0f}")
print(f"    Median: ${offerings.loc[has_amount, 'TOTALAMOUNTSOLD'].median():,.0f}")
print(f"    Min:    ${offerings.loc[has_amount, 'TOTALAMOUNTSOLD'].min():,.0f}")
print(f"    Max:    ${offerings.loc[has_amount, 'TOTALAMOUNTSOLD'].max():,.0f}")

# Merge offerings with issuers to get CIK
# Check if issuers has CIK
cik_col = None
for col in issuers.columns:
    if "CIK" in col.upper():
        cik_col = col
        break

if cik_col is None:
    # Look for it in other columns
    print(f"\n  No CIK column found in issuers. Columns: {list(issuers.columns)}")
    # Check if ENTITYCIK or similar exists
    for col in issuers.columns:
        print(f"    {col}: sample={issuers[col].iloc[0] if len(issuers) > 0 else 'N/A'}")
else:
    print(f"\n  CIK column found: {cik_col}")

# =====================================================================
# STEP 3: Build company-level capital raised from Form D
# =====================================================================
print("\n--- Step 3: Building company-level capital raised ---")

# Merge offerings with issuers on ACCESSIONNUMBER
merged = offerings.merge(issuers[["ACCESSIONNUMBER", cik_col] if cik_col else issuers.columns[:3]],
                         on="ACCESSIONNUMBER", how="left")

if cik_col:
    merged["cik_clean"] = pd.to_numeric(merged[cik_col], errors="coerce")

    # Convert SALE_DATE to datetime before aggregation
    merged["SALE_DATE"] = pd.to_datetime(merged["SALE_DATE"], errors="coerce")

    # Aggregate by CIK: total capital raised across all Form D filings
    company_capital = merged.groupby("cik_clean").agg(
        formd_total_sold=("TOTALAMOUNTSOLD", "sum"),
        formd_total_offered=("TOTALOFFERINGAMOUNT", "sum"),
        formd_n_filings=("ACCESSIONNUMBER", "nunique"),
        formd_first_filing=("SALE_DATE", "min"),
        formd_last_filing=("SALE_DATE", "max"),
    ).reset_index()

    # Remove zero/null amounts
    company_capital = company_capital[company_capital["formd_total_sold"] > 0].copy()
    company_capital["formd_log_capital"] = np.log1p(company_capital["formd_total_sold"])

    print(f"  Companies with Form D capital data: {len(company_capital):,}")
    print(f"  Capital raised stats:")
    print(f"    Mean:   ${company_capital['formd_total_sold'].mean():,.0f}")
    print(f"    Median: ${company_capital['formd_total_sold'].median():,.0f}")
    print(f"    N filings mean: {company_capital['formd_n_filings'].mean():.1f}")

    # Also extract REVENUERANGE for companies that report it
    if "REVENUERANGE" in merged.columns:
        has_rev = merged[merged["REVENUERANGE"].notna() &
                         ~merged["REVENUERANGE"].isin(["Decline to Disclose", "Not Applicable", ""])]
        if len(has_rev) > 0:
            rev_by_company = has_rev.groupby("cik_clean")["REVENUERANGE"].first().reset_index()
            rev_by_company.columns = ["cik_clean", "formd_revenue_range"]
            company_capital = company_capital.merge(rev_by_company, on="cik_clean", how="left")
            has_rev_count = company_capital["formd_revenue_range"].notna().sum()
            print(f"  Companies with revenue range: {has_rev_count:,}")

    # Also extract INDUSTRYGROUPTYPE
    if "INDUSTRYGROUPTYPE" in merged.columns:
        ind_by_company = merged.groupby("cik_clean")["INDUSTRYGROUPTYPE"].first().reset_index()
        ind_by_company.columns = ["cik_clean", "formd_industry"]
        company_capital = company_capital.merge(ind_by_company, on="cik_clean", how="left")

    # Save standalone Form D company file
    formd_out = os.path.join(data_dir, "FormD", "formd_company_capital.csv")
    company_capital.to_csv(formd_out, index=False)
    print(f"\n  Saved: {formd_out}")

# =====================================================================
# STEP 4: Match to our CIQ observer companies
# =====================================================================
print("\n--- Step 4: Matching to CIQ observer companies ---")

# Load CIQ-CIK crosswalk
xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
xwalk["cik_clean"] = pd.to_numeric(xwalk["cik"].astype(str).str.lstrip("0"), errors="coerce")
xwalk["companyid"] = xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)

# Merge Form D capital to CIQ via CIK
ciq_formd = xwalk[["companyid", "cik_clean", "companyname"]].merge(
    company_capital, on="cik_clean", how="left"
)

has_formd = ciq_formd["formd_total_sold"].notna()
print(f"  CIQ observer companies with CIK: {len(ciq_formd):,}")
print(f"  Matched to Form D capital data: {has_formd.sum():,} ({100*has_formd.mean():.1f}%)")

# Compare to existing CIQ transaction data
existing_deals = pd.read_csv(os.path.join(ciq_dir, "08_company_deal_amounts.csv"))
existing_deals["companyid"] = existing_deals["companyid"].astype(str).str.replace(".0", "", regex=False)
existing_cos = set(existing_deals["companyid"])

formd_cos = set(ciq_formd.loc[has_formd, "companyid"])
new_cos = formd_cos - existing_cos
both_cos = formd_cos & existing_cos

print(f"\n  Comparison to existing CIQ deal data:")
print(f"    CIQ transactions covers: {len(existing_cos):,} companies")
print(f"    Form D covers:           {formd_cos.__len__():,} companies")
print(f"    In both:                 {len(both_cos):,}")
print(f"    NEW from Form D:         {len(new_cos):,}")
print(f"    Combined coverage:       {len(existing_cos | formd_cos):,} companies")

# =====================================================================
# STEP 5: Create enhanced master file (original untouched)
# =====================================================================
print("\n--- Step 5: Creating enhanced master file ---")

# Load original master (DO NOT MODIFY)
master = pd.read_csv(os.path.join(data_dir, "table_a_company_master.csv"))
master["companyid"] = master["companyid"].astype(str)

# Merge Form D capital
formd_for_merge = ciq_formd[["companyid", "formd_total_sold", "formd_log_capital",
                              "formd_n_filings", "formd_first_filing", "formd_last_filing"]].copy()
formd_for_merge = formd_for_merge.dropna(subset=["formd_total_sold"])
formd_for_merge = formd_for_merge.drop_duplicates("companyid", keep="first")

master_enhanced = master.merge(formd_for_merge, on="companyid", how="left")

# Create a combined capital measure: use Form D if available, else CIQ transactions
if "log_capital_raised" in master_enhanced.columns:
    master_enhanced["log_capital_raised"] = pd.to_numeric(
        master_enhanced["log_capital_raised"], errors="coerce"
    ).fillna(0)
else:
    master_enhanced["log_capital_raised"] = 0
master_enhanced["formd_log_capital"] = master_enhanced["formd_log_capital"].fillna(0)

# Best available: prefer Form D (broader coverage), fall back to CIQ
master_enhanced["best_log_capital"] = master_enhanced["formd_log_capital"]
mask = (master_enhanced["best_log_capital"] == 0) & (master_enhanced["log_capital_raised"] > 0)
master_enhanced.loc[mask, "best_log_capital"] = master_enhanced.loc[mask, "log_capital_raised"]

has_any_capital = (master_enhanced["best_log_capital"] > 0).sum()
print(f"  Original master: {len(master):,} companies")
print(f"  Enhanced master: {len(master_enhanced):,} companies")
print(f"  Companies with ANY capital data: {has_any_capital:,} ({100*has_any_capital/len(master_enhanced):.1f}%)")
print(f"    From Form D: {(master_enhanced['formd_log_capital'] > 0).sum():,}")
print(f"    From CIQ transactions only: {mask.sum():,}")

# Save enhanced master as NEW file (original untouched)
enhanced_path = os.path.join(data_dir, "table_a_company_master_enhanced.csv")
master_enhanced.to_csv(enhanced_path, index=False)
print(f"\n  Saved ENHANCED master: {enhanced_path}")
print(f"  Original master UNTOUCHED: {os.path.join(data_dir, 'table_a_company_master.csv')}")

# =====================================================================
# STEP 6: Merge Ewens-Malenko for overlapping companies
# =====================================================================
print("\n\n--- Step 6: Merging Ewens-Malenko board composition ---")

em = pd.read_csv(os.path.join(data_dir, "Ewens_Malenko", "board_composition.csv"))

# Get the most recent year per startup from EM
em_latest = em.sort_values("year", ascending=False).drop_duplicates("cik1", keep="first")

# Match to our CIQ companies via CIK
xwalk["cik_int"] = xwalk["cik_clean"].astype("Int64")
em_latest["cik_int"] = em_latest["cik1"].astype("Int64")

em_for_merge = em_latest[["cik_int", "numOut", "numExecs", "numVCs", "year",
                           "financingRoundNumer"]].copy()
em_for_merge.columns = ["cik_int", "em_numOut", "em_numExecs", "em_numVCs",
                         "em_latest_year", "em_financing_round"]

ciq_em = xwalk[["companyid", "cik_int"]].merge(em_for_merge, on="cik_int", how="left")
ciq_em = ciq_em.dropna(subset=["em_numOut"])

print(f"  Ewens-Malenko startups: {em['cik1'].nunique():,}")
print(f"  CIQ observer companies matched to EM: {len(ciq_em):,}")

if len(ciq_em) > 0:
    # Add to enhanced master
    em_merge = ciq_em[["companyid", "em_numOut", "em_numExecs", "em_numVCs",
                        "em_latest_year", "em_financing_round"]].drop_duplicates("companyid")
    master_enhanced2 = master_enhanced.merge(em_merge, on="companyid", how="left")

    has_em = master_enhanced2["em_numOut"].notna().sum()
    print(f"  Enhanced master with EM data: {has_em:,} companies")
    print(f"  EM board composition (overlapping firms):")
    print(f"    Avg VC directors: {ciq_em['em_numVCs'].mean():.1f}")
    print(f"    Avg independent: {ciq_em['em_numOut'].mean():.1f}")
    print(f"    Avg executives: {ciq_em['em_numExecs'].mean():.1f}")

    # Save final enhanced master
    final_path = os.path.join(data_dir, "table_a_company_master_enhanced.csv")
    master_enhanced2.to_csv(final_path, index=False)
    print(f"\n  Saved FINAL enhanced master: {final_path}")

# =====================================================================
# SUMMARY
# =====================================================================
print(f"\n\n{'='*70}")
print("MERGE SUMMARY")
print(f"{'='*70}")

print(f"""
  ORIGINAL FILES (untouched):
    table_a_company_master.csv        (3,058 companies)
    CIQ_Extract/08_company_deal_amounts.csv  (624 companies with CIQ deal data)

  NEW FILES CREATED:
    FormD/formd_company_capital.csv   (Form D capital raised by company)
    table_a_company_master_enhanced.csv (3,058 companies + Form D + EM controls)

  CAPITAL RAISED COVERAGE IMPROVEMENT:
    Before (CIQ transactions only):  624 of 3,058 (20.4%)
    After (+ Form D):               {has_any_capital:,} of {len(master_enhanced):,} ({100*has_any_capital/len(master_enhanced):.1f}%)

  BOARD COMPOSITION CONTROLS (from Ewens-Malenko):
    Companies with EM data:          {has_em:,} of 3,058
    Adds: numVCs, numOut, numExecs as controls for board professionalization
""")
