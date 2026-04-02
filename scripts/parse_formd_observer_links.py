"""
Parse Form D filings to extract confirmed observer-company-date links.

Logic:
  1. Load all ISSUERS.tsv files (474K filings) -> match to our CIQ observed companies by CIK
  2. Load RELATEDPERSONS.tsv files (1.7M persons) -> match to our CIQ observers by name
  3. Load OFFERING.tsv files (467K offerings) -> get exact sale dates and amounts
  4. For each match: we now have CONFIRMED that Person X was associated with
     Company A's Form D filing on date Y

This gives us the ground truth link between observer, company, and investment date
that we've been missing from CIQ.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd
import numpy as np
import os
import re

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
formd_dir = os.path.join(data_dir, "FormD")

print("=" * 90)
print("FORM D PARSING: Extract Observer-Company-Date Links")
print("=" * 90)

# =====================================================================
# STEP 1: Load our CIQ observer persons and observed companies
# =====================================================================
print("\n--- Step 1: Load CIQ observers ---")

obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
obs["companyid"] = obs["companyid"].astype(str).str.replace(".0", "", regex=False)

co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_priv = set(co[(co["country"] == "United States") & (co["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
obs_us = obs[obs["companyid"].isin(us_priv)]

# Observer name lookup (for matching to Form D related persons)
def clean_name(s):
    if not isinstance(s, str):
        return ""
    return re.sub(r'[^a-z ]', '', s.lower().strip())

observer_names = {}  # (first_clean, last_clean) -> set of personids
for _, r in obs_us.iterrows():
    fn = clean_name(str(r.get("firstname", "")))
    ln = clean_name(str(r.get("lastname", "")))
    if fn and ln:
        key = (fn.split()[0] if fn else "", ln)  # first token of first name
        if key not in observer_names:
            observer_names[key] = set()
        observer_names[key].add(r["personid"])

print(f"  US private observers: {len(obs_us):,} records")
print(f"  Unique name keys: {len(observer_names):,}")

# CIQ CIK crosswalk (to match Form D issuers by CIK)
ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["companyid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik"] = ciq_xwalk["cik"].astype(str).str.strip()
cik_to_companyid = {}
for _, r in ciq_xwalk.iterrows():
    cik = str(r["cik"]).strip().lstrip("0")
    if cik and r["companyid"] in us_priv:
        cik_to_companyid[cik] = r["companyid"]

print(f"  CIK-to-companyid mappings (US private): {len(cik_to_companyid):,}")

# Also build a name-based issuer lookup (for companies without CIK match)
co_names = {}
for _, r in co[co["companyid"].astype(str).str.replace(".0", "", regex=False).isin(us_priv)].iterrows():
    name_clean = clean_name(str(r.get("companyname", "")))
    if name_clean:
        cid = str(r["companyid"]).replace(".0", "")
        co_names[name_clean] = cid

print(f"  Company name lookup: {len(co_names):,}")

# Observer -> observed companies mapping
obs_to_cos = {}
for _, r in obs_us.iterrows():
    pid = r["personid"]
    if pid not in obs_to_cos:
        obs_to_cos[pid] = set()
    obs_to_cos[pid].add(r["companyid"])

# =====================================================================
# STEP 2: Parse all Form D filings
# =====================================================================
print("\n--- Step 2: Parsing Form D filings ---")

quarters = sorted([d for d in os.listdir(formd_dir) if os.path.isdir(os.path.join(formd_dir, d))])
print(f"  Quarters to process: {len(quarters)}")

confirmed_links = []
n_issuers_matched = 0
n_persons_matched = 0
n_total_issuers = 0

for qi, q in enumerate(quarters):
    qdir = os.path.join(formd_dir, q)
    subdirs = [d for d in os.listdir(qdir) if os.path.isdir(os.path.join(qdir, d))]
    if not subdirs:
        continue
    inner = os.path.join(qdir, subdirs[0])

    # Load issuers
    iss_file = os.path.join(inner, "ISSUERS.tsv")
    rp_file = os.path.join(inner, "RELATEDPERSONS.tsv")
    off_file = os.path.join(inner, "OFFERING.tsv")

    if not all(os.path.exists(f) for f in [iss_file, rp_file, off_file]):
        continue

    try:
        issuers = pd.read_csv(iss_file, sep="\t", dtype=str, on_bad_lines="skip")
        rp = pd.read_csv(rp_file, sep="\t", dtype=str, on_bad_lines="skip")
        offering = pd.read_csv(off_file, sep="\t", dtype=str, on_bad_lines="skip")
    except Exception as e:
        print(f"  {q}: Error reading files: {str(e)[:50]}")
        continue

    n_total_issuers += len(issuers)

    # Match issuers to our observed companies by CIK
    issuers["cik_clean"] = issuers["CIK"].fillna("").astype(str).str.strip().str.lstrip("0")
    issuers["matched_companyid"] = issuers["cik_clean"].map(cik_to_companyid)

    # Also try name matching for unmatched
    issuers["name_clean"] = issuers["ENTITYNAME"].apply(clean_name)
    unmatched = issuers[issuers["matched_companyid"].isna()]
    for idx, row in unmatched.iterrows():
        nc = row["name_clean"]
        if nc in co_names:
            issuers.at[idx, "matched_companyid"] = co_names[nc]

    matched_issuers = issuers[issuers["matched_companyid"].notna()]
    n_issuers_matched += len(matched_issuers)

    if len(matched_issuers) == 0:
        continue

    # Get accession numbers for matched issuers
    matched_accessions = set(matched_issuers["ACCESSIONNUMBER"])

    # Get offering dates for these accessions
    off_dates = {}
    for _, o in offering[offering["ACCESSIONNUMBER"].isin(matched_accessions)].iterrows():
        acc = o["ACCESSIONNUMBER"]
        sale_date = o.get("SALE_DATE", "")
        off_dates[acc] = sale_date

    # Match related persons to our observers
    matched_rp = rp[rp["ACCESSIONNUMBER"].isin(matched_accessions)]

    for _, person in matched_rp.iterrows():
        fn = clean_name(str(person.get("FIRSTNAME", "")))
        ln = clean_name(str(person.get("LASTNAME", "")))
        if not fn or not ln:
            continue

        fn_first = fn.split()[0] if fn else ""
        name_key = (fn_first, ln)

        if name_key in observer_names:
            acc = person["ACCESSIONNUMBER"]
            issuer_row = matched_issuers[matched_issuers["ACCESSIONNUMBER"] == acc]
            if len(issuer_row) == 0:
                continue

            for _, iss in issuer_row.iterrows():
                observer_pids = observer_names[name_key]
                companyid = iss["matched_companyid"]
                sale_date = off_dates.get(acc, "")

                # Verify: is this observer actually listed as observing at this company?
                for pid in observer_pids:
                    observed_cos = obs_to_cos.get(pid, set())
                    if companyid in observed_cos:
                        # CONFIRMED: observer X is in Form D for company Y
                        confirmed_links.append({
                            "observer_personid": pid,
                            "observer_firstname": person.get("FIRSTNAME", ""),
                            "observer_lastname": person.get("LASTNAME", ""),
                            "companyid": companyid,
                            "company_name": iss.get("ENTITYNAME", ""),
                            "company_cik": iss.get("CIK", ""),
                            "accession_number": acc,
                            "sale_date": sale_date,
                            "relationship": person.get("RELATIONSHIP_1", ""),
                            "relationship_clarification": person.get("RELATIONSHIPCLARIFICATION", ""),
                            "quarter": q,
                        })
                        n_persons_matched += 1

    if (qi + 1) % 6 == 0:
        print(f"  Processed {qi+1}/{len(quarters)} quarters: {n_issuers_matched:,} issuer matches, {n_persons_matched:,} person matches")

print(f"\n  Total issuers processed: {n_total_issuers:,}")
print(f"  Issuers matched to our companies: {n_issuers_matched:,}")
print(f"  Related persons matched to our observers: {n_persons_matched:,}")
print(f"  Confirmed observer-company-date links: {len(confirmed_links):,}")

# =====================================================================
# STEP 3: Analyze and save results
# =====================================================================
print(f"\n--- Step 3: Analyze confirmed links ---")

if confirmed_links:
    links_df = pd.DataFrame(confirmed_links)
    links_df["sale_date_parsed"] = pd.to_datetime(links_df["sale_date"], errors="coerce", format="mixed")

    # Deduplicate: unique (observer, company, sale_date) triples
    links_dedup = links_df.drop_duplicates(subset=["observer_personid", "companyid", "sale_date"])

    print(f"  Unique confirmed links (deduped): {len(links_dedup):,}")
    print(f"  Unique observers: {links_dedup['observer_personid'].nunique():,}")
    print(f"  Unique companies: {links_dedup['companyid'].nunique():,}")
    print(f"  With parsed sale date: {links_dedup['sale_date_parsed'].notna().sum():,}")
    print(f"  Date range: {links_dedup['sale_date_parsed'].min()} to {links_dedup['sale_date_parsed'].max()}")

    # Relationship types
    print(f"\n  Relationship types:")
    for rel, n in links_dedup["relationship"].value_counts().head(10).items():
        print(f"    {str(rel):<30} {n:>5,}")

    # Compare to our existing network
    tb = pd.read_csv(os.path.join(data_dir, "table_b_observer_network.csv"))
    tb["observer_personid"] = tb["observer_personid"].astype(str).str.replace(".0", "", regex=False)
    tb["observed_companyid"] = tb["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
    existing_pairs = set(zip(tb["observer_personid"], tb["observed_companyid"]))
    confirmed_pairs = set(zip(links_dedup["observer_personid"], links_dedup["companyid"]))

    print(f"\n  Existing observer-company pairs (from CIQ): {len(existing_pairs):,}")
    print(f"  Confirmed via Form D: {len(confirmed_pairs):,}")
    print(f"  Overlap: {len(existing_pairs & confirmed_pairs):,}")
    print(f"  In Form D but NOT in CIQ network: {len(confirmed_pairs - existing_pairs):,}")

    # For confirmed links, what's the earliest sale date per company?
    earliest = links_dedup.groupby("companyid")["sale_date_parsed"].min().reset_index()
    earliest.columns = ["companyid", "formd_first_date"]
    print(f"\n  Companies with Form D investment date: {earliest['formd_first_date'].notna().sum():,}")

    # Compare to CIQ first PP date
    events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
    events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
    events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
    pp = events[events["eventtype"] == "Private Placements"]
    ciq_first_pp = pp.groupby("companyid")["announcedate"].min().reset_index()
    ciq_first_pp.columns = ["companyid", "ciq_first_pp"]

    comparison = earliest.merge(ciq_first_pp, on="companyid", how="inner")
    comparison["days_diff"] = (comparison["ciq_first_pp"] - comparison["formd_first_date"]).dt.days

    print(f"\n  Companies with both Form D and CIQ PP dates: {len(comparison):,}")
    if len(comparison) > 0:
        print(f"  Form D earlier: {(comparison['days_diff'] > 30).sum():,}")
        print(f"  Within 30 days: {(comparison['days_diff'].abs() <= 30).sum():,}")
        print(f"  CIQ earlier: {(comparison['days_diff'] < -30).sum():,}")
        print(f"  Mean diff: {comparison['days_diff'].mean():.0f} days")

    # Show examples
    print(f"\n  Example confirmed links:")
    for _, r in links_dedup.sample(min(10, len(links_dedup)), random_state=42).iterrows():
        print(f"    {r['observer_firstname']} {r['observer_lastname']} ({r['observer_personid']}) "
              f"-> {str(r['company_name'])[:30]} on {str(r['sale_date'])[:10]} [{r['relationship']}]")

    # Save
    outdir = os.path.join(data_dir, "FormD_Parsed")
    os.makedirs(outdir, exist_ok=True)
    links_dedup.to_csv(os.path.join(outdir, "confirmed_observer_company_links.csv"), index=False)
    earliest.to_csv(os.path.join(outdir, "company_first_formd_date.csv"), index=False)
    print(f"\n  Saved to Data/FormD_Parsed/")

else:
    print("  No confirmed links found!")

print("\n\nDone.")
