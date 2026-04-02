"""
Match CIQ VC firms to Preqin managers with validated matching.

Approach:
  1. Clean names (remove LLC, LP, Inc, etc.)
  2. Exact match on cleaned names -> "high" confidence
  3. Substring match -> validate with geography (city/state)
  4. Flag match quality
  5. Show top matches for manual review
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd
import numpy as np
import os, re

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
preqin_dir = os.path.join(data_dir, "Preqin")

print("=" * 80)
print("VALIDATED PREQIN MATCHING")
print("=" * 80)

# =====================================================================
# Load CIQ VC firms with location info
# =====================================================================
print("\n--- Loading CIQ VC firms ---")

# Observer network gives us VC firm IDs and names
tb = pd.read_csv(os.path.join(data_dir, "table_b_observer_network.csv"))
tb["vc_firm_companyid"] = tb["vc_firm_companyid"].astype(str).str.replace(".0", "", regex=False)

our_vcs = tb[["vc_firm_companyid", "vc_firm_name"]].drop_duplicates()
our_vcs = our_vcs.dropna(subset=["vc_firm_name"])

# Get location from CIQ company details
co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
co["companyid"] = co["companyid"].astype(str).str.replace(".0", "", regex=False)
co_loc = co[["companyid", "city", "country"]].drop_duplicates("companyid")

# Also get location from the all-positions file for VC firms specifically
pos = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
pos["companyid"] = pos["companyid"].astype(str).str.replace(".0", "", regex=False)
vc_types = {"Private Investment Firm", "Public Investment Firm", "Private Fund"}
vc_companies = pos[pos["companytypename"].isin(vc_types)][["companyid", "companyname"]].drop_duplicates("companyid")

# Merge location
our_vcs = our_vcs.merge(co_loc, left_on="vc_firm_companyid", right_on="companyid", how="left")

# Count observed companies per VC
vc_obs_count = tb.groupby("vc_firm_companyid")["observed_companyid"].nunique().reset_index()
vc_obs_count.columns = ["vc_firm_companyid", "n_observed"]
our_vcs = our_vcs.merge(vc_obs_count, on="vc_firm_companyid", how="left")

print(f"  CIQ VC firms: {len(our_vcs):,}")
print(f"  With city: {our_vcs['city'].notna().sum():,}")
print(f"  With country: {our_vcs['country'].notna().sum():,}")

# =====================================================================
# Load Preqin managers with location
# =====================================================================
print("\n--- Loading Preqin managers ---")
preqin_mgrs = pd.read_csv(os.path.join(preqin_dir, "manager_details_full.csv"))
print(f"  Preqin managers: {len(preqin_mgrs):,}")
print(f"  With city: {preqin_mgrs['firmcity'].notna().sum():,}")
print(f"  With state: {preqin_mgrs['firmstate'].notna().sum():,}")
print(f"  With country: {preqin_mgrs['firmcountry'].notna().sum():,}")

# =====================================================================
# Name cleaning function
# =====================================================================
def clean_firm_name(name):
    """Aggressively clean firm name for matching."""
    if not isinstance(name, str):
        return ""
    name = name.lower().strip()

    # Remove common legal suffixes
    suffixes = [
        ", llc", " llc", ", l.l.c.", " l.l.c.",
        ", l.p.", " l.p.", ", lp", " lp",
        ", inc.", " inc.", ", inc", " inc",
        ", ltd.", " ltd.", ", ltd", " ltd",
        ", corp.", " corp.", ", corp", " corp",
        " co.", " co,",
        " s.a.", " s.a.s", " sas", " ag", " gmbh", " plc",
    ]
    for s in suffixes:
        name = name.replace(s, "")

    # Remove common descriptive words that differ between databases
    remove_words = [
        "management", "advisors", "advisory", "adviser",
        "company", "group", "holding", "holdings",
        "international", "global", "services",
        "investment", "investments",
    ]
    words = name.split()
    # Only remove if more than 2 words remain
    filtered = [w for w in words if w not in remove_words]
    if len(filtered) >= 2:
        name = " ".join(filtered)
    else:
        name = " ".join(words)

    # Remove punctuation
    name = re.sub(r'[,.\-\'\"&()]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()

    return name


def clean_city(city):
    """Normalize city names."""
    if not isinstance(city, str):
        return ""
    return city.lower().strip().replace(".", "")


# =====================================================================
# Build Preqin lookup
# =====================================================================
print("\n--- Building Preqin lookup ---")
preqin_mgrs["name_clean"] = preqin_mgrs["firmname"].apply(clean_firm_name)
preqin_mgrs["city_clean"] = preqin_mgrs["firmcity"].apply(clean_city)
preqin_mgrs["state_clean"] = preqin_mgrs["firmstate"].fillna("").str.lower().str.strip()
preqin_mgrs["country_clean"] = preqin_mgrs["firmcountry"].fillna("").str.lower().str.strip()

# Index by cleaned name
preqin_by_name = {}
for _, r in preqin_mgrs.iterrows():
    nc = r["name_clean"]
    if nc and len(nc) >= 3:
        if nc not in preqin_by_name:
            preqin_by_name[nc] = []
        preqin_by_name[nc].append(r)

print(f"  Unique clean Preqin names: {len(preqin_by_name):,}")

# =====================================================================
# Matching
# =====================================================================
print("\n--- Matching ---")

our_vcs["name_clean"] = our_vcs["vc_firm_name"].apply(clean_firm_name)
our_vcs["city_clean"] = our_vcs["city"].apply(clean_city)

matches = []

for _, vc in our_vcs.iterrows():
    vc_name = vc["name_clean"]
    vc_city = vc["city_clean"]
    vc_country = str(vc.get("country", "")).lower().strip()

    if not vc_name or len(vc_name) < 3:
        continue

    # --- Try 1: Exact name match ---
    if vc_name in preqin_by_name:
        for p in preqin_by_name[vc_name]:
            # Check geography for validation
            geo_match = "no_geo"
            if vc_city and p["city_clean"]:
                if vc_city == p["city_clean"]:
                    geo_match = "city_match"
                elif vc_city in p["city_clean"] or p["city_clean"] in vc_city:
                    geo_match = "city_partial"
                else:
                    geo_match = "city_mismatch"

            # Country check
            if vc_country and p["country_clean"]:
                if "united states" in vc_country and "us" in p["country_clean"]:
                    country_ok = True
                elif vc_country[:5] == p["country_clean"][:5]:
                    country_ok = True
                else:
                    country_ok = False
            else:
                country_ok = True  # Can't check, assume OK

            quality = "high" if geo_match in ("city_match", "city_partial", "no_geo") and country_ok else "medium"

            matches.append({
                "ciq_vc_companyid": vc["vc_firm_companyid"],
                "ciq_vc_name": vc["vc_firm_name"],
                "ciq_city": vc.get("city", ""),
                "ciq_country": vc.get("country", ""),
                "n_observed": vc.get("n_observed", 0),
                "preqin_firm_id": p["firm_id"],
                "preqin_firm_name": p["firmname"],
                "preqin_city": p.get("firmcity", ""),
                "preqin_state": p.get("firmstate", ""),
                "preqin_country": p.get("firmcountry", ""),
                "preqin_industry": p.get("industryfocus", ""),
                "match_type": "exact_name",
                "geo_validation": geo_match,
                "country_ok": country_ok,
                "quality": quality,
            })
        continue

    # --- Try 2: Substring match (only if name is distinctive enough) ---
    if len(vc_name) < 5:
        continue

    # Extract the "core" name (first 2 significant words)
    core_words = [w for w in vc_name.split() if len(w) >= 3 and w not in ("the", "fund", "partners", "ventures", "capital")]
    if len(core_words) < 1:
        continue
    core = " ".join(core_words[:2])

    if len(core) < 5:
        continue

    best_match = None
    for pname, ps in preqin_by_name.items():
        if core in pname or pname in vc_name:
            # Found a substring match - validate with geography
            for p in ps:
                geo_match = "no_geo"
                if vc_city and p["city_clean"]:
                    if vc_city == p["city_clean"]:
                        geo_match = "city_match"
                    elif vc_city in p["city_clean"] or p["city_clean"] in vc_city:
                        geo_match = "city_partial"
                    else:
                        geo_match = "city_mismatch"

                country_ok = True
                if vc_country and p["country_clean"]:
                    if "united states" in vc_country and "us" not in p["country_clean"] and "united states" not in p["country_clean"]:
                        country_ok = False

                # Only keep substring matches that pass geography
                if geo_match in ("city_match", "city_partial") or (geo_match == "no_geo" and country_ok):
                    quality = "medium" if geo_match in ("city_match", "city_partial") else "low"

                    matches.append({
                        "ciq_vc_companyid": vc["vc_firm_companyid"],
                        "ciq_vc_name": vc["vc_firm_name"],
                        "ciq_city": vc.get("city", ""),
                        "ciq_country": vc.get("country", ""),
                        "n_observed": vc.get("n_observed", 0),
                        "preqin_firm_id": p["firm_id"],
                        "preqin_firm_name": p["firmname"],
                        "preqin_city": p.get("firmcity", ""),
                        "preqin_state": p.get("firmstate", ""),
                        "preqin_country": p.get("firmcountry", ""),
                        "preqin_industry": p.get("industryfocus", ""),
                        "match_type": "substring",
                        "geo_validation": geo_match,
                        "country_ok": country_ok,
                        "quality": quality,
                    })
                    break  # Take first geographic-validated match
            break  # Only try one substring match per VC

match_df = pd.DataFrame(matches)

# Deduplicate: keep highest quality match per CIQ VC
match_df["quality_rank"] = match_df["quality"].map({"high": 1, "medium": 2, "low": 3})
match_df = match_df.sort_values(["ciq_vc_companyid", "quality_rank"]).drop_duplicates("ciq_vc_companyid")

print(f"\n  Total matches: {len(match_df):,}")
print(f"  Unique CIQ VCs matched: {match_df['ciq_vc_companyid'].nunique():,}")
print(f"  Unique Preqin firms: {match_df['preqin_firm_id'].nunique():,}")

print(f"\n  By quality:")
for q, n in match_df["quality"].value_counts().items():
    print(f"    {q:<10} {n:>5,}")

print(f"\n  By match type:")
for mt, n in match_df["match_type"].value_counts().items():
    print(f"    {mt:<15} {n:>5,}")

print(f"\n  By geo validation:")
for gv, n in match_df["geo_validation"].value_counts().items():
    print(f"    {gv:<20} {n:>5,}")

# =====================================================================
# Show top matches for review
# =====================================================================
print(f"\n\n{'=' * 80}")
print("TOP 30 MATCHES (by observer network size) FOR MANUAL REVIEW")
print(f"{'=' * 80}")

top = match_df.sort_values("n_observed", ascending=False).head(30)
for _, r in top.iterrows():
    q = r["quality"].upper()
    geo = r["geo_validation"]
    print(f"\n  [{q:>6}] [{geo:<15}]")
    print(f"    CIQ:    {r['ciq_vc_name'][:50]}")
    print(f"    Preqin: {r['preqin_firm_name'][:50]}")
    print(f"    CIQ loc:    {r['ciq_city']}, {r['ciq_country']}")
    print(f"    Preqin loc: {r['preqin_city']}, {r['preqin_state']}, {r['preqin_country']}")
    print(f"    Observed companies: {int(r['n_observed'])}")
    if r.get("preqin_industry"):
        print(f"    Preqin industry: {str(r['preqin_industry'])[:60]}")

# =====================================================================
# Check fund coverage for matched VCs
# =====================================================================
print(f"\n\n{'=' * 80}")
print("FUND COVERAGE CHECK")
print(f"{'=' * 80}")

funds = pd.read_csv(os.path.join(preqin_dir, "fund_details_full.csv"))
perf = pd.read_csv(os.path.join(preqin_dir, "fund_performance_full.csv"))
perf["irr_num"] = pd.to_numeric(perf["net_irr_pcent"], errors="coerce")

matched_firm_ids = set(match_df["preqin_firm_id"].dropna().astype(int))
matched_funds = funds[funds["firm_id"].isin(matched_firm_ids)]
vc_matched_funds = matched_funds[matched_funds["fund_type"].str.contains("Venture|Seed|Early", case=False, na=False)]

perf_fund_ids = set(perf["fund_id"].dropna().astype(int))
vc_with_perf = vc_matched_funds[vc_matched_funds["fund_id"].isin(perf_fund_ids)]

irr_fund_ids = set(perf[perf["irr_num"].notna()]["fund_id"].dropna().astype(int))
vc_with_irr = vc_matched_funds[vc_matched_funds["fund_id"].isin(irr_fund_ids)]

print(f"\n  All matched Preqin firms:        {len(matched_firm_ids):,}")
print(f"  Funds at matched firms:          {len(matched_funds):,}")
print(f"  VC/Seed/Early funds:             {len(vc_matched_funds):,}")
print(f"  With any performance data:       {len(vc_with_perf):,}")
print(f"  With IRR data:                   {len(vc_with_irr):,}")
print(f"  Unique firms with VC+IRR:        {vc_with_irr['firm_id'].nunique():,}")

# By quality
for q in ["high", "medium", "low"]:
    q_firms = set(match_df[match_df["quality"] == q]["preqin_firm_id"].dropna().astype(int))
    q_funds = vc_matched_funds[vc_matched_funds["firm_id"].isin(q_firms)]
    q_irr = q_funds[q_funds["fund_id"].isin(irr_fund_ids)]
    print(f"\n  Quality '{q}':")
    print(f"    Firms: {len(q_firms):,}, VC funds: {len(q_funds):,}, With IRR: {len(q_irr):,}")

# Save
match_df.to_csv(os.path.join(preqin_dir, "vc_preqin_crosswalk_validated.csv"), index=False)
print(f"\n\nSaved: vc_preqin_crosswalk_validated.csv ({len(match_df):,} rows)")
print("Done.")
