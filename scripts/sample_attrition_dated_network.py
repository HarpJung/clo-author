"""Full sample attrition for the dated observer network."""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd, os

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")

print("=" * 80)
print("SAMPLE ATTRITION: DATED OBSERVER NETWORK")
print("=" * 80)

# =====================================================================
# SOURCE 1: CIQ OBSERVER DATA
# =====================================================================
print("\n" + "=" * 80)
print("SOURCE 1: CIQ OBSERVER DATA (ciq.ciqprofessional)")
print("=" * 80)

obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
obs["companyid"] = obs["companyid"].astype(str).str.replace(".0", "", regex=False)

print(f"\n  A. Raw CIQ observer records")
print(f"     Query: title LIKE '%observer%' AND boardflag = 1")
print(f"     Records:          {len(obs):>6,}")
print(f"     Unique persons:   {obs['personid'].nunique():>6,}")
print(f"     Unique companies: {obs['companyid'].nunique():>6,}")

co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
co["companyid"] = co["companyid"].astype(str).str.replace(".0", "", regex=False)
us_all = set(co[co["country"] == "United States"]["companyid"])
us_priv = set(co[(co["country"] == "United States") & (co["companytypename"] == "Private Company")]["companyid"])

obs_us = obs[obs["companyid"].isin(us_all)]
obs_us_priv = obs[obs["companyid"].isin(us_priv)]

print(f"\n  B. US filter")
print(f"     US companies:          {len(us_all):>6,}  (dropped {obs['companyid'].nunique() - len(us_all):,} non-US)")
print(f"     US private companies:  {len(us_priv):>6,}")
print(f"     US private observers:  {obs_us_priv['personid'].nunique():>6,}")
print(f"     US private records:    {len(obs_us_priv):>6,}")

# Observer -> VC firm mapping
pos = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
pos["personid"] = pos["personid"].astype(str).str.replace(".0", "", regex=False)
pos["companyid"] = pos["companyid"].astype(str).str.replace(".0", "", regex=False)

vc_types = {"Private Investment Firm", "Public Investment Firm", "Private Fund"}
vc_pos = pos[pos["companytypename"].isin(vc_types)]
us_obs_pids = set(obs_us_priv["personid"])
obs_with_vc = set(vc_pos[vc_pos["personid"].isin(us_obs_pids)]["personid"])
obs_without_vc = us_obs_pids - obs_with_vc

print(f"\n  C. VC affiliation")
print(f"     Observers with VC employer:    {len(obs_with_vc):>6,}")
print(f"     Observers without VC employer: {len(obs_without_vc):>6,}")
print(f"     Unique VC firms:               {vc_pos[vc_pos['personid'].isin(us_obs_pids)]['companyid'].nunique():>6,}")

# Observer -> observed company pairs
obs_pairs = obs_us_priv[["personid", "companyid"]].drop_duplicates()
print(f"\n  D. Observer-company pairs (US private)")
print(f"     Unique pairs:     {len(obs_pairs):>6,}")

# =====================================================================
# SOURCE 2: CIQ TRANSACTION / INVESTMENT DATA
# =====================================================================
print("\n\n" + "=" * 80)
print("SOURCE 2: CIQ TRANSACTION DATA (ciq.ciqtransaction)")
print("=" * 80)

trans = pd.read_csv(os.path.join(ciq_dir, "10_observed_company_transactions.csv"))
trans["companyid"] = trans["companyid"].astype(str).str.replace(".0", "", regex=False)

print(f"\n  A. Raw transactions at observed companies")
print(f"     Total transactions:     {len(trans):>6,}")
print(f"     Unique companies:       {trans['companyid'].nunique():>6,}")

vc_trans = trans[trans["transactionidtypeid"] == 1]
print(f"\n  B. VC/PE investments (type 1)")
print(f"     Transactions:           {len(vc_trans):>6,}")
print(f"     Unique companies:       {vc_trans['companyid'].nunique():>6,}")
print(f"     With round number:      {vc_trans['roundnumber'].notna().sum():>6,}")
print(f"     With transaction size:  {vc_trans['transactionsize'].notna().sum():>6,}")
print(f"     With comments:          {(vc_trans['comments'].notna() & (vc_trans['comments'].astype(str) != 'nan')).sum():>6,}")

print(f"\n  C. Investor names extracted from comments")
# Load parsed results
dated_inv = pd.read_csv(os.path.join(data_dir, "Dated_Network/vc_investments_from_comments.csv"))
print(f"     Investor mentions extracted: 13,296 (from 4,225 transactions)")
print(f"     Matched to CIQ VC firms:    {len(dated_inv):>6,}")
print(f"     Exact name match:           {(dated_inv['match_type'] == 'exact').sum():>6,}")
print(f"     Substring match:            {(dated_inv['match_type'] == 'substring').sum():>6,}")
print(f"     Unique VCs matched:         {dated_inv['vc_companyid'].nunique():>6,}")
print(f"     Unique portfolio cos:       {dated_inv['companyid'].nunique():>6,}")

# =====================================================================
# SOURCE 3: CIQ COMPANY RELATIONSHIPS
# =====================================================================
print("\n\n" + "=" * 80)
print("SOURCE 3: CIQ COMPANY RELATIONSHIPS (ciq.ciqcompanyrel)")
print("=" * 80)

vc_inv = pd.read_csv(os.path.join(ciq_dir, "09_vc_portfolio_investments.csv"))
vc_inv["vc_companyid"] = vc_inv["vc_companyid"].astype(str).str.replace(".0", "", regex=False)
vc_inv["portfolio_companyid"] = vc_inv["portfolio_companyid"].astype(str).str.replace(".0", "", regex=False)

print(f"\n  A. All VC investment relationships")
print(f"     Total links:            {len(vc_inv):>8,}")
print(f"     Unique VCs:             {vc_inv['vc_companyid'].nunique():>8,}")
print(f"     Unique portfolio cos:   {vc_inv['portfolio_companyid'].nunique():>8,}")

print(f"\n  B. By type:")
for t, n in vc_inv["companyreltypename"].value_counts().items():
    print(f"     {t:<35} {n:>8,}")

# How many are at our observed companies?
obs_cids = set(obs_us_priv["companyid"])
vc_at_obs = vc_inv[vc_inv["portfolio_companyid"].isin(obs_cids)]
print(f"\n  C. At our US private observed companies")
print(f"     Investment links:       {len(vc_at_obs):>6,}")
print(f"     Unique VCs:             {vc_at_obs['vc_companyid'].nunique():>6,}")
print(f"     Unique observed cos:    {vc_at_obs['portfolio_companyid'].nunique():>6,}")

# =====================================================================
# SOURCE 4: POSITION DATES
# =====================================================================
print("\n\n" + "=" * 80)
print("SOURCE 4: POSITION DATES (ciq.ciqprotoprofunction)")
print("=" * 80)

func = pd.read_csv(os.path.join(ciq_dir, "11_observer_position_dates.csv"))
proid_map = pd.read_csv(os.path.join(ciq_dir, "11b_observer_proid_mapping.csv"))
proid_map["proid"] = proid_map["proid"].astype(str).str.replace(".0", "", regex=False)
proid_map["personid"] = proid_map["personid"].astype(str).str.replace(".0", "", regex=False)
proid_map["companyid"] = proid_map["companyid"].astype(str).str.replace(".0", "", regex=False)
func["proid"] = func["proid"].astype(str).str.replace(".0", "", regex=False)
func = func.merge(proid_map[["proid", "personid", "companyid"]], on="proid", how="left")

# Observer at private company
obs_func = func[func["personid"].isin(us_obs_pids) & func["companyid"].isin(us_priv)]
pc = obs_func.groupby(["personid", "companyid"]).agg(
    has_start=("startyear", lambda x: x.notna().any()),
    has_end=("endyear", lambda x: x.notna().any()),
    is_current=("currentflag", "max"),
).reset_index()

print(f"\n  Observer-company pairs (US private): {len(pc):,}")
print(f"  With start year:  {pc['has_start'].sum():>5,} ({pc['has_start'].mean()*100:.1f}%)")
print(f"  With end year:    {pc['has_end'].sum():>5,} ({pc['has_end'].mean()*100:.1f}%)")
print(f"  Current:          {(pc['is_current']==1).sum():>5,} ({(pc['is_current']==1).mean()*100:.1f}%)")

# =====================================================================
# THE MERGE: BUILDING THE DATED NETWORK
# =====================================================================
print("\n\n" + "=" * 80)
print("THE MERGE: BUILDING THE DATED NETWORK")
print("=" * 80)

dated = pd.read_csv(os.path.join(data_dir, "Dated_Network/dated_observer_network.csv"))
dated["observer_personid"] = dated["observer_personid"].astype(str).str.replace(".0", "", regex=False)
dated["vc_companyid"] = dated["vc_companyid"].astype(str).str.replace(".0", "", regex=False)
dated["observed_companyid"] = dated["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
dated["investment_date"] = pd.to_datetime(dated["investment_date"], errors="coerce")

print(f"""
  Step 1: Start with observer-company pairs
     US private observer-company pairs:              {len(obs_pairs):>6,}

  Step 2: Observer must work at a VC firm
     Observers with VC employer:                     {len(obs_with_vc):>6,}
     (dropped {len(obs_without_vc):,} without VC affiliation)

  Step 3: Transaction comments must name the VC
     VC investment mentions parsed from comments:    {len(dated_inv):>6,}
     Unique VC-company pairs from parsing:           {dated_inv.groupby(['vc_companyid','companyid']).ngroups:>6,}

  Step 4: Cross-reference — observer at VC + VC invested in company
     Triple-confirmed dated links:                   {len(dated):>6,}
     Unique observers:                               {dated['observer_personid'].nunique():>6,}
     Unique VCs:                                     {dated['vc_companyid'].nunique():>6,}
     Unique observed companies:                      {dated['observed_companyid'].nunique():>6,}
     With exact investment date:                     {dated['investment_date'].notna().sum():>6,}

  Step 5: Also confirmed by ciqcompanyrel?
""")

# Check overlap with ciqcompanyrel
dated_vc_co = set(zip(dated["vc_companyid"], dated["observed_companyid"]))
rel_vc_co = set(zip(vc_inv["vc_companyid"], vc_inv["portfolio_companyid"]))
in_both = dated_vc_co & rel_vc_co
print(f"     Dated links also in ciqcompanyrel:          {len(in_both):>6,} of {len(dated_vc_co):,}")
print(f"     Dated links NOT in ciqcompanyrel:           {len(dated_vc_co - rel_vc_co):>6,}")

# =====================================================================
# FINAL SAMPLE USED IN REGRESSIONS
# =====================================================================
print("\n\n" + "=" * 80)
print("FINAL SAMPLE IN REGRESSIONS")
print("=" * 80)

# For company outcomes (Preqin deals)
preqin_dir = os.path.join(data_dir, "Preqin")
deals = pd.read_csv(os.path.join(preqin_dir, "vc_deals_full.csv"), low_memory=False)
deals["deal_date"] = pd.to_datetime(deals["deal_date"], errors="coerce")
us_deals = deals[deals["portfolio_company_country"].fillna("").str.contains("US|United States", case=False)]

import re
def clean_name(s):
    if not isinstance(s, str): return ""
    s = re.sub(r'[,.\-\'\"&()!]', ' ', s.lower().strip())
    for suf in [" inc", " llc", " corp", " ltd"]: s = s.replace(suf, "")
    return re.sub(r'\s+', ' ', s).strip()

co_det = co[co["companyid"].isin(us_priv)].copy()
co_det["nc"] = co_det["companyname"].apply(clean_name)
us_deals["nc"] = us_deals["portfolio_company_name"].fillna("").apply(clean_name)
ciq_names = dict(zip(co_det["nc"], co_det["companyid"]))
us_deals["ciq_cid"] = us_deals["nc"].map(ciq_names)

dated_cids = set(dated["observed_companyid"])
matched_deals = us_deals[us_deals["ciq_cid"].isin(dated_cids)]
matched_deals = matched_deals.dropna(subset=["deal_date"])

print(f"""
  A. Company Outcomes Test (Preqin deals):
     Dated network companies:               {len(dated_cids):>6,}
     Matched to Preqin by name:             {matched_deals['ciq_cid'].nunique():>6,}
     Total Preqin deals at matched cos:     {len(matched_deals):>6,}
     With deal size:                        {matched_deals['deal_financing_size_usd'].notna().sum():>6,}
""")

# For Form 4
trades = pd.read_csv(os.path.join(data_dir, "Form4/observer_form4_trades.csv"))
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
tr_xwalk["tr_personid"] = tr_xwalk["tr_personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk["ciq_personid"] = tr_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
trades["ciq_pid"] = trades["personid"].map(dict(zip(tr_xwalk["tr_personid"], tr_xwalk["ciq_personid"])))
trades = trades.dropna(subset=["ciq_pid"])
trades = trades[trades["trancode"].isin(["P", "S"])]

dated_pids = set(dated["observer_personid"])
traders_in_dated = set(trades["ciq_pid"]) & dated_pids

print(f"""  B. Form 4 Test:
     Dated network observers:               {len(dated_pids):>6,}
     Of those, have Form 4 trades:          {len(traders_in_dated):>6,}
     Their trades:                          {len(trades[trades['ciq_pid'].isin(traders_in_dated)]):>6,}
""")

# For Preqin fund performance
xwalk = pd.read_csv(os.path.join(preqin_dir, "vc_preqin_crosswalk_validated.csv"))
xwalk = xwalk[xwalk["quality"].isin(["high", "medium"])]
dated_vcs = set(dated["vc_companyid"])
matched_vcs = set(xwalk["ciq_vc_companyid"].astype(str)) & dated_vcs

print(f"""  C. Preqin Fund Performance Test:
     Dated network VCs:                     {len(dated_vcs):>6,}
     Matched to Preqin (high+medium):       {len(matched_vcs):>6,}
""")

# Date distribution
print(f"\n  D. Investment Date Distribution:")
dates = dated["investment_date"].dropna()
for yr, n in dates.dt.year.value_counts().sort_index().items():
    bar = "#" * min(n // 5, 40)
    print(f"     {int(yr)}: {n:>4} {bar}")

# Round distribution
print(f"\n  E. Round Number Distribution:")
for rn, n in dated["round_number"].value_counts().sort_index().head(15).items():
    if pd.notna(rn):
        print(f"     Round {int(rn):>2}: {n:>4,}")

print("\nDone.")
