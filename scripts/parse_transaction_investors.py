"""
Parse investor names from CIQ transaction comments to build a dated VC-to-company network.

Pipeline:
  1. Parse comments to extract investor names from each round
  2. Match extracted investor names to our CIQ VC firm names
  3. Build: VC Firm A invested in Company Y in Round N on Date D
  4. Cross-reference with observer database:
     Person X works at VC A AND observes at Company Y
     VC A invested in Company Y on Date D
     → Person X was likely placed as observer on/around Date D
  5. Output: dated observer network with confirmed VC-company-date links
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd
import numpy as np
import os
import re

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")

print("=" * 90)
print("PARSE TRANSACTION COMMENTS FOR INVESTOR NAMES")
print("=" * 90)

# === Load transactions ===
trans = pd.read_csv(os.path.join(ciq_dir, "10_observed_company_transactions.csv"))
trans["companyid"] = trans["companyid"].astype(str).str.replace(".0", "", regex=False)
trans["comments"] = trans["comments"].fillna("").astype(str)

# Focus on VC/PE investments (type 1)
vc_trans = trans[trans["transactionidtypeid"] == 1].copy()
vc_trans["close_date"] = pd.to_datetime(
    vc_trans.apply(lambda r: f"{int(r['closingyear'])}-{int(r['closingmonth']):02d}-{int(r['closingday']):02d}"
                   if pd.notna(r.get("closingyear")) and pd.notna(r.get("closingmonth")) and pd.notna(r.get("closingday"))
                   else None, axis=1), errors="coerce")
print(f"  VC/PE transactions: {len(vc_trans):,}")
print(f"  With comments: {(vc_trans['comments'] != '').sum():,}")

# === Load our VC firm names for matching ===
pos = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
pos["personid"] = pos["personid"].astype(str).str.replace(".0", "", regex=False)
pos["companyid"] = pos["companyid"].astype(str).str.replace(".0", "", regex=False)

vc_types = {"Private Investment Firm", "Public Investment Firm", "Private Fund"}
vc_cos = pos[pos["companytypename"].isin(vc_types)][["companyid", "companyname"]].drop_duplicates("companyid")

def clean_vc_name(name):
    if not isinstance(name, str):
        return ""
    name = name.lower().strip()
    # Remove common suffixes
    for suf in [", llc", " llc", ", l.p.", " l.p.", ", lp", " lp",
                ", inc.", " inc.", ", inc", " inc", ", ltd", " ltd",
                " management", " advisors", " advisory", " partners",
                " capital", " ventures", " fund"]:
        name = name.replace(suf, "")
    name = re.sub(r'[,.\-\'\"&()]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

vc_cos["name_clean"] = vc_cos["companyname"].apply(clean_vc_name)
# Build lookup: cleaned name -> (companyid, original name)
vc_lookup = {}
for _, r in vc_cos.iterrows():
    nc = r["name_clean"]
    if nc and len(nc) >= 3:
        vc_lookup[nc] = (r["companyid"], r["companyname"])

print(f"  VC firms for matching: {len(vc_lookup):,}")

# === Parse investor names from comments ===
print("\n--- Parsing comments ---")

def extract_investors(comment):
    """Extract investor names from a transaction comment."""
    investors = []
    comment = str(comment)

    # Patterns that introduce investor names
    patterns = [
        r'led by (?:new |returning )?investor[s]?,?\s*(.+?)(?:\s+on\s|\s+in\s|\.\s|$)',
        r'participation from (?:new |returning )?investor[s]?,?\s*(.+?)(?:\s+on\s|\s+in\s|\.\s|$)',
        r'(?:new|returning) investor[s]?,?\s*(.+?)(?:\s+on\s|\s+in\s|\.\s|and\s+other|$)',
        r'funding from (?:new |returning )?investor[s]?,?\s*(.+?)(?:\s+on\s|\s+in\s|\.\s|$)',
        r'invested in by\s+(.+?)(?:\s+on\s|\.\s|$)',
        r'will include participation from\s+(.+?)(?:\s+on\s|\.\s|$)',
        r'(?:transaction|round) (?:will |)(?:see |include[s]? )participation from\s+(.+?)(?:\s+on\s|\.\s|$)',
        r'funding (?:led by|from)\s+(.+?)(?:\s+on\s|\s+in\s|\.\s|$)',
    ]

    for pat in patterns:
        matches = re.findall(pat, comment, re.IGNORECASE)
        for match in matches:
            # Split on common delimiters
            parts = re.split(r',\s*|\s+and\s+|\s+along with\s+', match)
            for part in parts:
                part = part.strip()
                # Remove trailing phrases
                part = re.sub(r'\s+(?:on|in|during|for|pursuant|the company|as of|from).*', '', part, flags=re.IGNORECASE)
                part = part.strip(' .,;')
                if len(part) > 3 and len(part) < 100:
                    investors.append(part)

    # Also catch "X invested in the transaction"
    inv_pattern = r'(.+?)\s+invested in (?:the )?transaction'
    for match in re.findall(inv_pattern, comment, re.IGNORECASE):
        parts = re.split(r',\s*|\s+and\s+', match)
        for part in parts:
            part = part.strip()
            part = re.sub(r'^(?:New investor |Returning investor )', '', part, flags=re.IGNORECASE)
            if len(part) > 3 and len(part) < 100:
                investors.append(part)

    return list(set(investors))

# Parse all comments
all_investor_mentions = []
for _, r in vc_trans.iterrows():
    investors = extract_investors(r["comments"])
    for inv_name in investors:
        all_investor_mentions.append({
            "transactionid": r["transactionid"],
            "companyid": r["companyid"],
            "close_date": r["close_date"],
            "roundnumber": r["roundnumber"],
            "transactionsize": r["transactionsize"],
            "investor_raw": inv_name,
        })

mentions = pd.DataFrame(all_investor_mentions)
print(f"  Investor mentions extracted: {len(mentions):,}")
print(f"  From {mentions['transactionid'].nunique():,} transactions")
print(f"  Unique raw investor names: {mentions['investor_raw'].nunique():,}")

# === Match to our VC firms ===
print("\n--- Matching to CIQ VC firms ---")

mentions["inv_clean"] = mentions["investor_raw"].apply(clean_vc_name)

matched = []
unmatched_names = set()

for _, m in mentions.iterrows():
    inv_clean = m["inv_clean"]
    if not inv_clean or len(inv_clean) < 3:
        continue

    # Try exact match
    if inv_clean in vc_lookup:
        vc_cid, vc_name = vc_lookup[inv_clean]
        matched.append({**m.to_dict(), "vc_companyid": vc_cid, "vc_name": vc_name, "match_type": "exact"})
        continue

    # Try substring match (investor name contained in VC name or vice versa)
    found = False
    for vc_clean, (vc_cid, vc_name) in vc_lookup.items():
        if len(inv_clean) >= 5 and len(vc_clean) >= 5:
            if inv_clean in vc_clean or vc_clean in inv_clean:
                matched.append({**m.to_dict(), "vc_companyid": vc_cid, "vc_name": vc_name, "match_type": "substring"})
                found = True
                break

    if not found:
        unmatched_names.add(m["investor_raw"])

matched_df = pd.DataFrame(matched)
if len(matched_df) > 0:
    matched_df = matched_df.drop_duplicates(subset=["transactionid", "vc_companyid"])

print(f"  Matched investor-transaction pairs: {len(matched_df):,}")
print(f"  Unique VC firms matched: {matched_df['vc_companyid'].nunique():,}")
print(f"  Unique portfolio companies: {matched_df['companyid'].nunique():,}")
print(f"  Unmatched investor names: {len(unmatched_names):,}")

print(f"\n  By match type:")
for mt, n in matched_df["match_type"].value_counts().items():
    print(f"    {mt}: {n:,}")

# === Cross-reference with observer database ===
print("\n--- Cross-referencing with observers ---")

obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
obs["companyid"] = obs["companyid"].astype(str).str.replace(".0", "", regex=False)
co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_priv = set(co[(co["country"] == "United States") & (co["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
obs_us = obs[obs["companyid"].isin(us_priv)]

# Observer -> VC firms mapping
obs_to_vcs = {}
for _, r in pos[pos["companytypename"].isin(vc_types)].iterrows():
    pid = r["personid"]
    if pid in set(obs_us["personid"]):
        if pid not in obs_to_vcs:
            obs_to_vcs[pid] = set()
        obs_to_vcs[pid].add(r["companyid"])

# Observer -> observed companies
obs_to_cos = {}
for _, r in obs_us.iterrows():
    pid = r["personid"]
    if pid not in obs_to_cos:
        obs_to_cos[pid] = set()
    obs_to_cos[pid].add(r["companyid"])

# For each matched VC-company-date link, find the observer(s)
dated_network = []
for _, m in matched_df.iterrows():
    vc_cid = m["vc_companyid"]
    portfolio_cid = m["companyid"]

    # Find observers who work at this VC AND observe at this company
    for pid, vcs in obs_to_vcs.items():
        if vc_cid in vcs:
            observed = obs_to_cos.get(pid, set())
            if portfolio_cid in observed:
                dated_network.append({
                    "observer_personid": pid,
                    "vc_companyid": vc_cid,
                    "vc_name": m["vc_name"],
                    "observed_companyid": portfolio_cid,
                    "investment_date": m["close_date"],
                    "round_number": m["roundnumber"],
                    "investment_size_m": m["transactionsize"],
                    "investor_raw_name": m["investor_raw"],
                    "match_type": m["match_type"],
                })

dated_net = pd.DataFrame(dated_network)
if len(dated_net) > 0:
    dated_net = dated_net.drop_duplicates(subset=["observer_personid", "observed_companyid", "investment_date"])

print(f"\n  DATED OBSERVER NETWORK (triple-confirmed via transaction comments):")
print(f"  Total links: {len(dated_net):,}")
print(f"  Unique observers: {dated_net['observer_personid'].nunique():,}")
print(f"  Unique VCs: {dated_net['vc_companyid'].nunique():,}")
print(f"  Unique observed companies: {dated_net['observed_companyid'].nunique():,}")
print(f"  With investment date: {dated_net['investment_date'].notna().sum():,}")
print(f"  Date range: {dated_net['investment_date'].min()} to {dated_net['investment_date'].max()}")

# Compare to other approaches
print(f"\n  COMPARISON TO OTHER NETWORK APPROACHES:")

# Original inferred
tb = pd.read_csv(os.path.join(data_dir, "table_b_observer_network.csv"))
tb["vc_firm_companyid"] = tb["vc_firm_companyid"].astype(str).str.replace(".0", "", regex=False)
tb["observed_companyid"] = tb["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
orig_pairs = set(zip(tb["vc_firm_companyid"], tb["observed_companyid"]))

# ciqcompanyrel confirmed
vc_inv = pd.read_csv(os.path.join(ciq_dir, "09_vc_portfolio_investments.csv"))
vc_inv["vc_companyid"] = vc_inv["vc_companyid"].astype(str).str.replace(".0", "", regex=False)
vc_inv["portfolio_companyid"] = vc_inv["portfolio_companyid"].astype(str).str.replace(".0", "", regex=False)
rel_pairs = set(zip(vc_inv["vc_companyid"], vc_inv["portfolio_companyid"]))

# Transaction-comment confirmed
if len(dated_net) > 0:
    trans_pairs = set(zip(dated_net["vc_companyid"], dated_net["observed_companyid"]))
else:
    trans_pairs = set()

print(f"    Original inferred (table_b): {len(orig_pairs):,} VC-company pairs")
print(f"    ciqcompanyrel confirmed:     {len(rel_pairs & set((r[0], r[1]) for r in orig_pairs)):,}")
print(f"    Transaction-comment confirmed: {len(trans_pairs):,}")
print(f"    In ALL three:                 {len(orig_pairs & rel_pairs & trans_pairs) if trans_pairs else 0:,}")

# Show examples
if len(dated_net) > 0:
    print(f"\n  EXAMPLE DATED LINKS:")
    for _, r in dated_net.sample(min(15, len(dated_net)), random_state=42).iterrows():
        obs_name = obs_us[obs_us["personid"] == r["observer_personid"]][["firstname", "lastname"]].iloc[0] if len(obs_us[obs_us["personid"] == r["observer_personid"]]) > 0 else {"firstname": "?", "lastname": "?"}
        co_name = co[co["companyid"].astype(str).str.replace(".0","",regex=False) == r["observed_companyid"]]["companyname"].iloc[0] if len(co[co["companyid"].astype(str).str.replace(".0","",regex=False) == r["observed_companyid"]]) > 0 else "?"
        dt = str(r["investment_date"])[:10] if pd.notna(r["investment_date"]) else "?"
        print(f"    {obs_name['firstname']} {obs_name['lastname']} @ {r['vc_name'][:30]:<30} -> {str(co_name)[:30]:<30} Round {r['round_number']} on {dt} (${r['investment_size_m']}M)")

# Save
outdir = os.path.join(data_dir, "Dated_Network")
os.makedirs(outdir, exist_ok=True)
if len(matched_df) > 0:
    matched_df.to_csv(os.path.join(outdir, "vc_investments_from_comments.csv"), index=False)
if len(dated_net) > 0:
    dated_net.to_csv(os.path.join(outdir, "dated_observer_network.csv"), index=False)

print(f"\n  Saved to Data/Dated_Network/")
print("\nDone.")
