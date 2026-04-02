"""Check position date coverage for Form 4 observers."""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd, os

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")

# Position dates
func = pd.read_csv(os.path.join(ciq_dir, "11_observer_position_dates.csv"))
proid_map = pd.read_csv(os.path.join(ciq_dir, "11b_observer_proid_mapping.csv"))
proid_map["proid"] = proid_map["proid"].astype(str).str.replace(".0", "", regex=False)
proid_map["personid"] = proid_map["personid"].astype(str).str.replace(".0", "", regex=False)
proid_map["companyid"] = proid_map["companyid"].astype(str).str.replace(".0", "", regex=False)
func["proid"] = func["proid"].astype(str).str.replace(".0", "", regex=False)
func = func.merge(proid_map[["proid", "personid", "companyid"]], on="proid", how="left")

func["start_date"] = pd.to_datetime(
    func.apply(lambda r: f"{int(r['startyear'])}-01-01" if pd.notna(r.get("startyear")) else None, axis=1),
    errors="coerce")
func["end_date"] = pd.to_datetime(
    func.apply(lambda r: f"{int(r['endyear'])}-12-31" if pd.notna(r.get("endyear")) else None, axis=1),
    errors="coerce")

pc = func.groupby(["personid", "companyid"]).agg(
    start=("start_date", "min"), end=("end_date", "max"), is_current=("currentflag", "max")
).reset_index()

# Identify the 346 traders
obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
obs["companyid"] = obs["companyid"].astype(str).str.replace(".0", "", regex=False)
co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_priv = set(co[(co["country"] == "United States") & (co["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
obs_us = obs[obs["companyid"].isin(us_priv)]

trades = pd.read_csv(os.path.join(data_dir, "Form4/observer_form4_trades.csv"))
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
tr_xwalk["tr_personid"] = tr_xwalk["tr_personid"].astype(str).str.replace(".0", "", regex=False)
tr_xwalk["ciq_personid"] = tr_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
trades["ciq_pid"] = trades["personid"].map(dict(zip(tr_xwalk["tr_personid"], tr_xwalk["ciq_personid"])))
traders = set(trades.dropna(subset=["ciq_pid"])["ciq_pid"]) & set(obs_us["personid"])

print("=" * 70)
print("POSITION DATE COVERAGE FOR FORM 4 OBSERVERS")
print("=" * 70)
print(f"\nObservers with Form 4 trades: {len(traders):,}")

# (A) Observer at private company
obs_private = pc[pc["personid"].isin(traders) & pc["companyid"].isin(us_priv)]
print(f"\n(A) Observer role at PRIVATE company")
print(f"  Person-company pairs: {len(obs_private):,}")
print(f"  With start date: {obs_private['start'].notna().sum():,} ({obs_private['start'].notna().mean()*100:.1f}%)")
print(f"  With end date: {obs_private['end'].notna().sum():,}")
print(f"  Unique persons with start date: {obs_private[obs_private['start'].notna()]['personid'].nunique():,}")

# (B) BoardEx for public company dates
bd_xwalk = pd.read_csv(os.path.join(data_dir, "BoardEx/observer_boardex_crosswalk.csv"))
bd_xwalk["ciq_personid"] = bd_xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
bd_pos = pd.read_csv(os.path.join(data_dir, "BoardEx/observer_boardex_positions.csv"))
bd_pos["datestartrole"] = pd.to_datetime(bd_pos["datestartrole"], errors="coerce")
bd_pos["dateendrole"] = pd.to_datetime(bd_pos["dateendrole"], errors="coerce")
bd_did_to_ciq = dict(zip(bd_xwalk["directorid"], bd_xwalk["ciq_personid"]))
bd_pos["ciq_pid"] = bd_pos["directorid"].map(bd_did_to_ciq)
bd_pub = bd_pos[bd_pos["ciq_pid"].isin(traders) & bd_pos["orgtype"].isin(["Quoted", "Listed"])]

print(f"\n(B) BoardEx PUBLIC company positions (with exact dates)")
print(f"  Positions: {len(bd_pub):,}")
print(f"  Unique traders in BoardEx: {bd_pub['ciq_pid'].nunique():,} of {len(traders):,}")
print(f"  100% have start date, {bd_pub['dateendrole'].notna().mean()*100:.1f}% have end date")

# (C) Form 4 trade dates themselves give us timing!
# If an observer filed Form 4 at Public Co B, they were definitely an insider at B on that date
print(f"\n(C) Form 4 trade dates AS position evidence")
print(f"  Each Form 4 trade confirms the person was an insider at that public company on that date")
print(f"  This is BETTER than BoardEx for the public-side timing")

# Summary
traders_priv_dates = set(obs_private[obs_private["start"].notna()]["personid"])
traders_pub_boardex = set(bd_pub["ciq_pid"])
both = traders_priv_dates & traders_pub_boardex

print(f"\n=== COVERAGE SUMMARY ===")
print(f"  Need two things to time-match:")
print(f"    (1) When observer started at PRIVATE company (to know they were there for the event)")
print(f"    (2) When observer was insider at PUBLIC company (to know the trade was during their tenure)")
print(f"")
print(f"  (1) Private-side dates (from protoprofunction): {len(traders_priv_dates):,} of {len(traders):,} ({len(traders_priv_dates)/len(traders)*100:.1f}%)")
print(f"  (2) Public-side dates (from BoardEx):           {len(traders_pub_boardex):,} of {len(traders):,} ({len(traders_pub_boardex)/len(traders)*100:.1f}%)")
print(f"  Both:                                           {len(both):,} of {len(traders):,} ({len(both)/len(traders)*100:.1f}%)")
print(f"")
print(f"  BUT: Form 4 trade dates themselves confirm public-side timing (filing = was insider on that date)")
print(f"  So the binding constraint is PRIVATE-side dates: {len(traders_priv_dates):,} ({len(traders_priv_dates)/len(traders)*100:.1f}%)")
print(f"  For the other {len(traders) - len(traders_priv_dates):,}, we can use:")
print(f"    - CIQ transaction dates (deal closing dates at the private company)")
print(f"    - CIQ current/former flag as crude proxy")

# What does the CIQ transaction table give us for these?
trans = pd.read_csv(os.path.join(ciq_dir, "10_observed_company_transactions.csv"))
trans["companyid"] = trans["companyid"].astype(str).str.replace(".0", "", regex=False)
obs_cos_of_traders = set()
for _, r in obs_us[obs_us["personid"].isin(traders)].iterrows():
    obs_cos_of_traders.add(r["companyid"])

trans_at_trader_cos = trans[trans["companyid"].isin(obs_cos_of_traders)]
cos_with_trans = set(trans_at_trader_cos["companyid"])
print(f"\n  Transaction dates for trader observed companies: {len(cos_with_trans):,} of {len(obs_cos_of_traders):,}")

print("\nDone.")
