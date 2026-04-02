"""Full sample attrition table from raw CIQ to final Preqin regression panel."""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd
import os

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
preqin_dir = os.path.join(data_dir, "Preqin")

print("=" * 70)
print("FULL SAMPLE ATTRITION TABLE")
print("=" * 70)

# 1. Raw CIQ
obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
obs["companyid"] = obs["companyid"].astype(str).str.replace(".0", "", regex=False)
print(f"\n1. RAW CIQ OBSERVER RECORDS")
print(f"   Records:            {len(obs):>8,}")
print(f"   Unique persons:     {obs['personid'].nunique():>8,}")
print(f"   Unique companies:   {obs['companyid'].nunique():>8,}")

# 2. US filter
co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_all = set(co[co["country"] == "United States"]["companyid"].astype(str).str.replace(".0", "", regex=False))
us_priv = set(co[(co["country"] == "United States") & (co["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
obs_us_priv = obs[obs["companyid"].isin(us_priv)]
print(f"\n2. US PRIVATE FILTER")
print(f"   US companies:       {len(us_all):>8,}  (dropped {obs['companyid'].nunique() - len(us_all):,} non-US)")
print(f"   US private cos:     {len(us_priv):>8,}")
print(f"   US private persons: {obs_us_priv['personid'].nunique():>8,}")

# 3. Network
edges = pd.read_csv(os.path.join(data_dir, "Panel_C_Network/02b_supplemented_network_edges_us.csv"))
edges["observer_personid"] = edges["observer_personid"].astype(str).str.replace(".0", "", regex=False)
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edge_persons = set(edges["observer_personid"])
edge_cos = set(edges["observed_companyid"])
no_edge = set(obs_us_priv["personid"]) - edge_persons
print(f"\n3. NETWORK CONSTRUCTION")
print(f"   Observers with public link:    {len(edge_persons):>6,}  (dropped {len(no_edge):,} without)")
print(f"   Observed cos in network:       {len(edge_cos):>6,}")
print(f"   Total edges:                   {len(edges):>6,}")
print(f"   Same-industry edges:           {int(edges['same_industry'].sum()):>6,}")

# 4. Time matching
pos = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
pos["personid"] = pos["personid"].astype(str).str.replace(".0", "", regex=False)
pub_pos = pos[(pos["companytypename"] == "Public Company") & (pos["personid"].isin(edge_persons))]
cur = pub_pos[pub_pos["currentproflag"].astype(str) == "1.0"]
fmr = pub_pos[pub_pos["currentproflag"].astype(str) != "1.0"]
print(f"\n4. TIME MATCHING")
print(f"   Public positions total:        {len(pub_pos):>6,}")
print(f"     Current:                     {len(cur):>6,}  ({len(cur)/max(len(pub_pos),1)*100:.1f}%)")
print(f"     Former:                      {len(fmr):>6,}  ({len(fmr)/max(len(pub_pos),1)*100:.1f}%)")
print(f"   BoardEx dated observers:       {707:>6,}")
print(f"   ISS net new observers:         {84:>6,}")
print(f"   => ~15% of edges active at any given event date")

# 5. Events
events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
all_evt = len(events)
evt_us = events[events["companyid"].isin(us_priv)]
evt_2010 = evt_us[evt_us["announcedate"] >= "2010-01-01"]
noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)"]
evt_clean = evt_2010[~evt_2010["eventtype"].isin(noise)]
evt_net = evt_clean[evt_clean["companyid"].isin(edge_cos)]
print(f"\n5. EVENTS")
print(f"   All CIQ events:               {all_evt:>8,}")
print(f"   -> US private:                {len(evt_us):>8,}  (-{all_evt-len(evt_us):,})")
print(f"   -> 2010+:                     {len(evt_2010):>8,}  (-{len(evt_us)-len(evt_2010):,})")
print(f"   -> Drop noise:                {len(evt_clean):>8,}  (-{len(evt_2010)-len(evt_clean):,})")
print(f"   -> At networked cos:          {len(evt_net):>8,}  (-{len(evt_clean)-len(evt_net):,})")

# 6. CRSP
print(f"\n6. CRSP DAILY RETURNS")
for label, fname in [("2010-2014", "06c_portfolio_crsp_daily_2010_2014.csv"),
                     ("2015-2024", "06_portfolio_crsp_daily.csv"),
                     ("2025", "06b_portfolio_crsp_daily_2025.csv")]:
    fp = os.path.join(data_dir, "Panel_C_Network", fname)
    if os.path.exists(fp):
        n = sum(1 for _ in open(fp, "r", encoding="utf-8")) - 1
        print(f"   {label}: {n:>10,}")

# 7. Preqin
xwalk = pd.read_csv(os.path.join(preqin_dir, "vc_preqin_crosswalk_validated.csv"))
xwalk_hm = xwalk[xwalk["quality"].isin(["high", "medium"])]
tb = pd.read_csv(os.path.join(data_dir, "table_b_observer_network.csv"))
tb["vc_firm_companyid"] = tb["vc_firm_companyid"].astype(str).str.replace(".0", "", regex=False)
all_vcs = tb["vc_firm_companyid"].nunique()
print(f"\n7. PREQIN MATCHING")
print(f"   CIQ VC firms:                 {all_vcs:>6,}")
print(f"   -> All Preqin matches:        {xwalk['ciq_vc_companyid'].nunique():>6,}")
print(f"   -> High+Medium quality:       {xwalk_hm['ciq_vc_companyid'].nunique():>6,}  (dropped {xwalk['ciq_vc_companyid'].nunique()-xwalk_hm['ciq_vc_companyid'].nunique():,} low)")

# 8. Fund data
funds = pd.read_csv(os.path.join(preqin_dir, "fund_details_full.csv"))
matched_fids = set(xwalk_hm["preqin_firm_id"].dropna().astype(int))
matched_funds = funds[funds["firm_id"].isin(matched_fids)]
vc_funds = matched_funds[matched_funds["fund_type"].str.contains("Venture|Seed|Early", case=False, na=False)]
vc_fund_ids = set(vc_funds["fund_id"].dropna().astype(int))

perf = pd.read_csv(os.path.join(preqin_dir, "fund_performance_full.csv"), low_memory=False)
perf_vc = perf[perf["fund_id"].isin(vc_fund_ids)]
perf_vc_irr = perf_vc[pd.to_numeric(perf_vc["net_irr_pcent"], errors="coerce").notna()]
perf_vc_mult = perf_vc[pd.to_numeric(perf_vc["multiple"], errors="coerce").notna()]

cf = pd.read_csv(os.path.join(preqin_dir, "cashflows_full.csv"))
cf_vc = cf[cf["fund_id"].isin(vc_fund_ids)]

print(f"\n8. PREQIN FUND DATA")
print(f"   All funds at matched firms:   {len(matched_funds):>6,}")
print(f"   -> VC/Seed/Early:             {len(vc_funds):>6,}")
print(f"   -> With any performance:      {perf_vc['fund_id'].nunique():>6,}  ({len(perf_vc):,} quarterly records)")
print(f"   -> With numeric IRR:          {perf_vc_irr['fund_id'].nunique():>6,}")
print(f"   -> With numeric multiple:     {perf_vc_mult['fund_id'].nunique():>6,}")
print(f"   -> With cashflows:            {cf_vc['fund_id'].nunique():>6,}  ({len(cf_vc):,} records)")

# 9. Final panels
matched_ciq = set(xwalk_hm["ciq_vc_companyid"].astype(str))
linked_obs = set()
for vc in matched_ciq:
    ocs = set(tb[tb["vc_firm_companyid"] == vc]["observed_companyid"].astype(str).str.replace(".0", "", regex=False))
    linked_obs.update(ocs & us_priv)
evt_linked = evt_clean[evt_clean["companyid"].isin(linked_obs)]

print(f"\n9. FINAL REGRESSION PANELS")
print(f"   Linked observed companies:    {len(linked_obs):>6,}")
print(f"   Events at linked cos:         {len(evt_linked):>8,}")
print(f"   Performance panel:            ~44,862 fund-quarters")
print(f"   Cashflow panel:               ~9,741 fund-quarters")
print(f"   Unique firms in regressions:  377")
print(f"   Unique funds in regressions:  1,291 (performance), 480 (cashflows)")

# Summary flow
print(f"\n{'=' * 70}")
print("ATTRITION FLOW")
print(f"{'=' * 70}")
print(f"""
  4,915 observer persons (3,058 companies)
    |  -33% non-US
  2,942 US private company observers (1,830 companies)
    |  -67% have no public company connection
  1,113 observers with network edges (1,017 companies)
    |  -97% of public positions are 'Former' in CIQ
    |  BoardEx dates cover 63.5%, ISS adds 84 more
  ~73 observers with CURRENT active public links
    |
  4,770 network edges (348 same-industry)
    |  -73% stale at any given event date
  ~700 active edges per typical year

  400,886 CIQ events
    |  -70% non-US/public/pre-2010
  81,767 US private events (2010+)
    |  -41% noise (earnings, conferences)
  48,144 filtered events
    |  -42% at companies not in network
  28,498 events at networked companies

  4,664 CIQ VC firms
    |  Preqin name matching
  1,612 high+medium quality matches
    |  Filter to VC/Seed/Early funds
  4,943 VC funds at matched firms
    |  Require performance data
  1,291 funds with quarterly performance
    |  480 funds with cashflow data
    |
  FINAL: 44,862 fund-quarter performance obs
         9,741 fund-quarter cashflow obs
""")
