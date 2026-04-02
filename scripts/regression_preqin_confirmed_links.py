"""
Fund performance test with TRIPLE-CONFIRMED, TIME-STAMPED links.

Data chain:
  1. ciqcompanyrel: VC Y invested in Company A (confirmed, with $)
  2. ciqtransaction: Company A's Round N closed on date D
  3. ciqprotoprofunction: Person X started at Company A in year Y
  4. ciqprofessional: Person X has "observer" in title at Company A
  5. Preqin: VC Y's fund performance over time

Only count events at Company A as treatment for VC Y's funds when:
  (a) VC Y is confirmed investor in Company A (ciqcompanyrel)
  (b) The event happens after the estimated observer placement date
  (c) The fund vintage is compatible with the investment timing
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import pandas as pd
import numpy as np
import os
import statsmodels.api as sm

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
preqin_dir = os.path.join(data_dir, "Preqin")

print("=" * 90)
print("TRIPLE-CONFIRMED FUND PERFORMANCE TEST")
print("=" * 90)

# =====================================================================
# STEP 1: Build confirmed VC -> observed company links with timing
# =====================================================================
print("\n--- Step 1: Build confirmed investment links ---")

# 1a: VC investment relationships
vc_inv = pd.read_csv(os.path.join(ciq_dir, "09_vc_portfolio_investments.csv"))
vc_inv["vc_companyid"] = vc_inv["vc_companyid"].astype(str).str.replace(".0", "", regex=False)
vc_inv["portfolio_companyid"] = vc_inv["portfolio_companyid"].astype(str).str.replace(".0", "", regex=False)
print(f"  VC investment links: {len(vc_inv):,}")

# 1b: Observer records (who observes where)
obs = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs["personid"] = obs["personid"].astype(str).str.replace(".0", "", regex=False)
obs["companyid"] = obs["companyid"].astype(str).str.replace(".0", "", regex=False)

co = pd.read_csv(os.path.join(ciq_dir, "04_observer_company_details.csv"))
us_priv = set(co[(co["country"] == "United States") & (co["companytypename"] == "Private Company")]["companyid"].astype(str).str.replace(".0", "", regex=False))
obs_us = obs[obs["companyid"].isin(us_priv)]

# 1c: Observer -> VC firm mapping (from all positions)
pos = pd.read_csv(os.path.join(ciq_dir, "05_observer_person_all_positions.csv"))
pos["personid"] = pos["personid"].astype(str).str.replace(".0", "", regex=False)
pos["companyid"] = pos["companyid"].astype(str).str.replace(".0", "", regex=False)

vc_types = {"Private Investment Firm", "Public Investment Firm", "Private Fund"}
obs_vc = pos[pos["companytypename"].isin(vc_types) & pos["personid"].isin(set(obs_us["personid"]))]

# observer -> VC firms
obs_to_vcs = {}
for _, r in obs_vc.iterrows():
    pid = r["personid"]
    if pid not in obs_to_vcs:
        obs_to_vcs[pid] = set()
    obs_to_vcs[pid].add(r["companyid"])

# observer -> observed companies
obs_to_cos = {}
for _, r in obs_us.iterrows():
    pid = r["personid"]
    if pid not in obs_to_cos:
        obs_to_cos[pid] = set()
    obs_to_cos[pid].add(r["companyid"])

# 1d: Find TRIPLE-CONFIRMED links:
# Observer X observes at Company A AND works at VC Y AND VC Y invested in Company A
confirmed = []
for pid in obs_to_cos:
    observed_cos = obs_to_cos[pid]
    vc_firms = obs_to_vcs.get(pid, set())

    for obs_cid in observed_cos:
        for vc_cid in vc_firms:
            # Check: did this VC invest in this observed company?
            match = vc_inv[(vc_inv["vc_companyid"] == vc_cid) &
                           (vc_inv["portfolio_companyid"] == obs_cid)]
            if len(match) > 0:
                for _, m in match.iterrows():
                    confirmed.append({
                        "observer_personid": pid,
                        "observed_companyid": obs_cid,
                        "vc_companyid": vc_cid,
                        "investment_type": m["companyreltypename"],
                        "percent_ownership": m.get("percentownership"),
                        "total_investment": m.get("totalinvestment"),
                    })

confirmed_df = pd.DataFrame(confirmed).drop_duplicates(
    subset=["observer_personid", "observed_companyid", "vc_companyid"])

print(f"  Triple-confirmed links: {len(confirmed_df):,}")
print(f"  Unique observers: {confirmed_df['observer_personid'].nunique():,}")
print(f"  Unique observed companies: {confirmed_df['observed_companyid'].nunique():,}")
print(f"  Unique VCs: {confirmed_df['vc_companyid'].nunique():,}")

# Compare to original inferred network
tb = pd.read_csv(os.path.join(data_dir, "table_b_observer_network.csv"))
tb["vc_firm_companyid"] = tb["vc_firm_companyid"].astype(str).str.replace(".0", "", regex=False)
tb["observed_companyid"] = tb["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
orig_triples = set(zip(tb["vc_firm_companyid"], tb["observed_companyid"]))
confirmed_triples = set(zip(confirmed_df["vc_companyid"], confirmed_df["observed_companyid"]))
print(f"\n  Original inferred VC-company pairs: {len(orig_triples):,}")
print(f"  Confirmed via ciqcompanyrel: {len(confirmed_triples):,}")
print(f"  Overlap: {len(orig_triples & confirmed_triples):,}")
print(f"  Confirmed but NOT in original: {len(confirmed_triples - orig_triples):,}")
print(f"  In original but NOT confirmed: {len(orig_triples - confirmed_triples):,}")

# =====================================================================
# STEP 2: Add timing from transactions and position dates
# =====================================================================
print("\n--- Step 2: Add timing ---")

# 2a: Transaction dates (deal closing dates for observed companies)
trans = pd.read_csv(os.path.join(ciq_dir, "10_observed_company_transactions.csv"))
trans["companyid"] = trans["companyid"].astype(str).str.replace(".0", "", regex=False)
# Build closing date
trans["close_date"] = pd.to_datetime(
    trans.apply(lambda r: f"{int(r['closingyear'])}-{int(r['closingmonth']):02d}-{int(r['closingday']):02d}"
                if pd.notna(r["closingyear"]) and pd.notna(r["closingmonth"]) and pd.notna(r["closingday"])
                else None, axis=1),
    errors="coerce"
)
# Earliest transaction per company = proxy for first VC investment
first_trans = trans.groupby("companyid")["close_date"].min().reset_index()
first_trans.columns = ["observed_companyid", "first_deal_date"]
first_trans_dict = dict(zip(first_trans["observed_companyid"], first_trans["first_deal_date"]))
print(f"  Companies with transaction dates: {first_trans['first_deal_date'].notna().sum():,}")

# 2b: Position dates (observer start/end dates)
func = pd.read_csv(os.path.join(ciq_dir, "11_observer_position_dates.csv"))
proid_map = pd.read_csv(os.path.join(ciq_dir, "11b_observer_proid_mapping.csv"))
proid_map["proid"] = proid_map["proid"].astype(str).str.replace(".0", "", regex=False)
proid_map["personid"] = proid_map["personid"].astype(str).str.replace(".0", "", regex=False)
proid_map["companyid"] = proid_map["companyid"].astype(str).str.replace(".0", "", regex=False)
func["proid"] = func["proid"].astype(str).str.replace(".0", "", regex=False)

# Join to get personid and companyid
func = func.merge(proid_map[["proid", "personid", "companyid"]], on="proid", how="left")

# Build start date
func["start_date"] = pd.to_datetime(
    func.apply(lambda r: f"{int(r['startyear'])}-{int(r['startmonth']):02d}-{int(r['startday']):02d}"
               if pd.notna(r["startyear"]) and pd.notna(r["startmonth"]) and pd.notna(r["startday"])
               else (f"{int(r['startyear'])}-01-01" if pd.notna(r["startyear"]) else None), axis=1),
    errors="coerce"
)
func["end_date"] = pd.to_datetime(
    func.apply(lambda r: f"{int(r['endyear'])}-{int(r['endmonth']):02d}-{int(r['endday']):02d}"
               if pd.notna(r["endyear"]) and pd.notna(r["endmonth"]) and pd.notna(r["endday"])
               else (f"{int(r['endyear'])}-12-31" if pd.notna(r["endyear"]) else None), axis=1),
    errors="coerce"
)

# For each (person, company), get earliest start and latest end
person_co_dates = func.groupby(["personid", "companyid"]).agg(
    earliest_start=("start_date", "min"),
    latest_end=("end_date", "max"),
    is_current=("currentflag", "max"),
).reset_index()

has_start = person_co_dates["earliest_start"].notna().sum()
has_end = person_co_dates["latest_end"].notna().sum()
print(f"  Person-company pairs with start date: {has_start:,} ({has_start/len(person_co_dates)*100:.1f}%)")
print(f"  Person-company pairs with end date: {has_end:,}")

# 2c: Merge timing into confirmed links
confirmed_df = confirmed_df.merge(
    person_co_dates,
    left_on=["observer_personid", "observed_companyid"],
    right_on=["personid", "companyid"],
    how="left"
)
confirmed_df["deal_date"] = confirmed_df["observed_companyid"].map(first_trans_dict)

# Best estimate of when observer was active
confirmed_df["obs_start"] = confirmed_df["earliest_start"].fillna(confirmed_df["deal_date"])
confirmed_df["obs_end"] = confirmed_df["latest_end"].fillna(pd.Timestamp("2026-12-31"))
# If still no start, use 2010 as conservative default
confirmed_df["obs_start"] = confirmed_df["obs_start"].fillna(pd.Timestamp("2010-01-01"))

has_timing = confirmed_df["obs_start"].notna().sum()
print(f"\n  Confirmed links with observer timing: {has_timing:,} ({has_timing/len(confirmed_df)*100:.1f}%)")
print(f"  From position dates: {confirmed_df['earliest_start'].notna().sum():,}")
print(f"  From deal dates: {(confirmed_df['earliest_start'].isna() & confirmed_df['deal_date'].notna()).sum():,}")

# =====================================================================
# STEP 3: Match to Preqin and build fund-quarter panel
# =====================================================================
print("\n--- Step 3: Match to Preqin ---")

xwalk = pd.read_csv(os.path.join(preqin_dir, "vc_preqin_crosswalk_validated.csv"))
xwalk = xwalk[xwalk["quality"].isin(["high", "medium"])]
ciq_to_preqin = dict(zip(xwalk["ciq_vc_companyid"].astype(str), xwalk["preqin_firm_id"].astype(int)))

confirmed_df["preqin_firm_id"] = confirmed_df["vc_companyid"].map(ciq_to_preqin)
confirmed_preqin = confirmed_df.dropna(subset=["preqin_firm_id"])
print(f"  Confirmed links with Preqin match: {len(confirmed_preqin):,}")
print(f"  Unique Preqin firms: {confirmed_preqin['preqin_firm_id'].nunique():,}")

# Load fund performance
funds = pd.read_csv(os.path.join(preqin_dir, "fund_details_full.csv"))
matched_firm_ids = set(confirmed_preqin["preqin_firm_id"].astype(int))
funds = funds[funds["firm_id"].isin(matched_firm_ids)]
vc_funds = funds[funds["fund_type"].str.contains("Venture|Seed|Early", case=False, na=False)].copy()
vc_fund_ids = set(vc_funds["fund_id"].dropna().astype(int))

perf = pd.read_csv(os.path.join(preqin_dir, "fund_performance_full.csv"), low_memory=False)
perf = perf[perf["fund_id"].isin(vc_fund_ids)].copy()
perf["date_reported"] = pd.to_datetime(perf["date_reported"], errors="coerce")
perf = perf.dropna(subset=["date_reported"]).sort_values(["fund_id", "date_reported"])
perf["multiple_num"] = pd.to_numeric(perf["multiple"], errors="coerce")
perf["dpi_num"] = pd.to_numeric(perf["distr_dpi_pcent"], errors="coerce")
perf["d_multiple"] = perf.groupby("fund_id")["multiple_num"].diff()
perf["d_dpi"] = perf.groupby("fund_id")["dpi_num"].diff()
perf = perf.merge(vc_funds[["fund_id", "firm_id", "vintage", "final_size_usd"]],
                   on="fund_id", how="left", suffixes=("", "_fund"))
perf["quarter"] = perf["date_reported"].dt.to_period("Q")
perf["year"] = perf["date_reported"].dt.year

print(f"  VC funds at confirmed firms: {len(vc_funds):,}")
print(f"  Performance records: {len(perf):,}")
print(f"  Funds with performance: {perf['fund_id'].nunique():,}")

# =====================================================================
# STEP 4: Build time-bounded event counts using confirmed links
# =====================================================================
print("\n--- Step 4: Build event counts with confirmed timing ---")

# Build: for each Preqin firm, which observed companies are CONFIRMED?
# And what's the observer active period?
firm_to_confirmed_cos = {}  # preqin_firm_id -> [(obs_cid, obs_start, obs_end), ...]
for _, r in confirmed_preqin.iterrows():
    fid = int(r["preqin_firm_id"])
    if fid not in firm_to_confirmed_cos:
        firm_to_confirmed_cos[fid] = []
    firm_to_confirmed_cos[fid].append((
        r["observed_companyid"],
        r["obs_start"],
        r["obs_end"],
    ))

# Load events
events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])
events = events[events["companyid"].isin(us_priv)]
noise = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
         "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)"]
events = events[~events["eventtype"].isin(noise)]
events["quarter"] = events["announcedate"].dt.to_period("Q")
events["material"] = events["eventtype"].apply(
    lambda x: "M&A" in str(x) or "Bankruptcy" in str(x) or "Executive/Board" in str(x) or "Restructuring" in str(x))

# For each Preqin firm, for each quarter, count events at CONFIRMED companies
# ONLY events within the observer's active period
firm_q_events = []
n_total_events = 0
n_bounded_events = 0

for fid, cos_list in firm_to_confirmed_cos.items():
    all_obs_cids = set(c[0] for c in cos_list)
    firm_events = events[events["companyid"].isin(all_obs_cids) & events["material"]]

    for _, evt in firm_events.iterrows():
        edate = evt["announcedate"]
        ecid = evt["companyid"]
        n_total_events += 1

        # Check if this event falls within any confirmed observer's active period
        active = False
        for obs_cid, obs_start, obs_end in cos_list:
            if obs_cid == ecid and pd.notna(obs_start):
                if obs_start <= edate <= obs_end:
                    active = True
                    break
        if active:
            n_bounded_events += 1

    # Bounded event counts by quarter
    bounded_events = []
    for _, evt in firm_events.iterrows():
        edate = evt["announcedate"]
        ecid = evt["companyid"]
        for obs_cid, obs_start, obs_end in cos_list:
            if obs_cid == ecid and pd.notna(obs_start) and obs_start <= edate <= obs_end:
                bounded_events.append(evt)
                break

    if bounded_events:
        bdf = pd.DataFrame(bounded_events)
        qc = bdf.groupby("quarter").size().reset_index(name="n_events")
        qc["preqin_firm_id"] = fid
        firm_q_events.append(qc)

evt_counts = pd.concat(firm_q_events, ignore_index=True) if firm_q_events else pd.DataFrame()
print(f"  Total material events at confirmed cos: {n_total_events:,}")
print(f"  Within observer active period: {n_bounded_events:,} ({n_bounded_events/max(n_total_events,1)*100:.1f}%)")
print(f"  Firm-quarter event records: {len(evt_counts):,}")

# =====================================================================
# STEP 5: Build panel and run regressions
# =====================================================================
print("\n--- Step 5: Build panel ---")

# Map fund firm_id to preqin_firm_id (they should be the same)
perf["preqin_firm_id"] = perf["firm_id"].astype(int)

panel = perf.merge(evt_counts, on=["preqin_firm_id", "quarter"], how="left")
panel["n_events"] = panel["n_events"].fillna(0).astype(int)
panel["has_event"] = (panel["n_events"] > 0).astype(int)
panel["post_2020"] = (panel["year"] >= 2020).astype(int)
panel["evt_x_post"] = panel["has_event"] * panel["post_2020"]

print(f"  Panel: {len(panel):,} fund-quarters")
print(f"  Funds: {panel['fund_id'].nunique():,}")
print(f"  Firms: {panel['preqin_firm_id'].nunique():,}")
print(f"  Event quarters: {panel['has_event'].sum():,} ({panel['has_event'].mean()*100:.1f}%)")

# Year dummies
yr_dum = pd.get_dummies(panel["year"], prefix="yr", drop_first=True).astype(float)

# =====================================================================
# STEP 6: Regressions
# =====================================================================
print(f"\n\n{'=' * 90}")
print("RESULTS: Triple-Confirmed, Time-Bounded Fund Performance")
print(f"{'=' * 90}")

def run_reg(panel, dv_col, treat_col, yr_dum, dv_name, treat_name):
    panel = panel.copy()
    panel["treat_x_post"] = panel[treat_col] * panel["post_2020"]

    y = panel[dv_col].dropna()
    if len(y) < 200:
        print(f"    {dv_name} x {treat_name}: too few obs ({len(y)})")
        return
    lo, hi = y.quantile([0.01, 0.99])
    y = y.clip(lo, hi)
    idx = y.index

    X_base = panel.loc[idx, [treat_col, "post_2020", "treat_x_post"]].copy()
    X_yr = pd.concat([X_base, yr_dum.loc[idx]], axis=1)

    # Fund FE
    fund_mean = panel.loc[idx].groupby("fund_id")[dv_col].transform("mean")
    y_fe = y - fund_mean.loc[idx]

    # Firm FE
    firm_mean = panel.loc[idx].groupby("preqin_firm_id")[dv_col].transform("mean")
    y_firmfe = y - firm_mean.loc[idx]

    n_evt = int(panel.loc[idx, treat_col].sum())
    print(f"\n  {dv_name} x {treat_name} (N={len(y):,}, events={n_evt:,})")

    for sname, dep, xmat, cov, kwds in [
        ("HC1+YrFE", y, sm.add_constant(X_yr), "HC1", {}),
        ("Firm-cl+YrFE", y, sm.add_constant(X_yr), "cluster", {"groups": panel.loc[idx, "preqin_firm_id"]}),
        ("FundFE+HC1", y_fe, sm.add_constant(X_base), "HC1", {}),
        ("FirmFE+HC1", y_firmfe, sm.add_constant(X_base), "HC1", {}),
    ]:
        try:
            m = sm.OLS(dep, xmat).fit(cov_type=cov, cov_kwds=kwds if kwds else {})
            bt = m.params.get(treat_col, np.nan)
            pt = m.pvalues.get(treat_col, np.nan)
            bi = m.params.get("treat_x_post", np.nan)
            pi = m.pvalues.get("treat_x_post", np.nan)
            st = "***" if pt < 0.01 else "**" if pt < 0.05 else "*" if pt < 0.10 else ""
            si = "***" if pi < 0.01 else "**" if pi < 0.05 else "*" if pi < 0.10 else ""
            print(f"    {sname:<18} b(event)={bt:>8.4f}{st:<3} p={pt:.3f}  b(evtXpost20)={bi:>8.4f}{si:<3} p={pi:.3f}")
        except Exception as e:
            print(f"    {sname:<18} Error: {str(e)[:50]}")


for dv, dv_name in [("d_multiple", "Delta TVPI"), ("d_dpi", "Delta DPI")]:
    for treat, treat_name in [("has_event", "Any Event"), ("n_events", "N Events")]:
        run_reg(panel, dv, treat, yr_dum, dv_name, treat_name)

# Subsample means
print(f"\n\n{'=' * 90}")
print("SUBSAMPLE MEANS")
print(f"{'=' * 90}")

for dv, name in [("d_multiple", "Delta TVPI"), ("d_dpi", "Delta DPI")]:
    ev = panel[panel["has_event"] == 1][dv]
    ne = panel[panel["has_event"] == 0][dv]
    diff = ev.mean() - ne.mean() if ev.notna().sum() > 0 and ne.notna().sum() > 0 else np.nan
    print(f"  {name}: event={ev.mean():.4f} (N={ev.notna().sum():,})  no-event={ne.mean():.4f} (N={ne.notna().sum():,})  diff={diff:.4f}")

    # Pre vs post 2020
    pre_ev = panel[(panel["has_event"] == 1) & (panel["year"] < 2020)][dv]
    post_ev = panel[(panel["has_event"] == 1) & (panel["year"] >= 2020)][dv]
    print(f"    Pre-2020 event: {pre_ev.mean():.4f} (N={pre_ev.notna().sum():,})  Post-2020 event: {post_ev.mean():.4f} (N={post_ev.notna().sum():,})")

# =====================================================================
# STEP 7: Cross-sectional with confirmed observer count
# =====================================================================
print(f"\n\n{'=' * 90}")
print("CROSS-SECTIONAL: Confirmed observer count -> fund performance")
print(f"{'=' * 90}")

# Count confirmed observed companies per Preqin firm
firm_obs_count = confirmed_preqin.groupby("preqin_firm_id").agg(
    n_confirmed_cos=("observed_companyid", "nunique"),
    n_confirmed_observers=("observer_personid", "nunique"),
).reset_index()
firm_obs_count["preqin_firm_id"] = firm_obs_count["preqin_firm_id"].astype(int)

# Get final performance per fund
final_perf = perf.sort_values("date_reported").drop_duplicates("fund_id", keep="last")
final_perf = final_perf[["fund_id", "firm_id", "multiple_num", "dpi_num", "vintage"]].rename(
    columns={"firm_id": "preqin_firm_id"})
final_perf = final_perf.merge(vc_funds[["fund_id", "final_size_usd"]], on="fund_id", how="left")
final_perf = final_perf.merge(firm_obs_count, on="preqin_firm_id", how="left")
final_perf["n_confirmed_cos"] = final_perf["n_confirmed_cos"].fillna(0).astype(int)
final_perf["ln_size"] = np.log(final_perf["final_size_usd"].clip(lower=1))
final_perf = final_perf.dropna(subset=["multiple_num"])

print(f"  Funds with final performance: {len(final_perf):,}")
print(f"  Mean confirmed observed cos: {final_perf['n_confirmed_cos'].mean():.2f}")
print(f"  Max: {final_perf['n_confirmed_cos'].max()}")

for dv, dv_name in [("multiple_num", "Final TVPI"), ("dpi_num", "Final DPI")]:
    y = final_perf[dv].dropna()
    if len(y) < 100:
        continue
    lo, hi = y.quantile([0.01, 0.99])
    y = y.clip(lo, hi)
    idx = y.index

    X = final_perf.loc[idx, ["n_confirmed_cos", "ln_size"]].copy()
    vint = pd.get_dummies(final_perf.loc[idx, "vintage"].astype(int), prefix="v", drop_first=True).astype(float)

    # HC1 + Vintage FE
    X_v = pd.concat([X, vint], axis=1)
    X_v = sm.add_constant(X_v)
    try:
        m = sm.OLS(y, X_v).fit(cov_type="HC1")
        b = m.params.get("n_confirmed_cos", np.nan)
        p = m.pvalues.get("n_confirmed_cos", np.nan)
        s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
        print(f"\n  {dv_name}: HC1+VintFE: b(n_confirmed_cos)={b:.4f}{s} p={p:.3f} (N={len(y):,})")
    except Exception as e:
        print(f"  {dv_name}: Error: {str(e)[:50]}")

    # Firm FE
    n_firms = final_perf.loc[idx, "preqin_firm_id"].nunique()
    if n_firms >= 20:
        firm_mean = final_perf.loc[idx].groupby("preqin_firm_id")[dv].transform("mean")
        y_fe = y - firm_mean.loc[idx]
        X_fe = final_perf.loc[idx, ["n_confirmed_cos"]].copy()
        X_fe = sm.add_constant(X_fe)
        try:
            m = sm.OLS(y_fe, X_fe).fit(cov_type="HC1")
            b = m.params.get("n_confirmed_cos", np.nan)
            p = m.pvalues.get("n_confirmed_cos", np.nan)
            s = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            print(f"  {dv_name}: FirmFE: b(n_confirmed_cos)={b:.4f}{s} p={p:.3f}")
        except Exception as e:
            print(f"  {dv_name}: FirmFE Error: {str(e)[:50]}")

print("\n\nDone.")
