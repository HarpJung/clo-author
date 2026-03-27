"""Test 3: Placebo Tests.
1. Network shuffle: randomly reassign which portfolio companies are "connected"
2. Random event dates: keep real network but assign random dates
Run 500 iterations each, compute distribution of placebo coefficients.
Compare to actual coefficient.
Focused on M&A Buyer [-30,-1] and Bankruptcy [-30,-1].
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv, time
import statsmodels.formula.api as smf
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 110)
print("TEST 3: PLACEBO TESTS")
print("=" * 110)

# =====================================================================
# LOAD (abbreviated — same pipeline)
# =====================================================================
print("\n--- Loading ---")

events = pd.read_csv(os.path.join(ciq_dir, "06d_observer_all_events_full.csv"), low_memory=False)
events["companyid"] = events["companyid"].astype(str).str.replace(".0", "", regex=False)
events["announcedate"] = pd.to_datetime(events["announcedate"], errors="coerce")
events = events.dropna(subset=["announcedate"])
events["event_year"] = events["announcedate"].dt.year
events = events[(events["event_year"] >= 2015) & (events["event_year"] <= 2025)]

pub_cids = set()
with open(os.path.join(ciq_dir, "04_observer_company_details.csv"), "r", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        cid = str(r.get("companyid", "")).strip().replace(".0", "")
        if "public" in str(r.get("companytypename", "")).lower():
            pub_cids.add(cid)
events = events[~events["companyid"].isin(pub_cids)]

ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0"), errors="coerce")
cid_to_cik = dict(zip(ciq_xwalk["cid"], ciq_xwalk["cik_int"]))

panel_b_xwalk = pd.read_csv(os.path.join(data_dir, "Panel_B_Outcomes", "01_identifier_crosswalk.csv"))
panel_b_xwalk["cik_int"] = pd.to_numeric(panel_b_xwalk["cik"], errors="coerce")
panel_b_xwalk["linkdt"] = pd.to_datetime(panel_b_xwalk["linkdt"], errors="coerce")
panel_b_xwalk["linkenddt"] = pd.to_datetime(panel_b_xwalk["linkenddt"], errors="coerce").fillna(pd.Timestamp("2099-12-31"))
listing = panel_b_xwalk.groupby("cik_int").agg(first_listed=("linkdt", "min"), last_listed=("linkenddt", "max")).reset_index()
events["cik_int"] = events["companyid"].map(cid_to_cik)
events = events.merge(listing, on="cik_int", how="left")
events["was_public"] = (events["announcedate"] >= events["first_listed"]) & (events["announcedate"] <= events["last_listed"])
events = events[~events["was_public"].fillna(False)]

noise_types = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
               "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)",
               "Annual General Meeting", "Special/Extraordinary Shareholders Meeting",
               "Shareholder/Analyst Calls", "Special Calls", "Ex-Div Date (Regular)", "Ex-Div Date (Special)"]
events = events[~events["eventtype"].isin(noise_types)]

edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")
pxw = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
pxw["cik_int"] = pd.to_numeric(pxw["cik"], errors="coerce")
edges = edges.merge(pxw.drop_duplicates("cik_int", keep="first")[["cik_int", "permno"]].rename(
    columns={"cik_int": "portfolio_cik_int"}), on="portfolio_cik_int", how="inner")
edges["permno"] = pd.to_numeric(edges["permno"], errors="coerce").astype("Int64")

industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))
edges["same_industry"] = (edges["observed_companyid"].map(cid_to_cik).map(cik_to_sic2) ==
                           edges["portfolio_cik_int"].map(cik_to_sic2)).astype(int)

connected_set = set()
for _, row in edges.iterrows():
    connected_set.add((row["observed_companyid"], int(row["permno"])))

pmcik = dict(zip(pxw["permno"].dropna().astype(int), pxw["cik_int"].dropna().astype(int)))
pm_sic2 = {p: cik_to_sic2.get(c, "") for p, c in pmcik.items()}
obs_with_edges = set(edges["observed_companyid"])

pd_daily = pd.read_csv(os.path.join(panel_c_dir, "06_portfolio_crsp_daily.csv"))
pd_daily["date"] = pd.to_datetime(pd_daily["date"])
pd_daily["permno"] = pd.to_numeric(pd_daily["permno"], errors="coerce").dropna().astype(int)
pd_daily["ret"] = pd.to_numeric(pd_daily["ret"], errors="coerce")
pd_daily = pd_daily.dropna(subset=["ret"]).sort_values(["permno", "date"])
mkt_ret = pd_daily.groupby("date")["ret"].mean().rename("mkt_ret")
pd_daily = pd_daily.merge(mkt_ret, on="date", how="left")
pd_daily["aret"] = pd_daily["ret"] - pd_daily["mkt_ret"]
all_permnos = sorted(pd_daily["permno"].unique())

pmdata = {}
for p, g in pd_daily.groupby("permno"):
    pmdata[p] = (g["date"].values, g["aret"].values)


def calc_car30(permno, event_np):
    if permno not in pmdata:
        return None
    dates, rets = pmdata[permno]
    if len(dates) < 30:
        return None
    diffs = (dates - event_np).astype("timedelta64[D]").astype(int)
    mask = (diffs >= -30) & (diffs <= -1)
    wr = rets[mask]
    if len(wr) >= 10:
        return float(np.sum(wr))
    return None


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


bankruptcy_types = [t for t in events["eventtype"].unique() if "Bankruptcy" in str(t) or "bankrupt" in str(t).lower()]

event_groups = [
    ("M&A Buyer", lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Buyer")]),
    ("Bankruptcy", lambda df: df[df["eventtype"].isin(bankruptcy_types)]),
]

N_PLACEBO = 500

for group_name, group_fn in event_groups:
    grp = group_fn(events)
    grp = grp[grp["companyid"].isin(obs_with_edges)]
    grp_df = grp[["companyid", "announcedate", "event_year"]].drop_duplicates(subset=["companyid", "announcedate"])

    if len(grp_df) < 20:
        continue

    print(f"\n\n{'='*110}")
    print(f"{group_name} ({len(grp_df)} events)")
    print(f"{'='*110}")

    # =====================================================================
    # First compute the ACTUAL coefficient
    # =====================================================================
    print("\n  Computing actual coefficient...")
    actual_obs = []
    event_id = 0
    evl = grp_df.to_dict("records")

    for ev in evl:
        enp = np.datetime64(ev["announcedate"])
        ecid = ev["companyid"]
        obs_cik = cid_to_cik.get(ecid)
        obs_sic2 = cik_to_sic2.get(obs_cik, "") if obs_cik else ""

        for pmi, pm in enumerate(all_permnos):
            is_conn = 1 if (ecid, pm) in connected_set else 0
            if not is_conn and pmi % 10 != 0:
                continue
            car = calc_car30(pm, enp)
            if car is None:
                continue
            psic = pm_sic2.get(pm, "")
            si = 1 if (obs_sic2 and psic and obs_sic2 == psic) else 0
            actual_obs.append({
                "event_id": event_id, "connected": is_conn,
                "same_industry": si, "car_30": car,
            })
        event_id += 1

    actual_df = pd.DataFrame(actual_obs)
    actual_df["cx"] = actual_df["connected"] * actual_df["same_industry"]
    actual_df["eid_str"] = actual_df["event_id"].astype(str)

    m_actual = smf.ols("car_30 ~ connected + same_industry + cx", data=actual_df).fit(
        cov_type="cluster", cov_kwds={"groups": actual_df["eid_str"]})
    actual_coef = m_actual.params["cx"]
    actual_p = m_actual.pvalues["cx"]
    print(f"  ACTUAL: conn x same = {actual_coef:+.5f} (p={actual_p:.4f}{sig(actual_p)})")
    print(f"  N={len(actual_df):,}, connected={actual_df['connected'].sum():,}")

    # =====================================================================
    # PLACEBO 1: Network shuffle
    # =====================================================================
    print(f"\n  --- PLACEBO 1: NETWORK SHUFFLE ({N_PLACEBO} iterations) ---")
    print(f"  Randomly reassign which stocks are 'connected' for each event,")
    print(f"  keeping the number of connected stocks per event the same.")

    # Count connected per event
    conn_per_event = actual_df.groupby("event_id")["connected"].sum().to_dict()

    placebo_coefs_network = []
    t0 = time.time()

    for iteration in range(N_PLACEBO):
        # Shuffle: for each event, randomly pick the same number of stocks as "connected"
        shuffled = actual_df.copy()
        shuffled["connected"] = 0
        for eid, n_conn in conn_per_event.items():
            mask = shuffled["event_id"] == eid
            indices = shuffled[mask].index
            if len(indices) > 0 and n_conn > 0:
                chosen = np.random.choice(indices, size=min(int(n_conn), len(indices)), replace=False)
                shuffled.loc[chosen, "connected"] = 1

        shuffled["cx"] = shuffled["connected"] * shuffled["same_industry"]
        try:
            m = smf.ols("car_30 ~ connected + same_industry + cx", data=shuffled).fit(
                cov_type="cluster", cov_kwds={"groups": shuffled["eid_str"]})
            placebo_coefs_network.append(m.params["cx"])
        except:
            pass

        if (iteration + 1) % 100 == 0:
            elapsed = time.time() - t0
            print(f"    Iteration {iteration+1}/{N_PLACEBO} | {elapsed:.0f}s")

    placebo_coefs_network = np.array(placebo_coefs_network)
    p_empirical_network = np.mean(placebo_coefs_network >= actual_coef)

    print(f"\n  Network Shuffle Results:")
    print(f"    Actual coefficient:  {actual_coef:+.5f}")
    print(f"    Placebo mean:        {placebo_coefs_network.mean():+.5f}")
    print(f"    Placebo std:         {placebo_coefs_network.std():.5f}")
    print(f"    Placebo min/max:     {placebo_coefs_network.min():+.5f} / {placebo_coefs_network.max():+.5f}")
    print(f"    Empirical p-value:   {p_empirical_network:.4f} ({(1-p_empirical_network)*100:.1f}% of placebos are below actual)")
    print(f"    Actual > all {N_PLACEBO} placebos: {actual_coef > placebo_coefs_network.max()}")

    # =====================================================================
    # PLACEBO 2: Random event dates
    # =====================================================================
    print(f"\n  --- PLACEBO 2: RANDOM EVENT DATES ({N_PLACEBO} iterations) ---")
    print(f"  Keep real network, but assign random dates from 2015-2025.")

    # Get all trading dates
    trading_dates = sorted(pd_daily["date"].unique())
    trading_dates = [d for d in trading_dates if pd.Timestamp(d).year >= 2015]

    placebo_coefs_dates = []
    t0 = time.time()

    for iteration in range(N_PLACEBO):
        # Assign random dates to each event
        random_dates = np.random.choice(trading_dates, size=len(evl), replace=True)

        rand_obs = []
        for ei, (ev, rdate) in enumerate(zip(evl, random_dates)):
            enp = np.datetime64(rdate)
            ecid = ev["companyid"]
            obs_cik = cid_to_cik.get(ecid)
            obs_sic2 = cik_to_sic2.get(obs_cik, "") if obs_cik else ""

            for pmi, pm in enumerate(all_permnos):
                is_conn = 1 if (ecid, pm) in connected_set else 0
                if not is_conn and pmi % 10 != 0:
                    continue
                car = calc_car30(pm, enp)
                if car is None:
                    continue
                psic = pm_sic2.get(pm, "")
                si = 1 if (obs_sic2 and psic and obs_sic2 == psic) else 0
                rand_obs.append({
                    "event_id": ei, "connected": is_conn,
                    "same_industry": si, "car_30": car,
                })

        if not rand_obs:
            continue
        rand_df = pd.DataFrame(rand_obs)
        rand_df["cx"] = rand_df["connected"] * rand_df["same_industry"]
        rand_df["eid_str"] = rand_df["event_id"].astype(str)

        try:
            m = smf.ols("car_30 ~ connected + same_industry + cx", data=rand_df).fit(
                cov_type="cluster", cov_kwds={"groups": rand_df["eid_str"]})
            placebo_coefs_dates.append(m.params["cx"])
        except:
            pass

        if (iteration + 1) % 50 == 0:
            elapsed = time.time() - t0
            rm = (N_PLACEBO - iteration - 1) / (iteration + 1) * elapsed / 60
            print(f"    Iteration {iteration+1}/{N_PLACEBO} | {elapsed:.0f}s | ~{rm:.0f}min left")

    placebo_coefs_dates = np.array(placebo_coefs_dates)
    p_empirical_dates = np.mean(placebo_coefs_dates >= actual_coef)

    print(f"\n  Random Dates Results:")
    print(f"    Actual coefficient:  {actual_coef:+.5f}")
    print(f"    Placebo mean:        {placebo_coefs_dates.mean():+.5f}")
    print(f"    Placebo std:         {placebo_coefs_dates.std():.5f}")
    print(f"    Placebo min/max:     {placebo_coefs_dates.min():+.5f} / {placebo_coefs_dates.max():+.5f}")
    print(f"    Empirical p-value:   {p_empirical_dates:.4f} ({(1-p_empirical_dates)*100:.1f}% of placebos are below actual)")
    print(f"    Actual > all {N_PLACEBO} placebos: {actual_coef > placebo_coefs_dates.max()}")

    # =====================================================================
    # SUMMARY
    # =====================================================================
    print(f"\n  === SUMMARY for {group_name} ===")
    print(f"  Actual conn x same at [-30,-1]: {actual_coef:+.5f} (parametric p={actual_p:.4f}{sig(actual_p)})")
    print(f"  Network shuffle empirical p:    {p_empirical_network:.4f}{sig(p_empirical_network)}")
    print(f"  Random dates empirical p:       {p_empirical_dates:.4f}{sig(p_empirical_dates)}")


print("\n\nDone.")
