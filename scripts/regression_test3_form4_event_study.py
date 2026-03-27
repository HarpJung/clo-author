"""Test 3: Form 4 Insider Trading Event Study.
Do observers trade Public Firm B's stock around events at Private Firm A?

Design:
1. For each event at Private Firm A, check if the observer who sits on
   A's board made any trades at Public Firm B in the pre-event window
2. Compare trading frequency in event windows vs non-event windows
3. Compare purchases vs sales
4. Break down by event type (M&A Buyer, Bankruptcy, Exec/Board)
5. Compare same-industry vs different-industry trades
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv, time
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")
form4_dir = os.path.join(data_dir, "Form4")

print("=" * 110)
print("TEST 3: FORM 4 INSIDER TRADING EVENT STUDY")
print("=" * 110)

# =====================================================================
# STEP 1: Load Form 4 trades and link to CIQ observer personids
# =====================================================================
print("\n--- Loading Form 4 data ---")

trades = pd.read_csv(os.path.join(form4_dir, "observer_form4_trades.csv"))
trades["trandate"] = pd.to_datetime(trades["trandate"], errors="coerce")
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
trades = trades.dropna(subset=["trandate"])
print(f"  Trades: {len(trades):,}")

# Load CIQ-TR crosswalk
xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
xwalk["tr_personid"] = xwalk["tr_personid"].astype(str).str.replace(".0", "", regex=False)
xwalk["ciq_personid"] = xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
tr_to_ciq = dict(zip(xwalk["tr_personid"], xwalk["ciq_personid"]))

trades["ciq_personid"] = trades["personid"].map(tr_to_ciq)
trades = trades.dropna(subset=["ciq_personid"])
print(f"  Trades with CIQ match: {len(trades):,}")
print(f"  Unique CIQ persons: {trades['ciq_personid'].nunique():,}")

# =====================================================================
# STEP 2: Load observer network (person -> observed company -> portfolio company)
# =====================================================================
print("\n--- Loading network ---")

edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["observer_personid"] = edges["observer_personid"].astype(str).str.replace(".0", "", regex=False)
edges["portfolio_cik_int"] = pd.to_numeric(edges["portfolio_cik"], errors="coerce")

# Build: for each observer, which private companies do they observe?
observer_to_observed = edges.groupby("observer_personid")["observed_companyid"].apply(set).to_dict()

# Build: for each observer, which public companies are they connected to?
# Use CUSIP from trades to match to portfolio companies
# trades have cusip6+cusip2, edges have portfolio_cik
# We need to map trade company to portfolio company

# Get CIK for trade companies via CUSIP
# The trades have secid and ticker — use those to match
print(f"  Network edges: {len(edges):,}")
print(f"  Observers in network: {len(observer_to_observed):,}")

# =====================================================================
# STEP 3: Load events
# =====================================================================
print("\n--- Loading events ---")

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

noise_types = ["Announcements of Earnings", "Conferences", "Company Conference Presentations",
               "Earnings Calls", "Earnings Release Date", "Estimated Earnings Release Date (S&P Global Derived)",
               "Annual General Meeting", "Special/Extraordinary Shareholders Meeting",
               "Shareholder/Analyst Calls", "Special Calls", "Ex-Div Date (Regular)", "Ex-Div Date (Special)"]
events = events[~events["eventtype"].isin(noise_types)]
print(f"  Filtered events: {len(events):,}")

# =====================================================================
# STEP 4: For each observer-event pair, check if they traded around the event
# =====================================================================
print("\n--- Matching trades to events ---")

# Build event list: for each event, which observers were at that company?
obs_records = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs_records["personid"] = obs_records["personid"].astype(str).str.replace(".0", "", regex=False)
obs_records["companyid"] = obs_records["companyid"].astype(str).str.replace(".0", "", regex=False)
company_to_observers = obs_records.groupby("companyid")["personid"].apply(set).to_dict()

# For each observer who has Form 4 trades, get all their trades indexed by date
observer_trades = {}
for ciq_pid, grp in trades.groupby("ciq_personid"):
    observer_trades[ciq_pid] = grp[["trandate", "trancode", "acqdisp", "shares", "tprice",
                                      "cname", "ticker", "secid"]].sort_values("trandate")

print(f"  Observers with Form 4 data: {len(observer_trades):,}")

# Event groups
bankruptcy_types = [t for t in events["eventtype"].unique() if "Bankruptcy" in str(t)]
event_groups = [
    ("All Events", lambda df: df),
    ("M&A Buyer", lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Buyer")]),
    ("M&A Target", lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Target")]),
    ("Bankruptcy", lambda df: df[df["eventtype"].isin(bankruptcy_types)]),
    ("Exec/Board", lambda df: df[df["eventtype"] == "Executive/Board Changes - Other"]),
    ("CEO/CFO", lambda df: df[df["eventtype"].isin(["Executive Changes - CEO", "Executive Changes - CFO"])]),
]

for group_name, group_fn in event_groups:
    grp = group_fn(events)
    grp_df = grp[["companyid", "announcedate", "event_year", "eventtype"]].drop_duplicates(
        subset=["companyid", "announcedate"])

    print(f"\n\n{'='*110}")
    print(f"{group_name} ({len(grp_df):,} events)")
    print(f"{'='*110}")

    # For each event, find observers who were at that company and have Form 4 data
    event_trades_pre = []    # trades in [-30,-1] window
    event_trades_post = []   # trades in [0,+10] window
    baseline_trades = []     # trades in [-120,-31] window (baseline)
    n_event_observer_pairs = 0
    n_with_any_trade = 0

    for _, ev in grp_df.iterrows():
        ecid = ev["companyid"]
        edate = ev["announcedate"]

        # Which observers sit on this company's board?
        observers = company_to_observers.get(ecid, set())

        for obs_pid in observers:
            if obs_pid not in observer_trades:
                continue

            n_event_observer_pairs += 1
            obs_tr = observer_trades[obs_pid]

            # Count trades in different windows
            days_diff = (obs_tr["trandate"] - edate).dt.days

            pre_mask = (days_diff >= -30) & (days_diff <= -1)
            post_mask = (days_diff >= 0) & (days_diff <= 10)
            baseline_mask = (days_diff >= -120) & (days_diff <= -31)

            pre = obs_tr[pre_mask]
            post = obs_tr[post_mask]
            base = obs_tr[baseline_mask]

            if len(pre) > 0 or len(post) > 0:
                n_with_any_trade += 1

            for _, tr in pre.iterrows():
                event_trades_pre.append({
                    "observer_pid": obs_pid,
                    "event_company": ecid,
                    "event_date": edate,
                    "trade_date": tr["trandate"],
                    "days_before": (tr["trandate"] - edate).days,
                    "trancode": tr["trancode"],
                    "acqdisp": tr["acqdisp"],
                    "shares": tr["shares"],
                    "tprice": tr["tprice"],
                    "trade_company": tr["cname"],
                    "trade_ticker": tr["ticker"],
                })

            for _, tr in post.iterrows():
                event_trades_post.append({
                    "observer_pid": obs_pid,
                    "event_company": ecid,
                    "event_date": edate,
                    "trade_date": tr["trandate"],
                    "days_after": (tr["trandate"] - edate).days,
                    "trancode": tr["trancode"],
                    "acqdisp": tr["acqdisp"],
                    "shares": tr["shares"],
                    "tprice": tr["tprice"],
                    "trade_company": tr["cname"],
                    "trade_ticker": tr["ticker"],
                })

            for _, tr in base.iterrows():
                baseline_trades.append({
                    "observer_pid": obs_pid,
                    "trancode": tr["trancode"],
                    "acqdisp": tr["acqdisp"],
                    "shares": tr["shares"],
                })

    print(f"  Event-observer pairs: {n_event_observer_pairs:,}")
    print(f"  Pairs with any trade in [-30,+10]: {n_with_any_trade:,}")
    print(f"  Pre-event trades [-30,-1]: {len(event_trades_pre):,}")
    print(f"  Post-event trades [0,+10]: {len(event_trades_post):,}")
    print(f"  Baseline trades [-120,-31]: {len(baseline_trades):,}")

    if n_event_observer_pairs == 0:
        continue

    # Trading frequency comparison
    pre_rate = len(event_trades_pre) / n_event_observer_pairs  # trades per event-observer pair
    post_rate = len(event_trades_post) / n_event_observer_pairs
    # Baseline: 90-day window vs 30-day event window, normalize
    baseline_rate_per30 = (len(baseline_trades) / n_event_observer_pairs) * (30 / 90)

    print(f"\n  --- Trading Frequency ---")
    print(f"  Baseline rate (per 30 days):  {baseline_rate_per30:.4f} trades/pair")
    print(f"  Pre-event rate [-30,-1]:      {pre_rate:.4f} trades/pair")
    print(f"  Post-event rate [0,+10]:      {post_rate:.4f} trades/pair (per 10 days: {post_rate:.4f})")
    if baseline_rate_per30 > 0:
        print(f"  Pre/Baseline ratio:           {pre_rate/baseline_rate_per30:.2f}x")

    # Buy vs Sell breakdown
    if event_trades_pre:
        pre_df = pd.DataFrame(event_trades_pre)
        buys_pre = pre_df[pre_df["trancode"] == "P"]
        sells_pre = pre_df[pre_df["trancode"] == "S"]
        print(f"\n  --- Pre-Event Trade Breakdown [-30,-1] ---")
        print(f"  Total trades: {len(pre_df):,}")
        print(f"  Purchases (P): {len(buys_pre):,} ({len(buys_pre)/len(pre_df)*100:.1f}%)")
        print(f"  Sales (S):     {len(sells_pre):,} ({len(sells_pre)/len(pre_df)*100:.1f}%)")
        print(f"  Awards (A):    {len(pre_df[pre_df['trancode']=='A']):,}")
        print(f"  Other:         {len(pre_df[~pre_df['trancode'].isin(['P','S','A'])]):,}")

        if len(buys_pre) > 0:
            print(f"  Avg purchase shares: {buys_pre['shares'].mean():,.0f}")
        if len(sells_pre) > 0:
            print(f"  Avg sale shares:     {sells_pre['shares'].mean():,.0f}")

        # Day distribution
        print(f"\n  --- Pre-Event Day Distribution ---")
        day_counts = pre_df["days_before"].value_counts().sort_index()
        for d in range(-30, 0, 5):
            window = pre_df[(pre_df["days_before"] >= d) & (pre_df["days_before"] < d+5)]
            print(f"    [{d},{d+4}]: {len(window):,} trades")

    # Baseline buy vs sell
    if baseline_trades:
        base_df = pd.DataFrame(baseline_trades)
        base_buys = base_df[base_df["trancode"] == "P"]
        base_sells = base_df[base_df["trancode"] == "S"]
        print(f"\n  --- Baseline Trade Breakdown [-120,-31] ---")
        print(f"  Total: {len(base_df):,}")
        if len(base_df) > 0:
            print(f"  Purchases: {len(base_buys):,} ({len(base_buys)/len(base_df)*100:.1f}%)")
            print(f"  Sales:     {len(base_sells):,} ({len(base_sells)/len(base_df)*100:.1f}%)")

        # Chi-squared: is buy/sell ratio different in pre-event vs baseline?
        if len(event_trades_pre) > 0 and len(baseline_trades) > 0:
            pre_df_tc = pd.DataFrame(event_trades_pre)
            pre_buy_n = (pre_df_tc["trancode"] == "P").sum()
            pre_sell_n = (pre_df_tc["trancode"] == "S").sum()
            base_buy_n = (base_df["trancode"] == "P").sum()
            base_sell_n = (base_df["trancode"] == "S").sum()

            if pre_buy_n + pre_sell_n > 0 and base_buy_n + base_sell_n > 0:
                # Buy share in pre vs baseline
                pre_buy_share = pre_buy_n / (pre_buy_n + pre_sell_n) if (pre_buy_n + pre_sell_n) > 0 else 0
                base_buy_share = base_buy_n / (base_buy_n + base_sell_n) if (base_buy_n + base_sell_n) > 0 else 0
                print(f"\n  --- Buy Share Comparison ---")
                print(f"  Pre-event buy share:  {pre_buy_share:.3f} ({pre_buy_n} buys / {pre_buy_n+pre_sell_n} P+S)")
                print(f"  Baseline buy share:   {base_buy_share:.3f} ({base_buy_n} buys / {base_buy_n+base_sell_n} P+S)")

                if (pre_buy_n + pre_sell_n) >= 5 and (base_buy_n + base_sell_n) >= 5:
                    from scipy.stats import fisher_exact
                    table = [[pre_buy_n, pre_sell_n], [base_buy_n, base_sell_n]]
                    odds_ratio, p_fisher = fisher_exact(table)
                    print(f"  Fisher exact: OR={odds_ratio:.3f}, p={p_fisher:.4f}")

    # Post-event trades
    if event_trades_post:
        post_df = pd.DataFrame(event_trades_post)
        print(f"\n  --- Post-Event Trades [0,+10] ---")
        print(f"  Total: {len(post_df):,}")
        print(f"  Purchases: {(post_df['trancode']=='P').sum():,}")
        print(f"  Sales:     {(post_df['trancode']=='S').sum():,}")

    # Sample trades for inspection
    if event_trades_pre:
        pre_df = pd.DataFrame(event_trades_pre)
        print(f"\n  --- Sample Pre-Event Trades ---")
        for _, tr in pre_df.head(10).iterrows():
            print(f"    Observer {tr['observer_pid']}: {tr['trancode']} {tr.get('shares','?')} shares of {tr['trade_company'][:30]} on day {tr['days_before']} before event at company {tr['event_company']}")


print("\n\nDone.")
