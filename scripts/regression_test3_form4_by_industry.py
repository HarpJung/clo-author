"""Test 3: Form 4 trades split by same-industry vs different-industry.
For each event at Firm A, check if observer traded Firm B where:
- Firm B is same-industry as Firm A → "informed" channel
- Firm B is different-industry → baseline trading
"""

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import os, numpy as np, pandas as pd, csv
from scipy import stats

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
ciq_dir = os.path.join(data_dir, "CIQ_Extract")
panel_c_dir = os.path.join(data_dir, "Panel_C_Network")

print("=" * 110)
print("TEST 3: FORM 4 TRADES BY SAME vs DIFF INDUSTRY")
print("=" * 110)

# Load trades
trades = pd.read_csv(os.path.join(data_dir, "Form4", "observer_form4_trades.csv"))
trades["trandate"] = pd.to_datetime(trades["trandate"], errors="coerce")
trades["personid"] = trades["personid"].astype(str).str.replace(".0", "", regex=False)
trades = trades.dropna(subset=["trandate"])

# CIQ-TR crosswalk
xwalk = pd.read_csv(os.path.join(ciq_dir, "08_observer_tr_insider_crosswalk.csv"))
xwalk["tr_personid"] = xwalk["tr_personid"].astype(str).str.replace(".0", "", regex=False)
xwalk["ciq_personid"] = xwalk["ciq_personid"].astype(str).str.replace(".0", "", regex=False)
tr_to_ciq = dict(zip(xwalk["tr_personid"], xwalk["ciq_personid"]))
trades["ciq_personid"] = trades["personid"].map(tr_to_ciq)
trades = trades.dropna(subset=["ciq_personid"])

# Network edges with same_industry
edges = pd.read_csv(os.path.join(panel_c_dir, "02_observer_public_portfolio_edges.csv"))
edges["observed_companyid"] = edges["observed_companyid"].astype(str).str.replace(".0", "", regex=False)
edges["observer_personid"] = edges["observer_personid"].astype(str).str.replace(".0", "", regex=False)

industry = pd.read_csv(os.path.join(panel_c_dir, "05_industry_codes.csv"))
industry["cik_int"] = pd.to_numeric(industry["cik"], errors="coerce")
industry["sic2"] = industry["sic"].astype(str).str[:2]
cik_to_sic2 = dict(zip(industry["cik_int"], industry["sic2"]))
ciq_xwalk = pd.read_csv(os.path.join(ciq_dir, "07_ciq_cik_crosswalk.csv"))
ciq_xwalk["cid"] = ciq_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)
ciq_xwalk["cik_int"] = pd.to_numeric(ciq_xwalk["cik"].astype(str).str.strip().str.lstrip("0"), errors="coerce")
cid_to_cik = dict(zip(ciq_xwalk["cid"], ciq_xwalk["cik_int"]))

# Build trade_secid -> SIC2 mapping via ticker matching to CRSP
# Use the full crosswalk to get secid -> companyid -> CIK -> SIC2
full_xwalk = pd.read_csv(os.path.join(data_dir, "Form4", "observer_tr_ciq_full_crosswalk.csv"))
full_xwalk["secid"] = pd.to_numeric(full_xwalk["secid"], errors="coerce")
full_xwalk["companyid"] = full_xwalk["companyid"].astype(str).str.replace(".0", "", regex=False)

# secid -> CIQ companyid -> CIK -> SIC2
secid_to_cid = dict(zip(full_xwalk["secid"].dropna(), full_xwalk["companyid"]))
secid_to_sic2 = {}
for secid, cid in secid_to_cid.items():
    cik = cid_to_cik.get(cid)
    if cik:
        sic = cik_to_sic2.get(cik)
        if sic:
            secid_to_sic2[secid] = sic

# Also try direct ticker -> SIC from our CRSP data
pxw = pd.read_csv(os.path.join(panel_c_dir, "03_portfolio_permno_crosswalk.csv"))
pxw["cik_int"] = pd.to_numeric(pxw["cik"], errors="coerce")

print(f"Trade secids with SIC2: {len(secid_to_sic2):,} (of {trades['secid'].nunique():,} unique)")

# Assign SIC2 to each trade
trades["secid_num"] = pd.to_numeric(trades["secid"], errors="coerce")
trades["trade_sic2"] = trades["secid_num"].map(secid_to_sic2)

# Observer records (observer -> company)
obs_records = pd.read_csv(os.path.join(ciq_dir, "01_observer_records.csv"))
obs_records["personid"] = obs_records["personid"].astype(str).str.replace(".0", "", regex=False)
obs_records["companyid"] = obs_records["companyid"].astype(str).str.replace(".0", "", regex=False)
company_to_observers = obs_records.groupby("companyid")["personid"].apply(set).to_dict()

# Observer trades indexed
observer_trades = {}
for ciq_pid, grp in trades.groupby("ciq_personid"):
    observer_trades[ciq_pid] = grp

# Events
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


def sig(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return "   "


bankruptcy_types = [t for t in events["eventtype"].unique() if "Bankruptcy" in str(t)]

event_groups = [
    ("All Events", lambda df: df),
    ("M&A Buyer", lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Buyer")]),
    ("M&A Target", lambda df: df[(df["eventtype"] == "M&A Transaction Announcements") & (df["objectroletype"] == "Target")]),
    ("Bankruptcy", lambda df: df[df["eventtype"].isin(bankruptcy_types)]),
    ("Exec/Board", lambda df: df[df["eventtype"] == "Executive/Board Changes - Other"]),
]

for group_name, group_fn in event_groups:
    grp = group_fn(events)
    grp_df = grp[["companyid", "announcedate", "event_year"]].drop_duplicates(subset=["companyid", "announcedate"])

    print(f"\n\n{'='*110}")
    print(f"{group_name} ({len(grp_df):,} events)")
    print(f"{'='*110}")

    same_ind_pre = []
    diff_ind_pre = []
    unknown_ind_pre = []
    same_ind_base = []
    diff_ind_base = []
    unknown_ind_base = []
    n_pairs = 0

    for _, ev in grp_df.iterrows():
        ecid = ev["companyid"]
        edate = ev["announcedate"]

        # Event company SIC2
        ecik = cid_to_cik.get(ecid)
        esic2 = cik_to_sic2.get(ecik, "") if ecik else ""

        observers = company_to_observers.get(ecid, set())

        for obs_pid in observers:
            if obs_pid not in observer_trades:
                continue
            n_pairs += 1

            obs_tr = observer_trades[obs_pid]
            days_diff = (obs_tr["trandate"] - edate).dt.days

            pre = obs_tr[(days_diff >= -30) & (days_diff <= -1)]
            base = obs_tr[(days_diff >= -120) & (days_diff <= -31)]

            for _, tr in pre.iterrows():
                tsic2 = tr.get("trade_sic2", "")
                if pd.isna(tsic2) or tsic2 == "" or esic2 == "":
                    unknown_ind_pre.append(tr)
                elif tsic2 == esic2:
                    same_ind_pre.append(tr)
                else:
                    diff_ind_pre.append(tr)

            for _, tr in base.iterrows():
                tsic2 = tr.get("trade_sic2", "")
                if pd.isna(tsic2) or tsic2 == "" or esic2 == "":
                    unknown_ind_base.append(tr)
                elif tsic2 == esic2:
                    same_ind_base.append(tr)
                else:
                    diff_ind_base.append(tr)

    print(f"  Event-observer pairs: {n_pairs:,}")
    print(f"\n  Pre-event [-30,-1]:")
    print(f"    Same-industry trades:  {len(same_ind_pre):,}")
    print(f"    Diff-industry trades:  {len(diff_ind_pre):,}")
    print(f"    Unknown industry:      {len(unknown_ind_pre):,}")
    total_pre = len(same_ind_pre) + len(diff_ind_pre) + len(unknown_ind_pre)
    if total_pre > 0:
        print(f"    Same-ind share:        {len(same_ind_pre)/total_pre*100:.1f}%")

    print(f"\n  Baseline [-120,-31]:")
    print(f"    Same-industry trades:  {len(same_ind_base):,}")
    print(f"    Diff-industry trades:  {len(diff_ind_base):,}")
    print(f"    Unknown industry:      {len(unknown_ind_base):,}")
    total_base = len(same_ind_base) + len(diff_ind_base) + len(unknown_ind_base)
    if total_base > 0:
        print(f"    Same-ind share:        {len(same_ind_base)/total_base*100:.1f}%")

    # Compare same-industry trading rate pre vs baseline
    if n_pairs > 0:
        pre_same_rate = len(same_ind_pre) / n_pairs
        base_same_rate = (len(same_ind_base) / n_pairs) * (30 / 90)
        pre_diff_rate = len(diff_ind_pre) / n_pairs
        base_diff_rate = (len(diff_ind_base) / n_pairs) * (30 / 90)

        print(f"\n  Trading rates (per event-observer pair, per 30 days):")
        print(f"    Same-ind pre:     {pre_same_rate:.4f}")
        print(f"    Same-ind base:    {base_same_rate:.4f}")
        if base_same_rate > 0:
            print(f"    Same-ind ratio:   {pre_same_rate/base_same_rate:.2f}x")
        print(f"    Diff-ind pre:     {pre_diff_rate:.4f}")
        print(f"    Diff-ind base:    {base_diff_rate:.4f}")
        if base_diff_rate > 0:
            print(f"    Diff-ind ratio:   {pre_diff_rate/base_diff_rate:.2f}x")

    # Buy/sell breakdown by industry match
    for ind_name, ind_pre, ind_base in [("Same-industry", same_ind_pre, same_ind_base),
                                          ("Diff-industry", diff_ind_pre, diff_ind_base)]:
        if len(ind_pre) >= 5:
            pre_df = pd.DataFrame(ind_pre)
            buys = (pre_df["trancode"] == "P").sum()
            sells = (pre_df["trancode"] == "S").sum()
            if buys + sells > 0:
                print(f"\n  {ind_name} pre-event: {buys} buys, {sells} sells (buy share: {buys/(buys+sells)*100:.1f}%)")

            if len(ind_base) >= 5:
                base_df = pd.DataFrame(ind_base)
                b_buys = (base_df["trancode"] == "P").sum()
                b_sells = (base_df["trancode"] == "S").sum()
                if b_buys + b_sells > 0:
                    print(f"  {ind_name} baseline:  {b_buys} buys, {b_sells} sells (buy share: {b_buys/(b_buys+b_sells)*100:.1f}%)")

                if buys + sells >= 3 and b_buys + b_sells >= 3:
                    from scipy.stats import fisher_exact
                    table = [[buys, sells], [b_buys, b_sells]]
                    or_val, p_val = fisher_exact(table)
                    print(f"  Fisher exact: OR={or_val:.3f}, p={p_val:.4f}{sig(p_val)}")


print("\n\nDone.")
