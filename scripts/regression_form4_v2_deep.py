#!/usr/bin/env python3
"""
Form 4 Daily Event Study -- V2 Deep Tests
==========================================
Extends regression_form4_daily_eventstudy.py with:

  1. Same-industry vs different-industry trades (via CIQ-CIK-SIC crosswalk)
  2. Observer vs non-observer insider comparison (within-person baseline test)
  3. Returns after sells (profitability test for sells)
  4. Full clustering / FE battery on the stacked window panel
  5. NVCA 2020 DiD with all clustering / FE options

Author: Claude Code
"""

import sys, os, warnings, time
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import statsmodels.api as sm

T0 = time.time()

# ── paths ────────────────────────────────────────────────────────────────
BASE   = r"C:\Users\hjung\Documents\Claude\CorpAcct\Data"
FORM4  = os.path.join(BASE, "Form4", "observer_form4_trades.csv")
XWALK  = os.path.join(BASE, "CIQ_Extract", "08_observer_tr_insider_crosswalk.csv")
EVENTS = os.path.join(BASE, "CIQ_Extract", "06d_observer_all_events_full.csv")
OBSREC = os.path.join(BASE, "CIQ_Extract", "01_observer_records.csv")
CODET  = os.path.join(BASE, "CIQ_Extract", "04_observer_company_details.csv")
NETWORK = os.path.join(BASE, "table_b_observer_network.csv")
INDCODES = os.path.join(BASE, "Panel_C_Network", "05_industry_codes.csv")
INDCODES2 = os.path.join(BASE, "Panel_C_Network", "05b_new_industry_codes.csv")
CIQ_CIK = os.path.join(BASE, "CIQ_Extract", "07_ciq_cik_crosswalk.csv")
CRSP1  = os.path.join(BASE, "Panel_C_Network", "06_portfolio_crsp_daily.csv")
CRSP2  = os.path.join(BASE, "Panel_C_Network", "06b_portfolio_crsp_daily_2025.csv")

OUT_DIR = r"C:\Users\hjung\Documents\Claude\CorpAcct\clo-author\quality_reports"
os.makedirs(OUT_DIR, exist_ok=True)

# ── event types to DROP (noise / recurring / non-material) ───────────────
NOISE_EVENTS = {
    "Conferences", "Company Conference Presentations",
    "Announcements of Earnings", "Earnings Release Date", "Earnings Calls",
    "Estimated Earnings Release Date (S&P Global Derived)",
    "Annual General Meeting", "Board Meeting",
    "Index Constituent Adds", "Index Constituent Drops",
    "Shareholder/Analyst Calls", "Special/Extraordinary Shareholders Meeting",
    "Analyst/Investor Day", "Delayed Earnings Announcements",
    "Sales/Trading Statement Release Date", "Sales/Trading Statement Calls",
    "Interim Management Statement Release Date",
    "Interim Management Statement Calls",
    "Announcement of Interim Management Statement",
    "Operating Results Release Date", "Operating Results Calls",
    "Guidance/Update Calls", "Fixed Income Calls", "M&A Calls",
    "Address Changes", "Ticker Changes", "Name Changes",
    "Exchange Changes", "Fiscal Year End Changes", "Legal Structure Changes",
    "Corporate Guidance - New/Confirmed", "Corporate Guidance - Raised",
    "Corporate Guidance - Lowered",
    "Announcements of Sales/Trading Statement",
    "Announcement of Operating Results", "Delayed SEC Filings",
}

print("=" * 80)
print("FORM 4 DAILY EVENT STUDY -- V2 DEEP TESTS")
print("=" * 80)

# ══════════════════════════════════════════════════════════════════════════
# 1. LOAD DATA
# ══════════════════════════════════════════════════════════════════════════
print("\n[1] Loading data ...")

# -- Form 4 trades ---------------------------------------------------------
f4 = pd.read_csv(FORM4, low_memory=False)
print(f"    Form 4 raw rows: {len(f4):,}")
f4 = f4[f4["trancode"].isin(["P", "S"])].copy()
f4["trandate"] = pd.to_datetime(f4["trandate"], errors="coerce")
f4 = f4.dropna(subset=["trandate", "personid"])
f4["personid"] = f4["personid"].astype(int)
print(f"    Form 4 after P/S filter: {len(f4):,}")

# -- Crosswalk TR -> CIQ ---------------------------------------------------
xw = pd.read_csv(XWALK)
xw = xw[["tr_personid", "ciq_personid"]].dropna().drop_duplicates()
xw["tr_personid"]  = xw["tr_personid"].astype(int)
xw["ciq_personid"] = xw["ciq_personid"].astype(int)
f4 = f4.merge(xw, left_on="personid", right_on="tr_personid", how="inner")
print(f"    Form 4 after crosswalk merge: {len(f4):,}  "
      f"({f4['ciq_personid'].nunique()} CIQ persons)")

# -- Observer network -------------------------------------------------------
net = pd.read_csv(NETWORK)
obs_links = net[["observer_personid", "observed_companyid"]].drop_duplicates()
obs_links.columns = ["ciq_personid", "observed_companyid"]
obs_links["ciq_personid"] = obs_links["ciq_personid"].astype(int)
obs_links["observed_companyid"] = obs_links["observed_companyid"].astype(int)
print(f"    Observer links: {len(obs_links):,}")

# -- Company details --------------------------------------------------------
codet = pd.read_csv(CODET)
us_private = codet.loc[
    (codet["country"] == "United States") &
    (codet["companytypename"] == "Private Company"),
    "companyid"
].unique()
print(f"    US private companies: {len(us_private):,}")

# -- Material events --------------------------------------------------------
ev = pd.read_csv(EVENTS, low_memory=False)
print(f"    Events raw rows: {len(ev):,}")
ev["announcedate"] = pd.to_datetime(ev["announcedate"], errors="coerce")
ev = ev.dropna(subset=["announcedate", "companyid"])
ev = ev[ev["companyid"].astype(int).isin(us_private)]
ev = ev[ev["announcedate"].dt.year >= 2010]
ev = ev[~ev["eventtype"].isin(NOISE_EVENTS)]
ev = ev[ev["keydevtoobjectroletypeid"] == 1]
ev = ev[["keydevid", "companyid", "companyname", "eventtype",
         "keydeveventtypeid", "announcedate", "gvkey"]].drop_duplicates()
ev["companyid"] = ev["companyid"].astype(int)
print(f"    Events after filters: {len(ev):,}")

# -- CIQ-CIK crosswalk (for industry matching) -----------------------------
ciq_cik = pd.read_csv(CIQ_CIK, dtype={"cik": str})
ciq_cik = ciq_cik[["companyid", "cik"]].dropna().drop_duplicates()
ciq_cik["companyid"] = ciq_cik["companyid"].astype(int)
ciq_cik["cik"] = ciq_cik["cik"].astype(str).str.strip().str.lstrip("0")
print(f"    CIQ-CIK crosswalk: {len(ciq_cik):,}")

# -- Industry codes ---------------------------------------------------------
ind1 = pd.read_csv(INDCODES, dtype={"cik": str, "sic": str, "gvkey": str})
ind1["cik_clean"] = ind1["cik"].astype(str).str.strip().str.lstrip("0")
ind1["sic2"] = ind1["sic"].astype(str).str[:2]

ind2 = pd.read_csv(INDCODES2, dtype={"cik": str, "sic": str})
ind2["cik_clean"] = ind2["cik"].astype(str).str.strip().str.lstrip("0")
ind2["sic2"] = ind2["sic"].astype(str).str[:2]

# Combine: CIK -> SIC2 lookup (prefer ind1, fill with ind2)
cik_sic = pd.concat([
    ind1[["cik_clean", "sic2"]].rename(columns={"cik_clean": "cik"}),
    ind2[["cik_clean", "sic2"]].rename(columns={"cik_clean": "cik"}),
], ignore_index=True).drop_duplicates(subset=["cik"]).dropna()
cik_sic_dict = dict(zip(cik_sic["cik"], cik_sic["sic2"]))
print(f"    CIK -> SIC2 map entries: {len(cik_sic_dict):,}")

# -- CRSP daily returns -----------------------------------------------------
crsp1 = pd.read_csv(CRSP1, low_memory=False)
crsp2 = pd.read_csv(CRSP2, low_memory=False)
common_cols = list(set(crsp1.columns) & set(crsp2.columns))
crsp = pd.concat([crsp1[common_cols], crsp2[common_cols]], ignore_index=True)
crsp["date"] = pd.to_datetime(crsp["date"], errors="coerce")
crsp["ret"] = pd.to_numeric(crsp["ret"], errors="coerce")
crsp = crsp.dropna(subset=["date", "ret"])
crsp["permno"] = crsp["permno"].astype(int)
crsp = crsp.sort_values(["permno", "date"]).reset_index(drop=True)
print(f"    CRSP daily rows: {len(crsp):,}")

# ══════════════════════════════════════════════════════════════════════════
# 2. BUILD EVENT-OBSERVER-TRADE PANEL
# ══════════════════════════════════════════════════════════════════════════
print("\n[2] Building event-observer-trade panel ...")

ev_obs = ev.merge(obs_links, left_on="companyid", right_on="observed_companyid",
                  how="inner")
print(f"    Event-observer pairs: {len(ev_obs):,}")

panel = ev_obs.merge(
    f4[["ciq_personid", "trandate", "trancode", "ticker", "cname",
        "shares", "tprice", "shares_adj", "tprice_adj", "secid", "personid"]],
    on="ciq_personid", how="inner"
)
panel["day_gap"] = (panel["trandate"] - panel["announcedate"]).dt.days
panel = panel[(panel["day_gap"] >= -180) & (panel["day_gap"] <= 90)].copy()

def assign_window(d):
    if -180 <= d <= -91: return "baseline_180_91"
    elif -90 <= d <= -31: return "pre_90_31"
    elif -30 <= d <= -1:  return "pre_30_1"
    elif 0 <= d <= 30:    return "post_0_30"
    elif 31 <= d <= 90:   return "post_31_90"
    return None

panel["window"] = panel["day_gap"].apply(assign_window)
panel = panel.dropna(subset=["window"])
panel["is_buy"]  = (panel["trancode"] == "P").astype(int)
panel["is_sell"] = (panel["trancode"] == "S").astype(int)
panel["trade"]   = 1
panel["event_id"] = (panel["keydevid"].astype(str) + "_" +
                     panel["ciq_personid"].astype(str))
print(f"    Panel rows: {len(panel):,}  "
      f"(events: {panel['keydevid'].nunique():,}, "
      f"persons: {panel['ciq_personid'].nunique():,})")

# ── Build daily-level regression panel ────────────────────────────────────
print("    Building daily-level panel ...")
event_obs_pairs = panel[["keydevid", "ciq_personid", "event_id",
                          "announcedate", "companyid"]].drop_duplicates(
                              subset=["event_id"])
days = np.arange(-180, 91)
daily_rows = []
for _, row in event_obs_pairs.iterrows():
    for d in days:
        daily_rows.append({
            "event_id": row["event_id"], "keydevid": row["keydevid"],
            "ciq_personid": row["ciq_personid"],
            "announcedate": row["announcedate"],
            "companyid": row["companyid"], "day_gap": d,
        })
daily = pd.DataFrame(daily_rows)

trade_daily = panel.groupby(["event_id", "day_gap"]).agg(
    n_trades=("trade", "sum"), n_buys=("is_buy", "sum"),
    n_sells=("is_sell", "sum"),
).reset_index()
daily = daily.merge(trade_daily, on=["event_id", "day_gap"], how="left")
for c in ["n_trades", "n_buys", "n_sells"]:
    daily[c] = daily[c].fillna(0).astype(int)
daily["trade_ind"] = (daily["n_trades"] > 0).astype(int)
daily["buy_ind"]   = (daily["n_buys"]   > 0).astype(int)
daily["sell_ind"]  = (daily["n_sells"]  > 0).astype(int)
daily["window"] = daily["day_gap"].apply(assign_window)
daily = daily.dropna(subset=["window"])
daily["pre_90"]  = (daily["window"] == "pre_90_31").astype(int)
daily["pre_30"]  = (daily["window"] == "pre_30_1").astype(int)
daily["post_30"] = (daily["window"] == "post_0_30").astype(int)
daily["post_90"] = (daily["window"] == "post_31_90").astype(int)
daily["event_year"] = pd.to_datetime(daily["announcedate"]).dt.year
daily["post_2020"]  = (daily["event_year"] >= 2020).astype(int)
print(f"    Daily panel: {len(daily):,} rows, "
      f"mean trade_ind = {daily['trade_ind'].mean():.6f}")

# ══════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════
window_labels = {
    "baseline_180_91": "[-180,-91]", "pre_90_31": "[-90,-31]",
    "pre_30_1": "[-30,-1]", "post_0_30": "[0,+30]", "post_31_90": "[+31,+90]",
}
X_cols = ["pre_90", "pre_30", "post_30", "post_90"]


def run_ols(y, X, label, cov_specs, add_const=True, show_vars=None):
    """Run OLS with multiple covariance specs. show_vars: only print these."""
    Xc = sm.add_constant(X) if add_const else X
    models = []
    for spec in cov_specs:
        try:
            m = sm.OLS(y, Xc).fit(cov_type=spec["type"],
                                   cov_kwds=spec.get("kwds") or {})
            models.append((spec["label"], m))
        except Exception as e:
            print(f"    [Warning] {spec['label']} failed: {e}")
            models.append((spec["label"], None))

    first_m = next((m for _, m in models if m is not None), None)
    if first_m is None:
        print(f"\n--- {label} ---\n    All specs failed.")
        return None

    print(f"\n--- {label} ---")
    print(f"    N = {int(first_m.nobs):,}   R-sq = {first_m.rsquared:.6f}")

    var_names = list(X.columns) if show_vars is None else show_vars
    header = f"    {'Variable':>25s} {'Coef':>12s}"
    for lbl, _ in models:
        header += f" {lbl+' SE':>12s} {lbl+' t':>10s} {lbl+' p':>10s}"
    print(header)
    print("    " + "-" * (29 + 12 + len(models) * 34))

    for var in var_names:
        if var not in first_m.params:
            continue
        coef = first_m.params[var]
        line = f"    {var:>25s} {coef:>12.6f}"
        for lbl, m in models:
            if m is not None and var in m.params:
                se = m.bse[var]; t = m.tvalues[var]; p = m.pvalues[var]
                st = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
                line += f" {se:>12.6f} {t:>10.3f} {p:>10.4f}{st:>3s}"
            else:
                line += f" {'---':>12s} {'---':>10s} {'---':>10s}   "
        print(line)
    return first_m


def make_fe_dummies(df, col, prefix, drop_first=True):
    vals = np.sort(df[col].unique())
    if drop_first:
        vals = vals[1:]
    out = pd.DataFrame(index=df.index)
    for v in vals:
        out[f"{prefix}_{v}"] = (df[col] == v).astype(int)
    return out


# ══════════════════════════════════════════════════════════════════════════
# TEST 1: SAME-INDUSTRY vs DIFFERENT-INDUSTRY
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("[TEST 1] SAME-INDUSTRY vs DIFFERENT-INDUSTRY TRADES")
print("=" * 80)

# Route: CIQ companyid -> CIK (via 07_ciq_cik_crosswalk) -> SIC2 (via industry codes)
# This works for both private observed companies AND public traded companies.

# Build companyid -> SIC2 via CIQ-CIK crosswalk
comp_sic = ciq_cik.copy()
comp_sic["sic2"] = comp_sic["cik"].map(cik_sic_dict)
comp_sic = comp_sic.dropna(subset=["sic2"])
compid_sic_dict = dict(zip(comp_sic["companyid"], comp_sic["sic2"]))
print(f"    CIQ companyid -> SIC2 via CIK: {len(compid_sic_dict):,}")

# For event (private) companies: use two routes
# Route 1: companyid -> CIK -> SIC2 (via CIQ-CIK crosswalk)
panel["sic2_event_cik"] = panel["companyid"].map(compid_sic_dict)

# Route 2: event gvkey -> SIC2 (via industry codes gvkey -> SIC)
# Build gvkey -> SIC2 from industry codes
ind1_gvkey = ind1[["gvkey", "sic2"]].dropna().drop_duplicates(subset=["gvkey"])
ind1_gvkey["gvkey"] = ind1_gvkey["gvkey"].astype(str).str.strip().str.zfill(6)
gvkey_sic_dict = dict(zip(ind1_gvkey["gvkey"], ind1_gvkey["sic2"]))
print(f"    gvkey -> SIC2 map entries: {len(gvkey_sic_dict):,}")

# Events have gvkey; map to SIC2
ev_gvkey_sic = ev[["companyid", "gvkey"]].dropna(subset=["gvkey"]).drop_duplicates()
ev_gvkey_sic["gvkey_clean"] = ev_gvkey_sic["gvkey"].astype(str).str.strip().str.zfill(6)
ev_gvkey_sic["sic2_gvkey"] = ev_gvkey_sic["gvkey_clean"].map(gvkey_sic_dict)
compid_sic_gvkey = dict(zip(ev_gvkey_sic["companyid"], ev_gvkey_sic["sic2_gvkey"]))
n_gvk = sum(1 for v in compid_sic_gvkey.values() if pd.notna(v))
print(f"    Event companyid -> SIC2 via gvkey: {n_gvk} matches")

panel["sic2_event_gvk"] = panel["companyid"].map(compid_sic_gvkey)
# Use CIK route first, fill with gvkey route
panel["sic2_event"] = panel["sic2_event_cik"].fillna(panel["sic2_event_gvk"])
n_ev_sic = panel["sic2_event"].notna().sum()
print(f"    Panel trades with event-company SIC2: {n_ev_sic:,} / {len(panel):,}")

# For public traded companies: use the industry_codes files to map the
# Form4 ticker to SIC2. The 05b file has (CIK, permno, SIC), and the
# 05 file has (gvkey, CIK, SIC, NAICS, conm). We build a ticker -> SIC2
# lookup by matching Form4 company names to industry code company names.
obsrec = pd.read_csv(OBSREC)

# Build a comprehensive ticker -> SIC2 map using multiple strategies
# Strategy A: match Form4 conm (uppercase) to industry_codes conm
ind1_names = ind1[["conm", "sic2"]].dropna().drop_duplicates(subset=["conm"])
conm_sic_dict = dict(zip(ind1_names["conm"].str.upper().str.strip(),
                         ind1_names["sic2"]))

# Strategy B: for each unique Form4 (ticker, cname), try to match
f4_tickers = panel[["ticker", "cname"]].dropna().drop_duplicates()
def match_conm_to_sic2(cname):
    """Match Form4 company name to industry codes conm for SIC2."""
    if pd.isna(cname):
        return np.nan
    cu = cname.upper().strip()
    # Direct match
    if cu in conm_sic_dict:
        return conm_sic_dict[cu]
    # Try common suffix removals
    for sfx in [" INC", " CORP", " CO", " LTD", " LLC", " LP", " PLC",
                " NV", " SA", " AG", " SE", " LTD.", " INC.", " CORP.",
                " CO.", ",", "."]:
        stripped = cu.rstrip(sfx).strip()
        if stripped in conm_sic_dict:
            return conm_sic_dict[stripped]
    # Prefix match on first 20 chars
    prefix = cu[:20]
    for conm_key, sic_val in conm_sic_dict.items():
        if conm_key.startswith(prefix):
            return sic_val
    # Prefix match on first 12 chars (looser)
    prefix12 = cu[:12]
    for conm_key, sic_val in conm_sic_dict.items():
        if conm_key.startswith(prefix12):
            return sic_val
    return np.nan

f4_tickers["sic2_pub_match"] = f4_tickers["cname"].apply(match_conm_to_sic2)
ticker_sic2_dict = dict(zip(f4_tickers["ticker"], f4_tickers["sic2_pub_match"]))
n_matched = sum(1 for v in ticker_sic2_dict.values() if pd.notna(v))
print(f"    Ticker -> public SIC2 matches: {n_matched} / {len(ticker_sic2_dict)}")

# Assign public company SIC2 to panel
panel_sic = panel.copy()
panel_sic["sic2_public"] = panel_sic["ticker"].map(ticker_sic2_dict)
n_pub_sic = panel_sic["sic2_public"].notna().sum()
print(f"    Panel trades with public-company SIC2: {n_pub_sic:,} / {len(panel):,}")

# Same-industry flag
has_both = panel_sic["sic2_event"].notna() & panel_sic["sic2_public"].notna()
panel_sic["same_ind"] = np.nan
panel_sic.loc[has_both, "same_ind"] = (
    panel_sic.loc[has_both, "sic2_event"] == panel_sic.loc[has_both, "sic2_public"]
).astype(int)

print(f"    Trades with both SIC2: {has_both.sum():,}")
if has_both.sum() > 0:
    print(f"    Same-industry:    {int(panel_sic.loc[has_both, 'same_ind'].sum()):,}")
    print(f"    Diff-industry:    {(has_both & (panel_sic['same_ind']==0)).sum():,}")
    # Diagnostic: show distribution of SIC2 codes
    print(f"    Unique event SIC2 codes:  "
          f"{panel_sic.loc[has_both, 'sic2_event'].nunique()}")
    print(f"    Unique public SIC2 codes: "
          f"{panel_sic.loc[has_both, 'sic2_public'].nunique()}")
    cross = panel_sic.loc[has_both, ["sic2_event", "sic2_public"]].drop_duplicates()
    print(f"    Unique (event, public) SIC2 pairs: {len(cross)}")
    print("    Sample pairs:")
    for _, r in cross.head(15).iterrows():
        print(f"      event={r['sic2_event']}  public={r['sic2_public']}  "
              f"{'SAME' if r['sic2_event']==r['sic2_public'] else 'DIFF'}")

# Assign same_ind flag to event-observer pairs (majority rule)
eo_ind = (panel_sic.loc[has_both]
          .groupby("event_id")["same_ind"]
          .mean().reset_index()
          .rename(columns={"same_ind": "same_ind_share"}))
eo_ind["same_ind_flag"] = (eo_ind["same_ind_share"] > 0.5).astype(int)
daily_ind = daily.merge(eo_ind[["event_id", "same_ind_flag"]], on="event_id",
                        how="left")

# Run sub-sample regressions
for ind_val, ind_label in [(1, "SAME-INDUSTRY"), (0, "DIFFERENT-INDUSTRY")]:
    sub = daily_ind[daily_ind["same_ind_flag"] == ind_val]
    if len(sub) < 500:
        print(f"\n  {ind_label}: N = {len(sub):,} -- too few, skipping")
        continue
    print(f"\n  {ind_label}: N = {len(sub):,}  "
          f"(events: {sub['keydevid'].nunique():,})")
    specs = [
        {"label": "HC1", "type": "HC1"},
        {"label": "EvtCl", "type": "cluster",
         "kwds": {"groups": sub["event_id"].values}},
    ]
    run_ols(sub["trade_ind"], sub[X_cols],
            f"Trade Ind ~ Window [{ind_label}]", specs)
    run_ols(sub["sell_ind"], sub[X_cols],
            f"Sell Ind ~ Window [{ind_label}]", specs)

# Interaction test
sub_both = daily_ind.dropna(subset=["same_ind_flag"]).copy()
if len(sub_both) > 1000:
    sub_both["same_ind_flag"] = sub_both["same_ind_flag"].astype(int)
    for w in X_cols:
        sub_both[f"{w}_x_sameind"] = sub_both[w] * sub_both["same_ind_flag"]
    X_interact = X_cols + ["same_ind_flag"] + [f"{w}_x_sameind" for w in X_cols]
    specs = [
        {"label": "HC1", "type": "HC1"},
        {"label": "EvtCl", "type": "cluster",
         "kwds": {"groups": sub_both["event_id"].values}},
    ]
    run_ols(sub_both["trade_ind"], sub_both[X_interact],
            "Trade Ind ~ Window + SameInd + Window x SameInd", specs)
    run_ols(sub_both["sell_ind"], sub_both[X_interact],
            "Sell Ind ~ Window + SameInd + Window x SameInd", specs)


# ══════════════════════════════════════════════════════════════════════════
# TEST 2: WITHIN-PERSON BASELINE COMPARISON
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("[TEST 2] WITHIN-PERSON BASELINE: Observer Trading Around Events vs Away")
print("=" * 80)
print("  NOTE: Form 4 data only contains observer trades (no non-observer insiders).")
print("  This test compares each observer's event-window trading to their OWN baseline")
print("  and to their trading on days NOT linked to any event window.")

# For each observer, compute their total number of trades and trading days
# in our full Form 4 data (all dates, not just event windows).
obs_total = (f4.groupby("ciq_personid")
             .agg(total_trades=("trandate", "size"),
                  total_days=("trandate", "nunique"),
                  min_date=("trandate", "min"),
                  max_date=("trandate", "max"))
             .reset_index())
obs_total["calendar_days"] = (obs_total["max_date"] - obs_total["min_date"]).dt.days + 1
obs_total["baseline_daily_rate"] = obs_total["total_trades"] / obs_total["calendar_days"]
print(f"    Observers with any trades: {len(obs_total):,}")
print(f"    Median baseline daily rate: {obs_total['baseline_daily_rate'].median():.6f}")

# Compare: per-person daily trade rate in [-30,-1] event windows vs baseline
# This is already captured by the person FE regression. But let's show it
# descriptively: compute each observer's event-window rate vs their own
# overall rate.
window_order = ["baseline_180_91", "pre_90_31", "pre_30_1",
                "post_0_30", "post_31_90"]
window_days_map = {
    "baseline_180_91": 90, "pre_90_31": 60, "pre_30_1": 30,
    "post_0_30": 31, "post_31_90": 60,
}

obs_window = (panel.groupby(["ciq_personid", "window"])
              .agg(n_trades=("trade", "sum"),
                   n_events=("event_id", "nunique"))
              .reset_index())

# Per-person-window rate (normalized by events and days)
obs_window["days"] = obs_window["window"].map(window_days_map)
obs_window["rate"] = obs_window["n_trades"] / (obs_window["n_events"] * obs_window["days"])

# Compare each person's pre-30 rate to their baseline rate
pre30_rates = obs_window[obs_window["window"] == "pre_30_1"][
    ["ciq_personid", "rate"]].rename(columns={"rate": "pre30_rate"})
base_rates = obs_window[obs_window["window"] == "baseline_180_91"][
    ["ciq_personid", "rate"]].rename(columns={"rate": "base_rate"})
person_compare = pre30_rates.merge(base_rates, on="ciq_personid", how="inner")
person_compare["ratio"] = person_compare["pre30_rate"] / person_compare["base_rate"]
person_compare = person_compare.replace([np.inf, -np.inf], np.nan).dropna()

print(f"\n  Per-Person Pre-Event vs Baseline Rate:")
print(f"    Persons with both windows: {len(person_compare):,}")
print(f"    Mean pre30/baseline ratio:   {person_compare['ratio'].mean():.4f}")
print(f"    Median pre30/baseline ratio: {person_compare['ratio'].median():.4f}")
print(f"    Persons where pre30 > base:  {(person_compare['ratio'] > 1).sum():,} "
      f"({(person_compare['ratio'] > 1).mean()*100:.1f}%)")

# Signed-rank test (non-parametric)
from scipy import stats as sp_stats
if len(person_compare) >= 10:
    diffs = person_compare["pre30_rate"] - person_compare["base_rate"]
    sr_stat, sr_p = sp_stats.wilcoxon(diffs, alternative="greater")
    print(f"    Wilcoxon signed-rank (pre30 > base): stat={sr_stat:.1f}, p={sr_p:.4f}")

    # Paired t-test
    t_stat, t_p = sp_stats.ttest_rel(person_compare["pre30_rate"],
                                      person_compare["base_rate"])
    print(f"    Paired t-test (two-sided): t={t_stat:.3f}, p={t_p:.4f}")

# Also compare at the aggregate level
print("\n  Aggregate Trading Intensity by Window (per event-day):")
print("  " + "-" * 85)
print(f"  {'Window':>15s} {'Trades':>8s} {'Events':>8s} {'Days':>6s} "
      f"{'Rate':>14s} {'Ratio':>10s}")
print("  " + "-" * 85)
agg = panel.groupby("window").agg(
    n_trades=("trade", "sum"), n_events=("event_id", "nunique"),
).reindex(window_order)
agg["days"] = [window_days_map[w] for w in window_order]
agg["rate"] = agg["n_trades"] / (agg["n_events"] * agg["days"])
base_rate = agg.loc["baseline_180_91", "rate"]
for w in window_order:
    r = agg.loc[w]
    ratio = r["rate"] / base_rate if base_rate > 0 else np.nan
    print(f"  {window_labels[w]:>15s} {int(r['n_trades']):>8,} "
          f"{int(r['n_events']):>8,} {int(r['days']):>6d} "
          f"{r['rate']:>14.6f} {ratio:>10.4f}")


# ══════════════════════════════════════════════════════════════════════════
# TEST 3: RETURNS AFTER SELLS
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("[TEST 3] RETURNS AFTER SELLS -- PROFITABILITY TEST")
print("=" * 80)

# Map Form4 tickers to CRSP permnos via price matching
sells_pre = panel[
    (panel["trancode"] == "S") &
    (panel["window"].isin(["pre_30_1", "baseline_180_91"]))
].copy()
sells_pre["tprice_num"] = pd.to_numeric(sells_pre["tprice"], errors="coerce").abs()
sells_pre = sells_pre.dropna(subset=["tprice_num"])
print(f"    Sells in pre/baseline windows: {len(sells_pre):,}")

# Build ticker -> permno via price matching
crsp_prc = crsp[["permno", "date", "prc"]].copy()
crsp_prc["prc"] = crsp_prc["prc"].abs()
crsp_prc = crsp_prc.dropna(subset=["prc"])

# Match on date + close price
f4_prices = f4[["ticker", "trandate", "tprice"]].copy()
f4_prices["tprice_num"] = pd.to_numeric(f4_prices["tprice"], errors="coerce").abs()
f4_prices = f4_prices.dropna(subset=["tprice_num", "ticker"]).drop_duplicates()

f4_crsp = f4_prices.merge(crsp_prc, left_on="trandate", right_on="date", how="inner")
f4_crsp["prc_pct"] = (f4_crsp["tprice_num"] - f4_crsp["prc"]).abs() / f4_crsp["prc"]
f4_crsp_close = f4_crsp[f4_crsp["prc_pct"] < 0.02]

if len(f4_crsp_close) > 0:
    ticker_permno_map = (f4_crsp_close.groupby("ticker")["permno"]
                         .agg(lambda x: x.mode().iloc[0] if len(x.mode()) > 0
                              else x.iloc[0])
                         .to_dict())
else:
    ticker_permno_map = {}
print(f"    Ticker -> permno matches: {len(ticker_permno_map):,}")

sells_pre["permno"] = sells_pre["ticker"].map(ticker_permno_map)
sells_with_permno = sells_pre.dropna(subset=["permno"])
sells_with_permno["permno"] = sells_with_permno["permno"].astype(int)
print(f"    Sells with permno: {len(sells_with_permno):,}")

if len(sells_with_permno) > 10:
    # Compute cumulative CRSP returns from event date forward
    crsp_sorted = crsp.sort_values(["permno", "date"]).copy()
    crsp_sorted["ret_f"] = crsp_sorted["ret"].fillna(0)
    crsp_sorted["cum_idx"] = crsp_sorted.groupby("permno")["ret_f"].transform(
        lambda x: (1 + x).cumprod()
    )

    results_rows = []
    horizons = [30, 60, 90]
    for _, sell in sells_with_permno.iterrows():
        pn, ad = sell["permno"], sell["announcedate"]
        csub = crsp_sorted[crsp_sorted["permno"] == pn]
        if len(csub) < 10:
            continue
        pre_ev = csub[csub["date"] <= ad]
        if len(pre_ev) == 0:
            continue
        base_idx = pre_ev.iloc[-1]["cum_idx"]
        base_dt  = pre_ev.iloc[-1]["date"]
        for h in horizons:
            end_dt = ad + pd.Timedelta(days=h)
            post = csub[(csub["date"] > base_dt) & (csub["date"] <= end_dt)]
            if len(post) == 0:
                continue
            cum_ret = (post.iloc[-1]["cum_idx"] / base_idx) - 1.0
            results_rows.append({
                "event_id": sell["event_id"], "window": sell["window"],
                "horizon": h, "cum_ret": cum_ret,
            })

    ret_df = pd.DataFrame(results_rows)
    print(f"    Return observations: {len(ret_df):,}")

    if len(ret_df) > 0:
        print("\n  Mean Cumulative Return After Sell Trades")
        print("  " + "-" * 80)
        print(f"  {'Window':>15s} {'Horizon':>10s} {'N':>8s} {'Mean':>10s} "
              f"{'Median':>10s} {'SD':>10s} {'t':>8s}")
        print("  " + "-" * 80)
        for w in ["pre_30_1", "baseline_180_91"]:
            for h in horizons:
                sub = ret_df[(ret_df["window"] == w) & (ret_df["horizon"] == h)]
                if len(sub) < 5:
                    continue
                mn, md, sd = sub["cum_ret"].mean(), sub["cum_ret"].median(), sub["cum_ret"].std()
                t = mn / (sd / np.sqrt(len(sub))) if sd > 0 else np.nan
                print(f"  {window_labels.get(w, w):>15s} {f'[0,+{h}]':>10s} "
                      f"{len(sub):>8,} {mn:>10.4f} {md:>10.4f} {sd:>10.4f} {t:>8.3f}")

        # Diff-in-means
        print("\n  Diff-in-Means: Pre-Event Sells vs Baseline Sells")
        print("  " + "-" * 70)
        for h in horizons:
            pre = ret_df[(ret_df["window"] == "pre_30_1") &
                         (ret_df["horizon"] == h)]["cum_ret"]
            base = ret_df[(ret_df["window"] == "baseline_180_91") &
                          (ret_df["horizon"] == h)]["cum_ret"]
            if len(pre) < 5 or len(base) < 5:
                continue
            diff = pre.mean() - base.mean()
            se = np.sqrt(pre.var() / len(pre) + base.var() / len(base))
            t = diff / se if se > 0 else np.nan
            df_w = (se**4 / ((pre.var()/len(pre))**2/(len(pre)-1) +
                    (base.var()/len(base))**2/(len(base)-1))) if se > 0 else np.nan
            p = 2 * sp_stats.t.sf(abs(t), df_w) if not np.isnan(t) else np.nan
            st = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            print(f"  [0,+{h:>2d}]: Diff={diff:>9.4f}  SE={se:>9.4f}  "
                  f"t={t:>7.3f}  p={p:>7.4f} {st}")

        # Regression
        for h in horizons:
            sub_h = ret_df[ret_df["horizon"] == h].copy()
            sub_h["pre_sell"] = (sub_h["window"] == "pre_30_1").astype(int)
            if len(sub_h) < 20:
                continue
            specs = [{"label": "HC1", "type": "HC1"}]
            if sub_h["event_id"].nunique() > 1:
                specs.append({"label": "EvtCl", "type": "cluster",
                              "kwds": {"groups": sub_h["event_id"].values}})
            run_ols(sub_h["cum_ret"], sub_h[["pre_sell"]],
                    f"Cum Return [0,+{h}] ~ PreSell Dummy", specs)
else:
    print("    Cannot compute forward returns (no permno linkage).")


# ══════════════════════════════════════════════════════════════════════════
# TEST 4: FULL CLUSTERING / FE BATTERY
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("[TEST 4] FULL CLUSTERING / FE BATTERY")
print("=" * 80)

n_persons = daily["ciq_personid"].nunique()
print(f"    N = {len(daily):,}, events = {daily['keydevid'].nunique():,}, "
      f"persons = {n_persons}, years = {daily['event_year'].nunique()}")

# Specs 1-3: HC1, Event-Cl, Person-Cl
specs_base = [
    {"label": "HC1", "type": "HC1"},
    {"label": "EvtCl", "type": "cluster",
     "kwds": {"groups": daily["event_id"].values}},
    {"label": "PersCl", "type": "cluster",
     "kwds": {"groups": daily["ciq_personid"].values}},
]

for dv_name, dv in [("trade_ind", daily["trade_ind"]),
                     ("buy_ind", daily["buy_ind"]),
                     ("sell_ind", daily["sell_ind"])]:
    run_ols(dv, daily[X_cols],
            f"(1-3) {dv_name} ~ Window [HC1, Event-Cl, Person-Cl]", specs_base)

# Spec 4: Year FE + Event-Cl
print("\n  === Spec (4): Year FE + Event-Clustered ===")
year_fe = make_fe_dummies(daily, "event_year", "yr")
X_yr = pd.concat([daily[X_cols], year_fe], axis=1)
yr_names = list(year_fe.columns)
specs_yr = [{"label": "EvtCl", "type": "cluster",
             "kwds": {"groups": daily["event_id"].values}}]
for dv_name, dv in [("trade_ind", daily["trade_ind"]),
                     ("sell_ind", daily["sell_ind"])]:
    run_ols(dv, X_yr, f"(4) {dv_name} ~ Window + Year FE [Event-Cl]",
            specs_yr, show_vars=X_cols)

# Spec 5: Person FE + HC1  (within-transformation for tractability)
print("\n  === Spec (5): Person FE (within-transformation) + HC1 ===")
daily_dm = daily[X_cols + ["trade_ind", "sell_ind", "buy_ind",
                            "ciq_personid"]].copy()
for col in X_cols + ["trade_ind", "sell_ind", "buy_ind"]:
    daily_dm[col] = daily_dm[col] - daily_dm.groupby("ciq_personid")[col].transform("mean")

specs_dm = [{"label": "HC1", "type": "HC1"}]
for dv_name in ["trade_ind", "sell_ind"]:
    run_ols(daily_dm[dv_name], daily_dm[X_cols],
            f"(5) {dv_name} ~ Window (within-person) [HC1]",
            specs_dm, add_const=False)
del daily_dm


# ══════════════════════════════════════════════════════════════════════════
# TEST 5: NVCA 2020 DiD -- FULL SPECS
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("[TEST 5] NVCA 2020 DiD -- FULL CLUSTERING / FE BATTERY")
print("=" * 80)

for w in X_cols:
    daily[f"{w}_x_post2020"] = daily[w] * daily["post_2020"]
X_did = X_cols + ["post_2020"] + [f"{w}_x_post2020" for w in X_cols]

# 5a. Specs 1-3
specs_did = [
    {"label": "HC1", "type": "HC1"},
    {"label": "EvtCl", "type": "cluster",
     "kwds": {"groups": daily["event_id"].values}},
    {"label": "PersCl", "type": "cluster",
     "kwds": {"groups": daily["ciq_personid"].values}},
]
for dv_name, dv in [("trade_ind", daily["trade_ind"]),
                     ("sell_ind", daily["sell_ind"]),
                     ("buy_ind", daily["buy_ind"])]:
    run_ols(dv, daily[X_did],
            f"(1-3) {dv_name} ~ Window x Post2020 [HC1, Event-Cl, Person-Cl]",
            specs_did)

# 5b. Year FE + Event-Cl (drop post_2020 level, keep interactions)
print("\n  === Spec (4): Year FE + Event-Cl ===")
X_did_yr = [c for c in X_did if c != "post_2020"]
X_did_yr_df = pd.concat([daily[X_did_yr], year_fe], axis=1)
specs_yr_did = [{"label": "EvtCl", "type": "cluster",
                 "kwds": {"groups": daily["event_id"].values}}]
for dv_name, dv in [("trade_ind", daily["trade_ind"]),
                     ("sell_ind", daily["sell_ind"])]:
    run_ols(dv, X_did_yr_df,
            f"(4) {dv_name} ~ Window x Post2020 + Year FE [Event-Cl]",
            specs_yr_did, show_vars=X_did_yr)

# 5c. Person FE (within-transformation) + HC1
print("\n  === Spec (5): Person FE (within-transformation) + HC1 ===")
daily_dm2 = daily[X_did + ["trade_ind", "sell_ind", "ciq_personid"]].copy()
for col in X_did + ["trade_ind", "sell_ind"]:
    daily_dm2[col] = daily_dm2[col] - daily_dm2.groupby("ciq_personid")[col].transform("mean")
specs_dm2 = [{"label": "HC1", "type": "HC1"}]
for dv_name in ["trade_ind", "sell_ind"]:
    run_ols(daily_dm2[dv_name], daily_dm2[X_did],
            f"(5) {dv_name} ~ Window x Post2020 (within-person) [HC1]",
            specs_dm2, add_const=False)
del daily_dm2

# 5d. Split-sample
print("\n  === Split-sample regressions ===")
pre20  = daily[daily["post_2020"] == 0]
post20 = daily[daily["post_2020"] == 1]
print(f"    Pre-2020:  N = {len(pre20):,}")
print(f"    Post-2020: N = {len(post20):,}")

for period_df, plbl in [(pre20, "PRE-2020"), (post20, "POST-2020")]:
    if len(period_df) < 500:
        continue
    specs_split = [
        {"label": "HC1", "type": "HC1"},
        {"label": "EvtCl", "type": "cluster",
         "kwds": {"groups": period_df["event_id"].values}},
        {"label": "PersCl", "type": "cluster",
         "kwds": {"groups": period_df["ciq_personid"].values}},
    ]
    for dv_name, dv in [("trade_ind", period_df["trade_ind"]),
                         ("sell_ind", period_df["sell_ind"])]:
        run_ols(dv, period_df[X_cols], f"{dv_name} ~ Window [{plbl}]", specs_split)


# ══════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("SUMMARY: KEY pre_30 COEFFICIENTS ACROSS SPECIFICATIONS")
print("=" * 80)

# Re-run models to capture summary coefficients
m_hc = sm.OLS(daily["trade_ind"], sm.add_constant(daily[X_cols])).fit(cov_type="HC1")
m_ecl = sm.OLS(daily["trade_ind"], sm.add_constant(daily[X_cols])).fit(
    cov_type="cluster", cov_kwds={"groups": daily["event_id"]})
m_pcl = sm.OLS(daily["trade_ind"], sm.add_constant(daily[X_cols])).fit(
    cov_type="cluster", cov_kwds={"groups": daily["ciq_personid"]})
m_did = sm.OLS(daily["trade_ind"], sm.add_constant(daily[X_did])).fit(
    cov_type="cluster", cov_kwds={"groups": daily["event_id"]})

print(f"\n  A. Base Model: trade_ind ~ Window Dummies (pre_30 coefficient)")
print(f"  {'Spec':>30s} {'Coef':>10s} {'SE':>10s} {'t':>8s} {'p':>8s}")
print("  " + "-" * 70)
for lbl, m in [("HC1", m_hc), ("Event-Cl", m_ecl), ("Person-Cl", m_pcl)]:
    c, s, t, p = m.params["pre_30"], m.bse["pre_30"], m.tvalues["pre_30"], m.pvalues["pre_30"]
    st = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
    print(f"  {lbl:>30s} {c:>10.6f} {s:>10.6f} {t:>8.3f} {p:>8.4f} {st}")

print(f"\n  B. NVCA DiD: pre_30 x post_2020 (Event-Cl)")
c = m_did.params["pre_30_x_post2020"]
s = m_did.bse["pre_30_x_post2020"]
t = m_did.tvalues["pre_30_x_post2020"]
p = m_did.pvalues["pre_30_x_post2020"]
st = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
print(f"  {'pre_30 x post_2020':>30s} {c:>10.6f} {s:>10.6f} {t:>8.3f} {p:>8.4f} {st}")

# Split sample summary
for plbl, pdf in [("Pre-2020", pre20), ("Post-2020", post20)]:
    if len(pdf) < 500:
        continue
    m = sm.OLS(pdf["trade_ind"], sm.add_constant(pdf[X_cols])).fit(
        cov_type="cluster", cov_kwds={"groups": pdf["event_id"]})
    c, s, t, p = m.params["pre_30"], m.bse["pre_30"], m.tvalues["pre_30"], m.pvalues["pre_30"]
    st = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
    print(f"  {plbl + ' (Event-Cl)':>30s} {c:>10.6f} {s:>10.6f} {t:>8.3f} {p:>8.4f} {st}")

elapsed = time.time() - T0
print(f"\n  Total runtime: {elapsed:.1f} seconds")
print("\n" + "=" * 80)
print("DONE -- regression_form4_v2_deep.py")
print("=" * 80)
