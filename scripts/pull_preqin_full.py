"""
Pull FULL Preqin data from the larger tables, including benchmarks and fund terms.

Uses preqin.preqinfunddetails (75K funds) instead of preqin_gp (25K).
Uses preqin.preqinfundperformance (363K rows) instead of preqin_gp (168K).
Also pulls benchmarks, fund terms, investor portfolio, and manager investment types.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import psycopg2
import pandas as pd
import os, time

data_dir = "C:/Users/hjung/Documents/Claude/CorpAcct/Data"
preqin_dir = os.path.join(data_dir, "Preqin")
os.makedirs(preqin_dir, exist_ok=True)

conn = psycopg2.connect(host="wrds-pgdata.wharton.upenn.edu", port=9737,
                         dbname="wrds", user="harperjung", password="Wwjksnm9087yu!")
cur = conn.cursor()

def pull_and_save(query, filename, description):
    """Execute query, save to CSV, print summary."""
    print(f"\n--- {description} ---")
    time.sleep(3)
    cur.execute(query)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv(os.path.join(preqin_dir, filename), index=False)
    print(f"  Saved: {len(df):,} rows, {len(cols)} columns -> {filename}")
    return df

print("=" * 80)
print("FULL PREQIN DATA PULL (larger tables)")
print("=" * 80)

# =====================================================================
# 1. FUND DETAILS (75K funds - the LARGER table)
# =====================================================================
funds = pull_and_save("""
    SELECT fund_id, firm_id, fund_name, firm_name, vintage, fund_type,
           fundraising_launch_date, local_currency,
           target_size_lc, target_size_usd, target_size_eur,
           final_size_lc, final_size_usd, final_size_eur,
           latest_interim_close_size_usd, latest_interim_close_date,
           fund_status, final_close_date,
           fund_focus, fund_number_overall, fund_number_series,
           fund_structure, geographic_scope, region, industry,
           placement_agents, law_firm, administrator, auditor
    FROM preqin.preqinfunddetails
    ORDER BY firm_id, vintage
""", "fund_details_full.csv", "1. Fund Details (preqin.preqinfunddetails)")

vc_funds = funds[funds["fund_type"].str.contains("Venture|Seed|Early", case=False, na=False)]
print(f"  VC/Seed/Early funds: {len(vc_funds):,}")
print(f"  Unique firms: {funds['firm_id'].nunique():,}")

# =====================================================================
# 2. FUND PERFORMANCE (363K rows - the LARGER table)
# =====================================================================
perf = pull_and_save("""
    SELECT fund_id, date_reported, vintage,
           called_pcent, distr_dpi_pcent, value_rvpi_pcent,
           multiple, net_irr_pcent, benchmark_id
    FROM preqin.preqinfundperformance
    ORDER BY fund_id, date_reported
""", "fund_performance_full.csv", "2. Fund Performance (preqin.preqinfundperformance)")

perf["irr_num"] = pd.to_numeric(perf["net_irr_pcent"], errors="coerce")
perf["mult_num"] = pd.to_numeric(perf["multiple"], errors="coerce")
print(f"  Funds with IRR: {perf[perf['irr_num'].notna()]['fund_id'].nunique():,}")
print(f"  Date range: {perf['date_reported'].min()} to {perf['date_reported'].max()}")

# =====================================================================
# 3. FUND HISTORIC PERFORMANCE (legacy table - may have different funds)
# =====================================================================
hist_perf = pull_and_save("""
    SELECT fund_id, benchmark_id, date_reported, vintage,
           called_pcent, distr_dpi_pcent, value_rvpi_pcent,
           multiple, net_irr_pcent
    FROM preqin.fundhistoricperformance
    ORDER BY fund_id, date_reported
""", "fund_historic_performance.csv", "3. Fund Historic Performance (preqin.fundhistoricperformance)")

# Check overlap with main performance table
main_funds = set(perf["fund_id"].dropna().astype(int))
hist_funds = set(hist_perf["fund_id"].dropna().astype(int))
print(f"  Main perf funds: {len(main_funds):,}")
print(f"  Historic funds: {len(hist_funds):,}")
print(f"  Overlap: {len(main_funds & hist_funds):,}")
print(f"  Historic only: {len(hist_funds - main_funds):,}")

# =====================================================================
# 4. BENCHMARKS (vintage-year benchmarks)
# =====================================================================
bench = pull_and_save("""
    SELECT benchmark_id, benchmark_description, benchmark_fundtype_name,
           benchmark_geo_region_name, benchmark_geo_subregion_name,
           sizename, strategyname, qdate, fund_vintage, fund_category_id,
           fund_count,
           called_avg, called_med, called_weighted,
           distr_avg, distr_med, distr_weighted,
           val_avg, val_med, val_weighted,
           multiple_avg, multiple_med, multiple_q1, multiple_q3, multiple_weighted,
           irr_avg, irr_med, irr_q1, irr_q3, irr_weighted, irr_pooled
    FROM preqin.benchmarks
    ORDER BY benchmark_id, qdate, fund_vintage
""", "benchmarks.csv", "4. Benchmarks (preqin.benchmarks)")

# What benchmark types exist?
print(f"\n  Benchmark fund types:")
for bt, n in bench["benchmark_fundtype_name"].value_counts().head(10).items():
    print(f"    {bt:<40} {n:>6,}")

# =====================================================================
# 5. BENCHMARK PMEs (Public Market Equivalent)
# =====================================================================
pme = pull_and_save("""
    SELECT *
    FROM preqin.preqinbenchmarkspc
    ORDER BY benchmark_id, benchmark_vintage
""", "benchmarks_pme.csv", "5. Benchmark PMEs (preqin.preqinbenchmarkspc)")

# =====================================================================
# 6. FUND TERMS
# =====================================================================
terms = pull_and_save("""
    SELECT fund_id,
           numberinvestorsmin, numberinvestorsmax, returninginvestorspcent,
           fundtermscurrency,
           carriedinterestpercent, carriedinterestbasis,
           gpcatchupratepercent, hurdleratepercent,
           keymanclause, keymanclauselevel, keymanclausedescription,
           lpmajorityrequired_precent, fundformationcost_mn,
           annualmgmtfeeduringperiodpcent, annualmgmtfeeduringperiodpcentmo,
           frequencymgmtfeecollected,
           investmentperiodyears, investmentperiodyearsmodifier
    FROM preqin.preqinfundterms
    ORDER BY fund_id
""", "fund_terms.csv", "6. Fund Terms (preqin.preqinfundterms)")

# =====================================================================
# 7. MANAGER DETAILS (full table)
# =====================================================================
mgrs = pull_and_save("""
    SELECT firm_id, firmname, firmtype, lastupdated, status,
           sourceofcapital, mainfirmstrategy,
           firmcity, firmstate, firmcountry,
           about, established,
           staffcounttotal, staffcountmanagement, staffcountinvestment,
           firmtrait, profilecurrency,
           totalfundsraised10yearsmn,
           totalnumofportfoliocompanies, currentnumofportfoliocompanies,
           geofocus, countryfocus, industryfocus,
           isminorityowned, iswomenowned, listed_firm, firmethos
    FROM preqin.managerdetails
    ORDER BY firm_id
""", "manager_details_full.csv", "7. Manager Details (preqin.managerdetails)")

# =====================================================================
# 8. MANAGER INVESTMENT TYPES
# =====================================================================
inv_types = pull_and_save("""
    SELECT firm_id, firm_name, investment_type,
           company_size, company_situation,
           main_applied_strategies, expertise_provided,
           portcomp_ebitda_min_usd, portcomp_ebitda_max_usd,
           portcomp_annual_revenue_min_usd, portcomp_annual_revenue_max_usd,
           portcomp_companyvalue_min_usd, portcomp_companyvalue_max_usd,
           init_equity_inv_size_min_usd, init_equity_inv_size_max_us,
           equity_inv_size_min_usd, equity_inv_size_max_usd,
           transaction_size_min_usd, transaction_size_max_usd,
           holding_period_min, holding_period_max
    FROM preqin.managerinvestmenttypes
    ORDER BY firm_id
""", "manager_investment_types.csv", "8. Manager Investment Types (preqin.managerinvestmenttypes)")

# =====================================================================
# 9. INVESTOR PORTFOLIO (LP -> Fund commitments)
# =====================================================================
inv_port = pull_and_save("""
    SELECT firm_id, fund_id, commitment_currency,
           lp_commitment_mn, commitment_usd
    FROM preqin.investorportfolio
    ORDER BY firm_id, fund_id
""", "investor_portfolio.csv", "9. Investor Portfolio (preqin.investorportfolio)")

# =====================================================================
# 10. INVESTOR DETAILS (LP characteristics)
# =====================================================================
inv_det = pull_and_save("""
    SELECT firm_id, firm_name, currently_investing_pe, firm_type,
           web_address, firm_city, firm_state, firm_country,
           lp_currency_lpc,
           funds_under_management_usd, funds_under_management_eur,
           current_pe_allocation_pcent, current_pe_allocation_usd,
           target_pe_allocation_pcent, target_pe_allocation_usd,
           typically_invest_min_usd, typically_invest_max_usd,
           coinvest_with_gp, first_close_investor, separate_accounts,
           next12monthsnoinvestmentsmin, next12monthsnoinvestmentsmax,
           next12monthsallocationmin_pe_usd, next12monthsallocationmax_pe_usd
    FROM preqin.investordetails
    ORDER BY firm_id
""", "investor_details.csv", "10. Investor Details (preqin.investordetails)")

# =====================================================================
# 11. CASHFLOWS (re-pull from the full table)
# =====================================================================
cf = pull_and_save("""
    SELECT fund_id, firm_id, transaction_date, transaction_type,
           transaction_amount, cumulative_contribution,
           cumulative_distribution, net_cashflow
    FROM preqin.cashflow
    ORDER BY fund_id, transaction_date
""", "cashflows_full.csv", "11. Cashflows (preqin.cashflow)")

# =====================================================================
# 12. VC DEALS (the full deals table)
# =====================================================================
vc_deals = pull_and_save("""
    SELECT portfolio_company_id, deal_date, ventureid, stage,
           currency, deal_financing_size, deal_financing_size_usd,
           deal_status, investment_status,
           portfolio_company_name, portfolio_company_website,
           portfolio_company_state, portfolio_company_country,
           total_known_funding_usd,
           firm_about, year_established, firm_othernames,
           industry_classification, primary_industry,
           sub_industries, industry_verticals, industry_subverticals
    FROM preqin.preqindealsvc
    ORDER BY deal_date
""", "vc_deals_full.csv", "12. VC Deals (preqin.preqindealsvc)")

# =====================================================================
# 13. BUYOUT DEALS (for PE-backed observer companies)
# =====================================================================
bo_deals = pull_and_save("""
    SELECT portfolio_company_id, deal_date, buyout_id, investment_type,
           currency, deal_size, deal_size_usd, deal_size_equity,
           deal_status, investment_status,
           portfolio_company_name, portfolio_company_state,
           portfolio_company_country,
           deal_description,
           company_revenue, entry_revenue_multiple,
           ebitda, entry_ebitda_multiple,
           investmentstake, enterprisevalue, acquired_share_pcent,
           firm_about, year_established,
           industry_classification, primary_industry,
           sub_industries, industry_verticals
    FROM preqin.preqindealsbuyout
    ORDER BY deal_date
""", "buyout_deals_full.csv", "13. Buyout Deals (preqin.preqindealsbuyout)")

conn.close()

# =====================================================================
# SUMMARY
# =====================================================================
print(f"\n\n{'=' * 80}")
print("FULL PREQIN PULL COMPLETE")
print(f"{'=' * 80}")

for fname in sorted(os.listdir(preqin_dir)):
    if fname.endswith(".csv"):
        fp = os.path.join(preqin_dir, fname)
        size_mb = os.path.getsize(fp) / (1024 * 1024)
        with open(fp, "r", encoding="utf-8") as fh:
            n = sum(1 for _ in fh) - 1
        print(f"  {fname:<45} {n:>8,} rows  ({size_mb:.1f} MB)")

print(f"\nTotal files: {len([f for f in os.listdir(preqin_dir) if f.endswith('.csv')])}")
print("\nDone.")
