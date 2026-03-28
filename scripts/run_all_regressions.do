********************************************************************************
* Test 3: All Regressions in Stata
* Loads pre-computed CSVs from Data/Analysis_Ready/
* Runs all specifications, logs output
********************************************************************************

clear all
set more off
set maxvar 32767

global data_dir "C:/Users/hjung/Documents/Claude/CorpAcct/Data/Analysis_Ready"
global out_dir "C:/Users/hjung/Documents/Claude/CorpAcct/Output"
cap mkdir "$out_dir"

log using "$out_dir/stata_regressions.log", replace text

di "================================================================="
di "TEST 3: ALL REGRESSIONS IN STATA"
di "================================================================="
di ""

********************************************************************************
* SECTION 1: EVENT-TYPE CONTROL GROUP REGRESSIONS
********************************************************************************

foreach net in "original" "supplemented" {
  foreach evt in "ma_buyer" "bankruptcy" "ma_target" "exec_board" "ceo_cfo" {

    cap confirm file "$data_dir/control_group_`net'_`evt'.csv"
    if _rc != 0 {
      di "  SKIP: `net'/`evt' (file not found)"
      continue
    }

    di ""
    di "================================================================="
    di "  `=upper("`net'")' / `=upper("`evt'")'"
    di "================================================================="

    import delimited "$data_dir/control_group_`net'_`evt'.csv", clear
    destring *, replace force

    * Generate string cluster variables
    tostring event_id, gen(eid_str)
    tostring permno, gen(stock_str)

    tab connected, missing
    tab conn_x_same2, missing

    foreach var in car_30_adj car_10_adj car_5_adj car_1_adj {

      cap confirm variable `var'
      if _rc != 0 continue

      di ""
      di "  --- `var' ---"

      * (1) OLS + HC1
      cap noisily reg `var' connected same_ind_sic2 conn_x_same2, robust
      if _rc == 0 {
        di "  OLS + HC1: done"
      }

      * (2) Event-clustered
      cap noisily reg `var' connected same_ind_sic2 conn_x_same2, cluster(event_id)
      if _rc == 0 {
        di "  Event-clustered: done"
      }

      * (3) Stock-clustered
      cap noisily reg `var' connected same_ind_sic2 conn_x_same2, cluster(permno)
      if _rc == 0 {
        di "  Stock-clustered: done"
      }

      * (4) Year FE + Event-cluster
      cap noisily reghdfe `var' connected same_ind_sic2 conn_x_same2, ///
        absorb(event_year) cluster(event_id)
      if _rc == 0 {
        di "  YrFE + Event-cl: done"
      }
      else {
        * Fallback: year dummies
        cap noisily reg `var' connected same_ind_sic2 conn_x_same2 i.event_year, ///
          cluster(event_id)
        if _rc == 0 {
          di "  YrFE(dummies) + Event-cl: done"
        }
      }

      * (5) Year FE + Stock-cluster
      cap noisily reghdfe `var' connected same_ind_sic2 conn_x_same2, ///
        absorb(event_year) cluster(permno)
      if _rc == 0 {
        di "  YrFE + Stock-cl: done"
      }
      else {
        cap noisily reg `var' connected same_ind_sic2 conn_x_same2 i.event_year, ///
          cluster(permno)
        if _rc == 0 {
          di "  YrFE(dummies) + Stock-cl: done"
        }
      }

      * (6) Stock FE + Event-cluster
      cap noisily reghdfe `var' connected same_ind_sic2 conn_x_same2, ///
        absorb(permno) cluster(event_id)
      if _rc == 0 {
        di "  StockFE + Event-cl: done"
      }
      else {
        di "  StockFE + Event-cl: SKIP (reghdfe not installed or too many FE)"
      }
    }
  }
}

********************************************************************************
* SECTION 2: NVCA 2020 + CLAYTON 2025 SHOCKS
********************************************************************************

di ""
di "================================================================="
di "SECTION 2: NVCA 2020 + CLAYTON 2025 SHOCKS"
di "================================================================="

foreach net in "original" "supplemented" {

  cap confirm file "$data_dir/connected_`net'_all_events.csv"
  if _rc != 0 continue

  di ""
  di "=== `=upper("`net'")' NETWORK ==="

  import delimited "$data_dir/connected_`net'_all_events.csv", clear
  destring *, replace force

  gen same_x_post2020  = same_ind_sic2 * post_2020
  gen same_x_postjan25 = same_ind_sic2 * post_jan2025

  foreach var in car_30_adj car_10_adj car_5_adj car_1_adj {
    cap confirm variable `var'
    if _rc != 0 continue

    di ""
    di "  --- `var' ---"

    * NVCA 2020
    cap noisily reghdfe `var' same_ind_sic2 same_x_post2020, absorb(event_year)
    if _rc == 0 {
      di "  NVCA 2020: done"
    }
    else {
      cap noisily reg `var' same_ind_sic2 same_x_post2020 i.event_year
      di "  NVCA 2020 (dummies): done"
    }

    * Clayton 2025 (post-2020 only)
    preserve
    keep if event_year >= 2020
    cap noisily reghdfe `var' same_ind_sic2 same_x_postjan25, absorb(event_year)
    if _rc == 0 {
      di "  Clayton 2025: done"
    }
    else {
      cap noisily reg `var' same_ind_sic2 same_x_postjan25 i.event_year
      di "  Clayton 2025 (dummies): done"
    }
    restore
  }
}

********************************************************************************
* SECTION 3: ROBUSTNESS CHECKS
********************************************************************************

di ""
di "================================================================="
di "SECTION 3: ROBUSTNESS"
di "================================================================="

foreach net in "original" "supplemented" {
  foreach evt in "ma_buyer" "bankruptcy" {

    cap confirm file "$data_dir/control_group_`net'_`evt'.csv"
    if _rc != 0 continue

    di ""
    di "=== `=upper("`net'")' / `=upper("`evt'")' ==="

    import delimited "$data_dir/control_group_`net'_`evt'.csv", clear
    destring *, replace force

    local var "car_30_adj"

    * Baseline
    cap noisily reg `var' connected same_ind_sic2 conn_x_same2, cluster(event_id)
    di "  Baseline: done"

    * Winsorized
    preserve
    _pctile `var', p(1 99)
    replace `var' = r(r1) if `var' < r(r1)
    replace `var' = r(r2) if `var' > r(r2)
    cap noisily reg `var' connected same_ind_sic2 conn_x_same2, cluster(event_id)
    di "  Winsorized: done"
    restore

    * No penny stocks
    preserve
    keep if is_penny == 0
    cap noisily reg `var' connected same_ind_sic2 conn_x_same2, cluster(event_id)
    di "  No penny: done"
    restore

    * SIC3
    cap noisily reg `var' connected same_ind_sic3 conn_x_same3, cluster(event_id)
    di "  SIC3: done"

    * BHAR
    cap confirm variable car_30_bhar
    if _rc == 0 {
      gen cx_bhar = connected * same_ind_sic2
      cap noisily reg car_30_bhar connected same_ind_sic2 cx_bhar, cluster(event_id)
      di "  BHAR: done"
    }
  }
}

********************************************************************************
* SECTION 4: FORM 4 INSIDER TRADING
********************************************************************************

di ""
di "================================================================="
di "SECTION 4: FORM 4 INSIDER TRADING"
di "================================================================="

import delimited "$data_dir/form4_trades.csv", clear
destring *, replace force

tab is_buy pre_event, cell

* OLS + HC1
reg is_buy pre_event same_industry pre_x_same, robust

* Observer-clustered
* ciq_personid may be numeric — use directly or generate group
cap confirm string variable ciq_personid
if _rc == 0 {
  encode ciq_personid, gen(obs_id)
}
else {
  gen obs_id = ciq_personid
}
reg is_buy pre_event same_industry pre_x_same, cluster(obs_id)

* Year FE + HC1
cap noisily reghdfe is_buy pre_event same_industry pre_x_same, absorb(trade_year)
if _rc != 0 {
  reg is_buy pre_event same_industry pre_x_same i.trade_year, robust
}

* Logistic
logit is_buy pre_event same_industry pre_x_same, robust
margins, dydx(*)

di ""
di "================================================================="
di "DONE"
di "================================================================="

log close
