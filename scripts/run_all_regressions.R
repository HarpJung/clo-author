###############################################################################
# Test 3: All Regressions in R
# Loads pre-computed CSVs from Data/Analysis_Ready/
# Runs all specifications, produces tables
###############################################################################

library(lmtest)
library(sandwich)
library(fixest)    # for feols with clustering and FE
library(stargazer)

data_dir <- "C:/Users/hjung/Documents/Claude/CorpAcct/Data/Analysis_Ready"
out_dir  <- "C:/Users/hjung/Documents/Claude/CorpAcct/Output"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

sig <- function(p) {
  if (p < 0.01) return("***")
  if (p < 0.05) return("**")
  if (p < 0.10) return("*")
  return("")
}

cat("=================================================================\n")
cat("TEST 3: ALL REGRESSIONS IN R\n")
cat("=================================================================\n\n")

###############################################################################
# SECTION 1: EVENT-TYPE CONTROL GROUP REGRESSIONS
# For each network x event type: connected + same_ind + conn_x_same
# Multiple CAR windows, multiple clustering/FE specs
###############################################################################

networks  <- c("original", "supplemented")
evt_types <- c("ma_buyer", "bankruptcy", "ma_target", "exec_board", "ceo_cfo")
car_vars  <- c("car_30_adj", "car_10_adj", "car_5_adj", "car_1_adj")
car_labels <- c("CAR[-30,-1]", "CAR[-10,-1]", "CAR[-5,-1]", "CAR[-1,0]")

for (net in networks) {
  for (evt in evt_types) {

    fname <- file.path(data_dir, paste0("control_group_", net, "_", evt, ".csv"))
    if (!file.exists(fname)) {
      cat(sprintf("  SKIP: %s/%s (file not found)\n", net, evt))
      next
    }

    cat(sprintf("\n=== %s / %s ===\n", toupper(net), toupper(evt)))
    df <- read.csv(fname, stringsAsFactors = FALSE)
    df$eid_str   <- as.character(df$event_id)
    df$stock_str <- as.character(df$permno)
    cat(sprintf("  N=%s, connected=%s, conn_x_same=%s\n",
                format(nrow(df), big.mark=","),
                format(sum(df$connected), big.mark=","),
                format(sum(df$conn_x_same2), big.mark=",")))

    for (i in seq_along(car_vars)) {
      var   <- car_vars[i]
      label <- car_labels[i]
      if (!(var %in% names(df))) next

      sub <- df[!is.na(df[[var]]), ]
      if (nrow(sub) < 200) next

      cat(sprintf("\n  %s:\n", label))
      cat(sprintf("  %-25s %12s %8s %12s %8s %12s %8s\n",
                  "Spec", "connected", "p", "same_ind", "p", "conn_x_same", "p"))
      cat(paste0("  ", paste(rep("-", 81), collapse=""), "\n"))

      # (1) OLS + HC1
      tryCatch({
        m <- lm(as.formula(paste(var, "~ connected + same_ind_sic2 + conn_x_same2")), data=sub)
        se <- vcovHC(m, type="HC1")
        ct <- coeftest(m, vcov=se)
        cat(sprintf("  %-25s %+10.5f%s %8.4f %+10.5f%s %8.4f %+10.5f%s %8.4f\n",
                    "OLS + HC1",
                    ct["connected",1], sig(ct["connected",4]), ct["connected",4],
                    ct["same_ind_sic2",1], sig(ct["same_ind_sic2",4]), ct["same_ind_sic2",4],
                    ct["conn_x_same2",1], sig(ct["conn_x_same2",4]), ct["conn_x_same2",4]))
      }, error=function(e) cat(sprintf("  %-25s ERROR\n", "OLS + HC1")))

      # (2) Event-clustered
      tryCatch({
        m <- feols(as.formula(paste(var, "~ connected + same_ind_sic2 + conn_x_same2")),
                   data=sub, cluster=~eid_str)
        ct <- summary(m)$coeftable
        cat(sprintf("  %-25s %+10.5f%s %8.4f %+10.5f%s %8.4f %+10.5f%s %8.4f\n",
                    "Event-cluster",
                    ct["connected",1], sig(ct["connected",4]), ct["connected",4],
                    ct["same_ind_sic2",1], sig(ct["same_ind_sic2",4]), ct["same_ind_sic2",4],
                    ct["conn_x_same2",1], sig(ct["conn_x_same2",4]), ct["conn_x_same2",4]))
      }, error=function(e) cat(sprintf("  %-25s ERROR: %s\n", "Event-cluster", e$message)))

      # (3) Stock-clustered
      tryCatch({
        m <- feols(as.formula(paste(var, "~ connected + same_ind_sic2 + conn_x_same2")),
                   data=sub, cluster=~stock_str)
        ct <- summary(m)$coeftable
        cat(sprintf("  %-25s %+10.5f%s %8.4f %+10.5f%s %8.4f %+10.5f%s %8.4f\n",
                    "Stock-cluster",
                    ct["connected",1], sig(ct["connected",4]), ct["connected",4],
                    ct["same_ind_sic2",1], sig(ct["same_ind_sic2",4]), ct["same_ind_sic2",4],
                    ct["conn_x_same2",1], sig(ct["conn_x_same2",4]), ct["conn_x_same2",4]))
      }, error=function(e) cat(sprintf("  %-25s ERROR\n", "Stock-cluster")))

      # (4) Year FE + Event-cluster
      tryCatch({
        m <- feols(as.formula(paste(var, "~ connected + same_ind_sic2 + conn_x_same2 | event_year")),
                   data=sub, cluster=~eid_str)
        ct <- summary(m)$coeftable
        cat(sprintf("  %-25s %+10.5f%s %8.4f %+10.5f%s %8.4f %+10.5f%s %8.4f\n",
                    "YrFE + Event-cl",
                    ct["connected",1], sig(ct["connected",4]), ct["connected",4],
                    ct["same_ind_sic2",1], sig(ct["same_ind_sic2",4]), ct["same_ind_sic2",4],
                    ct["conn_x_same2",1], sig(ct["conn_x_same2",4]), ct["conn_x_same2",4]))
      }, error=function(e) cat(sprintf("  %-25s ERROR\n", "YrFE + Event-cl")))

      # (5) Year FE + Stock-cluster
      tryCatch({
        m <- feols(as.formula(paste(var, "~ connected + same_ind_sic2 + conn_x_same2 | event_year")),
                   data=sub, cluster=~stock_str)
        ct <- summary(m)$coeftable
        cat(sprintf("  %-25s %+10.5f%s %8.4f %+10.5f%s %8.4f %+10.5f%s %8.4f\n",
                    "YrFE + Stock-cl",
                    ct["connected",1], sig(ct["connected",4]), ct["connected",4],
                    ct["same_ind_sic2",1], sig(ct["same_ind_sic2",4]), ct["same_ind_sic2",4],
                    ct["conn_x_same2",1], sig(ct["conn_x_same2",4]), ct["conn_x_same2",4]))
      }, error=function(e) cat(sprintf("  %-25s ERROR\n", "YrFE + Stock-cl")))

      # (6) Stock FE + Event-cluster
      tryCatch({
        m <- feols(as.formula(paste(var, "~ connected + same_ind_sic2 + conn_x_same2 | permno")),
                   data=sub, cluster=~eid_str)
        ct <- summary(m)$coeftable
        cat(sprintf("  %-25s %+10.5f%s %8.4f %+10.5f%s %8.4f %+10.5f%s %8.4f\n",
                    "StockFE + Event-cl",
                    ct["connected",1], sig(ct["connected",4]), ct["connected",4],
                    ct["same_ind_sic2",1], sig(ct["same_ind_sic2",4]), ct["same_ind_sic2",4],
                    ct["conn_x_same2",1], sig(ct["conn_x_same2",4]), ct["conn_x_same2",4]))
      }, error=function(e) cat(sprintf("  %-25s ERROR\n", "StockFE + Event-cl")))
    }
  }
}

###############################################################################
# SECTION 2: NVCA 2020 + CLAYTON 2025 SHOCK INTERACTIONS
# Using connected-only datasets
###############################################################################

cat("\n\n=================================================================\n")
cat("SECTION 2: NVCA 2020 + CLAYTON 2025 SHOCKS\n")
cat("=================================================================\n")

for (net in networks) {
  fname <- file.path(data_dir, paste0("connected_", net, "_all_events.csv"))
  if (!file.exists(fname)) next

  cat(sprintf("\n=== %s NETWORK ===\n", toupper(net)))
  df <- read.csv(fname, stringsAsFactors = FALSE)
  df$same_x_post2020   <- df$same_ind_sic2 * df$post_2020
  df$same_x_postjan25  <- df$same_ind_sic2 * df$post_jan2025

  for (i in seq_along(car_vars)) {
    var   <- car_vars[i]
    label <- car_labels[i]
    if (!(var %in% names(df))) next
    sub <- df[!is.na(df[[var]]), ]
    if (nrow(sub) < 200) next

    cat(sprintf("\n  %s:\n", label))

    # NVCA 2020
    tryCatch({
      m <- feols(as.formula(paste(var, "~ same_ind_sic2 + same_x_post2020 | event_year")),
                 data=sub)
      ct <- summary(m)$coeftable
      cat(sprintf("    NVCA 2020 (same x post):  coef=%+10.5f%s  p=%.4f\n",
                  ct["same_x_post2020",1], sig(ct["same_x_post2020",4]), ct["same_x_post2020",4]))
    }, error=function(e) cat(sprintf("    NVCA 2020: ERROR\n")))

    # Clayton 2025 (post-2020 only)
    tryCatch({
      sub2 <- sub[sub$event_year >= 2020, ]
      if (nrow(sub2) > 100 && sum(sub2$post_jan2025) > 5) {
        m <- feols(as.formula(paste(var, "~ same_ind_sic2 + same_x_postjan25 | event_year")),
                   data=sub2)
        ct <- summary(m)$coeftable
        cat(sprintf("    Clayton 2025 (same x post): coef=%+10.5f%s  p=%.4f\n",
                    ct["same_x_postjan25",1], sig(ct["same_x_postjan25",4]), ct["same_x_postjan25",4]))
      }
    }, error=function(e) cat(sprintf("    Clayton 2025: ERROR\n")))
  }
}

###############################################################################
# SECTION 3: ROBUSTNESS — Winsorization, Penny Stocks, SIC3, BHAR
###############################################################################

cat("\n\n=================================================================\n")
cat("SECTION 3: ROBUSTNESS CHECKS\n")
cat("=================================================================\n")

for (net in networks) {
  for (evt in c("ma_buyer", "bankruptcy")) {
    fname <- file.path(data_dir, paste0("control_group_", net, "_", evt, ".csv"))
    if (!file.exists(fname)) next

    cat(sprintf("\n=== %s / %s ===\n", toupper(net), toupper(evt)))
    df <- read.csv(fname, stringsAsFactors = FALSE)
    df$eid_str <- as.character(df$event_id)

    var <- "car_30_adj"
    sub <- df[!is.na(df[[var]]), ]
    if (nrow(sub) < 200) next

    cat(sprintf("  CAR[-30,-1] conn_x_same2:\n"))

    # Baseline
    tryCatch({
      m <- feols(as.formula(paste(var, "~ connected + same_ind_sic2 + conn_x_same2")),
                 data=sub, cluster=~eid_str)
      ct <- summary(m)$coeftable
      cat(sprintf("    Baseline:     coef=%+10.5f%s  p=%.4f\n",
                  ct["conn_x_same2",1], sig(ct["conn_x_same2",4]), ct["conn_x_same2",4]))
    }, error=function(e) {})

    # Winsorized
    tryCatch({
      q01 <- quantile(sub[[var]], 0.01, na.rm=TRUE)
      q99 <- quantile(sub[[var]], 0.99, na.rm=TRUE)
      sub_w <- sub
      sub_w[[var]] <- pmin(pmax(sub_w[[var]], q01), q99)
      m <- feols(as.formula(paste(var, "~ connected + same_ind_sic2 + conn_x_same2")),
                 data=sub_w, cluster=~eid_str)
      ct <- summary(m)$coeftable
      cat(sprintf("    Winsorized:   coef=%+10.5f%s  p=%.4f\n",
                  ct["conn_x_same2",1], sig(ct["conn_x_same2",4]), ct["conn_x_same2",4]))
    }, error=function(e) {})

    # No penny stocks
    tryCatch({
      sub_np <- sub[sub$is_penny == 0, ]
      m <- feols(as.formula(paste(var, "~ connected + same_ind_sic2 + conn_x_same2")),
                 data=sub_np, cluster=~eid_str)
      ct <- summary(m)$coeftable
      cat(sprintf("    No penny:     coef=%+10.5f%s  p=%.4f  (N=%s)\n",
                  ct["conn_x_same2",1], sig(ct["conn_x_same2",4]), ct["conn_x_same2",4],
                  format(nrow(sub_np), big.mark=",")))
    }, error=function(e) {})

    # SIC3
    tryCatch({
      if (sum(sub$conn_x_same3, na.rm=TRUE) >= 5) {
        m <- feols(as.formula(paste(var, "~ connected + same_ind_sic3 + conn_x_same3")),
                   data=sub, cluster=~eid_str)
        ct <- summary(m)$coeftable
        cat(sprintf("    SIC3:         coef=%+10.5f%s  p=%.4f\n",
                    ct["conn_x_same3",1], sig(ct["conn_x_same3",4]), ct["conn_x_same3",4]))
      }
    }, error=function(e) {})

    # BHAR
    tryCatch({
      bvar <- "car_30_bhar"
      if (bvar %in% names(sub)) {
        sub_b <- sub[!is.na(sub[[bvar]]), ]
        sub_b$cx_b <- sub_b$connected * sub_b$same_ind_sic2
        m <- feols(as.formula(paste(bvar, "~ connected + same_ind_sic2 + cx_b")),
                   data=sub_b, cluster=~eid_str)
        ct <- summary(m)$coeftable
        cat(sprintf("    BHAR:         coef=%+10.5f%s  p=%.4f\n",
                    ct["cx_b",1], sig(ct["cx_b",4]), ct["cx_b",4]))
      }
    }, error=function(e) {})
  }
}

###############################################################################
# SECTION 4: FORM 4 INSIDER TRADING
###############################################################################

cat("\n\n=================================================================\n")
cat("SECTION 4: FORM 4 INSIDER TRADING\n")
cat("=================================================================\n")

f4 <- read.csv(file.path(data_dir, "form4_trades.csv"), stringsAsFactors = FALSE)
cat(sprintf("  N=%s, pre_event=%s, same_ind=%s, pre_x_same=%s\n",
            format(nrow(f4), big.mark=","),
            sum(f4$pre_event), sum(f4$same_industry), sum(f4$pre_x_same)))

# OLS
tryCatch({
  m <- lm(is_buy ~ pre_event + same_industry + pre_x_same, data=f4)
  se <- vcovHC(m, type="HC1")
  ct <- coeftest(m, vcov=se)
  cat("\n  OLS + HC1:\n")
  for (v in c("pre_event", "same_industry", "pre_x_same")) {
    cat(sprintf("    %-20s coef=%+10.4f%s  p=%.4f\n", v,
                ct[v,1], sig(ct[v,4]), ct[v,4]))
  }
}, error=function(e) cat("  OLS ERROR\n"))

# Observer-clustered
tryCatch({
  m <- feols(is_buy ~ pre_event + same_industry + pre_x_same,
             data=f4, cluster=~ciq_personid)
  ct <- summary(m)$coeftable
  cat("\n  Observer-clustered:\n")
  for (v in c("pre_event", "same_industry", "pre_x_same")) {
    cat(sprintf("    %-20s coef=%+10.4f%s  p=%.4f\n", v,
                ct[v,1], sig(ct[v,4]), ct[v,4]))
  }
}, error=function(e) cat("  Observer-cluster ERROR\n"))

# Year FE + HC1
tryCatch({
  m <- feols(is_buy ~ pre_event + same_industry + pre_x_same | trade_year,
             data=f4)
  ct <- summary(m)$coeftable
  cat("\n  Year FE + HC1:\n")
  for (v in c("pre_event", "same_industry", "pre_x_same")) {
    cat(sprintf("    %-20s coef=%+10.4f%s  p=%.4f\n", v,
                ct[v,1], sig(ct[v,4]), ct[v,4]))
  }
}, error=function(e) cat("  YrFE ERROR\n"))

# Logistic
tryCatch({
  m <- glm(is_buy ~ pre_event + same_industry + pre_x_same, data=f4, family=binomial)
  ct <- summary(m)$coefficients
  cat("\n  Logit:\n")
  for (v in c("pre_event", "same_industry", "pre_x_same")) {
    or <- exp(ct[v,1])
    cat(sprintf("    %-20s coef=%+10.4f  OR=%.3f  p=%.4f%s\n", v,
                ct[v,1], or, ct[v,4], sig(ct[v,4])))
  }
}, error=function(e) cat("  Logit ERROR\n"))


cat("\n\n=================================================================\n")
cat("DONE\n")
cat("=================================================================\n")
