# Databricks notebook source
# DBTITLE 1,V2.6: what changed
# MAGIC %md
# MAGIC ## Voluntary Cancellation Analysis, Aug–Sep 2026 (V2.6)
# MAGIC
# MAGIC **New in V2.6** (from the review of the V2.5 report and validation on 28 Oct)
# MAGIC 1. **No predictions.** The expected-volume model (V2.5 Cell 10) and everything built on it are gone: chart, tables, report section and insight. Aug–Sep is compared only with actual history: the run rate and the same month last year.
# MAGIC 2. **September against history, in plain percentages.** Cell 2 and the report show Aug and Sep against the run rate and against the same month a year earlier, not only the z-score. The May–Jun 2026 spike widens the spread enough to hide a real rise.
# MAGIC 3. **One non-watcher definition:** under 1 minute in the whole paid period (start to expiry, or to the as-of date), on any subscription or pass. Cell 4, Chart 4, Chart 6, the KPI and Cell 10 now all use it.
# MAGIC 4. **Revenue run rate on well-covered months only.** Months where fewer than 95% of cancelled subscriptions have a transaction (several in mid-2025) are left out of the revenue average and marked * on Chart 1b. Coverage by month is shown in Cell 2 and the report.
# MAGIC 5. **Bad expiry dates.** Paid periods over 5 years (`MAX_PAID_DAYS`, seen on some seasonal offers) no longer drive the gap rule or episodes. Seasonal offers are capped at 12 months for monthly-equivalent revenue.
# MAGIC 6. **Charts:**
# MAGIC    - **Chart 2:** competitions are numbered markers with a full-name key under the chart, so no name is cut off.
# MAGIC    - **Chart 6:** New, Win-back and Quick re-subscribe are stacked vertically and country names are wrapped, so every country is readable.
# MAGIC 7. **Report tables show every row.** The table limit was 50, which cut Aug–Sep from the revenue table.
# MAGIC 8. **Validation at the end** (last cells): runs the V2.6 checks on the same session.
# MAGIC
# MAGIC **Carried over:**
# MAGIC - **From V2.5:** voluntary only, Win-back defined by a minimum gap (7 days), episodes, monthly-equivalent revenue excluding tax.
# MAGIC - **From V2.4:** error-flag fixes, top-10% peak match days, `ASOF = "auto"`.
# MAGIC
# MAGIC **Notes:**
# MAGIC - Cells are renumbered: Cell 10 is non-watchers vs viewers, Cell 11 is returns as a share of expiries.
# MAGIC - The written notes for Charts 10b–10e still quote V2.3 figures. Update them from the tables after re-running.

# COMMAND ----------

# DBTITLE 1,Load data
library(AzureStor)#survivalanalysis

bl_endp_key2 <- storage_endpoint("https://mlblobman01.blob.core.windows.net/", key= dbutils.secrets.get(scope = "BlobAccessMLBMan07", key = "SportsBlobPS01"))

list_storage_containers(bl_endp_key2)

storage_account <- "mlblobman01"

cont_url <- "https://mlblobman01.blob.core.windows.net/dimsubscriber"

# cont <- blob_container(cont_url, key= dbutils.secrets.get(scope = "BlobAccessMLBMan07", key = "SportsBlobPS01"))

# list_blobs(cont)


dbutils.fs.ls("/mnt/dimsubscriber")

fileName = paste0("dbfs:/mnt/dimsubscriber/parquet/ALL/DIMSubscriber")

DIM_SubscribersInfo_SDF0 = sparklyr::spark_read_parquet( sc, fileName )

sparklyr::sdf_register(DIM_SubscribersInfo_SDF0,'DIM_SubscribersInfo_SDF')



# ─── Scale-aware Spark tuning (729M rows) ──────────────────────────────────
# At 729M rows, default shuffle.partitions=200 is too low for transformations.
conf <- sparklyr::spark_session(sc) %>% sparklyr::invoke("conf")
conf %>% sparklyr::invoke("set", "spark.sql.shuffle.partitions", "400")
conf %>% sparklyr::invoke("set", "spark.sql.files.maxPartitionBytes", as.character(128L * 1024L * 1024L))
print("shuffle.partitions=400, maxPartitionBytes=128MB")

# ─── Read full parquet (lazy, no caching) ───────────────────────────────────
fileName = paste0("dbfs:/mnt/viewershipinfo/parquet/ALL_V2/EntertainmentMovies_SD")

EntertainmentMovies_SD = sparklyr::spark_read_parquet(
  sc,
  name = "EntertainmentMovies_SD",
  path = fileName,
  memory = FALSE   # Don't cache entire 2.5 TB into memory — keep it lazy
)

sdf_register(EntertainmentMovies_SD, 'EntertainmentMovies_SD')
cat(sprintf("Full DataFrame: %d columns\n", length(colnames(EntertainmentMovies_SD))))

# ─── Column pruning: select only needed columns ────────────────────────────
COLUMNS_NEEDED <- c(
  "viewerId", "country", "deviceos", "startdate",
  "starttimeunixtime", "playingunixtimems", "bufferingunixtimems",
  "convivasessionid", "asset1", "asset_id", "assetname",
  "subscriptionid", "Live", "VOD", "Sports", "Entertainment",
  "sportname", "competitionName", "contentType",
  "dvhwt", "assetquality", "playingtimemin", "ContentType2"
  # add more columns here as needed
)

EntertainmentMovies_SD_ReqColumns <- EntertainmentMovies_SD %>%
  dplyr::select(dplyr::all_of(COLUMNS_NEEDED))

sdf_register(EntertainmentMovies_SD_ReqColumns, 'EntertainmentMovies_SD_ReqColumns')
cat(sprintf("Pruned DataFrame: %d columns\n", length(COLUMNS_NEEDED)))

print("Temp views registered: EntertainmentMovies_SD, EntertainmentMovies_SD_ReqColumns")

# COMMAND ----------

# DBTITLE 1,Cancellation extract (from V2.3)
DIM_SubscribersInfo_SDF_Cancellation = sdf_sql(sc,"select  Customer_ID , Customer_External_ID, Product_name, Offer_period, Subscription_ID,Subscription_latest,Offer_type,Subscription_status,Subscription_type,Subscription_start_date,Expiry_date,Subscription_calender_date,Cancellation_date,Cancellation_reason,D2C_B2B,promo,non_commercial,daily_weekly_flag,Acquisition_channel from DIM_SubscribersInfo_SDF where D2C_b2B='D2C' and offer_type='Subscription' and subscription_latest=1 and cancellation_date>='2024-01-01' and cancellation_date<='2026-09-30'  and product_type is null ")

# Final Dataset

Final_Cancellation_Dataset = DIM_SubscribersInfo_SDF_Cancellation %>%
  dplyr::filter(
    non_commercial != 1,
    daily_weekly_flag != 1,
    !is.na(Customer_External_ID),
    trimws(Customer_External_ID) != "",
    trimws(Customer_External_ID) != "null")

# COMMAND ----------

# DBTITLE 1,Management questions
# 1. What changed?
# Acquisition and churn
# --Comparison to historical cohorts
# --Cancellation split by new vs win-back and expected volume
# Non-watching
# -- This seems to be the second largest group?
# -- How do non-watchers compare with viewers on cancellations, actual churn and revenue?
# # Revenue
# # Total revenue impact vs usual cancellation run rate
# # Viewership
# # We prob need to look at beyond 1 min viewership so probably need different definitions eg look at MOU / total watch time / how many assets they watched.

# 2. Is this normal behaviour, particularly for win-backs?
# Previous cycles
# --The win-backs from Aug/Sep who cancelled - do they repeatedly subscribe, cancel and return around football? Is this typical behaviour for example
# --Are apparent new subscribers returning through duplicate accounts?
# Non-watching
# --Did non-watchers never try, or face login, entitlement or playback failures?
# --When did they cancel, was this repeated in past subscriptions, and what do cancellation reasons show?
# Viewing and cancellation
# --Did viewers watch once or repeatedly, have problems or continue watching after cancellation?
# --How did cancellations relate to fixtures, viewing, offers and renewal? What do reasons and complaints suggest?
# # Football-driven cycling and product or commercial problems remain hypotheses, not proven causes.

# 3. What happened after cancellation?
# Before expiry
# --How many kept watching, reversed cancellation, renewed or reached paid expiry, and when?
# After expiry
# --How many returned, how soon and through which channel?
# --Did returns align with football fixtures, and what did customers watch on return?

# COMMAND ----------

# DBTITLE 1,Cell 0: parameters and helpers
# =====================================================================
# Voluntary cancellation analysis on a START-STATUS basis (sparklyr + dplyr / sdf_sql)
#
# Premise confirmed by the 6-customer sample: every subscription STARTS as New or Win-back and
# ENDS as Churned. The status on the latest row of a cancelled subscription therefore says
# where it is in its lifecycle (still paid-up = new/winback, expired = churned), not what kind
# of customer it was. This script classifies every subscription by the status it STARTED with, so
# Aug, Sep and history are compared like for like.
#
# Columns used all appear in the sample extract (checked). Items marked [CHECK] are assumptions.
# =====================================================================

# ---------------------------------------------------------------------
# CELL 0: parameters and helpers
# ---------------------------------------------------------------------
library(sparklyr); library(dplyr); library(ggplot2); library(scales)
# SparkR is attached above dplyr on the search path and masks these verbs; point them back to dplyr for this script
for (f in c("collect", "filter", "mutate", "arrange", "distinct", "group_by", "select", "desc", "row_number", "coalesce"))
  assign(f, getExportedValue("dplyr", f), envir = globalenv())

P <- list(
  FROM = "2026-08-01", TO = "2026-09-30",     # cancellation window under review
  HIST_FROM = "2025-01-01",                   # history for the run-rate
  ASOF = "auto",                              # "auto" = last complete day in the subscription data (set in Cell 1); or a fixed "YYYY-MM-DD"
  MATURE_DAYS = "30",                         # days after expiry before 'returned?' is read
  VIEW_TBL = "EntertainmentMovies_SD",        # viewing table in the sample notebook (has errorlist, match_date, subscriptionid)
  SUFFIX_LEN = "6",                           # viewing.subscriptionid = Subscription_ID + 6-digit suffix [CHECK on full data]
  OUT_DIR = "/dbfs/FileStore/cancellation_charts",  # Cell 9 saves every chart here as PNG (download from /FileStore/...)
  PEAK_PCT = "0.90",                          # peak match day = match days at or above this percentile of viewers (0.90 = top 10%)
  MAX_PAID_DAYS = "1826",                     # V2.6: a paid period longer than this (5 years) is treated as a bad expiry date (seen on some seasonal offers)
  REV_MIN_COVERAGE = "0.95",                  # V2.6: a month counts in the revenue run rate only if at least 95% of its cancelled subscriptions have a transaction
  VOLUNTARY_ONLY = "1",                       # V2.5: 1 = count only voluntary cancellations (drops involuntary / broadcaster churn, failed charges, operator cancels); 0 = all
  WB_RULE = "gap",                            # V2.5: "gap" = a system 'winback' counts as Win-back only after a real break; "system" = use the system label as is
  WB_MIN_GAP = "7"                            # V2.5: days without paid access before a return counts as a Win-back (0-7 days = 'Quick re-subscribe')
)
fill <- function(s, p = P) { for (n in names(p)) s <- gsub(paste0("{", n, "}"), p[[n]], s, fixed = TRUE); s }
q    <- function(sql) sdf_sql(sc, fill(sql))
reg  <- function(sdf, name) { sparklyr::sdf_register(sdf, name); sdf }
P$VOLUNTARY_ONLY_TXT <- if (P$VOLUNTARY_ONLY == "1") " (voluntary cancellations only: involuntary / broadcaster churn, failed charges and operator cancels excluded)" else " (all cancellations, voluntary and involuntary)"
# Fallback if display_many() is not already defined in your environment
if (!exists("display_many")) display_many <- function(...) {
  l <- list(...); for (n in names(l)) { cat("\n###", n, "\n"); x <- l[[n]]; if (inherits(x, "tbl_spark")) x <- dplyr::collect(x); display(as.data.frame(x)) } }

# COMMAND ----------

# DBTITLE 1,Cell 1: subscription lifecycle
# ---------------------------------------------------------------------
# CELL 1: subscription lifecycle table (one row per subscription)
#   start_status = status on the FIRST version (new / winback)   [CHECK: same as StatusRank1]
#   rule_status  = 'new' if the customer's first subscription by start date, else 'winback'
#                  (matched StatusRank1 on 23 of 23 sample rows; use it as a cross-check)
# ---------------------------------------------------------------------
base_rows <- q("
  SELECT Customer_ID, Customer_External_ID, Subscription_ID, Subscription_version, Subscription_latest,
         Subscription_type, Subscription_status, to_date(Subscription_start_date) AS start_date,
         to_date(Expiry_date) AS expiry_date, to_date(Cancellation_date) AS cancel_date_raw,
         Cancellation_reason, churn_reason, Subscription_churn_type, Acquisition_channel, Product_name,
         Offer_period, Country, Source_system
  FROM DIM_SubscribersInfo_SDF
  WHERE D2C_B2B = 'D2C' AND offer_type = 'Subscription' AND product_type IS NULL
    AND non_commercial <> 1 AND daily_weekly_flag <> 1
    AND Customer_External_ID IS NOT NULL AND TRIM(Customer_External_ID) <> '' AND TRIM(Customer_External_ID) <> 'null'") %>%
  reg("base_rows")

first_v <- q("
  SELECT Subscription_ID, Subscription_type AS start_status FROM (
    SELECT Subscription_ID, Subscription_type,
           ROW_NUMBER() OVER (PARTITION BY Subscription_ID ORDER BY Subscription_version ASC) rn
    FROM base_rows WHERE Subscription_type IN ('new','winback')) WHERE rn = 1") %>% reg("first_v")

life <- q("
  WITH l AS (
    SELECT *,
      CASE WHEN datediff(expiry_date, start_date) > {MAX_PAID_DAYS} THEN 1 ELSE 0 END AS bad_expiry,
      -- V2.6: an implausible expiry (paid period over MAX_PAID_DAYS) is replaced by start + 365 days when measuring gaps
      CASE WHEN datediff(expiry_date, start_date) > {MAX_PAID_DAYS} THEN date_add(start_date, 365) ELSE expiry_date END AS expiry_for_gap
    FROM base_rows WHERE Subscription_latest = 1),
  o AS (
    SELECT l.*,
      ROW_NUMBER() OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS seq,
      MAX(expiry_for_gap) OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID
                             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_paid_through,
      LEAD(start_date) OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS next_start,
      LEAD(Acquisition_channel) OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS next_channel,
      LEAD(Subscription_ID) OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS next_sub_id
    FROM l),
  s AS (
    SELECT o.*,
      COALESCE(f.start_status, CASE WHEN seq = 1 THEN 'new' ELSE 'winback' END) AS start_status_sys,   -- system label (first version)
      CASE WHEN seq = 1 THEN 'new' ELSE 'winback' END AS rule_status,
      CASE WHEN Subscription_status = 'churned' THEN 'churned' ELSE 'active' END AS end_status,
      datediff(start_date, prev_paid_through) AS gap_since_prev_paid_through,   -- negative = started while still paid-up on an earlier subscription
      -- V2.5 voluntary flag: involuntary / broadcaster churn, payment failed, failed charge and operator / merchant cancels are NOT voluntary
      CASE WHEN Subscription_churn_type = 'Involuntary churn' OR churn_reason IN ('Broadcaster churn', 'Payment failed')
                OR Cancellation_reason LIKE '%Faild to Charge%' OR Cancellation_reason LIKE '%Failed to Charge%'
                OR Cancellation_reason LIKE 'Customer cancellation: Canceled By%' THEN 0 ELSE 1 END AS is_voluntary
    FROM o LEFT JOIN first_v f USING (Subscription_ID)),
  e AS (
    SELECT s.*,
      CASE WHEN {VOLUNTARY_ONLY} = 1 AND is_voluntary = 0 THEN NULL ELSE cancel_date_raw END AS cancel_date,
      -- V2.5 status: a system 'winback' within WB_MIN_GAP days of the previous paid-through date (or overlapping it) is a quick re-subscribe
      CASE WHEN '{WB_RULE}' = 'gap' AND start_status_sys = 'winback' AND gap_since_prev_paid_through IS NOT NULL
                AND gap_since_prev_paid_through <= {WB_MIN_GAP} THEN 'resub' ELSE start_status_sys END AS start_status,
      -- V2.5 episode = an unbroken stay with TOD; a new episode starts after a break of more than WB_MIN_GAP days
      SUM(CASE WHEN seq = 1 OR gap_since_prev_paid_through IS NULL OR gap_since_prev_paid_through > {WB_MIN_GAP} THEN 1 ELSE 0 END)
        OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS episode_no
    FROM s)
  SELECT e.*,
         datediff(expiry_date, start_date)  AS paid_period_days,
         datediff(cancel_date, start_date)  AS days_start_to_cancel,
         datediff(expiry_date, cancel_date) AS days_cancel_to_expiry,
         datediff(next_start, expiry_date)  AS days_expiry_to_next_start,
         -- V2.5 months of access one charge buys, for monthly-equivalent revenue
         CASE WHEN LOWER(Offer_period) LIKE 'annual%' OR LOWER(Offer_period) LIKE 'year%' THEN 12
              WHEN LOWER(Offer_period) LIKE 'semi%' OR LOWER(Offer_period) LIKE 'half%' THEN 6
              WHEN LOWER(Offer_period) LIKE 'quarter%' THEN 3
              WHEN LOWER(Offer_period) LIKE 'season%' THEN LEAST(12, GREATEST(1, ROUND(datediff(expiry_date, start_date) / 30.44)))
              ELSE 1 END AS months_covered,
         CASE WHEN cancel_date IS NULL AND cancel_date_raw IS NOT NULL THEN 'Involuntary (excluded)'
              WHEN cancel_date IS NULL THEN 'No cancel date'
              WHEN datediff(expiry_date, cancel_date) = 0 THEN 'Cancelled on expiry day'
              WHEN datediff(cancel_date, start_date) <= 1 THEN 'Cancelled on purchase day / next day'
              ELSE 'Cancelled mid-period' END AS cancel_type
  FROM e") %>% reg("life")

# Cross-check: how often do the two ways of getting start status agree? (sample: 100%)
display(q("SELECT start_status_sys = rule_status AS agree, COUNT(*) n FROM life GROUP BY 1"))
# Lifecycle summary: start status -> end status
display(q("SELECT start_status, end_status, COUNT(*) subs FROM life GROUP BY 1,2 ORDER BY 1,2"))

# Cohort under review (voluntary cancellations Aug-Sep) -- same filters as before
cohort <- q("SELECT * FROM life WHERE cancel_date >= '{FROM}' AND cancel_date <= '{TO}'") %>% reg("cohort")
cat("Cohort subscriptions:", sdf_nrow(cohort), "\n")


# Example: three Cell 2 outputs in one output window
df1 <- q("SELECT start_status_sys = rule_status AS agree, COUNT(*) n FROM life GROUP BY 1")
df2 <- q("SELECT start_status, end_status, COUNT(*) subs FROM life GROUP BY 1,2 ORDER BY 1,2")
df3 <- cohort   # already-collected R data.frame from Cell 2

display_many("how often do the two ways of getting start status agree?" = df1,
             "Lifecycle summary: start status -> end status" = df2,
             "Cohort under review (voluntary cancellations Aug-Sep)"=df3)

# Data end date: last complete day in the subscription data (the latest day may be partly loaded)
if (P$ASOF == "auto") {
  P$ASOF <- q("SELECT CAST(date_sub(MAX(d), 1) AS STRING) d FROM (
                 SELECT start_date d FROM life WHERE start_date <= current_date()
                 UNION ALL SELECT cancel_date FROM life WHERE cancel_date <= current_date())") %>% collect() %>% dplyr::pull(d)
}
cat("ASOF (status as of):", P$ASOF, "\n")

# V2.5 checks: what the voluntary filter removes, how the system label maps to the gap-based status,
# and how long Aug-Sep 'winback' cancellers had been away (system label)
df_v25_1 <- q("SELECT CASE WHEN is_voluntary = 1 THEN 'voluntary (kept)' ELSE 'involuntary / broadcaster / failed charge (excluded)' END AS cancellation,
                      COUNT(*) subs FROM life WHERE cancel_date_raw >= '{FROM}' AND cancel_date_raw <= '{TO}' GROUP BY 1")
df_v25_2 <- q("SELECT start_status_sys AS system_label, start_status AS status_used, COUNT(*) subs FROM cohort GROUP BY 1,2 ORDER BY 1,2")
df_v25_3 <- q("
  SELECT start_status_sys AS system_label,
         CASE WHEN start_status_sys = 'new' THEN 'first subscription'
              WHEN gap_since_prev_paid_through IS NULL THEN 'no earlier subscription in data'
              WHEN gap_since_prev_paid_through < 0 THEN '0 overlap: still paid-up'
              WHEN gap_since_prev_paid_through <= 1 THEN '1 0-1 days'
              WHEN gap_since_prev_paid_through <= 7 THEN '2 2-7 days'
              WHEN gap_since_prev_paid_through <= 30 THEN '3 8-30 days'
              WHEN gap_since_prev_paid_through <= 90 THEN '4 31-90 days' ELSE '5 91+ days' END AS days_since_previous_paid_through,
         COUNT(*) subs
  FROM cohort GROUP BY 1,2 ORDER BY 1,2")
df_v26_1 <- q("SELECT Offer_period, COUNT(*) subs_with_bad_expiry, MIN(datediff(expiry_date, start_date)) min_paid_days, MAX(datediff(expiry_date, start_date)) max_paid_days
                      FROM life WHERE bad_expiry = 1 GROUP BY 1 ORDER BY 2 DESC")
display_many("V2.6: subscriptions with an implausible expiry date (paid period over MAX_PAID_DAYS)" = df_v26_1,
             "V2.5: Aug-Sep cancellations kept / excluded by the voluntary filter" = df_v25_1,
             "V2.5: system label vs status used (gap rule)" = df_v25_2,
             "V2.5: days away before an Aug-Sep 'winback' subscription started (system label)" = df_v25_3)

# COMMAND ----------

# DBTITLE 1,Cell 2: Q1 what changed
# ---------------------------------------------------------------------
# CELL 2: Q1 what changed. Like-for-like on start status
# ---------------------------------------------------------------------
monthly <- q("
  SELECT date_format(cancel_date,'yyyy-MM') AS m, start_status, COUNT(DISTINCT Customer_External_ID) customers
  FROM life WHERE cancel_date >= '{HIST_FROM}' AND cancel_date <= '{TO}' GROUP BY 1,2") %>% collect()

tot <- monthly %>% group_by(m) %>% dplyr::summarise(customers = sum(customers), .groups = "drop")
base <- tot %>% filter(m < substr(P$FROM, 1, 7))
RR_LABEL <- sprintf("%s-%s", format(as.Date(paste0(min(base$m), "-01")), "%b %Y"), format(as.Date(paste0(max(base$m), "-01")), "%b %Y"))
cat(sprintf("Run rate %s: mean %.0f, sd %.0f. Sep 2026: %d (z = %.2f)\n", RR_LABEL, mean(base$customers), sd(base$customers),
            tot$customers[tot$m == "2026-09"], (tot$customers[tot$m == "2026-09"] - mean(base$customers)) / sd(base$customers)))
display(monthly %>% tidyr::pivot_wider(names_from = start_status, values_from = customers, values_fill = 0) %>% arrange(m))

# Like-for-like Aug vs Sep vs history: new and win-back by START status (churned status no longer a separate group)
display(q("
  SELECT date_format(cancel_date,'yyyy-MM') m, start_status, cancel_type, COUNT(*) subs
  FROM life WHERE cancel_date >= '2026-01-01' AND cancel_date <= '{TO}' GROUP BY 1,2,3 ORDER BY 1,2,3"))

# How active is the cancel? mid-period switch-off vs cancel recorded on expiry day vs same-day cancel
display(q("SELECT start_status, cancel_type, COUNT(*) subs, ROUND(AVG(paid_period_days),1) avg_paid_period
          FROM cohort GROUP BY 1,2 ORDER BY 1,2"))

# Speed of cancellation after sign-up (share cancelling within 1 / 7 / 14 / 30 days), history vs Aug-Sep
display(q("
  SELECT CASE WHEN cancel_date >= '{FROM}' THEN 'Aug-Sep 2026' ELSE 'History' END period, start_status, COUNT(*) subs,
         ROUND(AVG(CASE WHEN days_start_to_cancel <= 1  THEN 1.0 ELSE 0 END),3) within_1d,
         ROUND(AVG(CASE WHEN days_start_to_cancel <= 7  THEN 1.0 ELSE 0 END),3) within_7d,
         ROUND(AVG(CASE WHEN days_start_to_cancel <= 30 THEN 1.0 ELSE 0 END),3) within_30d,
         percentile_approx(days_start_to_cancel, 0.5) median_days
  FROM life WHERE cancel_date >= '{HIST_FROM}' AND cancel_date <= '{TO}' GROUP BY 1,2 ORDER BY 1,2"))

# Acquisition side: starts per week by start status (is sign-up volume up, or is cancelling up?)
display(q("SELECT date_trunc('week', start_date) wk, start_status, COUNT(DISTINCT Customer_External_ID) starts
          FROM life WHERE start_date >= '2026-01-01' AND start_date <= '{TO}' GROUP BY 1,2 ORDER BY 1,2"))

# Voluntary vs involuntary, using the churn fields on the subscription
display(q("SELECT start_status, Subscription_churn_type, churn_reason, COUNT(*) subs
          FROM life WHERE end_status = 'churned' AND expiry_date >= '{FROM}' AND expiry_date <= '{TO}' GROUP BY 1,2,3 ORDER BY subs DESC"))

# Revenue: value of the renewal that will not be billed = last charge on the subscription
last_charge <- q("
  SELECT Subscription_ID, Transaction_price_excl_TAX_USD AS last_charge_usd, Payment_type FROM (
    SELECT Subscription_ID, Transaction_price_excl_TAX_USD, Payment_type,
           ROW_NUMBER() OVER (PARTITION BY Subscription_ID ORDER BY Transaction_Date DESC) rn FROM DIM_Transactions_SD) WHERE rn = 1") %>%
  reg("last_charge")
# V2.5: monthly-equivalent value = last charge / months that charge covers (annual = 12), so annual plans do not inflate the figure
rev <- q("SELECT date_format(c.cancel_date,'yyyy-MM') m, c.start_status, COUNT(*) subs,
                  SUM(CASE WHEN l.Subscription_ID IS NULL THEN 1 ELSE 0 END) subs_without_charge,
                  ROUND(SUM(l.last_charge_usd / c.months_covered),0) monthly_value_forgone_usd,
                  ROUND(AVG(l.last_charge_usd / c.months_covered),2) avg_monthly_value,
                  ROUND(SUM(l.last_charge_usd),0) renewal_value_forgone_usd,
                  ROUND(AVG(l.last_charge_usd),2) avg_last_charge
           FROM life c LEFT JOIN last_charge l USING (Subscription_ID)
           WHERE c.cancel_date >= '{HIST_FROM}' AND c.cancel_date <= '{TO}' GROUP BY 1,2 ORDER BY 1,2") %>% collect()
# display(rev)


# Example: three Cell 2 outputs in one output window
df13_1 = monthly %>% tidyr::pivot_wider(names_from = start_status, values_from = customers, values_fill = 0) %>% arrange(m)

# Like-for-like Aug vs Sep vs history: new and win-back by START status (churned status no longer a separate group)
df13_2 = q("
  SELECT date_format(cancel_date,'yyyy-MM') m, start_status, cancel_type, COUNT(*) subs
  FROM life WHERE cancel_date >= '2026-01-01' AND cancel_date <= '{TO}' GROUP BY 1,2,3 ORDER BY 1,2,3")

# How active is the cancel? mid-period switch-off vs cancel recorded on expiry day vs same-day cancel
df13_3 = q("SELECT start_status, cancel_type, COUNT(*) subs, ROUND(AVG(paid_period_days),1) avg_paid_period
          FROM cohort GROUP BY 1,2 ORDER BY 1,2")

# Speed of cancellation after sign-up (share cancelling within 1 / 7 / 14 / 30 days), history vs Aug-Sep
df13_4 =q("
  SELECT CASE WHEN cancel_date >= '{FROM}' THEN 'Aug-Sep 2026' ELSE 'History' END period, start_status, COUNT(*) subs,
         ROUND(AVG(CASE WHEN days_start_to_cancel <= 1  THEN 1.0 ELSE 0 END),3) within_1d,
         ROUND(AVG(CASE WHEN days_start_to_cancel <= 7  THEN 1.0 ELSE 0 END),3) within_7d,
         ROUND(AVG(CASE WHEN days_start_to_cancel <= 30 THEN 1.0 ELSE 0 END),3) within_30d,
         percentile_approx(days_start_to_cancel, 0.5) median_days
  FROM life WHERE cancel_date >= '{HIST_FROM}' AND cancel_date <= '{TO}' GROUP BY 1,2 ORDER BY 1,2")

# Acquisition side: starts per week by start status (is sign-up volume up, or is cancelling up?)
df13_5 = q("SELECT date_trunc('week', start_date) wk, start_status, COUNT(DISTINCT Customer_External_ID) starts
          FROM life WHERE start_date >= '2026-01-01' AND start_date <= '{TO}' GROUP BY 1,2 ORDER BY 1,2")

# Voluntary vs involuntary, using the churn fields on the subscription
df13_6 =q("SELECT start_status, Subscription_churn_type, churn_reason, COUNT(*) subs
          FROM life WHERE end_status = 'churned' AND expiry_date >= '{FROM}' AND expiry_date <= '{TO}' GROUP BY 1,2,3 ORDER BY subs DESC")




display_many("Three Cell 2 outputs in one output window" = df13_1,
             "Like-for-like Aug vs Sep vs history: new and win-back by START status" = df13_2,
             " How active is the cancel? mid-period switch-off vs cancel recorded on expiry day vs same-day cancel" = df13_3,
             "Speed of cancellation after sign-up" = df13_4,
             "Acquisition side: starts per week by start status" = df13_5,
             "Voluntary vs involuntary, using the churn fields on the subscription" = df13_6
             )

# V2.6: Aug-Sep against the run rate and against the same month a year earlier (the z-score alone hides the change:
# the May-Jun 2026 spike widens the spread)
RR <- mean(base$customers)
last_year <- function(x) sprintf("%d-%s", as.integer(substr(x, 1, 4)) - 1, substr(x, 6, 7))
rr_tbl <- tot %>% filter(m >= substr(P$FROM, 1, 7), m <= substr(P$TO, 1, 7)) %>%
  mutate(run_rate = round(RR), vs_run_rate_pct = round(100 * (customers / RR - 1), 1),
         same_month_last_year = vapply(m, function(x) { y <- tot$customers[tot$m == last_year(x)]; if (length(y)) as.numeric(y) else NA_real_ }, numeric(1)),
         vs_last_year_pct = round(100 * (customers / same_month_last_year - 1), 1))
# Revenue coverage: share of cancelled subscriptions that have a transaction, by month
rev_cov <- rev %>% group_by(m) %>% dplyr::summarise(subs = sum(subs), without_charge = sum(subs_without_charge), .groups = "drop") %>%
  mutate(coverage_pct = round(100 * (1 - without_charge / subs), 1), used_in_revenue_run_rate = coverage_pct >= 100 * as.numeric(P$REV_MIN_COVERAGE))
display_many("Aug-Sep vs run rate and vs the same month last year (distinct customers)" = rr_tbl,
             "Revenue coverage by month (months under REV_MIN_COVERAGE are left out of the revenue run rate)" = rev_cov)

# Distinct cancelling customers in the cohort (a customer cancelling in both Aug and Sep counts once)
n_cust_augsep <- q("SELECT COUNT(DISTINCT Customer_External_ID) n FROM cohort") %>% collect() %>% dplyr::pull(n)
cat("Distinct cancelling customers", P$FROM, "to", P$TO, ":", n_cust_augsep, "\n")

# COMMAND ----------

# DBTITLE 1,Cell 3: viewing attributed to the subscription
# ---------------------------------------------------------------------
# CELL 3: viewing, attributed to the SUBSCRIPTION that was cancelled (not just the customer)
# Lesson from the sample: some customers watch under a World Cup pass or another live subscription,
# so a customer-level join marks them as viewers of a subscription they never used.
# ---------------------------------------------------------------------
v <- q("
  SELECT viewerId, to_date(startdate) AS vdate, convivasessionid, subscriptionid,
         CAST(substr(CAST(subscriptionid AS STRING), 1, length(CAST(subscriptionid AS STRING)) - {SUFFIX_LEN}) AS BIGINT) AS sub_id_view,
         playingunixtimems / 60000.0 AS minutes, startuperror, errorlist, interrupts, bufferingunixtimems / 60000.0 AS buffer_min,
         competitionName, ContentType2, Live, to_date(match_date) AS match_date, deviceos, planname, asset_id,
         -- V2.4: a blank or 'N/A' errorlist is NOT an error (V2.3 counted it, so almost every session was flagged)
         CASE WHEN (NULLIF(TRIM(errorlist), '') IS NOT NULL AND UPPER(TRIM(errorlist)) NOT IN ('N/A', 'NA', 'NULL')) OR startuperror = 1
              THEN 1 ELSE 0 END AS has_error,
         CASE WHEN UPPER(errorlist) LIKE '%ENTITL%' OR UPPER(errorlist) LIKE '%DO NOT HAVE ACCESS%' OR UPPER(errorlist) LIKE '%NEEDS_TO_UPGRADE%'
                   OR UPPER(errorlist) LIKE '%NOT AUTHORI%' OR errorlist LIKE '%صلاحية%' THEN 1 ELSE 0 END AS entitlement_error,
         CASE WHEN UPPER(errorlist) LIKE '%MAXIMUM_NUMBER_OF_USER_DEVICE%' OR UPPER(errorlist) LIKE '%MAXIMUM NUMBER OF D%'
                   OR UPPER(errorlist) LIKE '%ANOTHER BROWSER%' OR errorlist LIKE '%الحد الأقصى%' THEN 1 ELSE 0 END AS device_limit_error,
         CASE WHEN UPPER(errorlist) LIKE '%DRM%' OR UPPER(errorlist) LIKE '%WIDEVINE%' OR UPPER(errorlist) LIKE '%FAIRPLAY%'
                   OR UPPER(errorlist) LIKE '%PLAYREADY%' THEN 1 ELSE 0 END AS drm_error
  FROM {VIEW_TBL}
  WHERE viewerId IN (SELECT DISTINCT Customer_External_ID FROM cohort)") %>% reg("v")
# NB: use playingunixtimems. playingtimemin is NULL on ~18% of sample rows.
v_end <- q("SELECT CAST(MAX(vdate) AS STRING) d FROM v") %>% collect() %>% dplyr::pull(d)
cat("Viewing data ends:", v_end, "| ASOF:", P$ASOF, "\n")
if (!is.na(v_end) && v_end < P$ASOF) warning("Viewing data ends before ASOF: viewing after ", v_end, " is not counted.")

sub_view <- q("
  SELECT c.Subscription_ID, c.Customer_External_ID, c.start_status, c.cancel_type, c.start_date, c.cancel_date, c.expiry_date,
         c.Product_name, c.Country, c.Acquisition_channel, c.Cancellation_reason, c.churn_reason, c.days_start_to_cancel,
         -- on THIS subscription id, from start to cancel (or expiry if no cancel date)
         COUNT(DISTINCT CASE WHEN v.sub_id_view = c.Subscription_ID THEN v.convivasessionid END)                              AS own_sessions,
         COUNT(DISTINCT CASE WHEN v.sub_id_view = c.Subscription_ID AND v.minutes >= 1 THEN v.convivasessionid END)           AS own_played_sessions,
         COALESCE(SUM(CASE WHEN v.sub_id_view = c.Subscription_ID THEN v.minutes END),0)                                       AS own_minutes,
         COUNT(DISTINCT CASE WHEN v.sub_id_view = c.Subscription_ID THEN v.vdate END)                                          AS own_days,
         COUNT(DISTINCT CASE WHEN v.sub_id_view = c.Subscription_ID THEN v.asset_id END)                                       AS own_assets,
         SUM(CASE WHEN v.sub_id_view = c.Subscription_ID THEN v.has_error ELSE 0 END)                                          AS own_error_sessions,
         SUM(CASE WHEN v.sub_id_view = c.Subscription_ID THEN v.entitlement_error ELSE 0 END)                                  AS own_entitlement_errors,
         -- customer-level, any subscription or pass, inside the subscription window
         COUNT(DISTINCT CASE WHEN v.vdate BETWEEN c.start_date AND COALESCE(c.cancel_date, c.expiry_date) THEN v.convivasessionid END) AS any_sessions_in_window,
         COALESCE(SUM(CASE WHEN v.vdate BETWEEN c.start_date AND COALESCE(c.cancel_date, c.expiry_date) THEN v.minutes END),0)        AS any_minutes_in_window,
         COUNT(DISTINCT CASE WHEN v.vdate BETWEEN c.start_date AND COALESCE(c.cancel_date, c.expiry_date) THEN v.asset_id END)        AS any_assets_in_window,
         -- after cancelling, before paid expiry
         COUNT(DISTINCT CASE WHEN v.vdate > c.cancel_date AND v.vdate <= c.expiry_date THEN v.convivasessionid END)           AS sessions_cancel_to_expiry,
         COALESCE(SUM(CASE WHEN v.vdate > c.cancel_date AND v.vdate <= c.expiry_date THEN v.minutes END),0)                    AS minutes_cancel_to_expiry,
         COUNT(DISTINCT CASE WHEN v.vdate > date_sub(c.expiry_date,7) AND v.vdate <= c.expiry_date THEN v.vdate END)          AS days_in_final_week,
         -- after paid expiry (before the next subscription starts)
         COALESCE(SUM(CASE WHEN v.vdate > c.expiry_date AND (c.next_start IS NULL OR v.vdate < c.next_start) THEN v.minutes END),0) AS minutes_after_expiry,
         -- V2.6: whole paid period (start to expiry, or to ASOF if still running), any subscription or pass: the ONE non-watcher definition
         COALESCE(SUM(CASE WHEN v.vdate BETWEEN c.start_date AND LEAST(c.expiry_date, DATE'{ASOF}') THEN v.minutes END),0) AS minutes_paid_period
  FROM cohort c LEFT JOIN v ON c.Customer_External_ID = v.viewerId
  GROUP BY c.Subscription_ID, c.Customer_External_ID, c.start_status, c.cancel_type, c.start_date, c.cancel_date, c.expiry_date,
           c.Product_name, c.Country, c.Acquisition_channel, c.Cancellation_reason, c.churn_reason, c.days_start_to_cancel, c.next_start") %>%
  reg("sub_view")

# Engagement tiers (beyond 1 minute): MOU, days and assets
tiers <- q("
  SELECT *, CASE WHEN any_sessions_in_window = 0 THEN '0 no session in window'
                 WHEN any_minutes_in_window < 1 THEN '1 tried, under 1 minute'
                 WHEN any_minutes_in_window < 60 THEN '2 1-59 min'
                 WHEN any_minutes_in_window < 300 THEN '3 1-5 hours'
                 WHEN any_minutes_in_window < 1200 THEN '4 5-20 hours' ELSE '5 20+ hours' END AS mou_tier,
             CASE WHEN own_sessions = 0 AND any_sessions_in_window > 0 THEN 'watched only on another subscription / pass'
                  WHEN own_minutes >= 1 THEN 'watched this subscription' ELSE 'did not watch this subscription' END AS attribution
  FROM sub_view") %>% reg("tiers")
  
  df14_1 = q("SELECT start_status, mou_tier, attribution, COUNT(*) subs, ROUND(AVG(any_minutes_in_window),0) avg_min, ROUND(AVG(any_assets_in_window),1) avg_assets
          FROM tiers GROUP BY 1,2,3 ORDER BY 1,2,3")



display_many("Engagement tiers (beyond 1 minute): MOU, days and assets" = df14_1)
             

# COMMAND ----------

# DBTITLE 1,Cell 4: Q2 non-watchers
# ---------------------------------------------------------------------
# CELL 4: Q2 non-watchers: never tried, or tried and hit a problem?
# ---------------------------------------------------------------------
nw <- q("SELECT * FROM tiers WHERE minutes_paid_period < 1") %>% reg("nw")   # V2.6: whole paid period, any subscription or pass



# Session-level evidence for those customers inside their window: zero play with / without an error code, entitlement errors, device
df15_1 = q("
  SELECT n.start_status,
         COUNT(DISTINCT n.Subscription_ID) non_watching_subs,
         COUNT(DISTINCT CASE WHEN v.convivasessionid IS NULL THEN n.Subscription_ID END) never_opened_player,
         COUNT(DISTINCT CASE WHEN v.has_error = 1 THEN n.Subscription_ID END) had_error_coded_session,
         COUNT(DISTINCT CASE WHEN v.entitlement_error = 1 THEN n.Subscription_ID END) had_entitlement_error,
         COUNT(DISTINCT CASE WHEN v.device_limit_error = 1 THEN n.Subscription_ID END) had_device_limit_error,
         COUNT(DISTINCT CASE WHEN v.drm_error = 1 THEN n.Subscription_ID END) had_drm_error,
         COUNT(DISTINCT CASE WHEN v.convivasessionid IS NOT NULL AND v.has_error = 0 AND v.minutes < 1 THEN n.Subscription_ID END) opened_no_error_no_play
  FROM nw n LEFT JOIN v ON v.viewerId = n.Customer_External_ID AND v.vdate BETWEEN n.start_date AND LEAST(n.expiry_date, DATE'{ASOF}')
  GROUP BY 1")

df15_2 = q("SELECT regexp_extract(v.errorlist, '^(.{0,40})', 1) error_head, v.deviceos, COUNT(*) sessions
          FROM v WHERE v.has_error = 1 GROUP BY 1,2 ORDER BY sessions DESC")

# When did they cancel, and did the same customer do this before?
df15_3 = q("SELECT start_status, CASE WHEN days_start_to_cancel <= 1 THEN '0-1d' WHEN days_start_to_cancel <= 7 THEN '2-7d'
                 WHEN days_start_to_cancel <= 30 THEN '8-30d' ELSE '30d+' END tenure, cancel_type, COUNT(*) subs FROM nw GROUP BY 1,2,3 ORDER BY 1,2,3")
df15_4 = q("
  SELECT n.start_status, COUNT(DISTINCT n.Customer_External_ID) nonwatch_customers,
         COUNT(DISTINCT p.Customer_External_ID) with_earlier_subs,
         COUNT(DISTINCT CASE WHEN COALESCE(pv.m, 0) < 1 THEN p.Customer_External_ID END) with_an_earlier_unwatched_sub
  FROM nw n
  LEFT JOIN life p ON p.Customer_External_ID = n.Customer_External_ID AND p.start_date < n.start_date
  LEFT JOIN (SELECT sub_id_view, SUM(minutes) m FROM v GROUP BY 1) pv ON pv.sub_id_view = p.Subscription_ID
  GROUP BY 1")
    # v holds all viewing of cohort customers, so earlier subscriptions are covered
df15_5 = q("SELECT start_status, Cancellation_reason, churn_reason, Acquisition_channel, COUNT(*) subs FROM nw GROUP BY 1,2,3,4 ORDER BY subs DESC")

df15_6 = q("SELECT Product_name, Country, COUNT(*) subs FROM nw GROUP BY 1,2 HAVING COUNT(*) >= 10 ORDER BY subs DESC")

display_many(
"Session-level evidence for those customers inside their window: zero play with / without an error code, entitlement errors, device" = df15_1,
"Aggregation: Session-level evidence" = df15_2,
"When did they cancel, and did the same customer do this before?" = df15_3,
"Aggregation : When did they Cancel" = df15_4,
"Holds all viewing of cohort customers, so earlier subscriptions are covered" = df15_5,
"Aggregation : Holds all viewing of cohort customers" = df15_6
)

# COMMAND ----------

# DBTITLE 1,Cell 5: Q2 viewers, fixtures, event study
# ---------------------------------------------------------------------
# CELL 5: Q2 viewers: once or repeatedly, problems, continued after cancelling, link to fixtures
# ---------------------------------------------------------------------
df_16_1 = q("
  SELECT start_status,
         SUM(CASE WHEN own_days = 1 THEN 1 ELSE 0 END) one_day_on_this_sub,
         SUM(CASE WHEN own_days BETWEEN 2 AND 4 THEN 1 ELSE 0 END) two_to_four_days,
         SUM(CASE WHEN own_days >= 5 THEN 1 ELSE 0 END) five_plus_days,
         SUM(CASE WHEN sessions_cancel_to_expiry > 0 THEN 1 ELSE 0 END) kept_watching_after_cancel,
         SUM(CASE WHEN days_in_final_week > 0 THEN 1 ELSE 0 END) watched_in_final_week,
         SUM(CASE WHEN minutes_after_expiry > 0 THEN 1 ELSE 0 END) watched_after_expiry
  FROM tiers GROUP BY 1")
# Playback problems for viewers: error-coded and rebuffering sessions
df_16_2 = q("SELECT t.start_status, COUNT(DISTINCT t.Subscription_ID) subs, COUNT(*) sessions, SUM(v.has_error) error_sessions, ROUND(AVG(v.has_error), 4) error_session_rate,
                  COUNT(DISTINCT CASE WHEN v.has_error = 1 THEN t.Subscription_ID END) subs_with_error,
                  SUM(v.entitlement_error) entitlement_errors, SUM(v.device_limit_error) device_limit_errors, SUM(v.drm_error) drm_errors,
                  ROUND(AVG(CASE WHEN v.interrupts > 0 THEN 1.0 ELSE 0 END),3) share_sessions_with_interrupts
          FROM tiers t JOIN v ON v.viewerId = t.Customer_External_ID AND v.vdate BETWEEN t.start_date AND COALESCE(t.cancel_date, t.expiry_date)
          GROUP BY 1")

# Fixture calendar from the viewing table itself (real match dates, not a proxy)
fixtures <- q("
  SELECT match_date, competitionName, COUNT(DISTINCT viewerId) viewers FROM {VIEW_TBL}
  WHERE ContentType2 = 'Football' AND match_date >= '{HIST_FROM}' GROUP BY 1,2") %>% reg("fixtures")
peak_md <- q("SELECT match_date FROM (SELECT match_date, SUM(viewers) v, percent_rank() OVER (ORDER BY SUM(viewers)) pr FROM fixtures GROUP BY 1) WHERE pr >= {PEAK_PCT}") %>% reg("peak_md")
peak_days <- collect(peak_md)

# Distance from each cancellation / sign-up to the nearest peak match day; compare with the base rate
pk <- as.Date(peak_days$match_date)
cal <- data.frame(d = seq(as.Date("2025-01-01"), as.Date(P$TO), by = "day"))
cal$near_peak <- as.integer(sapply(cal$d, function(x) any(abs(as.numeric(x - pk)) <= 2))); cal$d <- as.character(cal$d)
sdf_copy_to(sc, cal, "peak_cal", overwrite = TRUE)
df_16_3 = q("SELECT 'sign-up' event, l.start_status, COUNT(*) n, ROUND(AVG(p.near_peak),3) share_within_2d_of_peak
           FROM life l JOIN peak_cal p ON p.d = CAST(l.start_date AS STRING) WHERE l.start_date >= '2026-01-01' AND l.start_date <= '{TO}' GROUP BY 1,2
           UNION ALL
           SELECT 'cancel', l.start_status, COUNT(*), ROUND(AVG(p.near_peak),3)
           FROM life l JOIN peak_cal p ON p.d = CAST(l.cancel_date AS STRING) WHERE l.cancel_date >= '2026-01-01' AND l.cancel_date <= '{TO}' GROUP BY 1,2")
cat("Base rate (share of 2026 days within 2 days of a peak match day):", round(mean(cal$near_peak[cal$d >= "2026-01-01"]), 3), "\n")

# Cancellations in the days around PEAK match days (event study): average cancellations per peak match day at each offset, by start status
ev_study <- q("
  SELECT c.start_status, datediff(c.d, f.match_date) AS offset_days,
         SUM(c.n) / (SELECT COUNT(*) FROM peak_md WHERE match_date >= '2026-01-01' AND match_date <= date_sub(DATE'{TO}', 7)) AS avg_cancels_per_peak_day
  FROM (SELECT cancel_date AS d, start_status, COUNT(*) n FROM life WHERE cancel_date >= '2026-01-01' AND cancel_date <= '{TO}' GROUP BY 1,2) c
  JOIN (SELECT match_date FROM peak_md WHERE match_date >= '2026-01-01' AND match_date <= date_sub(DATE'{TO}', 7)) f
    ON datediff(c.d, f.match_date) BETWEEN -3 AND 7
  GROUP BY 1,2 ORDER BY 1,2") %>% collect()   # neighbouring peak days overlap, so one cancellation can count under two match days
df_16_4 = ev_study


display_many("Q2 viewers: once or repeatedly, problems, continued after cancelling, link to fixtures" = df_16_1,
"Playback problems for viewers: error-coded and rebuffering sessions" = df_16_2,
"Sign-ups and cancellations within 2 days of a peak match day (compare with the base rate)" = df_16_3,
"Cancellations in the days around PEAK match days (event study), by start status" = df_16_4
)

# COMMAND ----------

# DBTITLE 1,Cell 6: Q2 cycling and duplicates
# ---------------------------------------------------------------------
# CELL 6: Q2 repeat cycling and duplicate accounts
# ---------------------------------------------------------------------
# V2.5: cycles counted as EPISODES (unbroken stays with TOD; a new episode starts after a break of more than WB_MIN_GAP days)
df_17_1 = q("
  SELECT CASE WHEN episodes >= 6 THEN '6+' WHEN episodes >= 4 THEN '4-5' ELSE CAST(episodes AS STRING) END AS episodes_bucket, COUNT(*) customers,
         ROUND(AVG(n_subs), 1) avg_subscription_ids, ROUND(AVG(quick_resubs), 2) avg_quick_resubs,
         ROUND(AVG(avg_break), 1) avg_break_between_episodes_days
  FROM (SELECT Customer_External_ID, MAX(episode_no) episodes, COUNT(*) n_subs,
               SUM(CASE WHEN seq > 1 AND gap_since_prev_paid_through <= {WB_MIN_GAP} THEN 1 ELSE 0 END) quick_resubs,
               AVG(CASE WHEN gap_since_prev_paid_through > {WB_MIN_GAP} THEN gap_since_prev_paid_through END) avg_break
        FROM life WHERE Customer_External_ID IN (SELECT Customer_External_ID FROM cohort WHERE start_status = 'winback') GROUP BY 1)
  GROUP BY 1 ORDER BY 1")
# Short chained sign-ups: 3+ subscriptions started within 7 days (sample: C2 bought on 23, 24 and 25 Aug)
df_17_2 = q("
  SELECT start_status, COUNT(DISTINCT Customer_External_ID) customers FROM (
    SELECT Customer_External_ID, start_status,
           COUNT(*) OVER (PARTITION BY Customer_External_ID ORDER BY day_no RANGE BETWEEN CURRENT ROW AND 7 FOLLOWING) n7
    FROM (SELECT *, datediff(start_date, DATE'2020-01-01') AS day_no FROM life WHERE start_date >= '{FROM}' AND start_date <= '{TO}')) WHERE n7 >= 3 GROUP BY 1")
# Same customer, several external ids (duplicate accounts); and 'new' subscribers who already have an earlier subscription elsewhere
df_17_3 = q("SELECT n_ext, COUNT(*) customer_ids FROM (SELECT Customer_ID, COUNT(DISTINCT Customer_External_ID) n_ext FROM life GROUP BY 1) GROUP BY 1 ORDER BY 1")
df_17_4 = q("
  SELECT date_format(c.cancel_date,'yyyy-MM') m, COUNT(DISTINCT c.Customer_External_ID) new_cancellers,
         COUNT(DISTINCT CASE WHEN e.Customer_ID IS NOT NULL THEN c.Customer_External_ID END) earlier_sub_on_other_account
  FROM cohort c LEFT JOIN life e ON e.Customer_ID = c.Customer_ID AND e.Customer_External_ID <> c.Customer_External_ID AND e.start_date < c.start_date
  WHERE c.start_status = 'new' GROUP BY 1 ORDER BY 1")
# Parallel paid subscriptions: customers who start a Win-back while still paid-up on another subscription
df_17_5 = q("SELECT start_status, COUNT(*) subs, SUM(CASE WHEN gap_since_prev_paid_through < 0 THEN 1 ELSE 0 END) started_while_still_paid_up
          FROM cohort GROUP BY 1")

display_many("Q2 repeat cycling: lifetime episodes per cancelling Win-back customer" = df_17_1,
"Short chained sign-ups: 3+ subscriptions started within 7 days" = df_17_2,
"Same customer, several external ids (duplicate accounts)" = df_17_3,
"'New' subscribers who already have an earlier subscription on another account" = df_17_4,
"Parallel paid subscriptions: customers who start a Win-back while still paid-up on another subscription" = df_17_5
)

# COMMAND ----------

# DBTITLE 1,Cell 7: Q3 before expiry
# ---------------------------------------------------------------------
# CELL 7: Q3 what happened after cancellation
# ---------------------------------------------------------------------
# Reversals: a later version of the same subscription has the cancellation removed   [CHECK on a few known ids]
reversed <- q("
  SELECT DISTINCT a.Subscription_ID FROM base_rows a JOIN base_rows b ON a.Subscription_ID = b.Subscription_ID
  WHERE a.cancel_date_raw IS NOT NULL AND b.Subscription_version > a.Subscription_version AND b.cancel_date_raw IS NULL") %>% reg("reversed")
# Renewal despite cancellation: a recurring charge dated after the cancel date
renewed <- q("
  SELECT DISTINCT c.Subscription_ID FROM cohort c JOIN DIM_Transactions_SD t ON t.Subscription_ID = c.Subscription_ID
  WHERE to_date(t.Transaction_Date) > c.cancel_date AND t.Payment_type = 'recurring'") %>% reg("renewed")

post <- q("
  SELECT c.*, CASE WHEN expiry_date <= '{ASOF}' THEN 1 ELSE 0 END reached_expiry,
         CASE WHEN date_add(expiry_date, {MATURE_DAYS}) <= '{ASOF}' THEN 1 ELSE 0 END matured,
         CASE WHEN r.Subscription_ID IS NOT NULL THEN 1 ELSE 0 END cancel_reversed,
         CASE WHEN n.Subscription_ID IS NOT NULL THEN 1 ELSE 0 END charged_after_cancel
  FROM cohort c LEFT JOIN reversed r USING (Subscription_ID) LEFT JOIN renewed n USING (Subscription_ID)") %>% reg("post")
df_18_1 = q("SELECT start_status, COUNT(*) subs, SUM(reached_expiry) reached_expiry, SUM(cancel_reversed) reversed, SUM(charged_after_cancel) charged_after_cancel,
                  SUM(CASE WHEN next_start <= expiry_date THEN 1 ELSE 0 END) new_sub_before_expiry FROM post GROUP BY 1")

display_many("Q3 what happened after cancellation: expiry, reversals, charges after cancel, new subscription before expiry" = df_18_1
)

# COMMAND ----------

# DBTITLE 1,Cell 7: Q3 after expiry


# After expiry: how many returned, how soon, through which channel (matured subscriptions only)
ret <- q("
  SELECT start_status, COUNT(*) matured_subs,
         SUM(CASE WHEN days_expiry_to_next_start >= 0 THEN 1 ELSE 0 END) returned,
         SUM(CASE WHEN days_expiry_to_next_start BETWEEN 0 AND 7  THEN 1 ELSE 0 END) d0_7,
         SUM(CASE WHEN days_expiry_to_next_start BETWEEN 8 AND 14 THEN 1 ELSE 0 END) d8_14,
         SUM(CASE WHEN days_expiry_to_next_start BETWEEN 15 AND 30 THEN 1 ELSE 0 END) d15_30
  FROM post WHERE matured = 1 GROUP BY 1") %>% collect()
df_19_1 = ret
df_19_2 = q("SELECT start_status, next_channel, COUNT(*) returns FROM post WHERE days_expiry_to_next_start >= 0 GROUP BY 1,2 ORDER BY returns DESC")

display_many("After expiry: how many returned and how soon (matured subscriptions only)" = df_19_1,
"After expiry: returns by channel of the next subscription" = df_19_2
)

# COMMAND ----------

# DBTITLE 1,Cell 7: Q3 returns vs fixtures
# Do returns line up with fixtures? Weekly returns vs weekly match-day viewers
ret_wk <- q("SELECT date_trunc('week', next_start) wk, start_status, COUNT(*) returns FROM post WHERE days_expiry_to_next_start >= 0 GROUP BY 1,2 ORDER BY 1,2") %>% collect()
fx_wk  <- q("SELECT date_trunc('week', match_date) wk, SUM(viewers) match_viewers, COUNT(DISTINCT competitionName) competitions FROM fixtures
          WHERE match_date >= '{FROM}' GROUP BY 1 ORDER BY 1") %>% collect()
df_20_1 = ret_wk; df_20_2 = fx_wk
# What did returners watch in their first 7 days back?
df_20_3 = q("
  SELECT p.start_status, v.competitionName, COUNT(DISTINCT p.Customer_External_ID) returners, ROUND(SUM(v.minutes),0) minutes
  FROM post p JOIN v ON v.viewerId = p.Customer_External_ID AND v.vdate BETWEEN p.next_start AND date_add(p.next_start, 7)
  WHERE p.days_expiry_to_next_start >= 0 GROUP BY 1,2 HAVING COUNT(DISTINCT p.Customer_External_ID) >= 20 ORDER BY minutes DESC")

display_many("Do returns line up with fixtures? Weekly returns" = df_20_1,
"Weekly match-day viewers and competitions" = df_20_2,
"What did returners watch in their first 7 days back?" = df_20_3
)

# COMMAND ----------

# DBTITLE 1,Cell 8: lifecycle timeline (commented)
# # ---------------------------------------------------------------------
# # CELL 8: lifecycle timeline chart for a handful of customers (ggplot2). Cell 9 follows with the management charts.
# # ---------------------------------------------------------------------
# ids <- c("1668440c-cd8f-4885-b433-db46c5807c9b", "50e81771-a410-41a3-980e-fda2c54240fe")   # replace with any customers
# tl <- q(sprintf("SELECT Customer_External_ID, Subscription_ID, start_status, start_date, expiry_date, cancel_date FROM life
#                   WHERE Customer_External_ID IN (%s) AND expiry_date >= '2026-07-01'", paste0("'", ids, "'", collapse = ","))) %>% collect() %>%
#   mutate(across(c(start_date, expiry_date, cancel_date), as.Date), cust = substr(Customer_External_ID, 1, 8)) %>%
#   group_by(cust) %>% arrange(start_date) %>% mutate(lane = row_number()) %>% ungroup()
# ggplot(tl) +
#   geom_segment(aes(x = start_date, xend = coalesce(cancel_date, expiry_date), y = lane, yend = lane, colour = start_status), linewidth = 4) +
#   geom_segment(data = filter(tl, !is.na(cancel_date)), aes(x = cancel_date, xend = expiry_date, y = lane, yend = lane, colour = start_status), linewidth = 4, alpha = .35) +
#   geom_point(data = filter(tl, !is.na(cancel_date)), aes(cancel_date, lane), shape = 4, size = 3, stroke = 1.2) +
#   facet_grid(cust ~ ., scales = "free_y", space = "free_y") +
#   scale_colour_manual(values = c(new = "#2a78d6", winback = "#eb6834"), name = "Started as") +
#   labs(title = "Subscription lifecycles: start status, cancellation (x) and paid access left (faded)", x = NULL, y = NULL) +
#   theme_minimal(base_size = 12) + theme(axis.text.y = element_blank(), panel.grid.minor = element_blank(), legend.position = "top")

# COMMAND ----------

# DBTITLE 1,Cell 9: chart setup
# ---------------------------------------------------------------------
# CELL 9: management charts (ggplot2). Every chart is built from the tables above and uses the
#         START-status split (New = blue, Win-back = orange, Quick re-subscribe = purple). PNGs are saved to P$OUT_DIR.
#         Run Cells 0-7 first (this cell needs: life, cohort, tiers, nw, post, fixtures, peak_md,
#         monthly, base, rev, ev_study, ret, ret_wk, fx_wk). Each chart follows in its own cell.
# ---------------------------------------------------------------------
BLUE <- "#2a78d6"; ORANGE <- "#eb6834"; GREY <- "#b8b7b0"; INK <- "#0b0b0b"; INK2 <- "#52514e"; GRID <- "#e6e5e1"; AREA <- "#c9c8c2"
PURPLE <- "#8e5bb5"
STATUS_LV <- c("New", "Win-back", "Quick re-subscribe")
COL <- c("New" = BLUE, "Win-back" = ORANGE, "Quick re-subscribe" = PURPLE)
lab_status <- function(x) factor(unname(c(new = "New", winback = "Win-back", resub = "Quick re-subscribe")[as.character(x)]), levels = STATUS_LV)
theme_cx <- function() theme_minimal(base_size = 12) + theme(
  panel.grid.minor = element_blank(), panel.grid.major.x = element_blank(), panel.grid.major.y = element_line(colour = GRID),
  plot.title = element_text(face = "bold", colour = INK), plot.title.position = "plot", plot.subtitle = element_text(colour = INK2),
  axis.text = element_text(colour = INK2), axis.title = element_text(colour = INK2), legend.position = "top", legend.justification = "left")
dir.create(P$OUT_DIR, showWarnings = FALSE, recursive = TRUE)
out_plot <- function(p, name, w = 11, h = 6) {
  print(p); try(ggsave(file.path(P$OUT_DIR, paste0(name, ".png")), p, width = w, height = h, dpi = 200), silent = TRUE); invisible(p)
}

# COMMAND ----------

# DBTITLE 1,Chart 1
# ---- Chart 1. Q1: monthly cancelling customers vs run rate, split by start status ----------------
hist_avg <- mean(base$customers)
m_df <- monthly %>% mutate(status = lab_status(start_status), period = ifelse(m >= "2026-08", "Aug-Sep 2026", "History"))
c1 <- ggplot(m_df, aes(m, customers, fill = status, alpha = period)) +
  geom_col(width = 0.75) +
  geom_hline(yintercept = hist_avg, linetype = "dashed", colour = INK) +
  annotate("text", x = 1, y = hist_avg, label = sprintf("%s average %s", RR_LABEL, comma(round(hist_avg))), hjust = 0, vjust = -0.6, size = 3.3, colour = INK) +
  scale_fill_manual(values = COL, name = "Started as") +
  scale_alpha_manual(values = c("History" = 0.45, "Aug-Sep 2026" = 1), guide = "none") +
  scale_y_continuous(labels = comma, expand = expansion(mult = c(0, 0.08))) +
  labs(title = "Monthly cancelling customers, by the status they started with",
       subtitle = sprintf("Voluntary cancellations only. Win-back = back after more than %s days away; Quick re-subscribe = within %s days. Aug-Sep 2026 at full colour.", P$WB_MIN_GAP, P$WB_MIN_GAP), x = NULL, y = "Customers") +
  theme_cx() + theme(axis.text.x = element_text(angle = 45, hjust = 1))
out_plot(c1, "01_monthly_cancellations_by_start_status")

# COMMAND ----------

# DBTITLE 1,Chart 1b
# ---- Chart 1b. Q1: renewal value forgone (last charge on each cancelled subscription) -----------
rv <- rev %>% mutate(status = lab_status(start_status), period = ifelse(m >= "2026-08", "Aug-Sep 2026", "History"))
rv_low <- rv %>% group_by(m) %>% dplyr::summarise(total = sum(monthly_value_forgone_usd, na.rm = TRUE), .groups = "drop") %>%
  dplyr::inner_join(rev_cov %>% filter(!used_in_revenue_run_rate) %>% dplyr::select(m), by = "m")
c1b <- ggplot(rv, aes(m, monthly_value_forgone_usd, fill = status, alpha = period)) +
  geom_col(width = 0.75) +
  geom_text(data = rv_low, aes(m, total, label = "*"), inherit.aes = FALSE, vjust = -0.2, size = 6, colour = INK) +
  scale_fill_manual(values = COL, name = "Started as") +
  scale_alpha_manual(values = c("History" = 0.45, "Aug-Sep 2026" = 1), guide = "none") +
  scale_y_continuous(labels = label_dollar(), expand = expansion(mult = c(0, 0.08))) +
  labs(title = "Monthly revenue lost to cancellations (monthly-equivalent)",
       subtitle = "Last charge on each cancelled subscription divided by the months it covers (annual = 12), USD excl. tax. An estimate, not realised loss.", x = NULL, y = "USD",
       caption = sprintf("* fewer than %.0f%% of that month's cancelled subscriptions have a transaction, so its value is understated.", 100 * as.numeric(P$REV_MIN_COVERAGE))) +
  theme_cx() + theme(axis.text.x = element_text(angle = 45, hjust = 1))
out_plot(c1b, "02_renewal_value_forgone")

# COMMAND ----------

# DBTITLE 1,Chart 2
# ---- Chart 2. Q1/Q2: weekly cancellations vs football calendar and viewing ----------------------
wk_c <- q("SELECT date_trunc('week', cancel_date) wk, start_status, COUNT(DISTINCT Customer_External_ID) customers
           FROM life WHERE cancel_date >= '{FROM}' AND cancel_date <= '{TO}' GROUP BY 1,2") %>% collect()
wk_v <- q("SELECT date_trunc('week', to_date(startdate)) wk, SUM(playingunixtimems) / 60000000.0 AS fb_min_m
           FROM {VIEW_TBL} WHERE ContentType2 = 'Football' AND to_date(startdate) >= '{FROM}' AND to_date(startdate) <= '{TO}' GROUP BY 1") %>% collect()
ev_first <- q("SELECT competitionName AS label, MIN(match_date) AS date, SUM(viewers) AS v FROM fixtures
               GROUP BY 1 HAVING MIN(match_date) >= '{FROM}' AND MIN(match_date) <= '{TO}' ORDER BY v DESC LIMIT 6") %>% collect()
d_from <- as.Date(P$FROM); d_to <- as.Date(P$TO)
wk_geom <- function(df) df %>% mutate(wk = as.Date(wk), x0 = pmax(wk, d_from), x1 = pmin(wk + 7, d_to + 1), days = as.numeric(x1 - x0), mid = x0 + days / 2)
P1 <- "Cancellations per day (customers)"; P2 <- "Win-back share of cancellations (%)"; P3 <- "Football viewing (million minutes per day)"
lv <- c(P1, P2, P3)
bars <- wk_geom(wk_c) %>% mutate(per_day = customers / days, status = lab_status(start_status), panel = factor(P1, lv)) %>%
  arrange(wk, desc(status)) %>% group_by(wk) %>% mutate(ymax = cumsum(per_day), ymin = ymax - per_day) %>% ungroup()   # Win-back at the bottom, New on top
shr  <- wk_geom(wk_c) %>% group_by(wk, mid, days) %>%
  summarise(value = 100 * sum(customers[start_status == "winback"]) / sum(customers), .groups = "drop") %>% mutate(panel = factor(P2, lv))
vw   <- wk_geom(wk_v) %>% transmute(mid, x0, x1, value = fb_min_m / days, panel = factor(P3, lv))
top_y <- bars %>% group_by(wk) %>% summarise(t = sum(per_day), .groups = "drop") %>% summarise(m = max(t)) %>% pull(m)
head_df <- tibble(mid = d_from, value = top_y * 1.7, panel = factor(P1, lv))
ev_first <- ev_first %>% mutate(date = as.Date(date)) %>% arrange(date) %>%
  mutate(panel = factor(P1, lv), num = row_number(), key = sprintf("%d  %s (%s)", num, label, format(date, "%d %b")))
ev_key <- paste(vapply(split(ev_first$key, ceiling(seq_along(ev_first$key) / 3)), paste, character(1), collapse = "      "), collapse = "\n")
c2 <- ggplot() +
  geom_rect(data = bars, aes(xmin = x0, xmax = x1, ymin = ymin, ymax = ymax, fill = status), colour = "white", linewidth = 0.3) +
  geom_line(data = shr, aes(mid, value), linewidth = 0.9, colour = INK) +
  geom_point(data = shr, aes(mid, value), size = 2.4, colour = INK) +
  geom_text(data = shr, aes(mid, value, label = sprintf("%.0f%%", value)), vjust = -0.9, size = 3.2, colour = INK) +
  geom_rect(data = vw, aes(xmin = x0, xmax = x1, ymin = 0, ymax = value), fill = AREA) +
  geom_blank(data = head_df, aes(mid, value)) +
  geom_vline(data = ev_first %>% select(-panel), aes(xintercept = date), linetype = "dashed", colour = INK2, linewidth = 0.4) +
  geom_label(data = ev_first, aes(date, Inf, label = num), vjust = 1.3, size = 3.4, fontface = "bold", colour = INK,
             fill = "white", label.size = 0.25, label.padding = unit(0.15, "lines")) +
  facet_grid(panel ~ ., scales = "free_y", switch = "y") +
  scale_fill_manual(values = COL, name = "Started as") +
  scale_x_date(date_breaks = "1 week", date_labels = "%d %b", expand = expansion(mult = 0.01)) +
  scale_y_continuous(labels = comma, expand = expansion(mult = c(0.02, 0.12))) +
  labs(title = "Cancellations and the football calendar",
       subtitle = "Weekly totals divided by days in the week. Numbered dashed lines = first match date of a competition that starts in the window (key below).\nTiming alone does not prove football caused the cancellations.",
       caption = ev_key, x = NULL, y = NULL) +
  theme_cx() + theme(strip.placement = "outside", strip.text.y.left = element_text(angle = 0, hjust = 1, face = "bold", colour = INK),
                     plot.caption = element_text(hjust = 0, size = 10, colour = INK, lineheight = 1.3), plot.caption.position = "plot")
out_plot(c2, "03_cancellations_vs_football_calendar", 11, 8.5)

# COMMAND ----------

# DBTITLE 1,Chart 3
# ---- Chart 3. Q1: days of paid access left at cancellation --------------------------------------
al <- q("SELECT start_status,
                CASE WHEN days_cancel_to_expiry <= 0 THEN '0 Expired / 0 days' WHEN days_cancel_to_expiry <= 7 THEN '1 1-7 days'
                     WHEN days_cancel_to_expiry <= 14 THEN '2 8-14 days' WHEN days_cancel_to_expiry <= 30 THEN '3 15-30 days' ELSE '4 30+ days' END AS bucket,
                COUNT(*) subs
         FROM cohort WHERE cancel_date IS NOT NULL GROUP BY 1,2") %>% collect() %>%
  group_by(start_status) %>% mutate(share = subs / sum(subs)) %>% ungroup() %>%
  mutate(status = lab_status(start_status), lbl = sub("^[0-9] ", "", bucket))
al_lv <- al %>% distinct(bucket, lbl) %>% arrange(bucket)
al <- al %>% mutate(lbl = factor(lbl, levels = al_lv$lbl))
al_pal <- setNames(c("#c9c8c2", "#e3e2dd", "#9ec5f4", ORANGE, INK)[as.integer(substr(al_lv$bucket, 1, 1)) + 1], al_lv$lbl)
c3 <- ggplot(al, aes(share, factor(status, levels = rev(levels(status))), fill = lbl)) +
  geom_col(position = position_stack(reverse = TRUE), width = 0.65) +
  geom_text(data = filter(al, share > 0.05), aes(label = percent(share, accuracy = 1), colour = lbl == "30+ days"),
            position = position_stack(vjust = 0.5, reverse = TRUE), size = 3.4, fontface = "bold") +
  scale_colour_manual(values = c("TRUE" = "white", "FALSE" = INK), guide = "none") +
  scale_fill_manual(values = al_pal, name = NULL) + scale_x_continuous(labels = percent, expand = expansion(mult = c(0, 0.02))) +
  labs(title = "Days of paid access left at cancellation", subtitle = "Most cancel with weeks of access still paid for, so the revenue effect lands at the next renewal.", x = NULL, y = NULL) +
  theme_cx() + theme(panel.grid.major.y = element_blank())
out_plot(c3, "04_paid_access_left_at_cancellation", 10, 4)

# COMMAND ----------

# DBTITLE 1,Chart 4
# ---- Chart 4. Q1/Q2: cancellers who never watched (no minute on this subscription or any other) ----
nv <- q("SELECT start_status, COUNT(*) subs, SUM(CASE WHEN minutes_paid_period < 1 THEN 1 ELSE 0 END) never FROM tiers GROUP BY 1") %>% collect()
nv <- bind_rows(mutate(nv, grp = as.character(lab_status(start_status))), nv %>% summarise(subs = sum(subs), never = sum(never)) %>% mutate(grp = "All")) %>%
  mutate(pct = 100 * never / subs, grp = factor(grp, levels = c("All", rev(STATUS_LV))))
c4 <- ggplot(nv, aes(pct, grp, colour = grp)) +
  geom_segment(aes(x = 0, xend = pct, yend = grp), colour = GRID, linewidth = 1.4) + geom_point(size = 5) +
  geom_text(aes(label = sprintf("%.1f%%  (%s subscriptions)", pct, comma(never))), hjust = -0.15, colour = INK, size = 3.6) +
  scale_colour_manual(values = c(COL, "All" = INK), guide = "none") +
  scale_x_continuous(expand = expansion(mult = c(0, 0.45)), labels = function(x) paste0(x, "%")) +
  labs(title = "Cancellers who never watched", subtitle = "Under 1 minute of viewing in the whole paid period (start to expiry, or to the as-of date), on any subscription or pass.", x = NULL, y = NULL) +
  theme_cx() + theme(panel.grid.major.y = element_blank())
out_plot(c4, "05_never_watched_share", 10, 4)

# COMMAND ----------

# DBTITLE 1,Chart 5
# ---- Chart 5. Q1: viewing depth beyond 1 minute (MOU tiers) -------------------------------------
dp <- q("SELECT start_status, mou_tier, COUNT(*) subs FROM tiers GROUP BY 1,2") %>% collect() %>%
  group_by(start_status) %>% mutate(share = subs / sum(subs)) %>% ungroup() %>%
  mutate(status = lab_status(start_status), tier = sub("^[0-9] ", "", mou_tier))
dp_lv <- dp %>% distinct(mou_tier, tier) %>% arrange(mou_tier)
dp <- dp %>% mutate(tier = factor(tier, levels = dp_lv$tier))
ramp <- colorRampPalette(c("#d6e4f7", "#08306b"))(5)
dp_pal <- setNames(c(GREY, ramp)[as.integer(substr(dp_lv$mou_tier, 1, 1)) + 1], dp_lv$tier)
c5 <- ggplot(dp, aes(share, factor(status, levels = rev(levels(status))), fill = tier)) +
  geom_col(position = position_stack(reverse = TRUE), width = 0.65) +
  geom_text(data = filter(dp, share > 0.05), aes(label = percent(share, accuracy = 1)), position = position_stack(vjust = 0.5, reverse = TRUE), size = 3.3, colour = INK) +
  scale_fill_manual(values = dp_pal, name = "Minutes watched in the subscription window") + scale_x_continuous(labels = percent, expand = expansion(mult = c(0, 0.02))) +
  labs(title = "How much did cancellers watch before cancelling?", subtitle = "Total minutes across any subscription / pass inside the subscription window.", x = NULL, y = NULL) +
  theme_cx() + theme(panel.grid.major.y = element_blank())
out_plot(c5, "06_viewing_depth_tiers", 11, 4)

# COMMAND ----------

# DBTITLE 1,Chart 6
# ---- Chart 6. Q2: non-watchers by product and country --------------------------------------------
hm <- q("SELECT start_status, Product_name, Country, COUNT(*) subs FROM nw GROUP BY 1,2,3") %>% collect()
topc <- hm %>% group_by(Country) %>% summarise(n = sum(subs), .groups = "drop") %>% arrange(desc(n)) %>% slice_head(n = 7) %>% pull(Country)
hm <- hm %>% mutate(Country = ifelse(Country %in% topc, Country, "Others"), status = lab_status(start_status)) %>%
  group_by(status, Product_name, Country) %>% summarise(subs = sum(subs), .groups = "drop")
c6 <- ggplot(hm, aes(factor(Country, levels = c(topc, "Others")), Product_name, fill = subs)) +
  geom_tile(colour = "white") + geom_text(aes(label = comma(subs), colour = subs > max(subs) * 0.55), size = 3.4) +
  scale_colour_manual(values = c("TRUE" = "white", "FALSE" = INK), guide = "none") +
  scale_fill_gradient(low = "#eef4fc", high = "#1f5fae", name = "Non-watching\nsubscriptions") +
  facet_wrap(~ status, ncol = 1) +
  scale_x_discrete(labels = function(x) vapply(x, function(s) paste(strwrap(s, 12), collapse = "\n"), character(1))) +
  labs(title = "Where the non-watching cancellers are: product x country", x = NULL, y = NULL) +
  theme_cx() + theme(panel.grid = element_blank(), legend.position = "right", strip.text = element_text(face = "bold", colour = INK, hjust = 0),
                     axis.text.x = element_text(size = 10, colour = INK, lineheight = 0.9), axis.text.y = element_text(size = 10, colour = INK))
out_plot(c6, "07_non_watchers_product_country", 12, 11)

# COMMAND ----------

# DBTITLE 1,Chart 7
# ---- Chart 7. Q2: when did watchers last watch, relative to the cancel date? ---------------------
lw <- q("SELECT start_status,
                CASE WHEN last_view > cancel_date THEN '0 Watched after cancel' WHEN last_view = cancel_date THEN '1 Cancel day'
                     WHEN datediff(cancel_date, last_view) <= 7 THEN '2 1-7 days before' WHEN datediff(cancel_date, last_view) <= 30 THEN '3 8-30 days before'
                     ELSE '4 30+ days before' END AS bucket, COUNT(*) subs
         FROM (SELECT t.Subscription_ID, t.start_status, t.cancel_date, MAX(v.vdate) AS last_view
               FROM tiers t JOIN v ON v.viewerId = t.Customer_External_ID AND v.vdate BETWEEN t.start_date AND t.expiry_date
               WHERE t.cancel_date IS NOT NULL AND v.minutes >= 1 GROUP BY t.Subscription_ID, t.start_status, t.cancel_date)
         GROUP BY 1,2") %>% collect() %>%
  group_by(start_status) %>% mutate(share = subs / sum(subs)) %>% ungroup() %>%
  mutate(status = lab_status(start_status), lbl = sub("^[0-9] ", "", bucket))
lw_lv <- lw %>% distinct(bucket, lbl) %>% arrange(bucket)
c7 <- ggplot(lw %>% mutate(lbl = factor(lbl, levels = lw_lv$lbl)), aes(lbl, share, fill = status)) +
  geom_col(position = position_dodge(width = 0.75), width = 0.65) +
  geom_text(aes(label = percent(share, accuracy = 1)), position = position_dodge(width = 0.75), vjust = -0.4, size = 3.2, colour = INK) +
  scale_fill_manual(values = COL, name = "Started as") + scale_y_continuous(labels = percent, expand = expansion(mult = c(0, 0.12))) +
  labs(title = "When did watchers last watch, relative to cancelling?", subtitle = "Last viewing day (1+ minute, any subscription) inside the paid window, among cancellers who watched at all.", x = NULL, y = NULL) +
  theme_cx()
out_plot(c7, "08_last_watch_vs_cancel_date")

# COMMAND ----------

# DBTITLE 1,Chart 8
# ---- Chart 8. Q2: how many cycles do cancelling win-backs have behind them? ---------------------
cy <- q("
  WITH w AS (SELECT Customer_External_ID, MAX(episode_no) episodes, COUNT(*) n_subs FROM life
             WHERE Customer_External_ID IN (SELECT Customer_External_ID FROM cohort WHERE start_status = 'winback') GROUP BY 1)
  SELECT measure, bucket, COUNT(*) customers FROM (
    SELECT 'Episodes (real stays with TOD)' AS measure,
           CASE WHEN episodes >= 6 THEN '6+' WHEN episodes >= 4 THEN '4-5' ELSE CAST(episodes AS STRING) END AS bucket FROM w
    UNION ALL
    SELECT 'Subscription IDs' AS measure,
           CASE WHEN n_subs >= 6 THEN '6+' WHEN n_subs >= 4 THEN '4-5' ELSE CAST(n_subs AS STRING) END AS bucket FROM w) x
  GROUP BY 1,2") %>% collect()
cy <- cy %>% group_by(measure) %>% mutate(share = customers / sum(customers)) %>% ungroup() %>% mutate(lbl = ifelse(bucket == "1", "1*", bucket))
c8 <- ggplot(cy, aes(factor(lbl, levels = c("1*", "2", "3", "4-5", "6+")), customers, fill = measure)) +
  geom_col(position = position_dodge(width = 0.75), width = 0.7) +
  geom_text(aes(label = sprintf("%s\n%s", comma(customers), percent(share, accuracy = 1))), position = position_dodge(width = 0.75), vjust = -0.3, size = 3.2) +
  scale_fill_manual(values = c("Episodes (real stays with TOD)" = ORANGE, "Subscription IDs" = GREY), name = NULL) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.2)), labels = comma) +
  labs(title = "Cancelling win-backs: how many times have they left and come back?",
       subtitle = sprintf("Episodes = unbroken stays with TOD (a new episode starts after more than %s days without paid access). Subscription IDs shown for comparison.", P$WB_MIN_GAP),
       x = "Lifetime count per customer", y = "Customers",
       caption = "* only one episode in this extract: the earlier stay is outside the D2C filter or the data window.") +
  theme_cx()
out_plot(c8, "09_winback_cycles", 10, 5.5)

# COMMAND ----------

# DBTITLE 1,Chart 9
# ---- Chart 9. Q2: which competitions did cancellers watch before cancelling? ---------------------
cp <- q("SELECT t.start_status, v.competitionName, COUNT(DISTINCT t.Subscription_ID) subs
         FROM tiers t JOIN v ON v.viewerId = t.Customer_External_ID AND v.vdate BETWEEN t.start_date AND COALESCE(t.cancel_date, t.expiry_date)
         WHERE v.ContentType2 = 'Football' AND v.competitionName IS NOT NULL AND v.minutes >= 1 GROUP BY 1,2") %>% collect()
den <- q("SELECT start_status, COUNT(*) total FROM cohort GROUP BY 1") %>% collect()
top5 <- cp %>% group_by(competitionName) %>% summarise(n = sum(subs), .groups = "drop") %>% arrange(desc(n)) %>% slice_head(n = 5) %>% pull(competitionName)
cp <- cp %>% filter(competitionName %in% top5) %>% inner_join(den, by = "start_status") %>%
  mutate(share = subs / total, status = lab_status(start_status), competitionName = factor(competitionName, levels = rev(top5)))
c9 <- ggplot(cp, aes(share, competitionName, fill = status)) +
  geom_col(position = position_dodge(width = 0.75), width = 0.65) +
  geom_text(aes(label = percent(share, accuracy = 1)), position = position_dodge(width = 0.75), hjust = -0.15, size = 3.2, colour = INK) +
  scale_fill_manual(values = COL, name = "Started as") + scale_x_continuous(labels = percent, expand = expansion(mult = c(0, 0.15))) +
  labs(title = "Share of cancellers who watched each competition before cancelling", subtitle = "Football sessions of 1+ minute inside the subscription window; denominator = all cancelling subscriptions of that start status.", x = NULL, y = NULL) +
  theme_cx() + theme(panel.grid.major.y = element_blank(), panel.grid.major.x = element_line(colour = GRID))
out_plot(c9, "10_competitions_watched_before_cancel", 10, 5)

# COMMAND ----------

# DBTITLE 1,Chart 10
# ---- Chart 10. Q2: event study, cancellations around peak match days ----------------------------
c10 <- ggplot(ev_study %>% mutate(status = lab_status(start_status)), aes(offset_days, avg_cancels_per_peak_day, colour = status)) +
  geom_vline(xintercept = 0, linetype = "dashed", colour = INK2) +
  geom_line(linewidth = 0.9) + geom_point(size = 2) +
  scale_colour_manual(values = COL, name = "Started as") + scale_x_continuous(breaks = -3:7) + scale_y_continuous(labels = comma, limits = c(0, NA)) +
  labs(title = "Cancellations around peak match days", subtitle = sprintf("Average cancellations per peak match day (top %.0f%% of match days by viewers), by days from the match. Overlapping match days count twice.", 100 * (1 - as.numeric(P$PEAK_PCT))),
       x = "Days relative to match day (0 = match day)", y = "Cancellations per match day") +
  theme_cx()
out_plot(c10, "11_event_study_peak_match_days", 10, 5.5)

# COMMAND ----------

# DBTITLE 1,Chart 10 explained
# MAGIC %md
# MAGIC ### Chart 10 explained: cancellations around peak match days
# MAGIC
# MAGIC **The question it answers.** Do customers cancel around big football days? For example, do they sign up for a big match, watch it and cancel straight after?
# MAGIC
# MAGIC **How to read the chart**
# MAGIC * **Peak match day:** one of the busiest match days by number of viewers. The cut-off is `P$PEAK_PCT` (0.90 = top 10%). The cell below prints how many peak days there are.
# MAGIC * **X axis:** days from the match. 0 is the match day, −3 three days before, +7 a week after.
# MAGIC * **Y axis:** cancellations on that day, averaged over all peak match days.
# MAGIC * **Lines:** blue = customers who started as New, orange = Win-back.
# MAGIC * **Rule of thumb:** a flat line means match days make no difference. A spike at 0 or just after means cancellations cluster around big matches.
# MAGIC
# MAGIC **The numbers** are in the summary table in the next cell. It is computed, so it always matches the current run. It compares the match day with (a) an average day in 2026, (b) the 3 days before and (c) the 7 days after.
# MAGIC
# MAGIC **Watch out for**
# MAGIC * **Overlapping windows:** peak days a few days apart share days, so one cancellation can count for two matches. Use the shape of the line, not totals.
# MAGIC * **Day of week (not checked):** big matches cluster on certain days, so part of a day-0 spike may be a weekday pattern.
# MAGIC * **Not proof:** this shows timing, not cause.

# COMMAND ----------

# DBTITLE 1,Chart 10 summary table
# ---- Chart 10 summary table (computed, so it always matches the current peak-day definition) ------------
any_day <- q("SELECT start_status, COUNT(*) / COUNT(DISTINCT cancel_date) AS avg_cancels_any_day FROM life
              WHERE cancel_date >= '2026-01-01' AND cancel_date <= '{TO}' GROUP BY 1") %>% collect()
n_peak <- q("SELECT COUNT(*) n FROM peak_md WHERE match_date >= '2026-01-01' AND match_date <= date_sub(DATE'{TO}', 7)") %>% collect() %>% dplyr::pull(n)
n_md   <- q("SELECT COUNT(DISTINCT match_date) n FROM fixtures WHERE match_date >= '2026-01-01' AND match_date <= date_sub(DATE'{TO}', 7)") %>% collect() %>% dplyr::pull(n)
ev10 <- ev_study %>% group_by(start_status) %>%
  dplyr::summarise(avg_3_days_before = mean(avg_cancels_per_peak_day[offset_days < 0]),
                   match_day = sum(avg_cancels_per_peak_day[offset_days == 0]),
                   avg_7_days_after = mean(avg_cancels_per_peak_day[offset_days > 0]), .groups = "drop") %>%
  dplyr::inner_join(any_day, by = "start_status") %>%
  mutate(day0_vs_any_day_pct = round(100 * (match_day / avg_cancels_any_day - 1), 1),
         day0_vs_3_days_before_pct = round(100 * (match_day / avg_3_days_before - 1), 1),
         after_vs_before_pct = round(100 * (avg_7_days_after / avg_3_days_before - 1), 1),
         dplyr::across(c(avg_cancels_any_day, avg_3_days_before, match_day, avg_7_days_after), ~ round(.x, 0)))
cat(sprintf("Peak match days = top %.0f%% of match days by viewers: %d of %d match days, Jan 2026 to %s.\n",
            100 * (1 - as.numeric(P$PEAK_PCT)), n_peak, n_md, format(as.Date(P$TO) - 7, "%d %b %Y")))
cat(sprintf("Share of 2026 days within 2 days of a peak match day: %.0f%% (lower = sharper test).\n",
            100 * mean(cal$near_peak[cal$d >= "2026-01-01"])))
display_many("Chart 10 summary: match day vs any day, the 3 days before and the 7 days after" = ev10)

# COMMAND ----------

# DBTITLE 1,Chart 10b
# ---- Chart 10b. Q2: same event study as Chart 10, split by the TOP 10 COMPETITIONS (by viewers, Jan 2026 to TO-7) ----
# FIFA World Cup is excluded here and analysed on its own in the World Cup cells below.
# For each competition: every match day of that competition is an 'event'. For each offset (-3..+7 days) we average the
# cancellations on that day over all the competition's match days, by start status.
# Uplift = match day (day 0) vs the average of the 3 days before (-3..-1).
daily_c <- q("SELECT cancel_date AS d, start_status, COUNT(*) n FROM life
              WHERE cancel_date >= '2025-12-25' AND cancel_date <= '{TO}' GROUP BY 1,2") %>% reg("daily_c")

top_comp <- q("SELECT competitionName, COUNT(DISTINCT match_date) match_days, SUM(viewers) viewers FROM fixtures
               WHERE match_date >= '2026-01-01' AND match_date <= date_sub(DATE'{TO}', 7) AND competitionName IS NOT NULL AND competitionName <> '' AND competitionName <> 'FIFA World Cup'
               GROUP BY 1 ORDER BY viewers DESC LIMIT 10") %>% collect()
sdf_copy_to(sc, top_comp, "top_comp", overwrite = TRUE)

ev_comp <- q("
  WITH f AS (
    SELECT DISTINCT x.competitionName, to_date(x.match_date) AS match_date FROM fixtures x JOIN top_comp t USING (competitionName)
    WHERE x.match_date >= '2026-01-01' AND x.match_date <= date_sub(DATE'{TO}', 7)),
  fn AS (SELECT *, COUNT(*) OVER (PARTITION BY competitionName) AS n_days FROM f)
  SELECT fn.competitionName, c.start_status, datediff(c.d, fn.match_date) AS offset_days,
         SUM(c.n) / MAX(fn.n_days) AS avg_cancels_per_match_day
  FROM fn JOIN daily_c c ON datediff(c.d, fn.match_date) BETWEEN -3 AND 7
  GROUP BY 1,2,3") %>% collect()

# Summary table: match day vs the days before / after, per competition and start status
comp_tbl <- ev_comp %>% group_by(competitionName, start_status) %>%
  summarise(pre_avg = mean(avg_cancels_per_match_day[offset_days < 0]),
            match_day = avg_cancels_per_match_day[offset_days == 0],
            post_avg = mean(avg_cancels_per_match_day[offset_days > 0]), .groups = "drop") %>%
  mutate(uplift_vs_pre_pct = round(100 * (match_day / pre_avg - 1), 1), across(c(pre_avg, match_day, post_avg), ~ round(.x, 0))) %>%
  inner_join(top_comp, by = "competitionName") %>%
  dplyr::arrange(desc(viewers), start_status) %>%
  dplyr::select(competitionName, match_days, viewers, start_status, pre_avg, match_day, post_avg, uplift_vs_pre_pct)
df_35_1 = comp_tbl

# Chart: one small panel per competition (ordered by viewers), New vs Win-back lines
comp_lv <- top_comp %>% dplyr::arrange(desc(viewers)) %>% mutate(lab = sprintf("%s\n(%d match days)", competitionName, match_days))
c10b <- ggplot(ev_comp %>% inner_join(comp_lv %>% dplyr::select(competitionName, lab), by = "competitionName") %>%
                 mutate(lab = factor(lab, levels = comp_lv$lab), status = lab_status(start_status)),
               aes(offset_days, avg_cancels_per_match_day, colour = status)) +
  geom_vline(xintercept = 0, linetype = "dashed", colour = INK2) +
  geom_line(linewidth = 0.8) + geom_point(size = 1.4) +
  facet_wrap(~ lab, ncol = 5, scales = "free_y") +
  scale_colour_manual(values = COL, name = "Started as") + scale_x_continuous(breaks = c(-3, 0, 3, 7)) + scale_y_continuous(labels = comma, limits = c(0, NA)) +
  labs(title = "Cancellations around match days: top 10 competitions (excluding FIFA World Cup)",
       subtitle = "Average cancellations per match day of each competition, by days from the match (0 = match day). Jan 2026 to mid-Sep 2026.\nCompetitions with many match days overlap their own windows, so their lines are flatter.",
       x = "Days relative to match day", y = "Cancellations per match day") +
  theme_cx() + theme(strip.text = element_text(face = "bold", colour = INK, size = 9))
out_plot(c10b, "11b_event_study_top10_competitions", 14, 7)

display_many("Summary table: match day vs the days before / after, per competition and start status" = df_35_1
)

# COMMAND ----------

# DBTITLE 1,Chart 10c
# ---- Chart 10c. Q2: same event study as Chart 10, for the TOP 10 individual MATCHES (by distinct viewers, Jan 2026 to TO-7) ----
# FIFA World Cup matches are excluded here and analysed on their own in the World Cup cells below.
# Each match is one event, so the values are the actual cancellations on each day around that match (not an average).
# Uplift = match day (day 0) vs the average of the 3 days before (-3..-1). Needs daily_c from the Chart 10b cell.
top_matches <- q("SELECT match_opid, competitionName, home_team, away_team, to_date(match_date) AS match_date, COUNT(DISTINCT viewerId) AS viewers
                  FROM {VIEW_TBL}
                  WHERE ContentType2 = 'Football' AND match_opid IS NOT NULL AND competitionName <> 'FIFA World Cup'
                    AND to_date(match_date) >= '2026-01-01' AND to_date(match_date) <= date_sub(DATE'{TO}', 7)
                  GROUP BY 1,2,3,4,5 ORDER BY viewers DESC LIMIT 10") %>% collect() %>%
  mutate(match_date = as.Date(match_date),
         lab = sprintf("%s v %s\n%s, %s (%sk viewers)", home_team, away_team, competitionName, format(match_date, "%d %b"), comma(round(viewers / 1000))))
sdf_copy_to(sc, top_matches %>% mutate(match_date = as.character(match_date)) %>% dplyr::select(match_opid, match_date), "top_matches", overwrite = TRUE)

ev_match <- q("SELECT m.match_opid, c.start_status, datediff(c.d, to_date(m.match_date)) AS offset_days, SUM(c.n) AS cancels
               FROM top_matches m JOIN daily_c c ON datediff(c.d, to_date(m.match_date)) BETWEEN -3 AND 7
               GROUP BY 1,2,3") %>% collect()

# Summary table per match and start status
match_tbl <- ev_match %>% group_by(match_opid, start_status) %>%
  summarise(pre_avg = mean(cancels[offset_days < 0]), match_day = sum(cancels[offset_days == 0]),
            post_avg = mean(cancels[offset_days > 0]), .groups = "drop") %>%
  mutate(uplift_vs_pre_pct = round(100 * (match_day / pre_avg - 1), 1), across(c(pre_avg, post_avg), ~ round(.x, 0))) %>%
  inner_join(top_matches %>% dplyr::select(match_opid, competitionName, home_team, away_team, match_date, viewers), by = "match_opid") %>%
  dplyr::arrange(desc(viewers), start_status) %>%
  dplyr::select(match_date, competitionName, home_team, away_team, viewers, start_status, pre_avg, match_day, post_avg, uplift_vs_pre_pct)
df_36_1 = match_tbl

# Chart: one small panel per match (ordered by viewers)
mt_lv <- top_matches %>% dplyr::arrange(desc(viewers))
c10c <- ggplot(ev_match %>% inner_join(top_matches %>% dplyr::select(match_opid, lab), by = "match_opid") %>%
                 mutate(lab = factor(lab, levels = mt_lv$lab), status = lab_status(start_status)),
               aes(offset_days, cancels, colour = status)) +
  geom_vline(xintercept = 0, linetype = "dashed", colour = INK2) +
  geom_line(linewidth = 0.8) + geom_point(size = 1.4) +
  facet_wrap(~ lab, ncol = 5) +
  scale_colour_manual(values = COL, name = "Started as") + scale_x_continuous(breaks = c(-3, 0, 3, 7)) + scale_y_continuous(labels = comma, limits = c(0, NA)) +
  labs(title = "Cancellations around the 10 most-watched matches (excluding FIFA World Cup)",
       subtitle = "Cancellations on each day from 3 days before to 7 days after the match (0 = match day). Same y-axis in every panel.\nMatches only a few days apart share days, so their windows overlap.",
       x = "Days relative to match day", y = "Cancellations per day") +
  theme_cx() + theme(strip.text = element_text(face = "bold", colour = INK, size = 8.5))
out_plot(c10c, "11c_event_study_top10_matches", 15, 7)

display_many("Summary table per match and start status (top 10 matches, excl. World Cup)" = df_36_1
)

# COMMAND ----------

# DBTITLE 1,Chart 10d
# ---- Chart 10d. Q2: FIFA World Cup on its own -------------------------------------------------------
# Part 1: same event study as Chart 10b, for World Cup match days only (avg cancellations per match day at each offset).
# Part 2: daily cancellations before, during and after the tournament, with World Cup match days marked.
# Needs daily_c from the Chart 10b cell.
wc_days <- q("SELECT to_date(match_date) AS match_date, SUM(viewers) AS viewers FROM fixtures
              WHERE competitionName = 'FIFA World Cup' AND match_date >= '2026-01-01' AND match_date <= '{TO}' GROUP BY 1
              HAVING SUM(viewers) >= 10000") %>% collect() %>%   # drops a stray 11 May row (2,770 viewers) that is not a tournament match day
  mutate(match_date = as.Date(match_date))
wc_from <- min(wc_days$match_date); wc_to <- max(wc_days$match_date)
cat(sprintf("FIFA World Cup: %d match days, %s to %s\n", nrow(wc_days), format(wc_from, "%d %b %Y"), format(wc_to, "%d %b %Y")))
sdf_copy_to(sc, wc_days %>% mutate(match_date = as.character(match_date)), "wc_days", overwrite = TRUE)

# Part 1: event study
ev_wc <- q("
  SELECT c.start_status, datediff(c.d, to_date(w.match_date)) AS offset_days,
         SUM(c.n) / (SELECT COUNT(*) FROM wc_days) AS avg_cancels_per_match_day
  FROM wc_days w JOIN daily_c c ON datediff(c.d, to_date(w.match_date)) BETWEEN -3 AND 7
  GROUP BY 1,2") %>% collect()
wc_tbl <- ev_wc %>% group_by(start_status) %>%
  summarise(pre_avg = mean(avg_cancels_per_match_day[offset_days < 0]), match_day = avg_cancels_per_match_day[offset_days == 0],
            post_avg = mean(avg_cancels_per_match_day[offset_days > 0]), .groups = "drop") %>%
  mutate(uplift_vs_pre_pct = round(100 * (match_day / pre_avg - 1), 1), across(c(pre_avg, match_day, post_avg), ~ round(.x, 0)))
df_37_1 = wc_tbl

c10d <- ggplot(ev_wc %>% mutate(status = lab_status(start_status)), aes(offset_days, avg_cancels_per_match_day, colour = status)) +
  geom_vline(xintercept = 0, linetype = "dashed", colour = INK2) +
  geom_line(linewidth = 0.9) + geom_point(size = 2) +
  scale_colour_manual(values = COL, name = "Started as") + scale_x_continuous(breaks = -3:7) + scale_y_continuous(labels = comma, limits = c(0, NA)) +
  labs(title = "FIFA World Cup: cancellations around match days",
       subtitle = sprintf("Average cancellations per World Cup match day (%d match days, %s to %s), by days from the match. Match days are close together, so windows overlap.",
                          nrow(wc_days), format(wc_from, "%d %b"), format(wc_to, "%d %b")),
       x = "Days relative to match day (0 = match day)", y = "Cancellations per match day") +
  theme_cx()
out_plot(c10d, "11d_event_study_world_cup", 10, 5.5)

# Part 2: daily cancellations around the tournament (4 weeks before to 4 weeks after)
wc_daily <- q(sprintf("SELECT cancel_date AS d, start_status, COUNT(*) n FROM life WHERE cancel_date >= '%s' AND cancel_date <= '%s' GROUP BY 1,2",
                      wc_from - 28, wc_to + 28)) %>% collect() %>% mutate(d = as.Date(d), status = lab_status(start_status))
wc_period <- wc_daily %>% mutate(period = ifelse(d < wc_from, "1 Before", ifelse(d <= wc_to, "2 During", "3 After"))) %>%
  group_by(status, period) %>% summarise(avg_daily_cancels = round(mean(n), 0), .groups = "drop")
df_37_2 = wc_period
c10d2 <- ggplot(wc_daily, aes(d, n, colour = status)) +
  annotate("rect", xmin = wc_from, xmax = wc_to, ymin = -Inf, ymax = Inf, fill = GRID, alpha = 0.6) +
  geom_vline(data = wc_days, aes(xintercept = match_date), colour = AREA, linewidth = 0.3) +
  geom_line(linewidth = 0.8) +
  scale_colour_manual(values = COL, name = "Started as") + scale_x_date(date_breaks = "1 week", date_labels = "%d %b") + scale_y_continuous(labels = comma, limits = c(0, NA)) +
  labs(title = "Daily cancellations before, during and after the FIFA World Cup",
       subtitle = "Shaded = tournament period; thin grey lines = World Cup match days.", x = NULL, y = "Cancellations per day") +
  theme_cx() + theme(axis.text.x = element_text(angle = 45, hjust = 1))
out_plot(c10d2, "11d2_daily_cancellations_world_cup", 12, 5.5)

display_many("FIFA World Cup event study: match day vs the days before / after, by start status" = df_37_1,
"FIFA World Cup: average daily cancellations before, during and after the tournament" = df_37_2
)

# COMMAND ----------

# DBTITLE 1,Chart 10e
# ---- Chart 10e. Q2: same event study as Chart 10c, for the TOP 10 FIFA WORLD CUP MATCHES (by distinct viewers) ----
# Each match is one event: actual cancellations on each day from 3 days before to 7 days after.
# Uplift = match day (day 0) vs the average of the 3 days before (-3..-1). Needs daily_c from the Chart 10b cell.
wc_matches <- q("SELECT match_opid, competitionName, home_team, away_team, to_date(match_date) AS match_date, COUNT(DISTINCT viewerId) AS viewers
                 FROM {VIEW_TBL}
                 WHERE ContentType2 = 'Football' AND match_opid IS NOT NULL AND competitionName = 'FIFA World Cup'
                   AND to_date(match_date) >= '2026-01-01' AND to_date(match_date) <= date_sub(DATE'{TO}', 7)
                 GROUP BY 1,2,3,4,5 ORDER BY viewers DESC LIMIT 10") %>% collect() %>%
  mutate(match_date = as.Date(match_date),
         lab = sprintf("%s v %s\n%s (%sk viewers)", home_team, away_team, format(match_date, "%d %b"), comma(round(viewers / 1000))))
sdf_copy_to(sc, wc_matches %>% mutate(match_date = as.character(match_date)) %>% dplyr::select(match_opid, match_date), "wc_matches", overwrite = TRUE)

ev_wc_match <- q("SELECT m.match_opid, c.start_status, datediff(c.d, to_date(m.match_date)) AS offset_days, SUM(c.n) AS cancels
                  FROM wc_matches m JOIN daily_c c ON datediff(c.d, to_date(m.match_date)) BETWEEN -3 AND 7
                  GROUP BY 1,2,3") %>% collect()

wc_match_tbl <- ev_wc_match %>% group_by(match_opid, start_status) %>%
  summarise(pre_avg = mean(cancels[offset_days < 0]), match_day = sum(cancels[offset_days == 0]),
            post_avg = mean(cancels[offset_days > 0]), .groups = "drop") %>%
  mutate(uplift_vs_pre_pct = round(100 * (match_day / pre_avg - 1), 1), across(c(pre_avg, post_avg), ~ round(.x, 0))) %>%
  inner_join(wc_matches %>% dplyr::select(match_opid, home_team, away_team, match_date, viewers), by = "match_opid") %>%
  dplyr::arrange(desc(viewers), start_status) %>%
  dplyr::select(match_date, home_team, away_team, viewers, start_status, pre_avg, match_day, post_avg, uplift_vs_pre_pct)
df_38_1 = wc_match_tbl

wc_lv <- wc_matches %>% dplyr::arrange(desc(viewers))
c10e <- ggplot(ev_wc_match %>% inner_join(wc_matches %>% dplyr::select(match_opid, lab), by = "match_opid") %>%
                 mutate(lab = factor(lab, levels = wc_lv$lab), status = lab_status(start_status)),
               aes(offset_days, cancels, colour = status)) +
  geom_vline(xintercept = 0, linetype = "dashed", colour = INK2) +
  geom_line(linewidth = 0.8) + geom_point(size = 1.4) +
  facet_wrap(~ lab, ncol = 5) +
  scale_colour_manual(values = COL, name = "Started as") + scale_x_continuous(breaks = c(-3, 0, 3, 7)) + scale_y_continuous(labels = comma, limits = c(0, NA)) +
  labs(title = "FIFA World Cup: cancellations around the 10 most-watched matches",
       subtitle = "Cancellations on each day from 3 days before to 7 days after the match (0 = match day). Same y-axis in every panel.\nWorld Cup matches are only days apart, so their windows overlap.",
       x = "Days relative to match day", y = "Cancellations per day") +
  theme_cx() + theme(strip.text = element_text(face = "bold", colour = INK, size = 8.5))
out_plot(c10e, "11e_event_study_world_cup_top10_matches", 15, 7)

display_many("FIFA World Cup: top 10 matches, match day vs the days before / after" = df_38_1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Charts 10b–10e: do big competitions and matches trigger cancellations?
# MAGIC
# MAGIC **Method (all four charts):** cancellations from 3 days before to 7 days after each match day, by start status. **Uplift** = match day vs the average of the 3 days before. Jan to mid-Sep 2026.
# MAGIC
# MAGIC **Key takeaways**
# MAGIC
# MAGIC * **Big knockout matches trigger a short spike, mainly among New customers.** The biggest spikes are on finals.
# MAGIC * **The FIFA World Cup works the other way:** customers stay during the tournament and cancel before and after it.
# MAGIC * **League football (Premier League, La Liga) has little effect.** Match days are almost every day, so there is no clear peak.
# MAGIC * **The effect lasts one day.** From day +1, cancellations go back towards normal.
# MAGIC
# MAGIC **Chart 10b: top 10 competitions (excl. World Cup)**
# MAGIC
# MAGIC | Competition | Match days | New uplift | Win-back uplift |
# MAGIC | --- | --- | --- | --- |
# MAGIC | UEFA Champions League | 33 | **+20%** | +9% |
# MAGIC | Friendlies | 26 | +15% | +9% |
# MAGIC | Ligue 1 | 68 | +8% | **+13%** |
# MAGIC | Premier League | 84 | +2% | +6% |
# MAGIC | Primera División | 108 | +3% | +6% |
# MAGIC | AFC Champions League Elite | 21 | −3% | −6% |
# MAGIC
# MAGIC Champions League nights stand out for New customers. Friendlies and Ligue 1 show moderate rises; the rest are within ±10%.
# MAGIC
# MAGIC **Chart 10c: top 10 matches (excl. World Cup)**
# MAGIC
# MAGIC | Match | Date | New uplift | Win-back uplift |
# MAGIC | --- | --- | --- | --- |
# MAGIC | PSG v Arsenal (UCL final) | 30 May | **+195%** (354 → 1,043) | **+95%** (805 → 1,572) |
# MAGIC | Atlético v Barcelona (UCL) | 14 Apr | +71% | +43% |
# MAGIC | Real Madrid v Bayern (UCL) | 7 Apr | +70% | +63% |
# MAGIC | Bayern v Real Madrid (UCL) | 15 Apr | +68% | +48% |
# MAGIC | Bayern v PSG (UCL) | 6 May | +64% | −14% |
# MAGIC | Barcelona v Real Madrid (La Liga) | 10 May | +14% | +20% |
# MAGIC | Arsenal v Atlético (UCL, both legs) | 29 Apr / 5 May | −13% / +5% | −21% / −35% |
# MAGIC
# MAGIC * **9 of the 10 are Champions League games from April–May.** Most show a large match-day spike.
# MAGIC * **The final has by far the biggest spike.** Customers likely signed up for the run-in and cancelled straight after the final.
# MAGIC
# MAGIC **Chart 10d: FIFA World Cup (33 match days, 11 Jun – 19 Jul 2026)**
# MAGIC
# MAGIC | Avg cancellations per day | 4 weeks before | During | 4 weeks after |
# MAGIC | --- | --- | --- | --- |
# MAGIC | New | 614 | **424** | 187 |
# MAGIC | Win-back | 1,048 | **575** | 292 |
# MAGIC
# MAGIC * **No match-day spike:** match day vs the 3 days before is New −5.5%, Win-back −8.5%.
# MAGIC * **Cancellations fall during the tournament** (New −31%, Win-back −45% vs the month before). Customers keep their subscription while matches are on.
# MAGIC * **The month before is high**, likely customers leaving after the club season and UCL final. **The month after is low**; possibly many World Cup sign-ups had not yet reached their renewal (not checked).
# MAGIC
# MAGIC **Chart 10e: top 10 World Cup matches**
# MAGIC
# MAGIC * **Only the final spikes:** Spain v Argentina (19 Jul), New **+108%** (159 → 331), Win-back +34%.
# MAGIC * **Smaller rises:** Argentina v Egypt (New +30%) and France v Spain (New +29%).
# MAGIC * **The rest are flat or down,** e.g. France v Morocco −23% and Brazil v Norway −19% (New).
# MAGIC
# MAGIC **What it means**
# MAGIC
# MAGIC * **The risk is at the end of a competition, not during it.** Finals (UCL 30 May, World Cup 19 Jul) show the biggest spikes.
# MAGIC * **New customers react most.** Many likely subscribe for one event and leave when it ends.
# MAGIC * **Retention action:** target customers acquired for a competition *before* its final, e.g. with an offer for the next season or competition.
# MAGIC
# MAGIC **Caveats**
# MAGIC
# MAGIC * **Overlapping windows:** matches only a few days apart share days, so one cancellation can count for more than one match.
# MAGIC * **Day of week not checked:** both finals fell on a weekend (Sat 30 May, Sun 19 Jul), so part of the spike may be a weekend pattern.
# MAGIC * **Timing, not cause:** the charts show when cancellations happen, not why.
# MAGIC * **Data fix:** a stray World Cup row on 11 May (2,770 viewers) was excluded.
# MAGIC
# MAGIC *All figures in this note are from the V2.3 run (28 Oct 2026), before V2.5 kept voluntary cancellations only and split out Quick re-subscribes. They will change: use the tables above each chart for current values, and update this note after re-running.*

# COMMAND ----------

# DBTITLE 1,Chart 11
# ---- Chart 11. Q3: what happened between cancelling and expiry? ---------------------------------
# 'Cancellation reversed' and 'Charged again after cancel' removed: the cohort is built from the latest row with a cancel date,
# so reversed cancellations can never be in it (0), and only 6 of ~41k subscriptions had a recurring charge after cancelling (0.0%).
# CUSTOMER level: one row per cancelling customer, one outcome each (mutually exclusive, so each bar sums to 100%).
#   start_status = status of the customer's most recent cancelled subscription in the cohort
#   outcome priority: new subscription before expiry > still in paid period > kept watching, then expired > stopped watching, then expired
bf_cust <- q("
  WITH s AS (
    SELECT p.Customer_External_ID, p.start_status, p.cancel_date, p.Subscription_ID, p.reached_expiry,
           CASE WHEN p.next_start <= p.expiry_date THEN 1 ELSE 0 END AS new_sub_before_expiry,
           CASE WHEN t.sessions_cancel_to_expiry > 0 THEN 1 ELSE 0 END AS kept_watching
    FROM post p JOIN tiers t ON t.Subscription_ID = p.Subscription_ID),
  c AS (
    SELECT Customer_External_ID,
           MAX_BY(start_status, struct(cancel_date, Subscription_ID)) AS start_status,
           MAX(new_sub_before_expiry) AS new_sub, MIN(reached_expiry) AS all_expired, MAX(kept_watching) AS kept_watching
    FROM s GROUP BY 1)
  SELECT start_status,
         CASE WHEN new_sub = 1 THEN '1 Started a new subscription before expiry'
              WHEN all_expired = 0 THEN '2 Still in paid period'
              WHEN kept_watching = 1 THEN '3 Kept watching, then expired'
              ELSE '4 Stopped watching, then expired' END AS outcome,
         COUNT(*) customers
  FROM c GROUP BY 1,2") %>% collect()
bf <- bf_cust %>% group_by(start_status) %>% mutate(total = sum(customers), share = customers / total) %>% ungroup() %>%
  mutate(status = lab_status(start_status), lbl = sub("^[0-9] ", "", outcome),
         y = factor(sprintf("%s\n(%s customers)", status, comma(total))))
bf_lv <- bf %>% distinct(outcome, lbl) %>% arrange(outcome)
bf <- bf %>% mutate(lbl = factor(lbl, levels = bf_lv$lbl))
print(bf %>% group_by(status) %>% summarise(customers = sum(customers), share_total = sum(share), .groups = "drop"))   # check: share_total = 1
bf_pal <- setNames(c("#08306b", "#9ec5f4", ORANGE, "#c9c8c2")[as.integer(substr(bf_lv$outcome, 1, 1))], bf_lv$lbl)
c11 <- ggplot(bf, aes(share, factor(y, levels = rev(sort(unique(y)))), fill = lbl)) +
  geom_col(position = position_stack(reverse = TRUE), width = 0.6) +
  geom_text(data = filter(bf, share > 0.03), aes(label = sprintf("%s\n%s", percent(share, accuracy = 1), comma(customers)), colour = lbl == bf_lv$lbl[1]),
            position = position_stack(vjust = 0.5, reverse = TRUE), size = 3.2) +
  scale_colour_manual(values = c("TRUE" = "white", "FALSE" = INK), guide = "none") +
  scale_fill_manual(values = bf_pal, name = NULL) + scale_x_continuous(labels = percent, expand = expansion(mult = c(0, 0.02))) +
  labs(title = "Between cancelling and paid expiry: what did cancelling customers do?",
       subtitle = sprintf("Customers who cancelled in Aug-Sep 2026, one outcome each (bars sum to 100%%). Status as of %s.", P$ASOF), x = NULL, y = NULL) +
  theme_cx() + theme(panel.grid.major.y = element_blank())
out_plot(c11, "12_before_expiry_outcomes", 11, 5)

# COMMAND ----------

# DBTITLE 1,Chart 11 explained
# MAGIC %md
# MAGIC ### Chart 11: Between cancelling and paid expiry (customer level)
# MAGIC
# MAGIC **How each customer is counted**
# MAGIC
# MAGIC * Each cancelling customer appears once, in one outcome. If more than one applies, the first one in this list wins:
# MAGIC   1. Started a new subscription before expiry
# MAGIC   2. Still in paid period (as of `P$ASOF`)
# MAGIC   3. Kept watching, then expired
# MAGIC   4. Stopped watching, then expired
# MAGIC * New or Win-back is taken from the customer's most recent cancelled subscription in Aug–Sep 2026.
# MAGIC
# MAGIC **Results:** see the table in the next cell. It is computed on every run, so it always matches the chart. With `ASOF = "auto"`, the "Still in paid period" share falls as more subscriptions reach expiry.
# MAGIC
# MAGIC **Checks:** the next cell prints two checks:
# MAGIC * each group's shares add up to 100%
# MAGIC * the total matches the number of distinct cancelling customers in the cohort
# MAGIC
# MAGIC The cohort has fewer customers than subscriptions because some customers cancelled more than one subscription.

# COMMAND ----------

# DBTITLE 1,Chart 11 table: customer-level outcomes between cancelling and paid expiry (uses bf from the Chart 11 cell) ----
# One outcome per customer, so shares within each start status sum to 100%
# ---- Chart 11 table: customer-level outcomes between cancelling and paid expiry (uses bf from the Chart 11 cell) ----
# One outcome per customer, so shares within each start status sum to 100%.
outcome_tbl <- bf %>%
  transmute(start_status = as.character(status), outcome = as.character(lbl), customers, share_pct = round(100 * share, 1), outcome_order = as.integer(lbl)) %>%
  dplyr::arrange(start_status, outcome_order) %>% dplyr::select(-outcome_order)
df_43_1 = outcome_tbl

# Checks: shares add to 100% per start status, and total customers = distinct cancelling customers in the cohort
chk <- bf %>% group_by(start_status = as.character(status)) %>% summarise(customers = sum(customers), share_total_pct = round(100 * sum(share), 1), .groups = "drop")   # unrounded shares (rounded ones can add to 99.9)
distinct_cust <- q("SELECT COUNT(DISTINCT Customer_External_ID) n FROM post") %>% collect() %>% pull(n)
df_43_2 = chk
cat(sprintf("Total customers in table: %s | Distinct cancelling customers in cohort: %s | Match: %s\n",
            comma(sum(chk$customers)), comma(distinct_cust), sum(chk$customers) == distinct_cust))

display_many("Customer-level outcomes between cancelling and paid expiry" = df_43_1,
"Checks: shares add to 100% per start status" = df_43_2
)

# COMMAND ----------

# DBTITLE 1,Chart 12
# ---- Chart 12. Q3: after expiry, how soon do they come back? -----------------------------------
rt <- ret %>% mutate(later = returned - (d0_7 + d8_14 + d15_30), not_back = matured_subs - returned) %>%
  tidyr::pivot_longer(c(d0_7, d8_14, d15_30, later, not_back), names_to = "when", values_to = "n") %>%
  mutate(share = n / matured_subs, status = lab_status(start_status),
         when = factor(when, levels = c("d0_7", "d8_14", "d15_30", "later", "not_back"),
                       labels = c("Back within 7 days", "8-14 days", "15-30 days", "More than 30 days", "Not back (yet)")))
rt_pal <- c("Back within 7 days" = "#08306b", "8-14 days" = BLUE, "15-30 days" = "#9ec5f4", "More than 30 days" = "#d6e4f7", "Not back (yet)" = "#e3e2dd")
c12 <- ggplot(rt, aes(share, factor(status, levels = rev(levels(status))), fill = when)) +
  geom_col(position = position_stack(reverse = TRUE), width = 0.65) +
  geom_text(data = filter(rt, share > 0.04), aes(label = percent(share, accuracy = 1), colour = when %in% c("Back within 7 days", "8-14 days")),
            position = position_stack(vjust = 0.5, reverse = TRUE), size = 3.3) +
  scale_colour_manual(values = c("TRUE" = "white", "FALSE" = INK), guide = "none") +
  scale_fill_manual(values = rt_pal, name = NULL) + scale_x_continuous(labels = percent, expand = expansion(mult = c(0, 0.02))) +
  labs(title = "After paid expiry: how soon did they come back?", subtitle = sprintf("Subscriptions whose expiry is at least %s days before %s, so the window is complete.", P$MATURE_DAYS, P$ASOF), x = NULL, y = NULL) +
  theme_cx() + theme(panel.grid.major.y = element_blank())
out_plot(c12, "13_return_speed_after_expiry", 11, 4)

# COMMAND ----------

# DBTITLE 1,Chart 13
# ---- Chart 13. Q3: do returns line up with the fixture calendar? ---------------------------------
Q1 <- "Returns per week (new subscription after expiry)"; Q2 <- "Match-day viewers per week (thousands, summed over days)"
lvq <- c(Q1, Q2)
rw <- ret_wk %>% mutate(wk = as.Date(wk), status = lab_status(start_status), panel = factor(Q1, lvq))
fw <- fx_wk %>% mutate(wk = as.Date(wk), value = match_viewers / 1000, panel = factor(Q2, lvq))
c13 <- ggplot() +
  geom_col(data = rw, aes(wk + 3.5, returns, fill = status), width = 6, colour = "white", linewidth = 0.3) +
  geom_col(data = fw, aes(wk + 3.5, value), fill = AREA, width = 6) +
  facet_grid(panel ~ ., scales = "free_y", switch = "y") +
  scale_fill_manual(values = COL, name = "Started as") + scale_x_date(date_breaks = "1 week", date_labels = "%d %b") + scale_y_continuous(labels = comma) +
  labs(title = "Returns vs the fixture calendar", subtitle = "A return = a new subscription starting on or after the previous one expired. Compare weeks with heavy match-day viewing against weeks with high returns.", x = NULL, y = NULL) +
  theme_cx() + theme(strip.placement = "outside", strip.text.y.left = element_text(angle = 0, hjust = 1, face = "bold", colour = INK))
out_plot(c13, "14_returns_vs_fixtures", 11, 6.5)

cat("Charts saved to", P$OUT_DIR, "\n")

# COMMAND ----------

# DBTITLE 1,Cell 10: Q1 non-watchers vs viewers
# ---------------------------------------------------------------------
# CELL 10: Q1 non-watchers vs viewers: cancellations, actual churn and revenue
#   Viewing is measured over the whole PAID period (start to expiry, or to ASOF if still running), on any
#   subscription or pass, so purchase-day cancellers are not counted as non-watchers just because their
#   start-to-cancel window was one day.
#   Actual churn (30 days) = matured subscription, no new subscription before expiry and none within MATURE_DAYS after.
#   Revenue (V2.5, monthly-equivalent): last charge / months it covers (annual = 12); value back = first charge of the next
#   subscription / months it covers, for returns within MATURE_DAYS. Full-charge figures are kept alongside.
# ---------------------------------------------------------------------
first_charge <- q("
  SELECT Subscription_ID, Transaction_price_excl_TAX_USD AS first_charge_usd FROM (
    SELECT Subscription_ID, Transaction_price_excl_TAX_USD,
           ROW_NUMBER() OVER (PARTITION BY Subscription_ID ORDER BY Transaction_Date ASC) rn FROM DIM_Transactions_SD) WHERE rn = 1") %>%
  reg("first_charge")

wv <- q("
  WITH w AS (
    SELECT c.Subscription_ID, SUM(v.minutes) AS m_paid, COUNT(DISTINCT v.asset_id) AS assets_paid
    FROM cohort c JOIN v ON v.viewerId = c.Customer_External_ID AND v.vdate BETWEEN c.start_date AND LEAST(c.expiry_date, DATE'{ASOF}')
    GROUP BY 1)
  SELECT p.Subscription_ID, p.Customer_External_ID, p.start_status, p.cancel_type, p.days_start_to_cancel, p.reached_expiry, p.matured,
         p.days_expiry_to_next_start, p.next_sub_id, p.next_start, p.expiry_date, p.months_covered,
         COALESCE(w.m_paid, 0) AS m_paid, COALESCE(w.assets_paid, 0) AS assets_paid,
         CASE WHEN COALESCE(w.m_paid, 0) < 1 THEN '1 Non-watcher (<1 min)'
              WHEN w.m_paid < 300 THEN '2 Light viewer (1 min-5 h)' ELSE '3 Regular viewer (5 h+)' END AS view_group,
         CASE WHEN COALESCE(p.next_start <= p.expiry_date, FALSE) THEN 1 ELSE 0 END AS new_sub_before_expiry,
         CASE WHEN COALESCE(p.days_expiry_to_next_start BETWEEN 0 AND {MATURE_DAYS}, FALSE) THEN 1 ELSE 0 END AS back_within_30d
  FROM post p LEFT JOIN w ON w.Subscription_ID = p.Subscription_ID") %>% reg("wv")

nwv <- q("
  SELECT w.start_status, w.view_group, COUNT(*) subs,
         ROUND(AVG(w.m_paid), 0) avg_minutes, ROUND(AVG(w.assets_paid), 1) avg_assets,
         ROUND(AVG(CASE WHEN w.cancel_type = 'Cancelled on purchase day / next day' THEN 1.0 ELSE 0 END), 3) share_cancel_day0_1,
         percentile_approx(w.days_start_to_cancel, 0.5) median_days_to_cancel,
         SUM(w.reached_expiry) reached_expiry, SUM(w.new_sub_before_expiry) new_sub_before_expiry,
         SUM(w.matured) matured,
         SUM(CASE WHEN w.matured = 1 THEN w.back_within_30d ELSE 0 END) back_within_30d,
         SUM(CASE WHEN w.matured = 1 AND w.back_within_30d = 0 AND w.new_sub_before_expiry = 0 THEN 1 ELSE 0 END) churned_30d,
         ROUND(SUM(COALESCE(l.last_charge_usd, 0) / w.months_covered), 0) monthly_value_forgone_usd,
         ROUND(SUM(CASE WHEN w.matured = 1 AND w.back_within_30d = 1 THEN COALESCE(f.first_charge_usd, 0) / COALESCE(nx.months_covered, 1) ELSE 0 END), 0) monthly_value_back_30d_usd,
         ROUND(SUM(COALESCE(l.last_charge_usd, 0)), 0) renewal_value_forgone_usd
  FROM wv w LEFT JOIN last_charge l ON l.Subscription_ID = w.Subscription_ID
            LEFT JOIN first_charge f ON f.Subscription_ID = w.next_sub_id
            LEFT JOIN life nx ON nx.Subscription_ID = w.next_sub_id
  GROUP BY 1,2 ORDER BY 1,2") %>% collect()

nwv_tbl <- nwv %>% group_by(start_status) %>%
  mutate(share_of_cancellations_pct = round(100 * subs / sum(subs), 1)) %>% ungroup() %>%
  mutate(group = sub("^[0-9] ", "", view_group),
         churned_30d_pct = round(100 * churned_30d / matured, 1),
         back_within_30d_pct = round(100 * back_within_30d / matured, 1),
         monthly_forgone_per_sub_usd = round(monthly_value_forgone_usd / subs, 2)) %>%
  dplyr::select(start_status, group, subs, share_of_cancellations_pct, avg_minutes, avg_assets, share_cancel_day0_1, median_days_to_cancel,
                reached_expiry, new_sub_before_expiry, matured, back_within_30d, back_within_30d_pct, churned_30d, churned_30d_pct,
                monthly_value_forgone_usd, monthly_forgone_per_sub_usd, monthly_value_back_30d_usd, renewal_value_forgone_usd)
display_many("Non-watchers vs viewers: cancellations, actual churn and revenue" = nwv_tbl)

# Chart 15: four comparisons side by side
nw_long <- nwv_tbl %>%
  dplyr::transmute(status = lab_status(start_status), group = factor(group, levels = unique(sub("^[0-9] ", "", sort(unique(nwv$view_group))))),
                   `Share of cancellations` = share_of_cancellations_pct / 100,
                   `Cancelled on day 0-1` = share_cancel_day0_1,
                   `Back within 30 days of expiry` = back_within_30d_pct / 100,
                   `Actual churn (not back in 30 days)` = churned_30d_pct / 100) %>%
  tidyr::pivot_longer(-c(status, group), names_to = "metric", values_to = "share") %>%
  mutate(metric = factor(metric, levels = c("Share of cancellations", "Cancelled on day 0-1", "Back within 30 days of expiry", "Actual churn (not back in 30 days)")))
c15 <- ggplot(nw_long, aes(group, share, fill = status)) +
  geom_col(position = position_dodge(width = 0.75), width = 0.65) +
  geom_text(aes(label = percent(share, accuracy = 1)), position = position_dodge(width = 0.75), vjust = -0.4, size = 3, colour = INK) +
  facet_wrap(~ metric, nrow = 1) +
  scale_fill_manual(values = COL, name = "Started as") + scale_y_continuous(labels = percent, expand = expansion(mult = c(0, 0.15))) +
  labs(title = "Non-watchers vs viewers: how they cancel, churn and come back",
       subtitle = sprintf("Viewing over the whole paid period on any subscription or pass. Churn and returns: subscriptions whose expiry is at least %s days before %s.", P$MATURE_DAYS, P$ASOF),
       x = NULL, y = NULL) +
  theme_cx() + theme(strip.text = element_text(face = "bold", colour = INK), axis.text.x = element_text(angle = 25, hjust = 1))
out_plot(c15, "16_nonwatchers_vs_viewers_outcomes", 14, 5.5)

# COMMAND ----------

# DBTITLE 1,Cell 11: Q3 returns as a share of expiries
# ---------------------------------------------------------------------
# CELL 11: Q3 returns as a share of expiries, by expiry week (are returns higher when football is on?)
#   For cohort subscriptions expiring each week: share back within 7 days and within MATURE_DAYS of expiry.
#   A week is only shown for a window once every expiry in it has had the full window by ASOF.
#   Football = match-day viewers in the expiry week and the following week (the window in which most returns happen).
# ---------------------------------------------------------------------
rx <- q("
  SELECT date_trunc('week', expiry_date) wk, start_status, COUNT(*) expiries,
         SUM(CASE WHEN COALESCE(next_start <= expiry_date, FALSE) THEN 1 ELSE 0 END) new_sub_before_expiry,
         SUM(CASE WHEN days_expiry_to_next_start BETWEEN 0 AND 7 THEN 1 ELSE 0 END) back_7d,
         SUM(CASE WHEN days_expiry_to_next_start BETWEEN 0 AND {MATURE_DAYS} THEN 1 ELSE 0 END) back_30d
  FROM post WHERE expiry_date <= '{ASOF}' GROUP BY 1,2") %>% collect() %>%
  mutate(wk = as.Date(wk),
         complete_7d  = wk + 6 + 7 <= as.Date(P$ASOF),
         complete_30d = wk + 6 + as.integer(P$MATURE_DAYS) <= as.Date(P$ASOF),
         back_7d_pct  = ifelse(complete_7d,  round(100 * back_7d / expiries, 1), NA),
         back_30d_pct = ifelse(complete_30d, round(100 * back_30d / expiries, 1), NA))
fx2 <- fx_wk %>% mutate(wk = as.Date(wk)) %>% arrange(wk) %>%
  mutate(match_viewers_this_and_next_wk = match_viewers + coalesce(dplyr::lead(match_viewers), 0)) %>%
  dplyr::select(wk, match_viewers, match_viewers_this_and_next_wk)
rx_tbl <- rx %>% dplyr::left_join(fx2, by = "wk") %>% arrange(wk, start_status) %>%
  dplyr::select(expiry_week = wk, start_status, expiries, new_sub_before_expiry, back_7d, back_7d_pct, back_30d, back_30d_pct, match_viewers_this_and_next_wk)
display_many("Returns as a share of expiries, by expiry week" = rx_tbl)

# Chart 16: expiries (bars), return rate (lines), football (bars), one panel each
R1 <- "Cohort expiries per week"; R2 <- "Back within 7 / 30 days (% of expiries)"; R3 <- "Match-day viewers, expiry week + next (thousands)"
lvr <- c(R1, R2, R3)
r_bars <- rx %>% mutate(status = lab_status(start_status), panel = factor(R1, lvr))
r_rate <- rx %>% dplyr::select(wk, start_status, back_7d_pct, back_30d_pct) %>%
  tidyr::pivot_longer(c(back_7d_pct, back_30d_pct), names_to = "window", values_to = "pct") %>% filter(!is.na(pct)) %>%
  mutate(status = lab_status(start_status), window = ifelse(window == "back_7d_pct", "within 7 days", "within 30 days"), panel = factor(R2, lvr))
r_fx <- fx2 %>% filter(wk >= min(rx$wk), wk <= max(rx$wk)) %>% mutate(value = match_viewers_this_and_next_wk / 1000, panel = factor(R3, lvr))
c16 <- ggplot() +
  geom_col(data = r_bars, aes(wk + 3.5, expiries, fill = status), width = 6, colour = "white", linewidth = 0.3) +
  geom_line(data = r_rate, aes(wk + 3.5, pct, colour = status, linetype = window), linewidth = 0.9) +
  geom_point(data = r_rate, aes(wk + 3.5, pct, colour = status), size = 1.6) +
  geom_col(data = r_fx, aes(wk + 3.5, value), fill = AREA, width = 6) +
  facet_grid(panel ~ ., scales = "free_y", switch = "y") +
  scale_fill_manual(values = COL, name = "Started as") + scale_colour_manual(values = COL, guide = "none") +
  scale_linetype_manual(values = c("within 7 days" = "dashed", "within 30 days" = "solid"), name = NULL) +
  scale_x_date(date_breaks = "1 week", date_labels = "%d %b") + scale_y_continuous(labels = comma) +
  labs(title = "Returns as a share of expiries, week by week",
       subtitle = sprintf("Cohort subscriptions by the week their paid access ended. Rates shown only for weeks with the full window by %s.", P$ASOF),
       x = NULL, y = NULL) +
  theme_cx() + theme(strip.placement = "outside", strip.text.y.left = element_text(angle = 0, hjust = 1, face = "bold", colour = INK),
                     axis.text.x = element_text(angle = 45, hjust = 1))
out_plot(c16, "17_returns_share_of_expiries", 12, 8)

# COMMAND ----------

# DBTITLE 1,HTML report
# ============ Voluntary Cancellation Analysis (Aug-Sep 2026): HTML report ============
# Same layout and style as the Cell 46 report: golden title and table headers, KPI cards, definitions box,
# key insights, then one section per question with the chart (grey palette, embedded PNG) and its table.
# Re-uses every chart built above (Charts 1-16 and 10b-10e) and re-colours it in grey. Run every cell above first.
library(ggplot2)
rp_fmt <- function(x) format(x, big.mark = ",", scientific = FALSE, trim = TRUE)
rp_esc <- function(x) { x <- as.character(x); x[is.na(x) | x == ""] <- "Unknown"; gsub("<", "&lt;", gsub("&", "&amp;", x)) }

# table with optional in-cell bars for % columns (Spark tables are collected, max_rows rows)
rp_table <- function(df, bar_cols = character(0), max_rows = 500) {   # V2.6: was 50, which cut Aug-Sep rows
  if (inherits(df, "tbl_spark")) df <- df %>% head(max_rows) %>% dplyr::collect()
  df <- as.data.frame(utils::head(as.data.frame(df), max_rows), check.names = FALSE)
  df[] <- lapply(df, function(v) if (is.numeric(v)) round(v, 2) else if (inherits(v, c("Date", "POSIXt"))) format(v, "%Y-%m-%d") else as.character(v))
  hdr <- paste0("<tr>", paste0("<th>", rp_esc(names(df)), "</th>", collapse = ""), "</tr>")
  rows <- vapply(seq_len(nrow(df)), function(i) {
    cells <- vapply(names(df), function(nm) {
      v <- df[[nm]][i]
      if (nm %in% bar_cols && is.numeric(v) && !is.na(v))
        sprintf("<td class='bar'><div style='width:%s%%;background:#7f7f7f'></div><span>%s%%</span></td>", min(max(v, 0), 100), v)
      else if (is.numeric(v) && !is.na(v)) sprintf("<td class='num'>%s</td>", rp_fmt(v))
      else sprintf("<td>%s</td>", rp_esc(v))
    }, character(1))
    paste0("<tr>", paste(cells, collapse = ""), "</tr>")
  }, character(1))
  paste0("<table>", hdr, paste(rows, collapse = ""), "</table>")
}
rp_kpi <- function(label, value, sub = "", color = "#404040")
  sprintf("<div class='kpi' style='border-top:4px solid %s'><div class='kv'>%s</div><div class='kl'>%s</div><div class='ks'>%s</div></div>", color, value, label, sub)
rp_img <- function(p, w = 10, h = 5) {
  f <- tempfile(fileext = ".png")
  ggsave(f, p, width = w, height = h, dpi = 100, bg = "white")
  sprintf("<div class='chart'><img src='data:image/png;base64,%s'/></div>", base64enc::base64encode(f))
}
rp_note <- function(x) sprintf("<p class='meta'>%s</p>", x)

# ---- grey re-styling of the charts above (Cell 46 palette: greys, black text) ----
GCOL <- c("New" = "#595959", "Win-back" = "#b3b3b3", "Quick re-subscribe" = "#1f1f1f")
rp_theme <- theme(text = element_text(colour = "black"), axis.text = element_text(colour = "black"),
                  plot.title = element_text(face = "bold", colour = "black", size = 14), plot.subtitle = element_text(colour = "grey30"),
                  panel.grid.minor = element_blank(), strip.background = element_rect(fill = "#d9d9d9", colour = NA),
                  strip.text = element_text(face = "bold", colour = "black"))
ramp    <- function(lv, from, to) setNames(colorRampPalette(c(from, to))(length(lv)), lv)
st_fill <- function(p) suppressMessages(p + scale_fill_manual(values = GCOL, name = "Started as") + rp_theme)
st_col  <- function(p) suppressMessages(p + scale_colour_manual(values = GCOL, name = "Started as") + rp_theme)

g1  <- st_fill(c1);  g1b <- st_fill(c1b); g2 <- st_fill(c2)
g3  <- suppressMessages(c3 + scale_fill_manual(values = ramp(levels(al$lbl), "#ececec", "#404040"), name = NULL) + rp_theme)
g4  <- suppressMessages(c4 + scale_colour_manual(values = c(GCOL, "All" = "#000000"), guide = "none") + rp_theme)
g5  <- suppressMessages(c5 + scale_fill_manual(values = ramp(levels(dp$tier), "#f2f2f2", "#7f7f7f"), name = "Minutes watched in the subscription window") + rp_theme)
g6  <- suppressMessages(c6 + scale_fill_gradient(low = "#f2f2f2", high = "#404040", name = "Non-watching\nsubscriptions") + rp_theme)
g7  <- st_fill(c7)
g8  <- suppressMessages(c8 + scale_fill_manual(values = c("Episodes (real stays with TOD)" = "#595959", "Subscription IDs" = "#c8c8c8"), name = NULL) + rp_theme)
g9  <- st_fill(c9)
g10 <- st_col(c10); g10b <- st_col(c10b); g10c <- st_col(c10c); g10d <- st_col(c10d); g10d2 <- st_col(c10d2); g10e <- st_col(c10e)
g11 <- suppressMessages(c11 + scale_fill_manual(values = ramp(levels(bf$lbl), "#404040", "#d9d9d9"), name = NULL) + rp_theme)
g12 <- suppressMessages(c12 + scale_fill_manual(values = ramp(levels(rt$when), "#404040", "#ececec"), name = NULL) + rp_theme)
g13 <- st_fill(c13)
g15 <- st_fill(c15)
g16 <- suppressMessages(c16 + scale_fill_manual(values = GCOL, name = "Started as") + scale_colour_manual(values = GCOL, guide = "none") + rp_theme)
PEAK_TXT <- sprintf("top %.0f%%", 100 * (1 - as.numeric(P$PEAK_PCT)))

# ---- numbers for the KPIs and the narrative (all computed from the tables above) ----
mm       <- c("2026-08", "2026-09")
n_augsep <- n_cust_augsep   # distinct customers: someone who cancelled in both Aug and Sep counts once
rr <- mean(base$customers); rr_sd <- sd(base$customers)
n_sep <- tot$customers[tot$m == "2026-09"]; z_sep <- (n_sep - rr) / rr_sd
ms       <- dplyr::filter(monthly, m %in% mm)
wb_share <- 100 * sum(ms$customers[ms$start_status == "winback"]) / sum(ms$customers)
rs_share <- 100 * sum(ms$customers[ms$start_status == "resub"]) / sum(ms$customers)
rv_augsep <- sum(rev$monthly_value_forgone_usd[rev$m %in% mm], na.rm = TRUE)
rv_full   <- sum(rev$renewal_value_forgone_usd[rev$m %in% mm], na.rm = TRUE)
rv_m      <- vapply(mm, function(x) sum(rev$monthly_value_forgone_usd[rev$m == x], na.rm = TRUE), numeric(1))
rv_hist   <- rev %>% dplyr::filter(m < substr(P$FROM, 1, 7)) %>% dplyr::group_by(m) %>% dplyr::summarise(v = sum(monthly_value_forgone_usd, na.rm = TRUE), .groups = "drop") %>%
  dplyr::inner_join(rev_cov %>% dplyr::filter(used_in_revenue_run_rate) %>% dplyr::select(m), by = "m")   # V2.6: well-covered months only
rv_left_out <- rev_cov %>% dplyr::filter(m < substr(P$FROM, 1, 7), !used_in_revenue_run_rate) %>% dplyr::pull(m)
sep_rr  <- rr_tbl[rr_tbl$m == substr(P$TO, 1, 7), ]; aug_rr <- rr_tbl[rr_tbl$m == substr(P$FROM, 1, 7), ]
nv_all   <- nv$pct[nv$grp == "All"]
al_2w    <- 100 * sum(al$subs[substr(al$bucket, 1, 1) %in% c("3", "4")]) / sum(al$subs)
cye      <- dplyr::filter(cy, measure == "Episodes (real stays with TOD)"); cyi <- dplyr::filter(cy, measure == "Subscription IDs")
cy4      <- 100 * sum(cye$customers[cye$bucket %in% c("4-5", "6+")]) / sum(cye$customers)
cy4_ids  <- 100 * sum(cyi$customers[cyi$bucket %in% c("4-5", "6+")]) / sum(cyi$customers)
ev_up <- ev_study %>% dplyr::group_by(start_status) %>%
  dplyr::summarise(pre = mean(avg_cancels_per_peak_day[offset_days < 0]), d0 = sum(avg_cancels_per_peak_day[offset_days == 0]),
                   post = mean(avg_cancels_per_peak_day[offset_days > 0]), .groups = "drop") %>%
  dplyr::mutate(day0_uplift_pct = round(100 * (d0 / pre - 1), 1), after_vs_pre_pct = round(100 * (post / pre - 1), 1))
upn <- ev_up[ev_up$start_status == "new", ]; upw <- ev_up[ev_up$start_status == "winback", ]
top_m <- match_tbl %>% dplyr::filter(start_status == "new") %>% head(1)
wcp <- wc_period %>% dplyr::mutate(status = as.character(status)) %>% tidyr::pivot_wider(names_from = period, values_from = avg_daily_cancels)
wcn <- wcp[wcp$status == "New", ]; wcw <- wcp[wcp$status == "Win-back", ]
oc <- function(st, o) outcome_tbl$share_pct[outcome_tbl$start_status == st & outcome_tbl$outcome == o]
ret_p <- ret %>% dplyr::mutate(returned_pct = round(100 * returned / matured_subs, 1), within_7d_pct = round(100 * d0_7 / matured_subs, 1),
                               within_30d_pct = round(100 * (d0_7 + d8_14 + d15_30) / matured_subs, 1))
# Cells 10-11 (non-watchers vs viewers, returns as a share of expiries)
gv  <- function(st, grp, col) { x <- nwv_tbl[[col]][nwv_tbl$start_status == st & nwv_tbl$group == grp]; if (length(x)) x else NA }
NWG <- "Non-watcher (<1 min)"; RVG <- "Regular viewer (5 h+)"
rx_rng <- rx %>% dplyr::filter(!is.na(back_30d_pct)) %>% dplyr::group_by(start_status) %>%
  dplyr::summarise(lo = min(back_30d_pct), hi = max(back_30d_pct), weeks = dplyr::n(), .groups = "drop")
rxn <- rx_rng[rx_rng$start_status == "new", ]; rxw <- rx_rng[rx_rng$start_status == "winback", ]
rpn <- ret_p[ret_p$start_status == "new", ]; rpw <- ret_p[ret_p$start_status == "winback", ]

insights <- c(
  sprintf(paste0("<b>%s</b> distinct customers cancelled voluntarily in Aug-Sep 2026. September (<b>%s</b>) is <b>%+.0f%%</b> against the ", RR_LABEL,
                 " monthly average of %s and <b>%+.0f%%</b> against September last year; August is %+.0f%% against August last year. (The z-score of %.2f understates this because the May-Jun 2026 spike widens the spread.)"),
          rp_fmt(n_augsep), rp_fmt(n_sep), sep_rr$vs_run_rate_pct, rp_fmt(round(rr)), sep_rr$vs_last_year_pct, aug_rr$vs_last_year_pct, z_sep),
  sprintf("Win-back customers (back after more than %s days away) make up <b>%.0f%%</b> of Aug-Sep cancellers and quick re-subscribers <b>%.0f%%</b>. Cancellations removed about <b>$%s</b> of monthly revenue in Aug and <b>$%s</b> in Sep (monthly-equivalent of the last charge), vs a monthly average of $%s over the %d history months with good transaction coverage (%d months left out because many subscriptions have no transaction). At full last charge the Aug-Sep total is $%s.",
          P$WB_MIN_GAP, wb_share, rs_share, rp_fmt(round(rv_m[1])), rp_fmt(round(rv_m[2])), rp_fmt(round(mean(rv_hist$v))), nrow(rv_hist), length(rv_left_out), rp_fmt(round(rv_full))),
  sprintf("<b>%.0f%%</b> of cancelled subscriptions still had more than 14 days of paid access left, so most of the revenue effect lands at the next renewal.", al_2w),
  sprintf("<b>%.1f%%</b> of cancelling subscriptions never watched (under 1 minute in the whole paid period, on any subscription or pass).", nv_all),
  sprintf("<b>%.0f%%</b> of cancelling Win-back customers have left and come back 4 or more times (episodes); counted by subscription IDs it would be %.0f%%.", cy4, cy4_ids),
  sprintf("On peak match days, cancellations are <b>%+.1f%%</b> (New) and <b>%+.1f%%</b> (Win-back) vs the 3 days before; the 7 days after average %+.1f%% / %+.1f%%. The most-watched match, %s v %s (%s, %s), shows New <b>%+.1f%%</b> on the day.",
          upn$day0_uplift_pct, upw$day0_uplift_pct, upn$after_vs_pre_pct, upw$after_vs_pre_pct,
          top_m$home_team, top_m$away_team, top_m$competitionName, format(as.Date(top_m$match_date), "%d %b"), top_m$uplift_vs_pre_pct),
  sprintf("FIFA World Cup: average daily cancellations were <b>%s</b> (New) and <b>%s</b> (Win-back) during the tournament, vs %s and %s in the 4 weeks before and %s and %s in the 4 weeks after.",
          rp_fmt(wcn[["2 During"]]), rp_fmt(wcw[["2 During"]]), rp_fmt(wcn[["1 Before"]]), rp_fmt(wcw[["1 Before"]]), rp_fmt(wcn[["3 After"]]), rp_fmt(wcw[["3 After"]])),
  sprintf("Between cancelling and expiry, <b>%.1f%%</b> of New and <b>%.1f%%</b> of Win-back cancellers stopped watching and then expired; %.1f%% / %.1f%% started a new subscription before expiry.",
          oc("New", "Stopped watching, then expired"), oc("Win-back", "Stopped watching, then expired"),
          oc("New", "Started a new subscription before expiry"), oc("Win-back", "Started a new subscription before expiry")),
  sprintf("After expiry (matured subscriptions), <b>%.1f%%</b> of New and <b>%.1f%%</b> of Win-back came back within 30 days (%.1f%% / %.1f%% within 7 days; %.1f%% / %.1f%% at any time up to %s).",
          rpn$within_30d_pct, rpw$within_30d_pct, rpn$within_7d_pct, rpw$within_7d_pct, rpn$returned_pct, rpw$returned_pct, P$ASOF),
  sprintf("Non-watchers (no minute in the whole paid period) are <b>%.1f%%</b> of New and <b>%.1f%%</b> of Win-back cancellations. Within 30 days of expiry, %.1f%% / %.1f%% of non-watchers did not come back, vs %.1f%% / %.1f%% of regular viewers (5 h+). Monthly revenue lost on non-watchers: $%s.",
          gv("new", NWG, "share_of_cancellations_pct"), gv("winback", NWG, "share_of_cancellations_pct"),
          gv("new", NWG, "churned_30d_pct"), gv("winback", NWG, "churned_30d_pct"), gv("new", RVG, "churned_30d_pct"), gv("winback", RVG, "churned_30d_pct"),
          rp_fmt(sum(nwv_tbl$monthly_value_forgone_usd[nwv_tbl$group == NWG], na.rm = TRUE))),
  sprintf("Week by week, the share back within 30 days of expiry ranged %.1f%%-%.1f%% for New and %.1f%%-%.1f%% for Win-back (%d complete expiry weeks).",
          rxn$lo, rxn$hi, rxw$lo, rxw$hi, as.integer(max(rxn$weeks, rxw$weeks)))
)

# ---- tables for each section ----
t_month <- monthly %>% tidyr::pivot_wider(names_from = start_status, values_from = customers) %>%
  dplyr::left_join(dplyr::rename(tot, total = customers), by = "m") %>% dplyr::arrange(m) %>% dplyr::rename(month = m)
t_rev  <- rev %>% dplyr::arrange(m, start_status) %>% dplyr::rename(month = m)
t_al   <- al %>% dplyr::arrange(status, lbl) %>% dplyr::transmute(start_status = as.character(status), paid_access_left = as.character(lbl), subs, share_pct = round(100 * share, 1))
t_nv   <- nv %>% dplyr::transmute(group = as.character(grp), subs, never_watched = never, share_pct = round(pct, 1))
t_dp   <- dp %>% dplyr::arrange(status, tier) %>% dplyr::transmute(start_status = as.character(status), minutes_tier = as.character(tier), subs, share_pct = round(100 * share, 1))
t_hm   <- hm %>% dplyr::arrange(dplyr::desc(subs)) %>% dplyr::transmute(start_status = as.character(status), Product_name, Country, subs)
t_lw   <- lw %>% dplyr::arrange(status, bucket) %>% dplyr::transmute(start_status = as.character(status), last_watch = lbl, subs, share_pct = round(100 * share, 1))
t_cy   <- cy %>% dplyr::arrange(measure, lbl) %>% dplyr::transmute(measure, lifetime_count = lbl, customers, share_pct = round(100 * share, 1))
t_cp   <- cp %>% dplyr::arrange(dplyr::desc(share)) %>% dplyr::transmute(competitionName = as.character(competitionName), start_status = as.character(status), subs, total, share_pct = round(100 * share, 1))
t_ev   <- ev_study %>% dplyr::mutate(v = round(avg_cancels_per_peak_day, 0)) %>% dplyr::select(start_status, offset_days, v) %>%
  dplyr::arrange(offset_days) %>% tidyr::pivot_wider(names_from = offset_days, values_from = v)
t_ev_up <- ev_up %>% dplyr::mutate(dplyr::across(c(pre, d0, post), ~ round(.x, 0))) %>%
  dplyr::rename(avg_3_days_before = pre, match_day = d0, avg_7_days_after = post)
t_wcp  <- wcp %>% dplyr::rename(start_status = status)
t_ret  <- ret_p %>% dplyr::select(start_status, matured_subs, d0_7, within_7d_pct, d8_14, d15_30, within_30d_pct, returned, returned_pct)

# ---- images (chart sizes as saved above) ----
img <- list(
  c1 = rp_img(g1, 11, 6), c1b = rp_img(g1b, 11, 6), c2 = rp_img(g2, 11, 8.5), c3 = rp_img(g3, 10, 4), c4 = rp_img(g4, 10, 4),
  c5 = rp_img(g5, 11, 4), c6 = rp_img(g6, 12, 11), c7 = rp_img(g7, 11, 6), c8 = rp_img(g8, 9, 5.5), c9 = rp_img(g9, 10, 5),
  c10 = rp_img(g10, 10, 5.5), c10b = rp_img(g10b, 14, 7), c10c = rp_img(g10c, 15, 7), c10d = rp_img(g10d, 10, 5.5),
  c10d2 = rp_img(g10d2, 12, 5.5), c10e = rp_img(g10e, 15, 7), c11 = rp_img(g11, 11, 5), c12 = rp_img(g12, 11, 4), c13 = rp_img(g13, 11, 6.5),
  c15 = rp_img(g15, 14, 5.5), c16 = rp_img(g16, 12, 8))

css <- "<style>
 body{font-family:Segoe UI,Arial,sans-serif;color:#2c3e50;margin:24px;background:#f7f9fb}
 h1{color:#000;background:#D4AF37;padding:12px 18px;border-radius:4px;margin-bottom:6px} h2{color:#000;border-bottom:2px solid #D4AF37;padding-bottom:4px;margin-top:32px} h3{color:#000}
 .chart{background:#fff;padding:8px;margin:8px 0 16px;box-shadow:0 1px 3px rgba(0,0,0,.08);display:inline-block} .chart img{max-width:100%}
 .meta{color:#7f8c8d;font-size:13px} .kpis{display:flex;flex-wrap:wrap;gap:12px;margin:16px 0}
 .kpi{background:#fff;border-radius:6px;padding:12px 16px;min-width:170px;box-shadow:0 1px 3px rgba(0,0,0,.1)}
 .kv{font-size:26px;font-weight:700;color:#000} .kl{font-size:13px;font-weight:600;color:#000} .ks{font-size:12px;color:#7f8c8d}
 table{border-collapse:collapse;background:#fff;font-size:13px;margin:8px 0 16px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
 th{background:#D4AF37;color:#000;padding:6px 10px;text-align:left} td{padding:5px 10px;border-bottom:1px solid #eee;color:#000}
 td.num{text-align:right} td.bar{position:relative;min-width:140px} td.bar div{height:16px;opacity:.75;border-radius:2px}
 td.bar span{position:absolute;left:12px;top:5px;font-size:12px;font-weight:600;color:#000}
 .box{background:#fff;border-left:4px solid #7f7f7f;padding:10px 16px;margin:8px 0} li{margin:6px 0}
 .grid{display:flex;flex-wrap:wrap;gap:24px} .grid h3{background:#D4AF37;color:#000;padding:6px 14px;border-radius:4px} .def{font-size:13px;background:#f2f2f2;border:1px solid #d9d9d9;padding:10px 14px}
</style>"
sp <- "share_pct"

html <- paste0(
  "<html><head><meta charset='utf-8'><title>Voluntary Cancellation Analysis ", P$FROM, " to ", P$TO, "</title>", css, "</head><body>",
  "<h1>Voluntary Cancellations Aug-Sep 2026 - What Changed, Who Cancelled, What Happened Next</h1>",
  sprintf("<div class='meta'>V2.5 | %s | Win-back gap rule: %s (%s days)</div>", P$VOLUNTARY_ONLY_TXT, P$WB_RULE, P$WB_MIN_GAP),
  sprintf("<div class='meta'>Cancellations %s to %s | Status as of %s | Generated %s</div>", P$FROM, P$TO, P$ASOF, Sys.Date()),
  "<div class='kpis'>",
  rp_kpi("Voluntary cancelling customers Aug-Sep 2026", rp_fmt(n_augsep), sprintf("Win-back %.0f%% | Quick re-subscribe %.0f%%", wb_share, rs_share), "#404040"),
  rp_kpi("September cancelling customers", rp_fmt(n_sep), sprintf("%+.0f%% vs run rate %s | %+.0f%% vs Sep last year", sep_rr$vs_run_rate_pct, rp_fmt(round(rr)), sep_rr$vs_last_year_pct), "#595959"),
  rp_kpi("Monthly revenue lost", paste0("$", rp_fmt(round(rv_augsep))), "Aug-Sep, monthly-equivalent (estimate)", "#7f7f7f"),
  rp_kpi("Never watched", sprintf("%.1f%%", nv_all), "of cancelling subscriptions (whole paid period)", "#a6a6a6"),
  rp_kpi("Back within 30 days of expiry", sprintf("%.1f%% / %.1f%%", rpn$within_30d_pct, rpw$within_30d_pct), "New / Win-back (matured subs)", "#c8c8c8"),
  "</div>",
  "<div class='def'><b>Definitions.</b> <b>Cohort</b>: D2C subscriptions with a cancellation dated ", P$FROM, " to ", P$TO, "", P$VOLUNTARY_ONLY_TXT,
  ". <b>New / Win-back / Quick re-subscribe</b>: the status the subscription <i>started</i> with. A system 'winback' counts as <b>Win-back</b> only if the customer had been without paid access for more than ", P$WB_MIN_GAP,
  " days; otherwise it is a <b>Quick re-subscribe</b>. <b>Episode</b>: an unbroken stay with TOD. <b>Monthly revenue lost</b>: last charge excluding tax (USD) divided by the months it covers (annual = 12). ",
  "<b>Run rate</b>: monthly cancelling customers ", RR_LABEL, ". <b>Peak match day</b>: ", PEAK_TXT, " of match days by viewers. ",
  "<b>Non-watcher</b>: under 1 minute in the whole paid period, on any subscription or pass. <b>Revenue run rate</b>: history months where at least ", round(100 * as.numeric(P$REV_MIN_COVERAGE)), "% of cancelled subscriptions have a transaction. ",
  "<b>Uplift</b>: match day vs the average of the 3 days before. <b>Matured</b>: expiry at least ", P$MATURE_DAYS, " days before ", P$ASOF, ".</div>",
  "<h2>Key Insights</h2><div class='box'><ul>", paste0("<li>", insights, "</li>", collapse = ""), "</ul></div>",

  "<h2>1. Q1 What changed: monthly cancellations by start status</h2>", img$c1,
  "<h3>Aug-Sep against the run rate and the same month last year</h3>", rp_table(rr_tbl), rp_table(t_month),
  "<h3>How long Aug-Sep 'winback' cancellers had been away (system label)</h3>", rp_table(df_v25_3),
  "<h3>Monthly revenue lost (monthly-equivalent)</h3>", img$c1b, rp_table(t_rev),
  "<h3>Revenue coverage by month</h3>", rp_table(rev_cov),
  "<h2>2. Cancellations and the football calendar</h2>", img$c2,
  rp_note("Weekly totals divided by days in the week. Timing alone does not prove football caused the cancellations."),
  "<h2>3. Days of paid access left at cancellation</h2>", img$c3, rp_table(t_al, sp),

  "<h2>4. Viewing before cancelling</h2>",
  "<h3>Cancellers who never watched</h3>", img$c4, rp_table(t_nv, sp),
  "<h3>Viewing depth (minutes watched)</h3>", img$c5, rp_table(t_dp, sp),
  "<h3>Non-watchers by product and country</h3>", img$c6, rp_table(t_hm, max_rows = 20),
  "<h3>When did watchers last watch?</h3>", img$c7, rp_table(t_lw, sp),
  "<h3>Viewers: once or repeatedly, after cancelling, after expiry</h3>", rp_table(df_16_1),
  "<h3>Playback problems</h3>", rp_table(df_16_2),
  "<h3>Non-watchers vs viewers: cancellations, actual churn and revenue</h3>", img$c15, rp_table(nwv_tbl),

  "<h2>5. Q2 Repeat cycling and duplicate accounts</h2>", img$c8, rp_table(t_cy, sp),
  "<div class='grid'><div><h3>Cycling detail (Win-back, by episodes)</h3>", rp_table(df_17_1), "</div>",
  "<div><h3>3+ subscriptions within 7 days</h3>", rp_table(df_17_2), "</div>",
  "<div><h3>Parallel paid subscriptions</h3>", rp_table(df_17_5), "</div></div>",
  "<h3>'New' cancellers with an earlier subscription on another account</h3>", rp_table(df_17_4),

  "<h2>6. Competitions watched before cancelling</h2>", img$c9, rp_table(t_cp, sp),

  "<h2>7. Cancellations around match days</h2>",
  "<h3>Peak match days (event study)</h3>", img$c10, rp_table(ev10), rp_table(t_ev),
  "<h3>Top 10 competitions (excl. FIFA World Cup)</h3>", img$c10b, rp_table(comp_tbl),
  "<h3>Top 10 matches (excl. FIFA World Cup)</h3>", img$c10c, rp_table(match_tbl),
  "<h3>FIFA World Cup: around match days</h3>", img$c10d, rp_table(wc_tbl),
  "<h3>FIFA World Cup: before, during and after</h3>", img$c10d2, rp_table(t_wcp),
  "<h3>FIFA World Cup: top 10 matches</h3>", img$c10e, rp_table(wc_match_tbl),
  rp_note("Matches only a few days apart share days, so one cancellation can count for more than one match. Timing, not cause."),

  "<h2>8. Q3 Between cancelling and paid expiry</h2>", img$c11, rp_table(outcome_tbl, sp),
  "<h3>Expiry, reversals and charges after cancel (subscriptions)</h3>", rp_table(df_18_1),

  "<h2>9. Q3 After expiry: returns</h2>", img$c12, rp_table(t_ret, c("returned_pct", "within_7d_pct")),
  "<h3>Returns by channel</h3>", rp_table(df_19_2),
  "<h3>Returns vs the fixture calendar</h3>", img$c13,
  "<h3>Returns as a share of expiries, week by week</h3>", img$c16, rp_table(rx_tbl, max_rows = 40),
  "<h3>What returners watched in their first 7 days back</h3>", rp_table(df_20_3, max_rows = 15),
  "</body></html>")

# ---- save + show ----
rp_out <- file.path(P$OUT_DIR, paste0("VoluntaryCancellation_Report_", P$ASOF, ".html"))
writeLines(html, rp_out, useBytes = TRUE)
cat("Saved:", rp_out, sprintf("(%.1f MB)\n", file.size(rp_out) / 1e6))

displayHTML(html)

# COMMAND ----------

# DBTITLE 1,Validation 1: row integrity, cohort recount, voluntary filter
# =====================================================================
# VALIDATION (V2.6), part 1 of 4: row integrity, cohort recount, voluntary filter, cancel dates
# Runs at the end, on the same session as the analysis above. "expect" says what a correct result looks like.
# =====================================================================
# V1. One row per subscription; no subscription with two 'latest' rows            expect: rows = distinct_subs; dup_latest = 0
df_v1_1 <- q("SELECT COUNT(*) rows, COUNT(DISTINCT Subscription_ID) distinct_subs FROM life")
df_v1_2 <- q("SELECT COUNT(*) dup_latest FROM (SELECT Subscription_ID FROM base_rows WHERE Subscription_latest = 1 GROUP BY 1 HAVING COUNT(*) > 1)")

# V2. Cohort recount straight from the source table, with the voluntary filter     expect: 'source, voluntary' = 'V2.6 cohort'
df_v2_1 <- q("
  WITH src AS (
    SELECT Subscription_ID, Customer_External_ID,
           CASE WHEN Subscription_churn_type = 'Involuntary churn' OR churn_reason IN ('Broadcaster churn', 'Payment failed')
                     OR Cancellation_reason LIKE '%Faild to Charge%' OR Cancellation_reason LIKE '%Failed to Charge%'
                     OR Cancellation_reason LIKE 'Customer cancellation: Canceled By%' THEN 0 ELSE 1 END AS vol
    FROM DIM_SubscribersInfo_SDF
    WHERE D2C_B2B = 'D2C' AND offer_type = 'Subscription' AND product_type IS NULL AND non_commercial <> 1 AND daily_weekly_flag <> 1
      AND Customer_External_ID IS NOT NULL AND TRIM(Customer_External_ID) NOT IN ('', 'null')
      AND Subscription_latest = 1 AND to_date(Cancellation_date) BETWEEN '{FROM}' AND '{TO}')
  SELECT 'source, voluntary' AS source, COUNT(DISTINCT Subscription_ID) subs, COUNT(DISTINCT Customer_External_ID) customers FROM src WHERE vol = 1
  UNION ALL SELECT 'source, excluded', COUNT(DISTINCT Subscription_ID), COUNT(DISTINCT Customer_External_ID) FROM src WHERE vol = 0
  UNION ALL SELECT 'V2.6 cohort', COUNT(DISTINCT Subscription_ID), COUNT(DISTINCT Customer_External_ID) FROM cohort")

# V3. Nothing involuntary left in the cohort; what the filter removed by month     expect: only voluntary churn or blank (still in paid period)
df_v3_1 <- q("SELECT Subscription_churn_type, churn_reason, regexp_extract(Cancellation_reason, '^(.{0,45})', 1) reason, COUNT(*) subs
              FROM cohort GROUP BY 1,2,3 ORDER BY subs DESC")
df_v3_2 <- q("SELECT date_format(cancel_date_raw, 'yyyy-MM') m, SUM(is_voluntary) kept, SUM(1 - is_voluntary) excluded
              FROM life WHERE cancel_date_raw >= '{HIST_FROM}' AND cancel_date_raw <= '{TO}' GROUP BY 1 ORDER BY 1")

# V4. Cancellations dated on or after paid expiry                                   expect: cancel_after_expiry small, most_negative about -1
df_v4_1 <- q("SELECT start_status, SUM(CASE WHEN days_cancel_to_expiry < 0 THEN 1 ELSE 0 END) cancel_after_expiry,
                     SUM(CASE WHEN days_cancel_to_expiry = 0 THEN 1 ELSE 0 END) cancel_on_expiry_day, MIN(days_cancel_to_expiry) most_negative
              FROM cohort GROUP BY 1")

display_many(
  "V1. Rows vs distinct subscriptions in life" = df_v1_1,
  "V1. Subscriptions with two 'latest' rows (expect 0)" = df_v1_2,
  "V2. Cohort recount from source, voluntary filter (expect source voluntary = V2.6 cohort)" = df_v2_1,
  "V3. Churn type and reason left in the cohort (expect no involuntary)" = df_v3_1,
  "V3. Cancellations kept / excluded by month" = df_v3_2,
  "V4. Cancellations dated on or after paid expiry" = df_v4_1)

# COMMAND ----------

# DBTITLE 1,Validation 2: gap rule, episodes, bad expiry dates
# VALIDATION (V2.6), part 2 of 4: gap rule, episodes, bad expiry dates

# V5. Gap rule applied exactly as defined                                            expect: no rows in the first table
df_v5_1 <- q("
  SELECT start_status_sys, start_status, gap_since_prev_paid_through, COUNT(*) subs FROM life
  WHERE NOT (
       (start_status_sys = 'new'     AND start_status = 'new')
    OR (start_status_sys = 'winback' AND gap_since_prev_paid_through IS NULL AND start_status = 'winback')
    OR (start_status_sys = 'winback' AND gap_since_prev_paid_through >  {WB_MIN_GAP} AND start_status = 'winback')
    OR (start_status_sys = 'winback' AND gap_since_prev_paid_through <= {WB_MIN_GAP} AND start_status = 'resub'))
  GROUP BY 1,2,3 ORDER BY subs DESC LIMIT 20")   # with WB_RULE = 'system' every resub row would be listed: expected
df_v5_2 <- q("SELECT start_status_sys, start_status, COUNT(*) subs, MIN(gap_since_prev_paid_through) min_gap, MAX(gap_since_prev_paid_through) max_gap
              FROM cohort GROUP BY 1,2 ORDER BY 1,2")   # V2.6: min_gap should no longer reach thousands of days

# V6. Episode numbers recomputed independently                                       expect: mismatches = 0; episodes_above_subs = 0
df_v6_1 <- q("
  WITH r AS (
    SELECT episode_no,
           1 + SUM(CASE WHEN seq > 1 AND (gap_since_prev_paid_through IS NULL OR gap_since_prev_paid_through > {WB_MIN_GAP}) THEN 1 ELSE 0 END)
               OVER (PARTITION BY Customer_External_ID ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS episode_check
    FROM life)
  SELECT COUNT(*) subs, SUM(CASE WHEN episode_no <> episode_check THEN 1 ELSE 0 END) mismatches FROM r")
df_v6_2 <- q("SELECT COUNT(*) customers, SUM(CASE WHEN episodes > n_subs THEN 1 ELSE 0 END) episodes_above_subs,
                     ROUND(AVG(n_subs), 2) avg_subs, ROUND(AVG(episodes), 2) avg_episodes
              FROM (SELECT Customer_External_ID, COUNT(*) n_subs, MAX(episode_no) episodes FROM life GROUP BY 1)")

# V14 (new). Bad expiry dates and the seasonal cap                                   expect: max_months_covered <= 12 for every offer period
df_v14_1 <- q("SELECT Offer_period, bad_expiry, COUNT(*) subs, MAX(months_covered) max_months_covered, MAX(paid_period_days) max_paid_days
               FROM cohort GROUP BY 1,2 ORDER BY 1,2")

display_many(
  "V5. Rows breaking the gap rule (expect none)" = df_v5_1,
  "V5. System label vs status used, with gap range" = df_v5_2,
  "V6. Episode numbers recomputed (expect 0 mismatches)" = df_v6_1,
  "V6. Episodes vs subscriptions per customer (expect episodes_above_subs = 0)" = df_v6_2,
  "V14. Bad expiry dates and months covered (expect max_months_covered <= 12)" = df_v14_1)

# COMMAND ----------

# DBTITLE 1,Validation 3: error flags, non-watchers, revenue
# VALIDATION (V2.6), part 3 of 4: error flags, non-watchers, revenue

# V7. Error flags                                                                    expect: blank_flagged_as_error = 0; error_rate a few percent
df_v7_1 <- q("SELECT COUNT(*) sessions, SUM(has_error) error_sessions, ROUND(AVG(has_error), 4) error_rate,
                     SUM(entitlement_error) entitlement_sessions, SUM(device_limit_error) device_limit_sessions, SUM(drm_error) drm_sessions,
                     SUM(CASE WHEN has_error = 1 AND NULLIF(TRIM(errorlist), '') IS NULL AND COALESCE(startuperror, 0) <> 1 THEN 1 ELSE 0 END) blank_flagged_as_error
              FROM v")

# V8. Non-watchers recounted straight from viewing                                   expect: recount = Chart 4 = Cell 10 for every status
df_v8_1 <- q("
  SELECT c.start_status, COUNT(*) subs, SUM(CASE WHEN COALESCE(w.m, 0) < 1 THEN 1 ELSE 0 END) nonwatch_recount
  FROM cohort c LEFT JOIN (
    SELECT c2.Subscription_ID, SUM(v.minutes) m
    FROM cohort c2 JOIN v ON v.viewerId = c2.Customer_External_ID AND v.vdate BETWEEN c2.start_date AND LEAST(c2.expiry_date, DATE'{ASOF}')
    GROUP BY 1) w USING (Subscription_ID)
  GROUP BY 1") %>% collect()
v8 <- df_v8_1 %>%
  dplyr::left_join(nv %>% dplyr::filter(grp != "All") %>% dplyr::transmute(start_status = c(New = "new", `Win-back` = "winback", `Quick re-subscribe` = "resub")[as.character(grp)], chart4 = never), by = "start_status") %>%
  dplyr::left_join(nwv %>% dplyr::filter(view_group == "1 Non-watcher (<1 min)") %>% dplyr::transmute(start_status, cell10 = subs), by = "start_status") %>%
  mutate(all_match = nonwatch_recount == chart4 & nonwatch_recount == cell10)

# V9. Revenue recomputed from the transaction table (excl. tax, monthly-equivalent)   expect: recount total = Cell 2 Aug-Sep total
df_v9_1 <- q("
  WITH lc AS (SELECT Subscription_ID, Transaction_price_excl_TAX_USD AS p,
                     ROW_NUMBER() OVER (PARTITION BY Subscription_ID ORDER BY Transaction_Date DESC) rn FROM DIM_Transactions_SD)
  SELECT c.start_status, COUNT(*) subs, SUM(CASE WHEN lc.Subscription_ID IS NULL THEN 1 ELSE 0 END) no_transaction,
         ROUND(SUM(lc.p), 0) full_charge_excl_tax, ROUND(SUM(lc.p / c.months_covered), 0) monthly_value_recount
  FROM cohort c LEFT JOIN lc ON lc.Subscription_ID = c.Subscription_ID AND lc.rn = 1 GROUP BY 1") %>% collect()
v9 <- tibble(recount_total = sum(df_v9_1$monthly_value_recount),
             cell2_total = round(sum(rev$monthly_value_forgone_usd[rev$m %in% substr(c(P$FROM, P$TO), 1, 7)], na.rm = TRUE)),
             report_total = round(rv_augsep)) %>% mutate(match = abs(recount_total - cell2_total) <= 3 & abs(report_total - cell2_total) <= 3)
df_v9_2 <- q("SELECT Offer_period, months_covered, COUNT(*) subs, ROUND(AVG(paid_period_days), 0) avg_paid_days FROM cohort GROUP BY 1,2 ORDER BY subs DESC")

display_many(
  "V7. Error flags (expect blank_flagged_as_error = 0)" = df_v7_1,
  "V8. Non-watchers: recount vs Chart 4 vs Cell 10 (expect all_match TRUE)" = v8,
  "V9. Revenue recomputed by status" = df_v9_1,
  "V9. Revenue totals: recount vs Cell 2 vs report (expect match TRUE)" = v9,
  "V9. Months covered by offer period" = df_v9_2)

# COMMAND ----------

# DBTITLE 1,Validation 4: data end, returns, report completeness, trace
# VALIDATION (V2.6), part 4 of 4: data end, returns, peak days, report completeness, 5-customer trace

# V10. Data end date                                                                 expect: ASOF = day before the latest date; max_view_date >= ASOF
df_v10_1 <- q("SELECT '{ASOF}' asof_used, MAX(start_date) max_sub_start, MAX(cancel_date_raw) max_cancel, (SELECT MAX(vdate) FROM v) max_view_date FROM life")

# V11. Return rates                                                                  expect: within_30d = the report's 30-day KPI
df_v11_1 <- q("SELECT start_status, COUNT(*) matured_subs,
                      ROUND(AVG(CASE WHEN days_expiry_to_next_start BETWEEN 0 AND {MATURE_DAYS} THEN 1.0 ELSE 0 END), 3) returned_within_30d,
                      ROUND(AVG(CASE WHEN days_expiry_to_next_start >= 0 THEN 1.0 ELSE 0 END), 3) returned_any_time
               FROM post WHERE matured = 1 GROUP BY 1")

# V12. Peak match days
df_v12_1 <- q("SELECT COUNT(*) peak_days_2026 FROM peak_md WHERE match_date >= '2026-01-01' AND match_date <= '{TO}'")
cat("Share of 2026 days within 2 days of a peak match day:", round(mean(cal$near_peak[cal$d >= "2026-01-01"]), 3), "\n")

# V15 (new). Report completeness: Aug and Sep rows present in every monthly report table, run-rate percentages recomputed
aug_sep_m <- substr(c(P$FROM, P$TO), 1, 7)
v15 <- tibble(table = c("monthly cancellations (t_month)", "revenue (t_rev)", "revenue coverage (rev_cov)", "vs run rate (rr_tbl)"),
              rows = c(nrow(t_month), nrow(t_rev), nrow(rev_cov), nrow(rr_tbl)),
              has_aug_and_sep = c(all(aug_sep_m %in% t_month$month), all(aug_sep_m %in% t_rev$month), all(aug_sep_m %in% rev_cov$m), all(aug_sep_m %in% rr_tbl$m)))
v15_rr <- rr_tbl %>% mutate(recount_vs_run_rate_pct = round(100 * (customers / mean(base$customers) - 1), 1),
                            match = abs(recount_vs_run_rate_pct - vs_run_rate_pct) < 0.11)

# V13. Manual trace: 5 random cohort customers, end to end
trace_ids <- q("SELECT Customer_External_ID FROM cohort ORDER BY rand(42) LIMIT 5") %>% collect() %>% dplyr::pull(1)
ids_sql <- paste0("'", trace_ids, "'", collapse = ",")
df_v13_1 <- q(paste0("SELECT Customer_External_ID, Subscription_ID, seq, start_status_sys, start_status, gap_since_prev_paid_through, episode_no,
                             is_voluntary, bad_expiry, start_date, cancel_date_raw, cancel_date, expiry_date, cancel_type, Offer_period, months_covered,
                             next_start, days_expiry_to_next_start
                      FROM life WHERE Customer_External_ID IN (", ids_sql, ") ORDER BY 1, seq"))
df_v13_2 <- q(paste0("SELECT t.Subscription_ID, t.start_status, t.mou_tier, t.attribution, t.own_minutes, t.any_minutes_in_window,
                             t.minutes_paid_period, t.sessions_cancel_to_expiry, l.last_charge_usd
                      FROM tiers t LEFT JOIN last_charge l USING (Subscription_ID) WHERE t.Customer_External_ID IN (", ids_sql, ")"))

display_many(
  "V10. Data end date and ASOF" = df_v10_1,
  "V11. Return rates: 30-day vs any time" = df_v11_1,
  "V12. Peak match days in 2026" = df_v12_1,
  "V15. Report tables include Aug and Sep (expect TRUE)" = v15,
  "V15. Run-rate percentages recomputed (expect match TRUE)" = v15_rr,
  "V13. Trace: 5 random customers, lifecycle" = df_v13_1,
  "V13. Trace: 5 random customers, viewing and last charge" = df_v13_2)
