# =====================================================================
# Voluntary cancellation analysis on a START-STATUS basis (sparklyr + dplyr / sdf_sql)
#
# Premise confirmed by the 6-customer sample: every subscription STARTS as New or Win-back and
# ENDS as Churned. The status on the latest row of a cancelled subscription therefore says
# where it is in its lifecycle (still paid-up = new/winback, expired = churned), not what kind
# of customer it was. This script classifies every subscription by the status it STARTED with, so
# Aug, Sep and history are compared like for like.
#
# Paste each CELL into a Databricks R cell, in order. NOT RUN by the author (no Spark access).
# Columns used all appear in the sample extract (checked). Items marked [CHECK] are assumptions.
# =====================================================================

# ---------------------------------------------------------------------
# CELL 0: parameters and helpers
# ---------------------------------------------------------------------
library(sparklyr); library(dplyr); library(ggplot2); library(scales)

P <- list(
  FROM = "2026-08-01", TO = "2026-09-30",     # cancellation window under review
  HIST_FROM = "2025-04-01",                   # history for the run-rate
  ASOF = "2026-10-13",                        # latest date in the data (sample ends 13 Oct 2026)
  MATURE_DAYS = "30",                         # days after expiry before 'returned?' is read
  VIEW_TBL = "EntertainmentMovies_SD",        # viewing table in the sample notebook (has errorlist, match_date, subscriptionid)
  SUFFIX_LEN = "6",                           # viewing.subscriptionid = Subscription_ID + 6-digit suffix [CHECK on full data]
  OUT_DIR = "/dbfs/FileStore/cancellation_charts"   # Cell 9 saves every chart here as PNG (download from /FileStore/...)
)
fill <- function(s, p = P) { for (n in names(p)) s <- gsub(paste0("{", n, "}"), p[[n]], s, fixed = TRUE); s }
q    <- function(sql) sdf_sql(sc, fill(sql))
reg  <- function(sdf, name) { sparklyr::sdf_register(sdf, name); sdf }

# ---------------------------------------------------------------------
# CELL 1: subscription lifecycle table (one row per subscription)
#   start_status = status on the FIRST version (new / winback)   [CHECK: same as StatusRank1]
#   rule_status  = 'new' if the customer's first subscription by start date, else 'winback'
#                  (matched StatusRank1 on 23 of 23 sample rows; use it as a cross-check)
# ---------------------------------------------------------------------
base_rows <- q("
  SELECT Customer_ID, Customer_External_ID, Subscription_ID, Subscription_version, Subscription_latest,
         Subscription_type, Subscription_status, to_date(Subscription_start_date) AS start_date,
         to_date(Expiry_date) AS expiry_date, to_date(Cancellation_date) AS cancel_date,
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
  WITH l AS (SELECT * FROM base_rows WHERE Subscription_latest = 1),
  o AS (
    SELECT l.*,
      ROW_NUMBER() OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS seq,
      MAX(expiry_date) OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID
                             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_paid_through,
      LEAD(start_date) OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS next_start,
      LEAD(Acquisition_channel) OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS next_channel,
      LEAD(Subscription_ID) OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS next_sub_id
    FROM l)
  SELECT o.*, COALESCE(f.start_status, CASE WHEN seq = 1 THEN 'new' ELSE 'winback' END) AS start_status,
         CASE WHEN seq = 1 THEN 'new' ELSE 'winback' END AS rule_status,
         CASE WHEN Subscription_status = 'churned' THEN 'churned' ELSE 'active' END AS end_status,
         datediff(expiry_date, start_date)  AS paid_period_days,
         datediff(cancel_date, start_date)  AS days_start_to_cancel,
         datediff(expiry_date, cancel_date) AS days_cancel_to_expiry,
         datediff(start_date, prev_paid_through) AS gap_since_prev_paid_through,   -- negative = overlap with an earlier subscription
         datediff(next_start, expiry_date)  AS days_expiry_to_next_start,
         CASE WHEN cancel_date IS NULL THEN 'No cancel date'
              WHEN datediff(expiry_date, cancel_date) = 0 THEN 'Cancelled on expiry day'
              WHEN datediff(cancel_date, start_date) <= 1 THEN 'Cancelled on purchase day / next day'
              ELSE 'Cancelled mid-period' END AS cancel_type
  FROM o LEFT JOIN first_v f USING (Subscription_ID)") %>% reg("life")

# Cross-check: how often do the two ways of getting start status agree? (sample: 100%)
display(q("SELECT start_status = rule_status AS agree, COUNT(*) n FROM life GROUP BY 1"))
# Lifecycle summary: start status -> end status
display(q("SELECT start_status, end_status, COUNT(*) subs FROM life GROUP BY 1,2 ORDER BY 1,2"))

# Cohort under review (voluntary cancellations Aug-Sep) -- same filters as before
cohort <- q("SELECT * FROM life WHERE cancel_date >= '{FROM}' AND cancel_date <= '{TO}'") %>% reg("cohort")
cat("Cohort subscriptions:", sdf_nrow(cohort), "\n")

# ---------------------------------------------------------------------
# CELL 2: Q1 what changed. Like-for-like on start status
# ---------------------------------------------------------------------
monthly <- q("
  SELECT date_format(cancel_date,'yyyy-MM') AS m, start_status, COUNT(DISTINCT Customer_External_ID) customers
  FROM life WHERE cancel_date >= '{HIST_FROM}' AND cancel_date <= '{TO}' GROUP BY 1,2") %>% collect()

tot <- monthly %>% group_by(m) %>% summarise(customers = sum(customers), .groups = "drop")
base <- tot %>% filter(m < "2026-08")
cat(sprintf("Run rate Apr25-Jul26: mean %.0f, sd %.0f. Sep 2026: %d (z = %.2f)\n", mean(base$customers), sd(base$customers),
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
  SELECT Subscription_ID, Transaction_price_incl_TAX_USD AS last_charge_usd, Payment_type FROM (
    SELECT Subscription_ID, Transaction_price_incl_TAX_USD, Payment_type,
           ROW_NUMBER() OVER (PARTITION BY Subscription_ID ORDER BY Transaction_Date DESC) rn FROM DIM_Transactions_SD) WHERE rn = 1") %>%
  reg("last_charge")
rev <- q("SELECT date_format(c.cancel_date,'yyyy-MM') m, c.start_status, COUNT(*) subs, ROUND(SUM(l.last_charge_usd),0) renewal_value_forgone_usd,
                  ROUND(AVG(l.last_charge_usd),2) avg_last_charge
           FROM life c LEFT JOIN last_charge l USING (Subscription_ID)
           WHERE c.cancel_date >= '{HIST_FROM}' AND c.cancel_date <= '{TO}' GROUP BY 1,2 ORDER BY 1,2") %>% collect()
display(rev)

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
         CASE WHEN errorlist IS NOT NULL OR startuperror = 1 THEN 1 ELSE 0 END AS has_error,
         CASE WHEN errorlist LIKE '%ntitlement%' THEN 1 ELSE 0 END AS entitlement_error
  FROM {VIEW_TBL}
  WHERE viewerId IN (SELECT DISTINCT Customer_External_ID FROM cohort)") %>% reg("v")
# NB: use playingunixtimems. playingtimemin is NULL on ~18% of sample rows.

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
         COALESCE(SUM(CASE WHEN v.vdate > c.expiry_date AND (c.next_start IS NULL OR v.vdate < c.next_start) THEN v.minutes END),0) AS minutes_after_expiry
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
display(q("SELECT start_status, mou_tier, attribution, COUNT(*) subs, ROUND(AVG(any_minutes_in_window),0) avg_min, ROUND(AVG(any_assets_in_window),1) avg_assets
          FROM tiers GROUP BY 1,2,3 ORDER BY 1,2,3"))

# ---------------------------------------------------------------------
# CELL 4: Q2 non-watchers: never tried, or tried and hit a problem?
# ---------------------------------------------------------------------
nw <- q("SELECT * FROM tiers WHERE own_minutes < 1 AND any_minutes_in_window < 1") %>% reg("nw")

# Session-level evidence for those customers inside their window: zero play with / without an error code, entitlement errors, device
display(q("
  SELECT n.start_status,
         COUNT(DISTINCT n.Subscription_ID) non_watching_subs,
         COUNT(DISTINCT CASE WHEN v.convivasessionid IS NULL THEN n.Subscription_ID END) never_opened_player,
         COUNT(DISTINCT CASE WHEN v.has_error = 1 THEN n.Subscription_ID END) had_error_coded_session,
         COUNT(DISTINCT CASE WHEN v.entitlement_error = 1 THEN n.Subscription_ID END) had_entitlement_error,
         COUNT(DISTINCT CASE WHEN v.convivasessionid IS NOT NULL AND v.has_error = 0 AND v.minutes < 1 THEN n.Subscription_ID END) opened_no_error_no_play
  FROM nw n LEFT JOIN v ON v.viewerId = n.Customer_External_ID AND v.vdate BETWEEN n.start_date AND COALESCE(n.cancel_date, n.expiry_date)
  GROUP BY 1"))
display(q("SELECT regexp_extract(v.errorlist, '^(.{0,40})', 1) error_head, v.deviceos, COUNT(*) sessions
          FROM v WHERE v.has_error = 1 GROUP BY 1,2 ORDER BY sessions DESC"))

# When did they cancel, and did the same customer do this before?
display(q("SELECT start_status, CASE WHEN days_start_to_cancel <= 1 THEN '0-1d' WHEN days_start_to_cancel <= 7 THEN '2-7d'
                 WHEN days_start_to_cancel <= 30 THEN '8-30d' ELSE '30d+' END tenure, cancel_type, COUNT(*) subs FROM nw GROUP BY 1,2,3 ORDER BY 1,2,3"))
display(q("
  SELECT n.start_status, COUNT(DISTINCT n.Customer_External_ID) nonwatch_customers,
         COUNT(DISTINCT p.Customer_External_ID) with_earlier_subs,
         COUNT(DISTINCT CASE WHEN COALESCE(pv.m, 0) < 1 THEN p.Customer_External_ID END) with_an_earlier_unwatched_sub
  FROM nw n
  LEFT JOIN life p ON p.Customer_External_ID = n.Customer_External_ID AND p.start_date < n.start_date
  LEFT JOIN (SELECT sub_id_view, SUM(minutes) m FROM v GROUP BY 1) pv ON pv.sub_id_view = p.Subscription_ID
  GROUP BY 1"))   # v holds all viewing of cohort customers, so earlier subscriptions are covered
display(q("SELECT start_status, Cancellation_reason, churn_reason, Acquisition_channel, COUNT(*) subs FROM nw GROUP BY 1,2,3,4 ORDER BY subs DESC"))
display(q("SELECT Product_name, Country, COUNT(*) subs FROM nw GROUP BY 1,2 HAVING COUNT(*) >= 10 ORDER BY subs DESC"))

# ---------------------------------------------------------------------
# CELL 5: Q2 viewers: once or repeatedly, problems, continued after cancelling, link to fixtures
# ---------------------------------------------------------------------
display(q("
  SELECT start_status,
         SUM(CASE WHEN own_days = 1 THEN 1 ELSE 0 END) one_day_on_this_sub,
         SUM(CASE WHEN own_days BETWEEN 2 AND 4 THEN 1 ELSE 0 END) two_to_four_days,
         SUM(CASE WHEN own_days >= 5 THEN 1 ELSE 0 END) five_plus_days,
         SUM(CASE WHEN sessions_cancel_to_expiry > 0 THEN 1 ELSE 0 END) kept_watching_after_cancel,
         SUM(CASE WHEN days_in_final_week > 0 THEN 1 ELSE 0 END) watched_in_final_week,
         SUM(CASE WHEN minutes_after_expiry > 0 THEN 1 ELSE 0 END) watched_after_expiry
  FROM tiers GROUP BY 1"))
# Playback problems for viewers: error-coded and rebuffering sessions
display(q("SELECT t.start_status, COUNT(DISTINCT t.Subscription_ID) subs, SUM(v.has_error) error_sessions, SUM(v.entitlement_error) entitlement_errors,
                  ROUND(AVG(CASE WHEN v.interrupts > 0 THEN 1.0 ELSE 0 END),3) share_sessions_with_interrupts
          FROM tiers t JOIN v ON v.viewerId = t.Customer_External_ID AND v.vdate BETWEEN t.start_date AND COALESCE(t.cancel_date, t.expiry_date)
          GROUP BY 1"))

# Fixture calendar from the viewing table itself (real match dates, not a proxy)
fixtures <- q("
  SELECT match_date, competitionName, COUNT(DISTINCT viewerId) viewers FROM {VIEW_TBL}
  WHERE ContentType2 = 'Football' AND match_date >= '{HIST_FROM}' GROUP BY 1,2") %>% reg("fixtures")
peak_md <- q("SELECT match_date FROM (SELECT match_date, SUM(viewers) v, percent_rank() OVER (ORDER BY SUM(viewers)) pr FROM fixtures GROUP BY 1) WHERE pr >= 0.75") %>% reg("peak_md")
peak_days <- collect(peak_md)

# Distance from each cancellation / sign-up to the nearest peak match day; compare with the base rate
pk <- as.Date(peak_days$match_date)
cal <- data.frame(d = seq(as.Date("2025-01-01"), as.Date(P$TO), by = "day"))
cal$near_peak <- as.integer(sapply(cal$d, function(x) any(abs(as.numeric(x - pk)) <= 2))); cal$d <- as.character(cal$d)
sdf_copy_to(sc, cal, "peak_cal", overwrite = TRUE)
display(q("SELECT 'sign-up' event, l.start_status, COUNT(*) n, ROUND(AVG(p.near_peak),3) share_within_2d_of_peak
           FROM life l JOIN peak_cal p ON p.d = CAST(l.start_date AS STRING) WHERE l.start_date >= '2026-01-01' AND l.start_date <= '{TO}' GROUP BY 1,2
           UNION ALL
           SELECT 'cancel', l.start_status, COUNT(*), ROUND(AVG(p.near_peak),3)
           FROM life l JOIN peak_cal p ON p.d = CAST(l.cancel_date AS STRING) WHERE l.cancel_date >= '2026-01-01' AND l.cancel_date <= '{TO}' GROUP BY 1,2"))
cat("Base rate (share of 2026 days within 2 days of a peak match day):", round(mean(cal$near_peak[cal$d >= "2026-01-01"]), 3), "\n")

# Cancellations in the days around PEAK match days (event study): average cancellations per peak match day at each offset, by start status
ev_study <- q("
  SELECT c.start_status, datediff(c.d, f.match_date) AS offset_days,
         SUM(c.n) / (SELECT COUNT(*) FROM peak_md WHERE match_date >= '2026-01-01' AND match_date <= date_sub(DATE'{TO}', 7)) AS avg_cancels_per_peak_day
  FROM (SELECT cancel_date AS d, start_status, COUNT(*) n FROM life WHERE cancel_date >= '2026-01-01' AND cancel_date <= '{TO}' GROUP BY 1,2) c
  JOIN (SELECT match_date FROM peak_md WHERE match_date >= '2026-01-01' AND match_date <= date_sub(DATE'{TO}', 7)) f
    ON datediff(c.d, f.match_date) BETWEEN -3 AND 7
  GROUP BY 1,2 ORDER BY 1,2") %>% collect()   # neighbouring peak days overlap, so one cancellation can count under two match days
display(ev_study)

# ---------------------------------------------------------------------
# CELL 6: Q2 repeat cycling and duplicate accounts
# ---------------------------------------------------------------------
display(q("
  SELECT CASE WHEN n_subs >= 6 THEN '6+' WHEN n_subs >= 4 THEN '4-5' ELSE CAST(n_subs AS STRING) END bucket, COUNT(*) customers,
         ROUND(AVG(avg_gap),1) avg_gap_days, ROUND(AVG(one_day_subs),2) avg_one_day_subs, ROUND(AVG(overlaps),2) avg_overlapping_subs
  FROM (SELECT Customer_External_ID, COUNT(*) n_subs, AVG(CASE WHEN seq > 1 THEN gap_since_prev_paid_through END) avg_gap,
               SUM(CASE WHEN paid_period_days <= 2 THEN 1 ELSE 0 END) one_day_subs, SUM(CASE WHEN gap_since_prev_paid_through < 0 THEN 1 ELSE 0 END) overlaps
        FROM life WHERE Customer_External_ID IN (SELECT Customer_External_ID FROM cohort WHERE start_status = 'winback') GROUP BY 1)
  GROUP BY 1 ORDER BY 1"))
# Short chained sign-ups: 3+ subscriptions started within 7 days (sample: C2 bought on 23, 24 and 25 Aug)
display(q("
  SELECT start_status, COUNT(DISTINCT Customer_External_ID) customers FROM (
    SELECT Customer_External_ID, start_status,
           COUNT(*) OVER (PARTITION BY Customer_External_ID ORDER BY day_no RANGE BETWEEN CURRENT ROW AND 7 FOLLOWING) n7
    FROM (SELECT *, datediff(start_date, DATE'2020-01-01') AS day_no FROM life WHERE start_date >= '{FROM}' AND start_date <= '{TO}')) WHERE n7 >= 3 GROUP BY 1"))
# Same customer, several external ids (duplicate accounts); and 'new' subscribers who already have an earlier subscription elsewhere
display(q("SELECT n_ext, COUNT(*) customer_ids FROM (SELECT Customer_ID, COUNT(DISTINCT Customer_External_ID) n_ext FROM life GROUP BY 1) GROUP BY 1 ORDER BY 1"))
display(q("
  SELECT date_format(c.cancel_date,'yyyy-MM') m, COUNT(DISTINCT c.Customer_External_ID) new_cancellers,
         COUNT(DISTINCT CASE WHEN e.Customer_ID IS NOT NULL THEN c.Customer_External_ID END) earlier_sub_on_other_account
  FROM cohort c LEFT JOIN life e ON e.Customer_ID = c.Customer_ID AND e.Customer_External_ID <> c.Customer_External_ID AND e.start_date < c.start_date
  WHERE c.start_status = 'new' GROUP BY 1 ORDER BY 1"))
# Parallel paid subscriptions: customers who start a Win-back while still paid-up on another subscription
display(q("SELECT start_status, COUNT(*) subs, SUM(CASE WHEN gap_since_prev_paid_through < 0 THEN 1 ELSE 0 END) started_while_still_paid_up
          FROM cohort GROUP BY 1"))

# ---------------------------------------------------------------------
# CELL 7: Q3 what happened after cancellation
# ---------------------------------------------------------------------
# Reversals: a later version of the same subscription has the cancellation removed   [CHECK on a few known ids]
reversed <- q("
  SELECT DISTINCT a.Subscription_ID FROM base_rows a JOIN base_rows b ON a.Subscription_ID = b.Subscription_ID
  WHERE a.cancel_date IS NOT NULL AND b.Subscription_version > a.Subscription_version AND b.cancel_date IS NULL") %>% reg("reversed")
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
display(q("SELECT start_status, COUNT(*) subs, SUM(reached_expiry) reached_expiry, SUM(cancel_reversed) reversed, SUM(charged_after_cancel) charged_after_cancel,
                  SUM(CASE WHEN next_start <= expiry_date THEN 1 ELSE 0 END) new_sub_before_expiry FROM post GROUP BY 1"))

# After expiry: how many returned, how soon, through which channel (matured subscriptions only)
ret <- q("
  SELECT start_status, COUNT(*) matured_subs,
         SUM(CASE WHEN days_expiry_to_next_start >= 0 THEN 1 ELSE 0 END) returned,
         SUM(CASE WHEN days_expiry_to_next_start BETWEEN 0 AND 7  THEN 1 ELSE 0 END) d0_7,
         SUM(CASE WHEN days_expiry_to_next_start BETWEEN 8 AND 14 THEN 1 ELSE 0 END) d8_14,
         SUM(CASE WHEN days_expiry_to_next_start BETWEEN 15 AND 30 THEN 1 ELSE 0 END) d15_30
  FROM post WHERE matured = 1 GROUP BY 1") %>% collect()
display(ret)
display(q("SELECT start_status, next_channel, COUNT(*) returns FROM post WHERE days_expiry_to_next_start >= 0 GROUP BY 1,2 ORDER BY returns DESC"))

# Do returns line up with fixtures? Weekly returns vs weekly match-day viewers
ret_wk <- q("SELECT date_trunc('week', next_start) wk, start_status, COUNT(*) returns FROM post WHERE days_expiry_to_next_start >= 0 GROUP BY 1,2 ORDER BY 1,2") %>% collect()
fx_wk  <- q("SELECT date_trunc('week', match_date) wk, SUM(viewers) match_viewers, COUNT(DISTINCT competitionName) competitions FROM fixtures
          WHERE match_date >= '{FROM}' GROUP BY 1 ORDER BY 1") %>% collect()
display(ret_wk); display(fx_wk)
# What did returners watch in their first 7 days back?
display(q("
  SELECT p.start_status, v.competitionName, COUNT(DISTINCT p.Customer_External_ID) returners, ROUND(SUM(v.minutes),0) minutes
  FROM post p JOIN v ON v.viewerId = p.Customer_External_ID AND v.vdate BETWEEN p.next_start AND date_add(p.next_start, 7)
  WHERE p.days_expiry_to_next_start >= 0 GROUP BY 1,2 HAVING COUNT(DISTINCT p.Customer_External_ID) >= 20 ORDER BY minutes DESC"))

# ---------------------------------------------------------------------
# CELL 8: lifecycle timeline chart for a handful of customers (ggplot2). Cell 9 follows with the management charts.
# ---------------------------------------------------------------------
ids <- c("1668440c-cd8f-4885-b433-db46c5807c9b", "50e81771-a410-41a3-980e-fda2c54240fe")   # replace with any customers
tl <- q(sprintf("SELECT Customer_External_ID, Subscription_ID, start_status, start_date, expiry_date, cancel_date FROM life
                  WHERE Customer_External_ID IN (%s) AND expiry_date >= '2026-07-01'", paste0("'", ids, "'", collapse = ","))) %>% collect() %>%
  mutate(across(c(start_date, expiry_date, cancel_date), as.Date), cust = substr(Customer_External_ID, 1, 8)) %>%
  group_by(cust) %>% arrange(start_date) %>% mutate(lane = row_number()) %>% ungroup()
ggplot(tl) +
  geom_segment(aes(x = start_date, xend = coalesce(cancel_date, expiry_date), y = lane, yend = lane, colour = start_status), linewidth = 4) +
  geom_segment(data = filter(tl, !is.na(cancel_date)), aes(x = cancel_date, xend = expiry_date, y = lane, yend = lane, colour = start_status), linewidth = 4, alpha = .35) +
  geom_point(data = filter(tl, !is.na(cancel_date)), aes(cancel_date, lane), shape = 4, size = 3, stroke = 1.2) +
  facet_grid(cust ~ ., scales = "free_y", space = "free_y") +
  scale_colour_manual(values = c(new = "#2a78d6", winback = "#eb6834"), name = "Started as") +
  labs(title = "Subscription lifecycles: start status, cancellation (x) and paid access left (faded)", x = NULL, y = NULL) +
  theme_minimal(base_size = 12) + theme(axis.text.y = element_blank(), panel.grid.minor = element_blank(), legend.position = "top")

# ---------------------------------------------------------------------
# CELL 9: management charts (ggplot2). Every chart is built from the tables above and uses the
#         START-status split (New = blue, Win-back = orange). PNGs are saved to P$OUT_DIR.
#         Run Cells 0-7 first (this cell needs: life, cohort, tiers, nw, post, fixtures, peak_md,
#         monthly, base, rev, ev_study, ret, ret_wk, fx_wk).
# ---------------------------------------------------------------------
BLUE <- "#2a78d6"; ORANGE <- "#eb6834"; GREY <- "#b8b7b0"; INK <- "#0b0b0b"; INK2 <- "#52514e"; GRID <- "#e6e5e1"; AREA <- "#c9c8c2"
COL <- c("New" = BLUE, "Win-back" = ORANGE)
lab_status <- function(x) factor(ifelse(x == "new", "New", "Win-back"), levels = c("New", "Win-back"))
theme_cx <- function() theme_minimal(base_size = 12) + theme(
  panel.grid.minor = element_blank(), panel.grid.major.x = element_blank(), panel.grid.major.y = element_line(colour = GRID),
  plot.title = element_text(face = "bold", colour = INK), plot.title.position = "plot", plot.subtitle = element_text(colour = INK2),
  axis.text = element_text(colour = INK2), axis.title = element_text(colour = INK2), legend.position = "top", legend.justification = "left")
dir.create(P$OUT_DIR, showWarnings = FALSE, recursive = TRUE)
out_plot <- function(p, name, w = 11, h = 6) {
  print(p); try(ggsave(file.path(P$OUT_DIR, paste0(name, ".png")), p, width = w, height = h, dpi = 200), silent = TRUE); invisible(p)
}

# ---- Chart 1. Q1: monthly cancelling customers vs run rate, split by start status ----------------
hist_avg <- mean(base$customers)
m_df <- monthly %>% mutate(status = lab_status(start_status), period = ifelse(m >= "2026-08", "Aug-Sep 2026", "History"))
c1 <- ggplot(m_df, aes(m, customers, fill = status, alpha = period)) +
  geom_col(width = 0.75) +
  geom_hline(yintercept = hist_avg, linetype = "dashed", colour = INK) +
  annotate("text", x = 1, y = hist_avg, label = sprintf("Apr 25-Jul 26 average %s", comma(round(hist_avg))), hjust = 0, vjust = -0.6, size = 3.3, colour = INK) +
  scale_fill_manual(values = COL, name = "Started as") +
  scale_alpha_manual(values = c("History" = 0.45, "Aug-Sep 2026" = 1), guide = "none") +
  scale_y_continuous(labels = comma, expand = expansion(mult = c(0, 0.08))) +
  labs(title = "Monthly cancelling customers, by the status they started with",
       subtitle = "Like for like: every cancelling subscription is New or Win-back by its first status. Aug-Sep 2026 at full colour.", x = NULL, y = "Customers") +
  theme_cx() + theme(axis.text.x = element_text(angle = 45, hjust = 1))
out_plot(c1, "01_monthly_cancellations_by_start_status")

# ---- Chart 1b. Q1: renewal value forgone (last charge on each cancelled subscription) -----------
rv <- rev %>% mutate(status = lab_status(start_status), period = ifelse(m >= "2026-08", "Aug-Sep 2026", "History"))
c1b <- ggplot(rv, aes(m, renewal_value_forgone_usd, fill = status, alpha = period)) +
  geom_col(width = 0.75) +
  scale_fill_manual(values = COL, name = "Started as") +
  scale_alpha_manual(values = c("History" = 0.45, "Aug-Sep 2026" = 1), guide = "none") +
  scale_y_continuous(labels = label_dollar(), expand = expansion(mult = c(0, 0.08))) +
  labs(title = "Renewal value that will not be billed, by month of cancellation",
       subtitle = "Last charge on each cancelled subscription (USD, incl. tax) from the Transaction table. An estimate, not realised loss.", x = NULL, y = "USD") +
  theme_cx() + theme(axis.text.x = element_text(angle = 45, hjust = 1))
out_plot(c1b, "02_renewal_value_forgone")

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
ev_first <- ev_first %>% mutate(date = as.Date(date), panel = factor(P1, lv), row = row_number())
c2 <- ggplot() +
  geom_rect(data = bars, aes(xmin = x0, xmax = x1, ymin = ymin, ymax = ymax, fill = status), colour = "white", linewidth = 0.3) +
  geom_line(data = shr, aes(mid, value), linewidth = 0.9, colour = INK) +
  geom_point(data = shr, aes(mid, value), size = 2.4, colour = INK) +
  geom_text(data = shr, aes(mid, value, label = sprintf("%.0f%%", value)), vjust = -0.9, size = 3.2, colour = INK) +
  geom_rect(data = vw, aes(xmin = x0, xmax = x1, ymin = 0, ymax = value), fill = AREA) +
  geom_blank(data = head_df, aes(mid, value)) +
  geom_vline(data = ev_first %>% select(-panel), aes(xintercept = date), linetype = "dashed", colour = INK2, linewidth = 0.4) +
  geom_text(data = ev_first, aes(date, Inf, label = label, vjust = 1.4 + 1.5 * (row - 1)), hjust = -0.05, size = 3.1, colour = INK2) +
  facet_grid(panel ~ ., scales = "free_y", switch = "y") +
  scale_fill_manual(values = COL, name = "Started as") +
  scale_x_date(date_breaks = "1 week", date_labels = "%d %b", expand = expansion(mult = 0.01)) +
  scale_y_continuous(labels = comma, expand = expansion(mult = c(0.02, 0.12))) +
  labs(title = "Cancellations and the football calendar",
       subtitle = "Weekly totals divided by days in the week. Dashed lines = first match date of a competition that starts in the window (from the viewing table).\nTiming alone does not prove football caused the cancellations.", x = NULL, y = NULL) +
  theme_cx() + theme(strip.placement = "outside", strip.text.y.left = element_text(angle = 0, hjust = 1, face = "bold", colour = INK))
out_plot(c2, "03_cancellations_vs_football_calendar", 11, 8)

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

# ---- Chart 4. Q1/Q2: cancellers who never watched (no minute on this subscription or any other) ----
nv <- q("SELECT start_status, COUNT(*) subs, SUM(CASE WHEN own_minutes < 1 AND any_minutes_in_window < 1 THEN 1 ELSE 0 END) never FROM tiers GROUP BY 1") %>% collect()
nv <- bind_rows(mutate(nv, grp = as.character(lab_status(start_status))), nv %>% summarise(subs = sum(subs), never = sum(never)) %>% mutate(grp = "All")) %>%
  mutate(pct = 100 * never / subs, grp = factor(grp, levels = c("All", "Win-back", "New")))
c4 <- ggplot(nv, aes(pct, grp, colour = grp)) +
  geom_segment(aes(x = 0, xend = pct, yend = grp), colour = GRID, linewidth = 1.4) + geom_point(size = 5) +
  geom_text(aes(label = sprintf("%.1f%%  (%s subscriptions)", pct, comma(never))), hjust = -0.15, colour = INK, size = 3.6) +
  scale_colour_manual(values = c("New" = BLUE, "Win-back" = ORANGE, "All" = INK), guide = "none") +
  scale_x_continuous(expand = expansion(mult = c(0, 0.45)), labels = function(x) paste0(x, "%")) +
  labs(title = "Cancellers who never watched", subtitle = "No viewing minute in the subscription window on this subscription or any other subscription / pass.", x = NULL, y = NULL) +
  theme_cx() + theme(panel.grid.major.y = element_blank())
out_plot(c4, "05_never_watched_share", 10, 4)

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

# ---- Chart 6. Q2: non-watchers by product and country --------------------------------------------
hm <- q("SELECT start_status, Product_name, Country, COUNT(*) subs FROM nw GROUP BY 1,2,3") %>% collect()
topc <- hm %>% group_by(Country) %>% summarise(n = sum(subs), .groups = "drop") %>% arrange(desc(n)) %>% slice_head(n = 7) %>% pull(Country)
hm <- hm %>% mutate(Country = ifelse(Country %in% topc, Country, "Others"), status = lab_status(start_status)) %>%
  group_by(status, Product_name, Country) %>% summarise(subs = sum(subs), .groups = "drop")
c6 <- ggplot(hm, aes(factor(Country, levels = c(topc, "Others")), Product_name, fill = subs)) +
  geom_tile(colour = "white") + geom_text(aes(label = comma(subs), colour = subs > max(subs) * 0.55), size = 3.4) +
  scale_colour_manual(values = c("TRUE" = "white", "FALSE" = INK), guide = "none") +
  scale_fill_gradient(low = "#eef4fc", high = "#1f5fae", name = "Non-watching\nsubscriptions") +
  facet_wrap(~ status, nrow = 1) +
  labs(title = "Where the non-watching cancellers are: product x country", x = NULL, y = NULL) +
  theme_cx() + theme(panel.grid = element_blank(), legend.position = "right", strip.text = element_text(face = "bold", colour = INK))
out_plot(c6, "07_non_watchers_product_country", 12, 5)

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

# ---- Chart 8. Q2: how many cycles do cancelling win-backs have behind them? ---------------------
cy <- q("SELECT CASE WHEN n_subs >= 6 THEN '6+' WHEN n_subs >= 4 THEN '4-5' ELSE CAST(n_subs AS STRING) END AS bucket, COUNT(*) customers
         FROM (SELECT Customer_External_ID, COUNT(*) n_subs FROM life
               WHERE Customer_External_ID IN (SELECT Customer_External_ID FROM cohort WHERE start_status = 'winback') GROUP BY 1) GROUP BY 1") %>% collect() %>%
  mutate(share = customers / sum(customers), lbl = ifelse(bucket == "1", "1*", bucket))
c8 <- ggplot(cy, aes(factor(lbl, levels = c("1*", "2", "3", "4-5", "6+")), customers)) +
  geom_col(fill = ORANGE, width = 0.65) +
  geom_text(aes(label = sprintf("%s\n%s", comma(customers), percent(share, accuracy = 1))), vjust = -0.3, size = 3.5, fontface = "bold") +
  scale_y_continuous(expand = expansion(mult = c(0, 0.18)), labels = comma) +
  labs(title = "Cancelling win-backs: how many cycles?", x = "Lifetime D2C subscriptions per customer", y = "Customers",
       caption = "* only one subscription in this extract: the earlier one is outside the D2C filter or the data window.") +
  theme_cx()
out_plot(c8, "09_winback_cycles", 9, 5.5)

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

# ---- Chart 10. Q2: event study, cancellations around peak match days ----------------------------
c10 <- ggplot(ev_study %>% mutate(status = lab_status(start_status)), aes(offset_days, avg_cancels_per_peak_day, colour = status)) +
  geom_vline(xintercept = 0, linetype = "dashed", colour = INK2) +
  geom_line(linewidth = 0.9) + geom_point(size = 2) +
  scale_colour_manual(values = COL, name = "Started as") + scale_x_continuous(breaks = -3:7) + scale_y_continuous(labels = comma, limits = c(0, NA)) +
  labs(title = "Cancellations around peak match days", subtitle = "Average cancelling subscriptions per peak match day (top quartile by viewers), by days from the match. Overlapping match days count twice.",
       x = "Days relative to match day (0 = match day)", y = "Cancellations per match day") +
  theme_cx()
out_plot(c10, "11_event_study_peak_match_days", 10, 5.5)

# ---- Chart 11. Q3: what happened between cancelling and expiry? ---------------------------------
bf <- q("SELECT p.start_status, COUNT(*) subs, SUM(p.reached_expiry) reached_expiry, SUM(p.cancel_reversed) reversed, SUM(p.charged_after_cancel) charged_again,
                SUM(CASE WHEN p.next_start <= p.expiry_date THEN 1 ELSE 0 END) new_sub_before_expiry,
                SUM(CASE WHEN t.sessions_cancel_to_expiry > 0 THEN 1 ELSE 0 END) kept_watching
         FROM post p JOIN tiers t ON t.Subscription_ID = p.Subscription_ID GROUP BY 1") %>% collect() %>%
  tidyr::pivot_longer(c(reached_expiry, reversed, charged_again, new_sub_before_expiry, kept_watching), names_to = "outcome", values_to = "n") %>%
  mutate(share = n / subs, status = lab_status(start_status),
         outcome = factor(outcome, levels = c("kept_watching", "reversed", "charged_again", "new_sub_before_expiry", "reached_expiry"),
                          labels = c("Kept watching after cancelling", "Cancellation reversed", "Charged again after cancel", "Started a new subscription before expiry", "Reached paid expiry")))
c11 <- ggplot(bf, aes(share, factor(outcome, levels = rev(levels(outcome))), fill = status)) +
  geom_col(position = position_dodge(width = 0.75), width = 0.65) +
  geom_text(aes(label = percent(share, accuracy = 0.1)), position = position_dodge(width = 0.75), hjust = -0.15, size = 3.2, colour = INK) +
  scale_fill_manual(values = COL, name = "Started as") + scale_x_continuous(labels = percent, expand = expansion(mult = c(0, 0.15))) +
  labs(title = "Between cancelling and paid expiry", subtitle = "Share of cancelling subscriptions. Reversal and 'charged again' need the version history and recurring transactions [CHECK on full data].", x = NULL, y = NULL) +
  theme_cx() + theme(panel.grid.major.y = element_blank(), panel.grid.major.x = element_line(colour = GRID))
out_plot(c11, "12_before_expiry_outcomes", 11, 5)

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
