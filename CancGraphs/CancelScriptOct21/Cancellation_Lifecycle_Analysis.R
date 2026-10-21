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
  SUFFIX_LEN = "6"                            # viewing.subscriptionid = Subscription_ID + 6-digit suffix [CHECK on full data]
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
display(q("SELECT date_format(c.cancel_date,'yyyy-MM') m, c.start_status, COUNT(*) subs, ROUND(SUM(l.last_charge_usd),0) renewal_value_forgone_usd,
                  ROUND(AVG(l.last_charge_usd),2) avg_last_charge
           FROM life c LEFT JOIN last_charge l USING (Subscription_ID)
           WHERE c.cancel_date >= '{HIST_FROM}' AND c.cancel_date <= '{TO}' GROUP BY 1,2 ORDER BY 1,2"))

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
peak_days <- q("SELECT match_date FROM (SELECT match_date, SUM(viewers) v, percent_rank() OVER (ORDER BY SUM(viewers)) pr FROM fixtures GROUP BY 1) WHERE pr >= 0.75") %>% collect()

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

# Cancellations in the days after each match day (event study): average daily cancels at offsets -3..+7 vs match day
display(q("
  SELECT datediff(c.d, f.match_date) AS offset_days, COUNT(*) / COUNT(DISTINCT f.match_date) AS avg_cancels_per_match_day_offset
  FROM (SELECT cancel_date AS d, COUNT(*) n FROM life WHERE cancel_date >= '2026-01-01' AND cancel_date <= '{TO}' GROUP BY 1) c
  JOIN (SELECT DISTINCT match_date FROM fixtures WHERE match_date >= '2026-01-01') f ON datediff(c.d, f.match_date) BETWEEN -3 AND 7
  GROUP BY 1 ORDER BY 1"))   # note: counts cancel DAYS; weight by n if you want volumes

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
display(q("
  SELECT start_status, COUNT(*) matured_subs,
         SUM(CASE WHEN days_expiry_to_next_start >= 0 THEN 1 ELSE 0 END) returned,
         SUM(CASE WHEN days_expiry_to_next_start BETWEEN 0 AND 7  THEN 1 ELSE 0 END) d0_7,
         SUM(CASE WHEN days_expiry_to_next_start BETWEEN 8 AND 14 THEN 1 ELSE 0 END) d8_14,
         SUM(CASE WHEN days_expiry_to_next_start BETWEEN 15 AND 30 THEN 1 ELSE 0 END) d15_30
  FROM post WHERE matured = 1 GROUP BY 1"))
display(q("SELECT start_status, next_channel, COUNT(*) returns FROM post WHERE days_expiry_to_next_start >= 0 GROUP BY 1,2 ORDER BY returns DESC"))

# Do returns line up with fixtures? Weekly returns vs weekly match-day viewers
display(q("SELECT date_trunc('week', next_start) wk, COUNT(*) returns FROM post WHERE days_expiry_to_next_start >= 0 GROUP BY 1 ORDER BY 1"))
display(q("SELECT date_trunc('week', match_date) wk, SUM(viewers) match_viewers, COUNT(DISTINCT competitionName) competitions FROM fixtures
          WHERE match_date >= '{FROM}' GROUP BY 1 ORDER BY 1"))
# What did returners watch in their first 7 days back?
display(q("
  SELECT p.start_status, v.competitionName, COUNT(DISTINCT p.Customer_External_ID) returners, ROUND(SUM(v.minutes),0) minutes
  FROM post p JOIN v ON v.viewerId = p.Customer_External_ID AND v.vdate BETWEEN p.next_start AND date_add(p.next_start, 7)
  WHERE p.days_expiry_to_next_start >= 0 GROUP BY 1,2 HAVING COUNT(DISTINCT p.Customer_External_ID) >= 20 ORDER BY minutes DESC"))

# ---------------------------------------------------------------------
# CELL 8: lifecycle timeline chart for a handful of customers (ggplot2)
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
