# =====================================================================
# Voluntary cancellations Aug-Sep 2026: deep-dive (sparklyr + dplyr / sdf_sql)
# Paste each "CELL" into its own Databricks R cell and run top to bottom.
# Written to extend the existing notebook (uses its `sc` connection and the
# DIM_SubscribersInfo_SDF / SportsEntertainmentMovies_SD / DIM_Transactions_SD views).
#
# NOT RUN by the author (no Spark here). Column names marked [ASSUMED] are not
# visible in the original notebook: check them with CELL 0 and edit the PARAMS.
# =====================================================================

# ---------------------------------------------------------------------
# CELL 0: setup, parameters, helpers, schema check
# ---------------------------------------------------------------------
library(sparklyr); library(dplyr)

P <- list(
  FROM       = "2026-08-01",   # cancellation window start
  TO         = "2026-09-30",   # cancellation window end
  HIST_FROM  = "2025-04-01",   # history for run-rate comparison
  ASOF       = "2026-10-12",   # data snapshot date (used to decide which windows have matured)
  MATURE_DAYS= "30",           # days after expiry needed before "returned?" is measured
  ASSET_COL  = "assetid",      # [ASSUMED] asset / content id column in SportsEntertainmentMovies_SD
  TXN_DATE   = "Transaction_date",              # [ASSUMED] date column in DIM_Transactions_SD
  TXN_AMT    = "Transaction_price_incl_TAX_USD" # [ASSUMED] amount column in DIM_Transactions_SD
)
fill <- function(s, p = P) { for (n in names(p)) s <- gsub(paste0("{", n, "}"), p[[n]], s, fixed = TRUE); s }
q    <- function(sql) sdf_sql(sc, fill(sql))
reg  <- function(sdf, name) { sparklyr::sdf_register(sdf, name); sdf }
show <- function(sdf, n = 200) display(sdf %>% head(n))   # Databricks display()

# Check the columns I had to assume. Edit P above if names differ.
display(q("DESCRIBE DIM_Transactions_SD"))
display(q("DESCRIBE SportsEntertainmentMovies_SD"))
display(q("DESCRIBE DIM_SubscribersInfo_SDF"))

# ---------------------------------------------------------------------
# CELL 1: like-for-like cohort (classify by position in the customer's own history,
# NOT by current status, so Aug and Sep and history are comparable)
# ---------------------------------------------------------------------
subs_all <- q("
  WITH s AS (
    SELECT Customer_ID, Customer_External_ID, Subscription_ID,
           to_date(Subscription_start_date) AS start_date,
           to_date(Expiry_date)             AS expiry_date,
           to_date(Cancellation_date)       AS cancel_date,
           Cancellation_reason, Subscription_type AS status_now, Subscription_status,
           Offer_period, Product_name, Country, Tier, Payment_method, Acquisition_channel,
           Campaign_name, Coupon_code, promo
    FROM DIM_SubscribersInfo_SDF
    WHERE D2C_B2B = 'D2C' AND offer_type = 'Subscription' AND subscription_latest = 1
      AND product_type IS NULL AND non_commercial <> 1 AND daily_weekly_flag <> 1
      AND Customer_External_ID IS NOT NULL
      AND TRIM(Customer_External_ID) <> '' AND TRIM(Customer_External_ID) <> 'null'
  )
  SELECT s.*,
    ROW_NUMBER() OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS sub_seq,
    LAG(expiry_date) OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS prev_expiry,
    LAG(cancel_date) OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS prev_cancel,
    LAG(start_date)  OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS prev_start,
    LEAD(start_date) OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS next_start,
    LEAD(Acquisition_channel) OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS next_channel,
    LEAD(Subscription_ID) OVER (PARTITION BY Customer_External_ID ORDER BY start_date, Subscription_ID) AS next_sub_id
  FROM s") %>% reg("subs_all")

cohort <- q("
  SELECT *,
    CASE WHEN sub_seq = 1 THEN 'New' ELSE 'Win-back' END AS signup_type,
    date_format(cancel_date, 'yyyy-MM')       AS cancel_month,
    datediff(cancel_date, start_date)         AS days_start_to_cancel,
    datediff(expiry_date, cancel_date)        AS days_cancel_to_expiry,
    datediff(start_date, prev_expiry)         AS days_gap_since_prev_expiry
  FROM subs_all
  WHERE cancel_date >= '{FROM}' AND cancel_date <= '{TO}'") %>% reg("cohort")

cat("Cohort subscriptions:", sdf_nrow(cohort), "\n")
# Reconcile with the notebook: expect 41,383 subscriptions
display(q("SELECT signup_type, status_now, cancel_month, COUNT(DISTINCT Customer_External_ID) customers,
                  COUNT(DISTINCT Subscription_ID) subs
           FROM cohort GROUP BY 1,2,3 ORDER BY 3,1,2"))

# ---------------------------------------------------------------------
# CELL 2: Q1 run rate. Like-for-like monthly cancellations vs history
# ---------------------------------------------------------------------
hist <- q("
  SELECT date_format(cancel_date,'yyyy-MM') AS m,
         CASE WHEN sub_seq = 1 THEN 'New' ELSE 'Win-back' END AS signup_type,
         COUNT(DISTINCT Customer_External_ID) AS customers
  FROM subs_all
  WHERE cancel_date >= '{HIST_FROM}' AND cancel_date <= '{TO}'
  GROUP BY 1,2") %>% collect()

tot <- hist %>% group_by(m) %>% summarise(customers = sum(customers), .groups = "drop") %>% arrange(m)
base <- tot %>% filter(m < "2026-08")
cat(sprintf("Run rate (Apr25-Jul26): mean %.0f  sd %.0f\n", mean(base$customers), sd(base$customers)))
tot <- tot %>% mutate(z_vs_history = round((customers - mean(base$customers)) / sd(base$customers), 2))
display(tot)
display(hist %>% tidyr::pivot_wider(names_from = signup_type, values_from = customers, values_fill = 0) %>% arrange(m))

# Speed of cancellation (how soon after sign-up), by signup type and month
display(q("
  SELECT cancel_month, signup_type,
    CASE WHEN days_start_to_cancel <= 3 THEN '0-3d' WHEN days_start_to_cancel <= 7 THEN '4-7d'
         WHEN days_start_to_cancel <= 14 THEN '8-14d' WHEN days_start_to_cancel <= 30 THEN '15-30d'
         WHEN days_start_to_cancel <= 90 THEN '31-90d' ELSE '90d+' END AS tenure_at_cancel,
    COUNT(DISTINCT Customer_External_ID) customers
  FROM cohort GROUP BY 1,2,3 ORDER BY 1,2,3"))

# Same speed measure across the full history (is "cancel within a week" new?)
display(q("
  SELECT date_format(cancel_date,'yyyy-MM') m,
    CASE WHEN sub_seq = 1 THEN 'New' ELSE 'Win-back' END AS signup_type,
    COUNT(*) subs,
    ROUND(AVG(CASE WHEN datediff(cancel_date, start_date) <= 7 THEN 1.0 ELSE 0 END),3) AS share_cancel_within_7d,
    percentile_approx(datediff(cancel_date, start_date), 0.5) AS median_days_to_cancel
  FROM subs_all WHERE cancel_date >= '{HIST_FROM}' AND cancel_date <= '{TO}'
  GROUP BY 1,2 ORDER BY 1,2"))

# Acquisition side: new / win-back starts per week (is sign-up volume up, or is propensity to cancel up?)
display(q("
  SELECT date_trunc('week', start_date) AS wk,
    CASE WHEN sub_seq = 1 THEN 'New' ELSE 'Win-back' END AS signup_type,
    COUNT(DISTINCT Customer_External_ID) starts
  FROM subs_all
  WHERE start_date >= '2026-01-01' AND start_date <= '{TO}' AND (promo IS NULL OR promo <> 1)
  GROUP BY 1,2 ORDER BY 1,2"))

# Cohort survival: of subs started in week W, what % cancelled within 7 / 14 / 30 days?
display(q("
  SELECT date_trunc('week', start_date) AS start_wk,
    CASE WHEN sub_seq = 1 THEN 'New' ELSE 'Win-back' END AS signup_type,
    COUNT(*) starts,
    ROUND(AVG(CASE WHEN cancel_date IS NOT NULL AND datediff(cancel_date,start_date) <= 7  THEN 1.0 ELSE 0 END),3) AS cancel_7d,
    ROUND(AVG(CASE WHEN cancel_date IS NOT NULL AND datediff(cancel_date,start_date) <= 14 THEN 1.0 ELSE 0 END),3) AS cancel_14d,
    ROUND(AVG(CASE WHEN cancel_date IS NOT NULL AND datediff(cancel_date,start_date) <= 30 THEN 1.0 ELSE 0 END),3) AS cancel_30d
  FROM subs_all
  WHERE start_date >= '2026-05-01' AND start_date <= date_sub('{ASOF}', 30)
  GROUP BY 1,2 ORDER BY 1,2"))

# ---------------------------------------------------------------------
# CELL 3: revenue (needs DIM_Transactions_SD; check column names in CELL 0)
# The price on DIM_SubscribersInfo is 0.00 for cancelled rows, so use transactions.
# ---------------------------------------------------------------------
txn_sub <- q("
  SELECT Subscription_ID,
         COUNT(*)                         AS n_charges,
         SUM({TXN_AMT})                   AS paid_to_date_usd,
         MAX(to_date({TXN_DATE}))         AS last_charge_date
  FROM DIM_Transactions_SD
  GROUP BY Subscription_ID") %>% reg("txn_sub")

last_charge <- q("
  SELECT Subscription_ID, {TXN_AMT} AS last_charge_usd FROM (
    SELECT Subscription_ID, {TXN_AMT},
           ROW_NUMBER() OVER (PARTITION BY Subscription_ID ORDER BY {TXN_DATE} DESC) rn
    FROM DIM_Transactions_SD) WHERE rn = 1") %>% reg("last_charge")

cohort_rev <- q("
  SELECT c.*, t.n_charges, t.paid_to_date_usd, t.last_charge_date, l.last_charge_usd
  FROM cohort c LEFT JOIN txn_sub t USING (Subscription_ID) LEFT JOIN last_charge l USING (Subscription_ID)") %>%
  reg("cohort_rev")

# Revenue at risk = value of the next renewal that will not be billed (last charge as proxy)
display(q("
  SELECT cancel_month, signup_type, COUNT(*) subs,
         ROUND(SUM(last_charge_usd),0) AS renewal_value_forgone_usd,
         ROUND(AVG(last_charge_usd),2) AS avg_last_charge,
         ROUND(AVG(paid_to_date_usd),2) AS avg_paid_to_date
  FROM cohort_rev GROUP BY 1,2 ORDER BY 1,2"))

# Run-rate comparison: same measure for every historical month
display(q("
  SELECT date_format(s.cancel_date,'yyyy-MM') m,
         CASE WHEN s.sub_seq = 1 THEN 'New' ELSE 'Win-back' END AS signup_type,
         COUNT(*) subs, ROUND(SUM(l.last_charge_usd),0) AS renewal_value_forgone_usd
  FROM subs_all s LEFT JOIN last_charge l USING (Subscription_ID)
  WHERE s.cancel_date >= '{HIST_FROM}' AND s.cancel_date <= '{TO}'
  GROUP BY 1,2 ORDER BY 1,2"))

# ---------------------------------------------------------------------
# CELL 4: viewing base, restricted to cohort customers, NO 1-minute filter
# (so we can separate "never tried" from "tried but < 1 min")
# ---------------------------------------------------------------------
v_cohort <- q("
  SELECT v.viewerid, v.convivasessionid, to_date(v.startdate) AS vdate,
         v.playingunixtimems / 60000.0 AS minutes, v.{ASSET_COL} AS asset_id,
         v.competitionName, v.ContentType2, v.sports, v.Live
  FROM SportsEntertainmentMovies_SD v
  WHERE v.viewerid IN (SELECT DISTINCT Customer_External_ID FROM cohort)") %>% reg("v_cohort")

# Per-subscription viewing in three windows: during subscription / cancel->expiry / after expiry.
# Several definitions of 'viewing' are produced side by side (MOU, sessions, days, assets, depth).
sub_view <- q("
  SELECT c.Subscription_ID, c.Customer_External_ID, c.signup_type, c.status_now, c.cancel_month,
         c.Product_name, c.Country, c.Acquisition_channel, c.Cancellation_reason,
         c.start_date, c.cancel_date, c.expiry_date, c.days_start_to_cancel,
         -- window 1: from sign-up to cancel date
         COALESCE(SUM(CASE WHEN v.vdate BETWEEN c.start_date AND c.cancel_date THEN v.minutes END),0)                       AS mou_during,
         COUNT(DISTINCT CASE WHEN v.vdate BETWEEN c.start_date AND c.cancel_date THEN v.convivasessionid END)               AS sessions_during,
         COUNT(DISTINCT CASE WHEN v.vdate BETWEEN c.start_date AND c.cancel_date THEN v.vdate END)                          AS days_during,
         COUNT(DISTINCT CASE WHEN v.vdate BETWEEN c.start_date AND c.cancel_date THEN v.asset_id END)                       AS assets_during,
         COUNT(DISTINCT CASE WHEN v.vdate BETWEEN c.start_date AND c.cancel_date AND v.minutes >= 1 THEN v.convivasessionid END) AS sessions_ge1_during,
         COALESCE(SUM(CASE WHEN v.vdate BETWEEN c.start_date AND c.cancel_date AND v.ContentType2='Football' THEN v.minutes END),0) AS football_mou_during,
         COUNT(DISTINCT CASE WHEN v.vdate BETWEEN c.start_date AND c.cancel_date AND v.Live = 1 THEN v.convivasessionid END) AS live_sessions_during,
         -- window 2: after cancel, before expiry (paid access still running)
         COALESCE(SUM(CASE WHEN v.vdate > c.cancel_date AND v.vdate <= c.expiry_date THEN v.minutes END),0)                  AS mou_cancel_to_expiry,
         COUNT(DISTINCT CASE WHEN v.vdate > c.cancel_date AND v.vdate <= c.expiry_date THEN v.vdate END)                      AS days_cancel_to_expiry,
         COUNT(DISTINCT CASE WHEN v.vdate > date_sub(c.expiry_date,7) AND v.vdate <= c.expiry_date THEN v.vdate END)          AS days_last7_before_expiry,
         -- window 3: after paid expiry
         COALESCE(SUM(CASE WHEN v.vdate > c.expiry_date THEN v.minutes END),0)                                                AS mou_after_expiry,
         -- any time (all history on record)
         COUNT(DISTINCT v.convivasessionid)                                                                                   AS sessions_ever
  FROM cohort c LEFT JOIN v_cohort v ON c.Customer_External_ID = v.viewerid
  GROUP BY c.Subscription_ID, c.Customer_External_ID, c.signup_type, c.status_now, c.cancel_month, c.Product_name,
           c.Country, c.Acquisition_channel, c.Cancellation_reason, c.start_date, c.cancel_date, c.expiry_date, c.days_start_to_cancel") %>%
  reg("sub_view")

# Engagement tiers: sensitivity to the 'watcher' definition (answers 'beyond 1 minute')
sub_tier <- q("
  SELECT *,
    CASE WHEN sessions_ever = 0 THEN '0 never tried (no session ever)'
         WHEN sessions_during = 0 THEN '1 no viewing during this subscription'
         WHEN mou_during < 1 THEN '2 tried, under 1 minute'
         WHEN mou_during < 60 THEN '3 1-59 min'
         WHEN mou_during < 300 THEN '4 1-5 hours'
         WHEN mou_during < 1200 THEN '5 5-20 hours'
         ELSE '6 20+ hours' END AS mou_tier,
    CASE WHEN days_during >= 5 AND assets_during >= 5 THEN 'habitual (5+ days, 5+ assets)'
         WHEN days_during >= 2 THEN 'repeat (2-4 days)'
         WHEN days_during = 1 THEN 'single-day'
         ELSE 'none' END AS depth_tier
  FROM sub_view") %>% reg("sub_tier")

display(q("SELECT signup_type, mou_tier, COUNT(DISTINCT Customer_External_ID) customers,
                  ROUND(AVG(mou_during),1) avg_mou, ROUND(AVG(sessions_during),1) avg_sessions,
                  ROUND(AVG(days_during),1) avg_days, ROUND(AVG(assets_during),1) avg_assets,
                  ROUND(AVG(football_mou_during/NULLIF(mou_during,0)),3) football_share
           FROM sub_tier GROUP BY 1,2 ORDER BY 1,2"))
display(q("SELECT signup_type, depth_tier, COUNT(DISTINCT Customer_External_ID) customers
           FROM sub_tier GROUP BY 1,2 ORDER BY 1,2"))

# Watcher vs non-watcher under 4 different thresholds
display(q("
  SELECT signup_type,
    COUNT(*) subs,
    SUM(CASE WHEN sessions_ever = 0 THEN 1 ELSE 0 END)           AS never_any_session,
    SUM(CASE WHEN sessions_during = 0 THEN 1 ELSE 0 END)         AS none_during_sub,
    SUM(CASE WHEN mou_during < 1 THEN 1 ELSE 0 END)              AS lt_1_min,
    SUM(CASE WHEN mou_during < 30 THEN 1 ELSE 0 END)             AS lt_30_min,
    SUM(CASE WHEN days_during < 2 THEN 1 ELSE 0 END)             AS lt_2_days
  FROM sub_tier GROUP BY 1"))

# Cancellations, 'actual churn' and revenue: viewers vs non-watchers (uses CELL 3 too)
display(q("
  SELECT t.signup_type, CASE WHEN t.mou_during < 1 THEN 'Non-watcher (<1 min during sub)' ELSE 'Viewer' END AS grp,
         COUNT(*) subs,
         SUM(CASE WHEN t.expiry_date <= '{ASOF}' THEN 1 ELSE 0 END) AS reached_expiry,
         ROUND(SUM(r.last_charge_usd),0) renewal_value_forgone_usd,
         ROUND(AVG(r.paid_to_date_usd),2) avg_paid_to_date_usd
  FROM sub_tier t JOIN cohort_rev r USING (Subscription_ID)
  GROUP BY 1,2 ORDER BY 1,2"))

# ---------------------------------------------------------------------
# CELL 5: Q2 non-watchers. Did they try? When did they cancel? Repeat pattern? Reasons?
# ---------------------------------------------------------------------
nw <- q("SELECT * FROM sub_tier WHERE mou_during < 1") %>% reg("nw")

# (a) tried vs never tried
display(q("SELECT signup_type, mou_tier, COUNT(*) subs FROM nw GROUP BY 1,2 ORDER BY 1,2"))

# (b) timing: when after sign-up did they cancel?
display(q("
  SELECT signup_type,
    SUM(CASE WHEN days_start_to_cancel = 0 THEN 1 ELSE 0 END) day0,
    SUM(CASE WHEN days_start_to_cancel BETWEEN 1 AND 3 THEN 1 ELSE 0 END) d1_3,
    SUM(CASE WHEN days_start_to_cancel BETWEEN 4 AND 7 THEN 1 ELSE 0 END) d4_7,
    SUM(CASE WHEN days_start_to_cancel BETWEEN 8 AND 30 THEN 1 ELSE 0 END) d8_30,
    SUM(CASE WHEN days_start_to_cancel > 30 THEN 1 ELSE 0 END) d30plus,
    percentile_approx(days_start_to_cancel, 0.5) median_days
  FROM nw GROUP BY 1"))

# (c) repeat pattern: did the same customer also not watch in their PRIOR subscriptions?
prior <- q("
  SELECT s.Customer_External_ID, s.Subscription_ID, s.sub_seq, s.start_date,
         COALESCE(s.cancel_date, s.expiry_date) AS end_date,
         COALESCE(SUM(CASE WHEN v.vdate BETWEEN s.start_date AND COALESCE(s.cancel_date, s.expiry_date) THEN v.minutes END),0) AS mou
  FROM subs_all s LEFT JOIN v_cohort v ON s.Customer_External_ID = v.viewerid
  WHERE s.Customer_External_ID IN (SELECT DISTINCT Customer_External_ID FROM nw)
    AND s.Subscription_ID NOT IN (SELECT Subscription_ID FROM cohort)
  GROUP BY 1,2,3,4,5") %>% reg("prior")
display(q("
  SELECT n.signup_type,
         COUNT(DISTINCT n.Customer_External_ID) nonwatch_customers,
         COUNT(DISTINCT p.Customer_External_ID) with_prior_subs,
         COUNT(DISTINCT CASE WHEN p.mou < 1 THEN p.Customer_External_ID END) with_a_prior_nonwatch_sub,
         ROUND(AVG(CASE WHEN p.mou < 1 THEN 1.0 ELSE 0 END),3) share_of_prior_subs_nonwatch
  FROM nw n LEFT JOIN prior p USING (Customer_External_ID) GROUP BY 1"))

# (d) reasons and channel (reason is only populated for Web cancellations; compare by channel)
display(q("SELECT signup_type, Acquisition_channel, Cancellation_reason, COUNT(*) subs
           FROM nw GROUP BY 1,2,3 ORDER BY subs DESC"))
display(q("SELECT Product_name, Country, signup_type, COUNT(*) subs, ROUND(AVG(days_start_to_cancel),1) avg_days
           FROM nw GROUP BY 1,2,3 HAVING COUNT(*) >= 10 ORDER BY subs DESC"))

# (e) failure signals: login / entitlement / playback.
# NOT AVAILABLE in the current tables. Replace the table and column names below with your
# auth / playback-error sources, then run. Left commented so the notebook does not break.
# auth <- q("SELECT user_id, event_date, event_type, error_code FROM <AUTH_EVENTS_TABLE>
#            WHERE event_date BETWEEN '{FROM}' AND '{TO}'")            # login attempts/failures
# ent  <- q("SELECT user_id, event_date, error_code FROM <ENTITLEMENT_ERRORS_TABLE>")
# play <- q("SELECT viewerid, startdate, error_code FROM <PLAYBACK_ERRORS_TABLE>")
# Then join to `nw` on Customer_External_ID within [start_date, cancel_date] and tabulate:
#   attempted login? login failed? entitlement error? playback error? none of these (never tried)?

# ---------------------------------------------------------------------
# CELL 6: Q2 viewers: once or repeatedly, continued after cancelling, tied to fixtures
# ---------------------------------------------------------------------
display(q("
  SELECT signup_type,
    SUM(CASE WHEN days_during = 1 THEN 1 ELSE 0 END)  watched_one_day,
    SUM(CASE WHEN days_during BETWEEN 2 AND 4 THEN 1 ELSE 0 END) watched_2_4_days,
    SUM(CASE WHEN days_during >= 5 THEN 1 ELSE 0 END) watched_5plus_days,
    SUM(CASE WHEN mou_cancel_to_expiry > 0 THEN 1 ELSE 0 END)   kept_watching_after_cancel,
    SUM(CASE WHEN days_last7_before_expiry > 0 THEN 1 ELSE 0 END) watched_in_last_7d_before_expiry,
    SUM(CASE WHEN mou_after_expiry > 0 THEN 1 ELSE 0 END) watched_after_expiry
  FROM sub_tier WHERE mou_during >= 1 GROUP BY 1"))

# Playback-problem proxy available today: many very short sessions and no long ones
display(q("
  SELECT c.signup_type,
    SUM(CASE WHEN x.short_sessions >= 3 AND x.long_sessions = 0 THEN 1 ELSE 0 END) AS short_sessions_only_3plus,
    COUNT(*) customers
  FROM cohort c JOIN (
     SELECT v.viewerid,
            SUM(CASE WHEN v.minutes < 2 THEN 1 ELSE 0 END)  short_sessions,
            SUM(CASE WHEN v.minutes >= 10 THEN 1 ELSE 0 END) long_sessions
     FROM v_cohort v GROUP BY 1) x ON c.Customer_External_ID = x.viewerid
  GROUP BY 1"))

# Fixture calendar proxy: 'peak football days' = days where total football minutes are in the top quartile
fixtures <- q("
  SELECT vdate, SUM(minutes) AS fb_minutes, COUNT(DISTINCT viewerid) AS fb_viewers
  FROM v_cohort WHERE ContentType2 = 'Football' AND vdate >= '{HIST_FROM}' GROUP BY vdate") %>% collect()
thr <- quantile(fixtures$fb_minutes, 0.75)
peak_days <- fixtures %>% filter(fb_minutes >= thr) %>% transmute(peak_date = as.character(vdate))
# Calendar with a near-peak flag (within 2 days of a peak day), joined by date (avoids correlated subqueries)
pk  <- as.Date(peak_days$peak_date)
cal <- data.frame(d = seq(as.Date("2025-01-01"), as.Date(P$TO), by = "day"))
cal$near_peak <- as.integer(sapply(cal$d, function(x) any(abs(as.numeric(x - pk)) <= 2)))
cal$d <- as.character(cal$d)
sdf_copy_to(sc, cal, "peak_cal", overwrite = TRUE)
cat("Base rate (share of 2026 days within 2 days of a peak day):",
    round(mean(cal$near_peak[cal$d >= "2026-01-01"]), 3), "\n")

# Do sign-ups and cancellations cluster near peak days more than that base rate?
display(q("
  SELECT 'start' AS event, CASE WHEN s.sub_seq = 1 THEN 'New' ELSE 'Win-back' END signup_type,
         COUNT(*) n, ROUND(AVG(p.near_peak),3) share_near_peak
  FROM subs_all s JOIN peak_cal p ON p.d = CAST(s.start_date AS STRING)
  WHERE s.start_date BETWEEN '2026-01-01' AND '{TO}' GROUP BY 1,2
  UNION ALL
  SELECT 'cancel', CASE WHEN s.sub_seq = 1 THEN 'New' ELSE 'Win-back' END, COUNT(*), ROUND(AVG(p.near_peak),3)
  FROM subs_all s JOIN peak_cal p ON p.d = CAST(s.cancel_date AS STRING)
  WHERE s.cancel_date BETWEEN '2026-01-01' AND '{TO}' GROUP BY 1,2"))

# Weekly cancellations by type vs weekly football minutes (extends the notebook's chart)
display(q("
  SELECT date_trunc('week', c.cancel_date) wk, c.signup_type, COUNT(DISTINCT c.Customer_External_ID) cancels
  FROM cohort c GROUP BY 1,2 ORDER BY 1,2"))
display(q("SELECT date_trunc('week', vdate) wk, ROUND(SUM(minutes),0) football_minutes, COUNT(DISTINCT viewerid) viewers
           FROM v_cohort WHERE ContentType2 = 'Football' AND vdate BETWEEN '{FROM}' AND '{TO}' GROUP BY 1 ORDER BY 1"))

# Offers and renewal: campaigns / coupons / promo and billing cycle in the cohort
display(q("SELECT signup_type, Campaign_name, COUNT(*) subs, ROUND(AVG(days_start_to_cancel),1) avg_days
           FROM cohort GROUP BY 1,2 ORDER BY subs DESC"))
display(q("SELECT signup_type, Offer_period, COUNT(*) subs, ROUND(AVG(days_cancel_to_expiry),1) avg_days_left
           FROM cohort GROUP BY 1,2 ORDER BY 1,3 DESC"))
display(q("SELECT signup_type, CASE WHEN Coupon_code IS NULL OR TRIM(Coupon_code) = '' THEN 'no coupon' ELSE 'coupon' END cpn,
                  COUNT(*) subs FROM cohort GROUP BY 1,2"))

# ---------------------------------------------------------------------
# CELL 7: Q2 win-back cycling. Is repeat subscribe-cancel typical?
# ---------------------------------------------------------------------
cycle <- q("
  SELECT Customer_External_ID,
         COUNT(*) AS n_subs,
         SUM(CASE WHEN cancel_date IS NOT NULL THEN 1 ELSE 0 END) AS n_cancelled,
         ROUND(AVG(datediff(COALESCE(cancel_date, expiry_date), start_date)),1) AS avg_sub_length_days,
         ROUND(AVG(CASE WHEN sub_seq > 1 THEN datediff(start_date, prev_expiry) END),1) AS avg_gap_days,
         MIN(start_date) first_start, MAX(start_date) last_start
  FROM subs_all
  WHERE Customer_External_ID IN (SELECT DISTINCT Customer_External_ID FROM cohort WHERE signup_type = 'Win-back')
  GROUP BY 1") %>% reg("cycle")

display(q("SELECT CASE WHEN n_subs >= 6 THEN '6+' WHEN n_subs >= 4 THEN '4-5' ELSE CAST(n_subs AS STRING) END bucket,
                  COUNT(*) customers, ROUND(AVG(avg_sub_length_days),1) avg_len, ROUND(AVG(avg_gap_days),1) avg_gap
           FROM cycle GROUP BY 1 ORDER BY 1"))

# Baseline: are Aug-Sep win-back cancellers different from win-backs who cancelled in Jan-Jul 2026?
display(q("
  SELECT CASE WHEN cancel_date >= '{FROM}' THEN 'Aug-Sep 2026' ELSE 'Jan-Jul 2026' END period,
         COUNT(*) subs,
         ROUND(AVG(sub_seq),2) avg_sub_seq,
         ROUND(AVG(datediff(start_date, prev_expiry)),1) avg_gap_before_return,
         percentile_approx(datediff(cancel_date, start_date), 0.5) median_days_to_cancel,
         ROUND(AVG(CASE WHEN datediff(cancel_date, start_date) <= 7 THEN 1.0 ELSE 0 END),3) share_cancel_within_7d
  FROM subs_all WHERE sub_seq > 1 AND cancel_date BETWEEN '2026-01-01' AND '{TO}'
  GROUP BY 1"))

# Football-anchored cycling: for each win-back, did the PRIOR cycle also start/cancel near a peak day?
display(q("
  SELECT CASE WHEN a.near_peak = 1 THEN 'start near peak' ELSE 'start off-peak' END start_pos,
         CASE WHEN b.near_peak = 1 THEN 'cancel near peak' ELSE 'cancel off-peak' END cancel_pos,
         COUNT(*) subs
  FROM subs_all s
  JOIN peak_cal a ON a.d = CAST(s.start_date AS STRING)
  JOIN peak_cal b ON b.d = CAST(s.cancel_date AS STRING)
  WHERE s.sub_seq > 1 AND s.start_date >= '2026-01-01'
    AND s.Customer_External_ID IN (SELECT Customer_External_ID FROM cohort WHERE signup_type = 'Win-back')
  GROUP BY 1,2"))

# ---------------------------------------------------------------------
# CELL 8: Q2 duplicate accounts behind 'new' subscribers
# ---------------------------------------------------------------------
# (a) One Customer_ID mapped to several Customer_External_IDs (account re-creation)
display(q("
  SELECT n_ext, COUNT(*) customer_ids FROM (
    SELECT Customer_ID, COUNT(DISTINCT Customer_External_ID) n_ext FROM subs_all GROUP BY 1) GROUP BY 1 ORDER BY 1"))

# (b) 'New' cancellers whose Customer_ID has an earlier subscription under another external id
display(q("
  SELECT c.cancel_month, COUNT(DISTINCT c.Customer_External_ID) new_cancellers,
         COUNT(DISTINCT CASE WHEN e.Customer_ID IS NOT NULL THEN c.Customer_External_ID END) have_earlier_sub_other_account
  FROM cohort c
  LEFT JOIN (SELECT Customer_ID, Customer_External_ID, MIN(start_date) first_start FROM subs_all GROUP BY 1,2) e
    ON c.Customer_ID = e.Customer_ID AND c.Customer_External_ID <> e.Customer_External_ID AND e.first_start < c.start_date
  WHERE c.signup_type = 'New' GROUP BY 1 ORDER BY 1"))

# (c) shared identifiers. Replace with columns you hold (hashed email / phone / device / payment token).
# DUP_KEY <- "payment_token_hash"    # [ASSUMED] not in the notebook's column list
# display(q(sprintf("
#   SELECT c.cancel_month, COUNT(DISTINCT c.Customer_External_ID) new_cancellers,
#          COUNT(DISTINCT CASE WHEN o.Customer_External_ID IS NOT NULL THEN c.Customer_External_ID END) share_key_with_earlier_account
#   FROM cohort c LEFT JOIN subs_all o
#     ON c.%s = o.%s AND c.Customer_External_ID <> o.Customer_External_ID AND o.start_date < c.start_date
#   WHERE c.signup_type = 'New' GROUP BY 1", DUP_KEY, DUP_KEY)))

# ---------------------------------------------------------------------
# CELL 9: Q3 what happened after cancellation
# ---------------------------------------------------------------------
post <- q("
  SELECT c.Subscription_ID, c.Customer_External_ID, c.signup_type, c.cancel_month, c.cancel_date, c.expiry_date,
         c.days_cancel_to_expiry, c.next_start, c.next_channel, c.next_sub_id,
         CASE WHEN c.expiry_date <= '{ASOF}' THEN 1 ELSE 0 END                                AS reached_expiry,
         CASE WHEN date_add(c.expiry_date, {MATURE_DAYS}) <= '{ASOF}' THEN 1 ELSE 0 END         AS matured_for_return,
         CASE WHEN c.next_start IS NOT NULL AND c.next_start <= c.expiry_date THEN 1 ELSE 0 END AS new_sub_before_expiry,
         CASE WHEN c.next_start > c.expiry_date THEN datediff(c.next_start, c.expiry_date) END  AS days_expiry_to_return,
         s.Subscription_status AS status_now
  FROM cohort c JOIN subs_all s USING (Subscription_ID)") %>% reg("post")

# (a) renewal despite cancellation: a charge dated after the cancel date on the same subscription
renewed <- q("
  SELECT DISTINCT p.Subscription_ID FROM post p JOIN DIM_Transactions_SD t ON t.Subscription_ID = p.Subscription_ID
  WHERE to_date(t.{TXN_DATE}) > p.cancel_date") %>% reg("renewed")

display(q("
  SELECT p.signup_type, p.cancel_month, COUNT(*) subs,
         SUM(reached_expiry) reached_paid_expiry,
         SUM(CASE WHEN r.Subscription_ID IS NOT NULL THEN 1 ELSE 0 END) charged_after_cancel_reversal_or_renewal,
         SUM(new_sub_before_expiry) started_new_sub_before_expiry
  FROM post p LEFT JOIN renewed r USING (Subscription_ID) GROUP BY 1,2 ORDER BY 1,2"))

# (b) did they keep watching before expiry? (from sub_view)
display(q("
  SELECT signup_type, COUNT(*) subs,
         SUM(CASE WHEN mou_cancel_to_expiry > 0 THEN 1 ELSE 0 END) watched_after_cancel,
         ROUND(AVG(CASE WHEN mou_cancel_to_expiry > 0 THEN mou_cancel_to_expiry END),0) avg_mou_after_cancel,
         SUM(CASE WHEN days_last7_before_expiry > 0 THEN 1 ELSE 0 END) watched_final_week
  FROM sub_view GROUP BY 1"))

# (c) after expiry: how many returned, how soon, through which channel. Only matured subs.
display(q("
  SELECT signup_type,
         COUNT(*) matured_subs,
         SUM(CASE WHEN days_expiry_to_return IS NOT NULL THEN 1 ELSE 0 END) returned,
         SUM(CASE WHEN days_expiry_to_return BETWEEN 0 AND 7 THEN 1 ELSE 0 END)  within_7d,
         SUM(CASE WHEN days_expiry_to_return BETWEEN 8 AND 14 THEN 1 ELSE 0 END) d8_14,
         SUM(CASE WHEN days_expiry_to_return BETWEEN 15 AND 30 THEN 1 ELSE 0 END) d15_30
  FROM post WHERE matured_for_return = 1 GROUP BY 1"))
display(q("SELECT signup_type, next_channel, COUNT(*) returns FROM post WHERE days_expiry_to_return IS NOT NULL GROUP BY 1,2 ORDER BY 3 DESC"))

# (d) do returns line up with fixtures? weekly returns vs weekly football minutes
display(q("SELECT date_trunc('week', next_start) wk, signup_type, COUNT(*) returns
           FROM post WHERE days_expiry_to_return IS NOT NULL GROUP BY 1,2 ORDER BY 1,2"))
display(q("SELECT date_trunc('week', vdate) wk, ROUND(SUM(minutes),0) football_minutes
           FROM v_cohort WHERE ContentType2 = 'Football' AND vdate >= '{FROM}' GROUP BY 1 ORDER BY 1"))

# (e) what did returners watch in their first 7 days back?
display(q("
  SELECT p.signup_type, v.competitionName, COUNT(DISTINCT p.Customer_External_ID) returners,
         ROUND(SUM(v.minutes),0) minutes
  FROM post p JOIN v_cohort v ON v.viewerid = p.Customer_External_ID
   AND v.vdate BETWEEN p.next_start AND date_add(p.next_start, 7)
  WHERE p.days_expiry_to_return IS NOT NULL
  GROUP BY 1,2 HAVING COUNT(DISTINCT p.Customer_External_ID) >= 20 ORDER BY minutes DESC"))

# ---------------------------------------------------------------------
# CELL 10: optional, export every summary for the deck
# (re-run the display() queries above with collect() and write to xlsx / csv as needed)
# ---------------------------------------------------------------------
