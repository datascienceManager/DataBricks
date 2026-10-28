# =====================================================================
# Validation checks for VoluntaryCancellation_All V2.5
# Run in new cells AFTER Cell 7 of the V2.5 notebook (needs: base_rows, life, cohort, v, tiers, nw, last_charge, post, fixtures).
# Each check recounts a V2.5 figure independently from the source tables, or tests that a V2.5 rule did what it says.
# "expect" comments say what a correct result looks like (not a fixed number: V2.5 figures are new).
# =====================================================================

# ---- V1. Row integrity ------------------------------------------------------------------------------
# expect: rows = distinct_subs; dup_latest = 0
df_v1_1 <- q("SELECT COUNT(*) rows, COUNT(DISTINCT Subscription_ID) distinct_subs FROM life")
df_v1_2 <- q("SELECT COUNT(*) dup_latest FROM (SELECT Subscription_ID FROM base_rows WHERE Subscription_latest = 1 GROUP BY 1 HAVING COUNT(*) > 1)")

# ---- V2. Cohort recount straight from the source table, WITH the voluntary filter -------------------------------
# expect: subs and customers equal the V2.5 cohort counts (row 'V2.5 cohort'); 'excluded' = Cell 1's excluded count
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
  UNION ALL SELECT 'V2.5 cohort', COUNT(DISTINCT Subscription_ID), COUNT(DISTINCT Customer_External_ID) FROM cohort")

# ---- V3. Voluntary filter: nothing involuntary left in the cohort --------------------------------------------
# expect: no row with Involuntary churn / Broadcaster churn / Payment failed / Faild to Charge / Canceled By
df_v3_1 <- q("SELECT Subscription_churn_type, churn_reason, regexp_extract(Cancellation_reason, '^(.{0,45})', 1) reason, COUNT(*) subs
              FROM cohort GROUP BY 1,2,3 ORDER BY subs DESC")
# What was removed, by month, Jan 2025 - Sep 2026 (the run-rate history): expect the large Mar-Jun 2026 Apple involuntary block here
df_v3_2 <- q("SELECT date_format(cancel_date_raw, 'yyyy-MM') m, SUM(is_voluntary) kept, SUM(1 - is_voluntary) excluded
              FROM life WHERE cancel_date_raw >= '{HIST_FROM}' AND cancel_date_raw <= '{TO}' GROUP BY 1 ORDER BY 1")

# ---- V4. Cancellations dated on or after paid expiry -----------------------------------------------------------
# expect: cancel_after_expiry small (V2.3: 69, all by 1 day)
df_v4_1 <- q("SELECT start_status, SUM(CASE WHEN days_cancel_to_expiry < 0 THEN 1 ELSE 0 END) cancel_after_expiry,
                     SUM(CASE WHEN days_cancel_to_expiry = 0 THEN 1 ELSE 0 END) cancel_on_expiry_day, MIN(days_cancel_to_expiry) most_negative
              FROM cohort GROUP BY 1")

# ---- V5. Gap rule: are New / Win-back / Quick re-subscribe assigned exactly as defined? ----------------------
# expect: 0 rows from the first query (every row obeys the rule); second query = the mapping Cell 1 shows
df_v5_1 <- q("
  SELECT start_status_sys, start_status, gap_since_prev_paid_through, COUNT(*) subs FROM life
  WHERE NOT (
       (start_status_sys = 'new'     AND start_status = 'new')
    OR (start_status_sys = 'winback' AND gap_since_prev_paid_through IS NULL AND start_status = 'winback')
    OR (start_status_sys = 'winback' AND gap_since_prev_paid_through >  {WB_MIN_GAP} AND start_status = 'winback')
    OR (start_status_sys = 'winback' AND gap_since_prev_paid_through <= {WB_MIN_GAP} AND start_status = 'resub'))
  GROUP BY 1,2,3 ORDER BY subs DESC LIMIT 20")   # with WB_RULE = 'system' every resub row would be listed here; that is expected
df_v5_2 <- q("SELECT start_status_sys, start_status, COUNT(*) subs, MIN(gap_since_prev_paid_through) min_gap, MAX(gap_since_prev_paid_through) max_gap
              FROM cohort GROUP BY 1,2 ORDER BY 1,2")

# ---- V6. Episodes: recomputed independently ----------------------------------------------------------------------
# expect: mismatches = 0; episodes never exceed subscriptions
df_v6_1 <- q("
  WITH r AS (
    SELECT Customer_External_ID, Subscription_ID, episode_no,
           1 + SUM(CASE WHEN seq > 1 AND (gap_since_prev_paid_through IS NULL OR gap_since_prev_paid_through > {WB_MIN_GAP}) THEN 1 ELSE 0 END)
               OVER (PARTITION BY Customer_External_ID ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS episode_check
    FROM life)
  SELECT COUNT(*) subs, SUM(CASE WHEN episode_no <> episode_check THEN 1 ELSE 0 END) mismatches FROM r")
df_v6_2 <- q("SELECT COUNT(*) customers, SUM(CASE WHEN episodes > n_subs THEN 1 ELSE 0 END) episodes_above_subs,
                     ROUND(AVG(n_subs), 2) avg_subs, ROUND(AVG(episodes), 2) avg_episodes
              FROM (SELECT Customer_External_ID, COUNT(*) n_subs, MAX(episode_no) episodes FROM life GROUP BY 1)")

# ---- V7. Error flags in V2.5 Cell 3 ----------------------------------------------------------------------------
# expect: error_sessions about 2-6% of sessions (V2.3 run: 394k real error text + 428k startuperror of 14.9M), NOT ~90%
df_v7_1 <- q("SELECT COUNT(*) sessions, SUM(has_error) error_sessions, ROUND(AVG(has_error), 4) error_rate,
                     SUM(entitlement_error) entitlement_sessions, SUM(device_limit_error) device_limit_sessions, SUM(drm_error) drm_sessions,
                     SUM(CASE WHEN has_error = 1 AND NULLIF(TRIM(errorlist), '') IS NULL AND COALESCE(startuperror, 0) <> 1 THEN 1 ELSE 0 END) blank_flagged_as_error
              FROM v")   # expect blank_flagged_as_error = 0

# ---- V8. Non-watching: start-to-cancel vs whole paid period ------------------------------------------------------
df_v8_1 <- q("
  SELECT c.start_status, COUNT(*) subs,
         SUM(CASE WHEN COALESCE(w.m_to_cancel, 0) < 1 THEN 1 ELSE 0 END) nonwatch_start_to_cancel,
         SUM(CASE WHEN COALESCE(w.m_to_expiry, 0) < 1 THEN 1 ELSE 0 END) nonwatch_start_to_expiry
  FROM cohort c LEFT JOIN (
    SELECT c2.Subscription_ID,
           SUM(CASE WHEN v.vdate BETWEEN c2.start_date AND COALESCE(c2.cancel_date, c2.expiry_date) THEN v.minutes END) m_to_cancel,
           SUM(CASE WHEN v.vdate BETWEEN c2.start_date AND LEAST(c2.expiry_date, DATE'{ASOF}') THEN v.minutes END) m_to_expiry
    FROM cohort c2 JOIN v ON v.viewerId = c2.Customer_External_ID GROUP BY 1) w USING (Subscription_ID)
  GROUP BY 1")   # nonwatch_start_to_expiry should equal the Non-watcher rows of Cell 11

# ---- V9. Revenue: excluding tax, monthly-equivalent, recomputed -------------------------------------------------
# expect: monthly_value_recount = the Aug-Sep total of rev$monthly_value_forgone_usd (Cell 2); months_covered sensible per offer period
df_v9_1 <- q("
  WITH lc AS (SELECT Subscription_ID, Transaction_price_excl_TAX_USD AS p,
                     ROW_NUMBER() OVER (PARTITION BY Subscription_ID ORDER BY Transaction_Date DESC) rn FROM DIM_Transactions_SD)
  SELECT c.start_status, COUNT(*) subs, SUM(CASE WHEN lc.Subscription_ID IS NULL THEN 1 ELSE 0 END) no_transaction,
         ROUND(SUM(lc.p), 0) full_charge_excl_tax, ROUND(SUM(lc.p / c.months_covered), 0) monthly_value_recount
  FROM cohort c LEFT JOIN lc ON lc.Subscription_ID = c.Subscription_ID AND lc.rn = 1 GROUP BY 1")
df_v9_2 <- q("SELECT Offer_period, months_covered, COUNT(*) subs, ROUND(AVG(paid_period_days), 0) avg_paid_days FROM cohort GROUP BY 1,2 ORDER BY subs DESC")
cat("Cell 2 monthly value forgone, Aug-Sep:", sum(rev$monthly_value_forgone_usd[rev$m %in% substr(c(P$FROM, P$TO), 1, 7)], na.rm = TRUE), "\n")

# ---- V10. Data end date ----------------------------------------------------------------------------------------
# expect: ASOF = the day before the latest date; max_view_date >= ASOF (else Cell 3 warns)
df_v10_1 <- q("SELECT '{ASOF}' asof_used, MAX(start_date) max_sub_start, MAX(cancel_date_raw) max_cancel, (SELECT MAX(vdate) FROM v) max_view_date FROM life")

# ---- V11. Return rates: 30-day window vs any time ----------------------------------------------------------------
df_v11_1 <- q("SELECT start_status, COUNT(*) matured_subs,
                      ROUND(AVG(CASE WHEN days_expiry_to_next_start BETWEEN 0 AND {MATURE_DAYS} THEN 1.0 ELSE 0 END), 3) returned_within_30d,
                      ROUND(AVG(CASE WHEN days_expiry_to_next_start >= 0 THEN 1.0 ELSE 0 END), 3) returned_any_time
               FROM post WHERE matured = 1 GROUP BY 1")   # within_30d should equal the report's 30-day KPI

# ---- V12. Peak match days in V2.5 --------------------------------------------------------------------------------
df_v12_1 <- q("SELECT COUNT(*) peak_days_2026 FROM peak_md WHERE match_date >= '2026-01-01' AND match_date <= '{TO}'")
cat("Share of 2026 days within 2 days of a peak match day:", round(mean(cal$near_peak[cal$d >= "2026-01-01"]), 3), "(V2.3 was 0.733; expect much lower)\n")

# ---- V13. Manual trace: 5 random cohort customers, end to end ---------------------------------------------------
trace_ids <- q("SELECT Customer_External_ID FROM cohort ORDER BY rand(42) LIMIT 5") %>% collect() %>% dplyr::pull(1)
ids_sql <- paste0("'", trace_ids, "'", collapse = ",")
df_v13_1 <- q(paste0("SELECT Customer_External_ID, Subscription_ID, seq, start_status_sys, start_status, gap_since_prev_paid_through, episode_no,
                             is_voluntary, start_date, cancel_date_raw, cancel_date, expiry_date, cancel_type, Offer_period, months_covered,
                             next_start, days_expiry_to_next_start
                      FROM life WHERE Customer_External_ID IN (", ids_sql, ") ORDER BY 1, seq"))
df_v13_2 <- q(paste0("SELECT t.Subscription_ID, t.start_status, t.mou_tier, t.attribution, t.own_minutes, t.any_minutes_in_window,
                             t.sessions_cancel_to_expiry, l.last_charge_usd
                      FROM tiers t LEFT JOIN last_charge l USING (Subscription_ID) WHERE t.Customer_External_ID IN (", ids_sql, ")"))

display_many(
  "V1. Rows vs distinct subscriptions in life" = df_v1_1,
  "V1. Subscriptions with two 'latest' rows (expect 0)" = df_v1_2,
  "V2. Cohort recount from source, with the voluntary filter (expect source voluntary = V2.5 cohort)" = df_v2_1,
  "V3. Churn type and reason left in the cohort (expect no involuntary)" = df_v3_1,
  "V3. Cancellations kept / excluded by month (run-rate history)" = df_v3_2,
  "V4. Cancellations dated on or after paid expiry" = df_v4_1,
  "V5. Rows breaking the gap rule (expect none)" = df_v5_1,
  "V5. System label vs status used, with gap range" = df_v5_2,
  "V6. Episode numbers recomputed (expect 0 mismatches)" = df_v6_1,
  "V6. Episodes vs subscriptions per customer (expect episodes_above_subs = 0)" = df_v6_2,
  "V7. Error flags (expect blank_flagged_as_error = 0)" = df_v7_1,
  "V8. Non-watchers: start to cancel vs whole paid period" = df_v8_1,
  "V9. Revenue recomputed excl. tax, monthly-equivalent" = df_v9_1,
  "V9. Months covered by offer period" = df_v9_2,
  "V10. Data end date and ASOF" = df_v10_1,
  "V11. Return rates: 30-day vs any time" = df_v11_1,
  "V12. Peak match days in 2026" = df_v12_1,
  "V13. Trace: 5 random customers, lifecycle" = df_v13_1,
  "V13. Trace: 5 random customers, viewing and last charge" = df_v13_2)
