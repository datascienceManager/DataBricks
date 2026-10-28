# =====================================================================
# Validation checks for VoluntaryCancellation_All V2.3
# Run AFTER Cells 0-7 of Cancellation_Lifecycle_Analysis.R (needs: base_rows, life, cohort, v, tiers, nw, last_charge, post, fixtures).
# Each check prints the figure in the report next to an independent recount, or tests a definition.
# Expected values from the 28 Oct run are in the comments.
# =====================================================================

# ---- V1. Row integrity --------------------------------------------------------------------------
# One row per subscription in life; no subscription with two 'latest' rows.          expect: rows = distinct, dup_latest = 0
display(q("SELECT COUNT(*) rows, COUNT(DISTINCT Subscription_ID) distinct_subs FROM life"))
display(q("SELECT COUNT(*) dup_latest FROM (SELECT Subscription_ID FROM base_rows WHERE Subscription_latest = 1 GROUP BY 1 HAVING COUNT(*) > 1)"))

# ---- V2. Cohort recount straight from the source table ------------------------------------------
# expect: 41,112 subscriptions (cohort) | customers: 39,986 distinct (report header says 40,787 = Aug + Sep counted separately)
display(q("
  SELECT COUNT(DISTINCT Subscription_ID) subs, COUNT(DISTINCT Customer_External_ID) customers
  FROM DIM_SubscribersInfo_SDF
  WHERE D2C_B2B = 'D2C' AND offer_type = 'Subscription' AND product_type IS NULL AND non_commercial <> 1 AND daily_weekly_flag <> 1
    AND Customer_External_ID IS NOT NULL AND TRIM(Customer_External_ID) NOT IN ('', 'null')
    AND Subscription_latest = 1 AND to_date(Cancellation_date) BETWEEN '{FROM}' AND '{TO}'"))
display(q("SELECT COUNT(*) customers_cancelling_in_both_aug_and_sep FROM (
             SELECT Customer_External_ID FROM cohort GROUP BY 1 HAVING COUNT(DISTINCT date_format(cancel_date,'yyyy-MM')) = 2)"))   # expect 801

# ---- V3. Is the cohort really 'voluntary'? ----------------------------------------------------------
# The cohort is 'any cancellation date in the window'. This shows how much is involuntary / failed charge / operator unsub.
display(q("SELECT Subscription_churn_type, churn_reason, regexp_extract(Cancellation_reason, '^(.{0,45})', 1) reason, COUNT(*) subs
           FROM cohort GROUP BY 1,2,3 ORDER BY subs DESC"))

# ---- V4. Cancellations dated ON or AFTER paid expiry ----------------------------------------------
# The 'Expired / 0 days' bucket (2,183 subs) mixes cancel-on-expiry-day with cancel dated AFTER expiry (lapse records).
display(q("SELECT start_status, SUM(CASE WHEN days_cancel_to_expiry < 0 THEN 1 ELSE 0 END) cancel_after_expiry,
                  SUM(CASE WHEN days_cancel_to_expiry = 0 THEN 1 ELSE 0 END) cancel_on_expiry_day,
                  MIN(days_cancel_to_expiry) most_negative
           FROM cohort GROUP BY 1"))

# ---- V5. The July 2026 break in 'Cancelled on expiry day' ----------------------------------------
# Win-back expiry-day cancels: 9,313 in Jun -> 666 in Jul. If reason / channel / churn type differ before and after,
# the recording changed and the run-rate comparison is not like for like.
display(q("SELECT date_format(cancel_date,'yyyy-MM') m, cancel_type, Acquisition_channel, Subscription_churn_type, COUNT(*) subs
           FROM life WHERE cancel_date >= '2026-03-01' AND cancel_date <= '{TO}' AND cancel_type = 'Cancelled on expiry day'
           GROUP BY 1,2,3,4 ORDER BY 1, subs DESC"))

# ---- V6. Error flag: blank errorlist is being counted as an error --------------------------------
# Report: non-watchers 'had_error_coded_session' = 590 / 622, i.e. almost everyone who opened the player.
display(q("SELECT COUNT(*) sessions,
                  SUM(CASE WHEN errorlist IS NOT NULL THEN 1 ELSE 0 END) errorlist_not_null_CURRENT_RULE,
                  SUM(CASE WHEN NULLIF(TRIM(errorlist), '') IS NOT NULL AND UPPER(TRIM(errorlist)) NOT IN ('N/A','NA','NULL') THEN 1 ELSE 0 END) real_error_text,
                  SUM(CASE WHEN startuperror = 1 THEN 1 ELSE 0 END) startuperror_1
           FROM v"))

# Corrected flags (use these to replace has_error / entitlement_error in Cell 3)
v_fix <- q("
  SELECT *,
    CASE WHEN (NULLIF(TRIM(errorlist), '') IS NOT NULL AND UPPER(TRIM(errorlist)) NOT IN ('N/A','NA','NULL')) OR startuperror = 1 THEN 1 ELSE 0 END AS has_error_fix,
    CASE WHEN UPPER(errorlist) LIKE '%ENTITL%' OR UPPER(errorlist) LIKE '%NOT_ENTITLED%' OR UPPER(errorlist) LIKE '%DO NOT HAVE ACCESS%'
              OR UPPER(errorlist) LIKE '%NEEDS_TO_UPGRADE%' OR UPPER(errorlist) LIKE '%NOT AUTHORI%' OR errorlist LIKE '%صلاحية%' THEN 1 ELSE 0 END AS entitlement_fix,
    CASE WHEN UPPER(errorlist) LIKE '%MAXIMUM_NUMBER_OF_USER_DEVICE%' OR UPPER(errorlist) LIKE '%MAXIMUM NUMBER OF D%'
              OR UPPER(errorlist) LIKE '%ANOTHER BROWSER%' OR errorlist LIKE '%الحد الأقصى%' THEN 1 ELSE 0 END AS device_limit_fix,
    CASE WHEN UPPER(errorlist) LIKE '%DRM%' OR UPPER(errorlist) LIKE '%WIDEVINE%' OR UPPER(errorlist) LIKE '%FAIRPLAY%' OR UPPER(errorlist) LIKE '%PLAYREADY%' THEN 1 ELSE 0 END AS drm_fix
  FROM v") %>% reg("v_fix")

# Non-watchers re-run with corrected flags          (old: new 1,766 / 1,173 never opened / 590 'error'; winback 6,344 / 5,722 / 622)
display(q("
  SELECT n.start_status, COUNT(DISTINCT n.Subscription_ID) non_watching_subs,
         COUNT(DISTINCT CASE WHEN x.convivasessionid IS NULL THEN n.Subscription_ID END) never_opened_player,
         COUNT(DISTINCT CASE WHEN x.has_error_fix = 1 THEN n.Subscription_ID END) real_error,
         COUNT(DISTINCT CASE WHEN x.entitlement_fix = 1 THEN n.Subscription_ID END) entitlement_error,
         COUNT(DISTINCT CASE WHEN x.device_limit_fix = 1 THEN n.Subscription_ID END) device_limit_error,
         COUNT(DISTINCT CASE WHEN x.drm_fix = 1 THEN n.Subscription_ID END) drm_error,
         COUNT(DISTINCT CASE WHEN x.convivasessionid IS NOT NULL AND x.has_error_fix = 0 THEN n.Subscription_ID END) opened_no_error
  FROM nw n LEFT JOIN v_fix x ON x.viewerId = n.Customer_External_ID AND x.vdate BETWEEN n.start_date AND COALESCE(n.cancel_date, n.expiry_date)
  GROUP BY 1"))

# Viewers re-run with corrected flags               (old: error_sessions 1,371,539 new / 2,019,553 winback)
display(q("
  SELECT t.start_status, COUNT(DISTINCT t.Subscription_ID) subs, COUNT(*) sessions, SUM(x.has_error_fix) error_sessions,
         ROUND(AVG(x.has_error_fix), 4) error_session_rate,
         COUNT(DISTINCT CASE WHEN x.has_error_fix = 1 THEN t.Subscription_ID END) subs_with_any_error,
         COUNT(DISTINCT CASE WHEN x.entitlement_fix = 1 THEN t.Subscription_ID END) subs_with_entitlement_error,
         COUNT(DISTINCT CASE WHEN x.device_limit_fix = 1 THEN t.Subscription_ID END) subs_with_device_limit_error
  FROM tiers t JOIN v_fix x ON x.viewerId = t.Customer_External_ID AND x.vdate BETWEEN t.start_date AND COALESCE(t.cancel_date, t.expiry_date)
  GROUP BY 1"))

# ---- V7. Non-watching measured over the whole paid period (start -> expiry), not start -> cancel ---
# 46% of Win-back non-watchers cancelled on day 0-1, so their 'window' was a day or less.     old: 1,766 / 6,344
display(q("
  SELECT c.start_status, COUNT(*) subs,
         SUM(CASE WHEN COALESCE(w.m_to_cancel, 0) < 1 THEN 1 ELSE 0 END) nonwatch_start_to_cancel,
         SUM(CASE WHEN COALESCE(w.m_to_expiry, 0) < 1 THEN 1 ELSE 0 END) nonwatch_start_to_expiry
  FROM cohort c LEFT JOIN (
    SELECT c2.Subscription_ID,
           SUM(CASE WHEN v.vdate BETWEEN c2.start_date AND COALESCE(c2.cancel_date, c2.expiry_date) THEN v.minutes END) m_to_cancel,
           SUM(CASE WHEN v.vdate BETWEEN c2.start_date AND c2.expiry_date THEN v.minutes END) m_to_expiry
    FROM cohort c2 JOIN v ON v.viewerId = c2.Customer_External_ID GROUP BY 1) w USING (Subscription_ID)
  GROUP BY 1"))

# ---- V8. Viewing attribution: what is 'watched only on another subscription / pass'? -------------
# 6,341 Win-back subs fall here. If most of that viewing carries the ID of an EARLIER, already-expired subscription
# of the same customer, the ID mapping (SUFFIX_LEN) is wrong, not the customer.
display(q("
  SELECT CASE WHEN l.Subscription_ID IS NULL THEN 'viewing id not found in life (pass / other product)'
              WHEN l.Customer_External_ID <> t.Customer_External_ID THEN 'id belongs to another customer'
              WHEN l.expiry_date < t.start_date THEN 'earlier subscription, already expired'
              WHEN l.start_date > COALESCE(t.cancel_date, t.expiry_date) THEN 'later subscription'
              ELSE 'parallel subscription, paid-up at the time' END AS viewing_on,
         COUNT(DISTINCT t.Subscription_ID) subs, COUNT(*) sessions
  FROM tiers t JOIN v ON v.viewerId = t.Customer_External_ID AND v.vdate BETWEEN t.start_date AND COALESCE(t.cancel_date, t.expiry_date)
  LEFT JOIN life l ON l.Subscription_ID = v.sub_id_view
  WHERE t.attribution = 'watched only on another subscription / pass'
  GROUP BY 1 ORDER BY subs DESC"))
# Spot-check: 10 example rows to eyeball subscriptionid vs Subscription_ID
display(q("SELECT t.Subscription_ID, CAST(v.subscriptionid AS STRING) viewing_subscriptionid, v.sub_id_view, v.planname, v.vdate
           FROM tiers t JOIN v ON v.viewerId = t.Customer_External_ID AND v.vdate BETWEEN t.start_date AND COALESCE(t.cancel_date, t.expiry_date)
           WHERE t.attribution = 'watched only on another subscription / pass' LIMIT 10"))

# ---- V9. Revenue coverage -------------------------------------------------------------------------
# $1,085,405 = sum of last charge. How many cohort subs have NO transaction (counted as $0)? What drives the Aug avg of ~$30?
display(q("SELECT c.start_status, COUNT(*) subs, SUM(CASE WHEN l.Subscription_ID IS NULL THEN 1 ELSE 0 END) no_transaction,
                  ROUND(SUM(l.last_charge_usd), 0) last_charge_total, ROUND(MAX(l.last_charge_usd), 2) max_charge
           FROM cohort c LEFT JOIN last_charge l USING (Subscription_ID) GROUP BY 1"))
display(q("SELECT date_format(c.cancel_date,'yyyy-MM') m, c.Offer_period, COUNT(*) subs, ROUND(AVG(l.last_charge_usd), 2) avg_last_charge,
                  ROUND(SUM(l.last_charge_usd), 0) total
           FROM cohort c LEFT JOIN last_charge l USING (Subscription_ID) GROUP BY 1,2 ORDER BY 1, total DESC"))

# ---- V10. Data end date (ASOF) ---------------------------------------------------------------------
# ASOF is 2026-10-13 but weekly returns run to the week of 26 Oct. Set ASOF to the latest complete date.
display(q("SELECT MAX(start_date) max_sub_start, MAX(cancel_date) max_cancel, (SELECT MAX(vdate) FROM v) max_view_date FROM life"))

# ---- V11. Return rate on a fixed 30-day window (comparable across subs) ----------------------------
# Report: 22.3% / 40.3% = returned at ANY time up to data end. Fixed 30-day: expect ~17.3% / 31.8%.
display(q("SELECT start_status, COUNT(*) matured_subs,
                  ROUND(AVG(CASE WHEN days_expiry_to_next_start BETWEEN 0 AND {MATURE_DAYS} THEN 1.0 ELSE 0 END), 3) returned_within_30d,
                  ROUND(AVG(CASE WHEN days_expiry_to_next_start >= 0 THEN 1.0 ELSE 0 END), 3) returned_any_time
           FROM post WHERE matured = 1 GROUP BY 1"))

# ---- V12. Peak match day definition ------------------------------------------------------------------
# Top-quartile match days +/-2 covers 73% of 2026 days. Show coverage for tighter cut-offs.
display(q("
  WITH d AS (SELECT match_date, SUM(viewers) v FROM fixtures WHERE match_date >= '2026-01-01' AND match_date <= '{TO}' GROUP BY 1),
  r AS (SELECT match_date, percent_rank() OVER (ORDER BY v) pr FROM d),
  cal AS (SELECT explode(sequence(DATE'2026-01-01', DATE'{TO}', INTERVAL 1 DAY)) day)
  SELECT cut, COUNT(DISTINCT r.match_date) peak_days,
         ROUND(COUNT(DISTINCT CASE WHEN r.match_date IS NOT NULL THEN cal.day END) / (SELECT COUNT(*) FROM cal), 3) share_of_days_within_2d
  FROM (SELECT explode(array(0.75, 0.90, 0.95)) cut) k
  CROSS JOIN cal
  LEFT JOIN r ON r.pr >= k.cut AND abs(datediff(cal.day, r.match_date)) <= 2
  GROUP BY 1 ORDER BY 1"))

# ---- V13. Manual trace: 5 random cohort customers end to end ---------------------------------------
# Pick 5 customers and check every figure by hand against the source tables (status history, transactions, viewing).
trace_ids <- q("SELECT Customer_External_ID FROM cohort ORDER BY rand(42) LIMIT 5") %>% collect() %>% dplyr::pull(1)
ids_sql <- paste0("'", trace_ids, "'", collapse = ",")
display(q(paste0("SELECT Customer_External_ID, Subscription_ID, seq, start_status, rule_status, start_date, cancel_date, expiry_date,
                         cancel_type, days_cancel_to_expiry, next_start, days_expiry_to_next_start
                  FROM life WHERE Customer_External_ID IN (", ids_sql, ") ORDER BY 1, start_date")))
display(q(paste0("SELECT Subscription_ID, start_status, mou_tier, attribution, own_minutes, any_minutes_in_window, sessions_cancel_to_expiry
                  FROM tiers WHERE Customer_External_ID IN (", ids_sql, ")")))
