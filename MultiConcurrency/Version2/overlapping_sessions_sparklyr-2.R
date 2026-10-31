# Overlapping sessions per viewer + asset (sparklyr / dplyr)
# Finds viewers with 3+ unique sessions playing at once for more than 5 minutes.
# Requires: sparklyr, dplyr, dbplyr >= 2.3.0 (for the dplyr:: prefix inside mutate/summarise)

library(sparklyr)

# ---- Parameters ------------------------------------------------------------
min_concurrent <- 3L    # "more than 2" unique sessions
min_seconds    <- 300   # "more than 5 minutes"

# ---- Input -----------------------------------------------------------------
# sc <- sparklyr::spark_connect(...)   # on Databricks, sc already exists
raw <- dplyr::tbl(sc, "your_table")    # needs: viewerId, asset_id, convivasessionid,
                                       #        starttimeunixtime (sec), playingtimemin

# ---- 1. One row per unique session (end = start + playing minutes) --------
sessions <- raw %>%
  dplyr::filter(playingtimemin > 0) %>%
  dplyr::mutate(en_row = starttimeunixtime + playingtimemin * 60) %>%
  dplyr::group_by(viewerId, asset_id, convivasessionid) %>%
  dplyr::summarise(st          = min(starttimeunixtime),
                   en          = max(en_row),
                   deviceid    = min(deviceid),
                   device_type = min(dvhwt),
                   playing_min = sum(playingtimemin)) %>%
  dplyr::ungroup()

# ---- 2. Start (+1) and end (-1) events -------------------------------------
events <- dplyr::union_all(
  sessions %>% dplyr::transmute(viewerId, asset_id, ts = st, delta = 1L),
  sessions %>% dplyr::transmute(viewerId, asset_id, ts = en, delta = -1L)
)

# ---- 3. Running concurrency between consecutive events ---------------------
segments <- events %>%
  dplyr::group_by(viewerId, asset_id) %>%
  dplyr::arrange(ts, delta) %>%                      # ends before starts on ties
  dplyr::mutate(concurrent = cumsum(delta),
                next_ts    = dplyr::lead(ts)) %>%
  dplyr::ungroup() %>%
  dplyr::filter(!is.na(next_ts), next_ts > ts) %>%   # drop zero-length slices
  dplyr::mutate(seg_secs = next_ts - ts,
                hot = as.integer(concurrent >= min_concurrent))

# ---- 4. Group consecutive "hot" slices into runs ---------------------------
runs <- segments %>%
  dplyr::group_by(viewerId, asset_id) %>%
  dplyr::arrange(ts) %>%
  dplyr::mutate(prev_hot = dplyr::lag(hot),
                new_run  = dplyr::if_else(is.na(prev_hot) | prev_hot != hot, 1L, 0L),
                run_id   = cumsum(new_run)) %>%
  dplyr::ungroup()

hot_runs <- runs %>%
  dplyr::filter(hot == 1L) %>%
  dplyr::group_by(viewerId, asset_id, run_id) %>%
  dplyr::summarise(run_start       = min(ts),
                   run_end         = max(next_ts),
                   run_secs        = sum(seg_secs),
                   peak_concurrent = max(concurrent)) %>%
  dplyr::ungroup() %>%
  dplyr::filter(run_secs > min_seconds)

# ---- 5. Per viewer + asset summary -----------------------------------------
viewer_summary <- hot_runs %>%
  dplyr::group_by(viewerId, asset_id) %>%
  dplyr::summarise(n_runs            = dplyr::n(),
                   total_overlap_min = sum(run_secs) / 60,
                   longest_run_min   = max(run_secs) / 60,
                   peak_concurrent   = max(peak_concurrent)) %>%
  dplyr::ungroup()

# ---- 6. Unique sessions that took part in those runs -----------------------
sessions_in_runs <- sessions %>%
  dplyr::inner_join(hot_runs, by = c("viewerId", "asset_id")) %>%
  dplyr::filter(st < run_end, en > run_start) %>%
  dplyr::group_by(viewerId, asset_id) %>%
  dplyr::summarise(n_sessions = dplyr::n_distinct(convivasessionid)) %>%
  dplyr::ungroup()

# ---- 7. The overlapping sessions themselves (one row per session) ---------
# Every session that played during a run of 3+ concurrent sessions lasting > 5 min.
overlap_sessions <- sessions %>%
  dplyr::inner_join(hot_runs, by = c("viewerId", "asset_id")) %>%
  dplyr::filter(st < run_end, en > run_start) %>%
  dplyr::distinct(viewerId, asset_id, convivasessionid, deviceid, device_type,
                  st, playing_min) %>%
  dplyr::transmute(viewerId,
                   asset_id,
                   convivasessionid,
                   deviceid,
                   device_type,
                   starttimeunixtime = st,
                   start_utc         = from_unixtime(st),   # Spark SQL function
                   playingtimemin    = playing_min) %>%
  dplyr::arrange(viewerId, asset_id, starttimeunixtime) %>%
  dplyr::collect()

# ---- 8. Final result -------------------------------------------------------
result <- viewer_summary %>%
  dplyr::left_join(sessions_in_runs, by = c("viewerId", "asset_id")) %>%
  dplyr::arrange(dplyr::desc(n_sessions)) %>%
  dplyr::collect()

print(result)
print(overlap_sessions)
# Totals: viewers flagged and sessions involved
# dplyr::summarise(result, viewers = dplyr::n(), sessions = sum(n_sessions))
