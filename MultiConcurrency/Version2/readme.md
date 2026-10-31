I've updated `overlapping_sessions_sparklyr.R` so it now returns one row for every overlapping session with the fields you asked for. It is still untested on Spark.

The new table is `overlap_sessions`, step 7 of the script. It has one row per session for each overlap period it falls in. Its columns:
- `viewerId`
- `asset_id`
- `convivasessionid`
- `deviceid`, plus `device_type` (from `dvhwt`)
- `starttimeunixtime`, plus `start_utc` as a readable time
- `playingtimemin`
- `run_id`, `run_start_utc`, `run_end_utc`, `run_min` and `peak_concurrent` for the overlap period the session belongs to

I made two changes to the script:
- **Step 1:** the one-row-per-session step now also keeps `deviceid`, `device_type` and the total `playingtimemin` for each session.
- **Step 7:** the new detail table is built from step 1. The summary and the `result` table are unchanged.

One session can appear more than once if it falls inside more than one overlap period. To get one row per session, add `dplyr::distinct(viewerId, asset_id, convivasessionid, .keep_all = TRUE)` before `dplyr::collect()` in step 7. The `start_utc` columns use `from_unixtime`, which follows your Spark session's time zone, so on a cluster set to another zone they will not read as UTC.
