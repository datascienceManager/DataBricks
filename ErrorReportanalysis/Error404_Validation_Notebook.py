# Databricks notebook source
# MAGIC %md
# MAGIC # Error 404 Deep-Dive: validation notebook
# MAGIC Re-checks every number in the Error 404 deck and closes the open items.
# MAGIC Run it in the same cluster/session where `EntertainmentMovies_SD` already exists (cell 1 of `Error_ReportJ_V7`).
# MAGIC
# MAGIC | Section | Closes |
# MAGIC |---|---|
# MAGIC | 1 Base view + totals | tie-out of 217,195 / 6,356,227 / 12.53 per 10K / 3.42% |
# MAGIC | 2 Monthly | average per day and % of errors (both trend charts), YoY pp |
# MAGIC | 3 Daily series + spikes | **Open item:** the 71 spike days, threshold 67, median 14, level counts (5,000 / 10,000) |
# MAGIC | 4 Top 15 days | device / type / asset share per day |
# MAGIC | 5 Hour of day | **Caveat:** exact hourly values (data time zone and Doha time) |
# MAGIC | 6 Device OS | **Caveat:** full device list incl. iOS; coverage of sessions and 404s |
# MAGIC | 7 Content / CDN | **Fixed item:** shares on the right denominator |
# MAGIC | 8 Top-N, repeat bands | Top 15 sums, competition share base, repeat exposure |
# MAGIC | 9 Geo (source system) | **Conflict with Error 360:** 404s outside the 22 markets, straight from `ViewerSessions` |
# MAGIC | 10 Jan-Oct view | like-for-like with Error 360 (31 Oct cut-off) |
# MAGIC | 11 Scorecard | PASS / FAIL for all expected values |

# COMMAND ----------

# ---- 0. Parameters ---------------------------------------------------------------------------
END_DATE = "2026-11-03"        # same cut-off as the report; late-arriving data can move counts by a few rows
YEAR = 2026
FLEET_SPIKE_LEVELS = (5000, 10000)   # proposed "high" and "critical" levels used in the deck

COVERED = ['algeria','bahrain','egypt','iraq','jordan','kuwait','lebanon','libya','morocco','oman','qatar',
           'saudi arabia','syria','tunisia','united arab emirates','yemen','occupied palestinian territory',
           'south sudan','mauritania','sudan','somalia','chad']
covered_sql = ",".join("'%s'" % c for c in COVERED)

import pandas as pd, numpy as np
pd.set_option("display.width", 220); pd.set_option("display.max_columns", 50)

# COMMAND ----------

# ---- 1. Base view: identical rules to the report (startuperror/errorlist, standalone 404) -------
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v404 AS
SELECT viewerId, startdate, year(startdate) AS yr, month(startdate) AS mn,
       coalesce(hour(starttime), -1)               AS hr,
       coalesce(lower(trim(country)), 'unknown')   AS country,
       coalesce(deviceos, 'Unknown')               AS deviceos,
       CASE WHEN Live = 1 THEN 'Live' ELSE 'VOD' END AS live_vod,
       coalesce(ContentType2, 'Unknown')           AS content_type,
       coalesce(cdn, 'Unknown')                    AS cdn,
       coalesce(assetname, asset1, 'Unknown')      AS asset,
       coalesce(competitionName, 'Unknown')        AS competition,
       CASE WHEN startuperror = '1' OR (errorlist IS NOT NULL AND trim(errorlist) <> '') THEN 1 ELSE 0 END AS is_err,
       CASE WHEN errorlist RLIKE '(?<![0-9])404(?![0-9])' THEN 1 ELSE 0 END AS is_404,
       errorlist
FROM EntertainmentMovies_SD
WHERE startdate BETWEEN DATE'2025-01-01' AND DATE'{END_DATE}'
  AND viewerId IS NOT NULL AND viewerId NOT LIKE '%.%'
""")

tot = spark.sql("""
SELECT yr, COUNT(*) AS sessions, SUM(is_err) AS err, SUM(is_404) AS e404, SUM(is_err) - SUM(is_404) AS other,
       COUNT(DISTINCT CASE WHEN is_404 = 1 THEN viewerId END) AS viewers_404,
       ROUND(SUM(is_404) / COUNT(*) * 10000, 3)  AS per10k,
       ROUND(SUM(is_404) / SUM(is_err) * 100, 3) AS pct_of_errors
FROM v404 GROUP BY yr ORDER BY yr""").toPandas()
display(tot)
T26 = tot[tot.yr == 2026].iloc[0]; T25 = tot[tot.yr == 2025].iloc[0]
FLEET_AVG = float(T26.per10k)
print("Fleet average per 10K sessions, 2026 =", FLEET_AVG)

# COMMAND ----------

# ---- 2. Monthly: average 404 per day, average of daily % of errors (the two trend charts) ------
daily = spark.sql("""
SELECT startdate, yr, mn, COUNT(*) AS sessions, SUM(is_err) AS err, SUM(is_404) AS e404,
       SUM(is_err) - SUM(is_404) AS other
FROM v404 GROUP BY startdate, yr, mn ORDER BY startdate""").toPandas()
daily["startdate"] = pd.to_datetime(daily["startdate"])
daily["pct_err"] = np.where(daily.err > 0, daily.e404 / daily.err * 100, np.nan)

mon = (daily.groupby(["yr", "mn"])
       .agg(days=("startdate", "nunique"), e404=("e404", "sum"), other=("other", "sum"),
            avg_daily_404=("e404", "mean"), avg_daily_pct=("pct_err", "mean"),
            pooled_pct=("e404", "sum")).reset_index())
mon["pooled_pct"] = mon.e404 / (mon.e404 + mon.other) * 100
mon["avg_daily_404"] = mon.avg_daily_404.round(1); mon["avg_daily_pct"] = mon.avg_daily_pct.round(2); mon["pooled_pct"] = mon.pooled_pct.round(2)
display(mon)

m26 = mon[mon.yr == 2026].set_index("mn"); m25 = mon[mon.yr == 2025].set_index("mn")
yoy = (m26.avg_daily_pct - m25.avg_daily_pct).round(2).rename("yoy_pp_2026_vs_2025")
display(yoy.to_frame())
print("Months where 2026 avg/day > 2025:", [int(m) for m in m26.index if m in m25.index and m26.loc[m, "avg_daily_404"] > m25.loc[m, "avg_daily_404"]])
print("2026 peak month by avg/day:", m26.avg_daily_404.idxmax(), m26.avg_daily_404.max(), "| 2025 peak:", m25.avg_daily_404.idxmax(), m25.avg_daily_404.max())

# COMMAND ----------

# ---- 3. Daily series + spike threshold (report rule: median + 3 * MAD, MAD scaled by 1.4826) ----
d26 = daily[daily.yr == 2026].sort_values("startdate").reset_index(drop=True)
med = d26.e404.median()
mad = 1.4826 * (d26.e404 - med).abs().median()          # same constant as R::mad
thr = med + 3 * max(mad, 1)
d26["spike"] = d26.e404 > thr
d26["level"] = np.select([d26.e404 >= FLEET_SPIKE_LEVELS[1], d26.e404 >= FLEET_SPIKE_LEVELS[0], d26.spike],
                         ["critical (10,000+)", "high (5,000-9,999)", "spike (above line)"], "normal")
print(f"days in series = {len(d26)} | median day = {med:.1f} | MAD = {mad:.2f} | spike line = {thr:.1f}")
print("spike days (> line):", int(d26.spike.sum()))
print("days >= 5,000:", int((d26.e404 >= FLEET_SPIKE_LEVELS[0]).sum()), "| days >= 10,000:", int((d26.e404 >= FLEET_SPIKE_LEVELS[1]).sum()))
print("Top 15 share of 2026 404s: %.1f%%" % (d26.nlargest(15, "e404").e404.sum() / d26.e404.sum() * 100))
display(d26.level.value_counts().rename_axis("level").reset_index(name="days"))
display(d26.sort_values("e404", ascending=False).head(15)[["startdate", "sessions", "err", "e404", "level"]])
# the daily series behind the spike chart (export this to rebuild the full spike chart)
spark.createDataFrame(d26.assign(startdate=d26.startdate.dt.date)).createOrReplaceTempView("daily404_2026")
display(spark.table("daily404_2026").orderBy("startdate"))

# COMMAND ----------

# ---- 4. Top 15 days: top device, top 404 type and top asset with their share of the day ----------
top_days = tuple(str(x.date()) for x in d26.nlargest(15, "e404").startdate)
q = f"""
WITH t AS (SELECT * FROM v404 WHERE is_404 = 1 AND startdate IN {top_days}),
dev AS (SELECT startdate, deviceos AS k, COUNT(*) c FROM t GROUP BY startdate, deviceos),
ast AS (SELECT startdate, asset    AS k, COUNT(*) c FROM t GROUP BY startdate, asset),
tot AS (SELECT startdate, COUNT(*) n FROM t GROUP BY startdate),
typ AS (SELECT startdate,
          CASE WHEN errorlist RLIKE '(?i)manifest' THEN 'Manifest 404'
               WHEN errorlist RLIKE '(?i)playlist' THEN 'Playlist 404 (HLS)'
               WHEN errorlist RLIKE '(?i)(segment|fragment|chunk)' THEN 'Segment 404'
               WHEN errorlist RLIKE '(?i)(license|drm|widevine|fairplay)' THEN 'DRM / License 404'
               WHEN errorlist RLIKE '-12938' THEN 'Apple AVPlayer -12938'
               WHEN errorlist RLIKE '(?i)(BAD_HTTP_STATUS|unexpected HTTP status)' THEN 'Android/ExoPlayer 404'
               ELSE 'Other 404' END AS k, COUNT(*) c FROM t GROUP BY 1, 2)
SELECT tot.startdate, tot.n AS e404,
       (SELECT k FROM (SELECT k, c, ROW_NUMBER() OVER (ORDER BY c DESC) r FROM dev WHERE dev.startdate = tot.startdate) WHERE r = 1) AS top_device,
       (SELECT ROUND(MAX(c) / tot.n * 100) FROM dev WHERE dev.startdate = tot.startdate) AS top_device_pct,
       (SELECT k FROM (SELECT k, c, ROW_NUMBER() OVER (ORDER BY c DESC) r FROM typ WHERE typ.startdate = tot.startdate) WHERE r = 1) AS top_type,
       (SELECT k FROM (SELECT k, c, ROW_NUMBER() OVER (ORDER BY c DESC) r FROM ast WHERE ast.startdate = tot.startdate) WHERE r = 1) AS top_asset,
       (SELECT ROUND(MAX(c) / tot.n * 100) FROM ast WHERE ast.startdate = tot.startdate) AS top_asset_pct
FROM tot ORDER BY e404 DESC"""
display(spark.sql(q))

# COMMAND ----------

# ---- 5. Hour of day: exact values for the hour chart (replaces the values read from the image) ---
hr = spark.sql("""
SELECT yr, hr, COUNT(*) AS sessions, SUM(is_404) AS e404, ROUND(SUM(is_404) / COUNT(*) * 10000, 2) AS per10k
FROM v404 WHERE hr >= 0 GROUP BY yr, hr ORDER BY yr, hr""").toPandas()
display(hr.pivot(index="hr", columns="yr", values="per10k"))
print("2026 peak hour:", int(hr[hr.yr == 2026].sort_values("per10k").iloc[-1].hr), "| x fleet average: %.2f" % (hr[hr.yr == 2026].per10k.max() / FLEET_AVG))

# Doha time. ASSUMPTION: `starttime` is UTC. Check with your DBA / ViewerSessions definition before using this.
hr_doha = spark.sql("""
SELECT hour(from_utc_timestamp(starttime, 'Asia/Qatar')) AS hr_doha, COUNT(*) AS sessions, SUM(is_404) AS e404,
       ROUND(SUM(is_404) / COUNT(*) * 10000, 2) AS per10k
FROM (SELECT starttime, CASE WHEN errorlist RLIKE '(?<![0-9])404(?![0-9])' THEN 1 ELSE 0 END AS is_404
      FROM EntertainmentMovies_SD
      WHERE year(startdate) = 2026 AND startdate <= DATE'""" + END_DATE + """'
        AND viewerId IS NOT NULL AND viewerId NOT LIKE '%.%')
GROUP BY 1 ORDER BY 1""").toPandas()
display(hr_doha)
morning = hr_doha[(hr_doha.hr_doha >= 6) & (hr_doha.hr_doha <= 11)]
print("Doha 06:00-11:59 per10k = %.2f vs day average %.2f -> x%.2f (Error 360 reports 2.4x)" %
      (morning.e404.sum() / morning.sessions.sum() * 1e4, FLEET_AVG, morning.e404.sum() / morning.sessions.sum() * 1e4 / FLEET_AVG))

# COMMAND ----------

# ---- 6. Device OS: the full list (the report table showed 12 values covering 117.5M of 173.3M sessions) ----
dev = spark.sql("""
SELECT deviceos, COUNT(*) AS sessions, SUM(is_404) AS e404,
       ROUND(SUM(is_404) / COUNT(*) * 10000, 2) AS per10k,
       ROUND(SUM(is_404) / (SELECT SUM(is_404) FROM v404 WHERE yr = 2026) * 100, 2) AS share_of_404,
       ROUND(COUNT(*) / (SELECT COUNT(*) FROM v404 WHERE yr = 2026) * 100, 2) AS share_of_sessions
FROM v404 WHERE yr = 2026 GROUP BY deviceos ORDER BY e404 DESC, sessions DESC""").toPandas()
display(dev)
print("sessions in table: %s of %s | 404s in table: %s of %s" % (f"{dev.sessions.sum():,}", f"{int(T26.sessions):,}", f"{dev.e404.sum():,}", f"{int(T26.e404):,}"))
print("Rows with an empty/NULL OS label:", int(dev[dev.deviceos.isin(['Unknown', ''])].sessions.sum()))

# COMMAND ----------

# ---- 7. Content / CDN: shares on the right denominator (Live+VOD and the content cut are two views of the same 404s) ----
cont = spark.sql("""
SELECT 'Live/VOD' AS cut, live_vod AS value, COUNT(*) AS sessions, SUM(is_404) AS e404 FROM v404 WHERE yr = 2026 GROUP BY live_vod
UNION ALL
SELECT 'Content', content_type, COUNT(*), SUM(is_404) FROM v404 WHERE yr = 2026 GROUP BY content_type
UNION ALL
SELECT 'CDN', cdn, COUNT(*), SUM(is_404) FROM v404 WHERE yr = 2026 GROUP BY cdn""").toPandas()
cont["per10k"] = (cont.e404 / cont.sessions * 1e4).round(2)
cont["share_of_404s_correct"] = (cont.e404 / float(T26.e404) * 100).round(1)     # denominator = 217,195 for every cut
cont["share_of_sessions"] = (cont.sessions / float(T26.sessions) * 100).round(1)
display(cont.sort_values(["cut", "e404"], ascending=[True, False]))
for c_ in ("Live/VOD", "Content", "CDN"):
    print(c_, "404s add to", int(cont[cont.cut == c_].e404.sum()), "(expected", int(T26.e404), ")")

# COMMAND ----------

# ---- 8. Top-N tables: sums against the total, competition share base, repeat exposure --------------
def topn(col, n=15, where="yr = 2026"):
    return spark.sql(f"""SELECT {col} AS name, COUNT(*) AS sessions, SUM(is_404) AS e404,
                         ROUND(SUM(is_404) / COUNT(*) * 10000, 2) AS per10k,
                         COUNT(DISTINCT CASE WHEN is_404 = 1 THEN viewerId END) AS viewers_404
                         FROM v404 WHERE {where} GROUP BY {col} HAVING SUM(is_404) > 0
                         ORDER BY e404 DESC LIMIT {n}""").toPandas()
for col, label in [("asset", "assets"), ("country", "countries"), ("competition", "competitions")]:
    t = topn(col)
    t["x_fleet_avg"] = (t.per10k / FLEET_AVG).round(1)
    t["share_of_all_404s"] = (t.e404 / float(T26.e404) * 100).round(2)
    print(f"--- Top 15 {label}: 404 sum = {int(t.e404.sum()):,} = {t.e404.sum() / float(T26.e404) * 100:.1f}% of 2026 404s")
    display(t)

# sport-only base used by the report's competition shares (unknown / non-sport excluded)
sport = spark.sql("SELECT SUM(is_404) AS e FROM v404 WHERE yr = 2026 AND competition <> 'Unknown'").first()["e"]
print("404s with a known competition:", sport, "(the report's competition 'share' column divides by about this)")

rep = spark.sql("""
WITH v AS (SELECT viewerId, SUM(is_404) AS s FROM v404 WHERE yr = 2026 GROUP BY viewerId HAVING SUM(is_404) > 0)
SELECT CASE WHEN s = 1 THEN '1 session' WHEN s <= 5 THEN '2-5' WHEN s <= 20 THEN '6-20' ELSE '21+' END AS band,
       COUNT(*) AS viewers, SUM(s) AS sessions FROM v GROUP BY 1""").toPandas()
rep["viewer_share"] = (rep.viewers / rep.viewers.sum() * 100).round(1); rep["session_share"] = (rep.sessions / rep.sessions.sum() * 100).round(1)
rep["sessions_per_viewer"] = (rep.sessions / rep.viewers).round(1)
display(rep.sort_values("viewers", ascending=False))

# COMMAND ----------

# ---- 9. GEO CHECK AT SOURCE: why the report shows 0 sessions outside the 22 markets ------------------
# The report's source query (cell 1) already has  WHERE country IN (<22 markets>) , so EntertainmentMovies_SD
# cannot contain outside sessions and the geo "sanity check" is 100% covered by construction.
# This cell reruns the same query on [dbviews].[ViewerSessions] WITHOUT that filter (uses jdbcUrl / jdbcUsername /
# jdbcPassword already defined in cell 1) and counts 404 sessions inside vs outside the 22 markets.
# SQL Server has no regex look-arounds, so the 404 rule is approximated with LIKE patterns (no digit next to 404).
geo_query = f"""
SELECT year(startdate) AS yr,
       CASE WHEN lower(trim(country)) IN ({covered_sql}) THEN 'Covered (22 markets)'
            WHEN country IS NULL OR trim(country) = '' THEN 'Unknown' ELSE 'Outside' END AS geo,
       COUNT(*) AS sessions,
       SUM(CASE WHEN startuperror = '1' OR (errorlist IS NOT NULL AND ltrim(rtrim(errorlist)) <> '') THEN 1 ELSE 0 END) AS err,
       SUM(CASE WHEN errorlist LIKE '%404%' AND errorlist NOT LIKE '%[0-9]404%' AND errorlist NOT LIKE '%404[0-9]%' THEN 1 ELSE 0 END) AS e404
FROM [dbviews].[ViewerSessions] (nolock)
WHERE startdate >= '2025-01-01' AND startdate <= '{END_DATE}'
  AND viewerid IS NOT NULL AND viewerid NOT LIKE '%.%'
GROUP BY year(startdate),
       CASE WHEN lower(trim(country)) IN ({covered_sql}) THEN 'Covered (22 markets)'
            WHEN country IS NULL OR trim(country) = '' THEN 'Unknown' ELSE 'Outside' END"""
geo_sdf = (spark.read.format("jdbc").option("url", jdbcUrl).option("user", jdbcUsername).option("password", jdbcPassword)
           .option("query", geo_query).load())
geo = geo_sdf.toPandas()
geo["pct_of_year_404"] = (geo.e404 / geo.groupby("yr").e404.transform("sum") * 100).round(2)
geo["pct_of_year_errors"] = (geo.err / geo.groupby("yr").err.transform("sum") * 100).round(2)
display(geo.sort_values(["yr", "geo"]))
# Error 360 says: ~2% of 404s and ~19% of all error sessions are outside the 22 markets (Jan-Oct 2026).

# COMMAND ----------

# ---- 10. Like-for-like with Error 360: Jan-Oct 2026 (cut-off 31 Oct) --------------------------------
lfl = spark.sql("""
SELECT COUNT(*) AS sessions, SUM(is_err) AS err, SUM(is_404) AS e404,
       ROUND(SUM(is_404) / SUM(is_err) * 100, 2) AS pct_404_of_errors
FROM v404 WHERE yr = 2026 AND startdate <= DATE'2026-10-31'""").first()
print(dict(lfl.asDict()))
print("Error 360 (all countries): 404 = 219.7K, errors = 8.10M, 2.7%. After removing its outside-market share (2% / 19%):",
      "404 ~ %.1fK, errors ~ %.2fM" % (219.7 * 0.98, 8.10 * 0.81))
jan_oct25 = spark.sql("SELECT SUM(is_err) AS err FROM v404 WHERE yr = 2025 AND startdate <= DATE'2025-10-31'").first()["err"]
print("2025 Jan-Oct error sessions (22 markets):", jan_oct25, "| Error 360 (all countries): 2.70M")

# COMMAND ----------

# ---- 11. Scorecard: expected values from the report / deck -> PASS or FAIL ---------------------------
checks = []
def chk(name, expected, actual, tol=0):
    ok = abs(float(expected) - float(actual)) <= tol
    checks.append((name, expected, actual, "PASS" if ok else "FAIL"))

chk("2026 sessions", 173282794, T26.sessions)
chk("2026 error sessions", 6356227, T26.err)
chk("2026 404 sessions", 217195, T26.e404)
chk("2026 other errors", 6139032, T26.other)
chk("2026 404 viewers", 16586, T26.viewers_404)
chk("2025 sessions", 108478924, T25.sessions)
chk("2025 error sessions", 3114080, T25.err)
chk("2025 404 sessions", 3022, T25.e404)
chk("2025 404 viewers", 1772, T25.viewers_404)
chk("2026 404 per 10K", 12.53, T26.per10k, 0.005)
chk("2025 404 per 10K", 0.28, T25.per10k, 0.005)
chk("2026 404 % of errors", 3.42, T26.pct_of_errors, 0.005)
chk("2025 404 % of errors", 0.10, T25.pct_of_errors, 0.005)
chk("Mar 2026 avg 404 per day", 4764.3, m26.loc[3, "avg_daily_404"], 0.05)
chk("Mar 2026 avg of daily % of errors", 33.79, m26.loc[3, "avg_daily_pct"], 0.005)
chk("Aug 2025 avg 404 per day", 27.3, m25.loc[8, "avg_daily_404"], 0.05)
chk("Nov 2026 days of data", 3, m26.loc[11, "days"])
chk("Spike line (median + 3 MAD)", 67, round(thr), 0)
chk("Median day", 14, round(med), 0)
chk("Spike days 2026", 71, int(d26.spike.sum()))
chk("Worst day 404 sessions", 25046, d26.e404.max())
chk("Days >= 10,000", 3, int((d26.e404 >= 10000).sum()))
chk("Top 15 days 404 sum", 132774, d26.nlargest(15, "e404").e404.sum())
chk("Days in 2026 series", 307, len(d26))
chk("Device table 404s (all OS)", 217195, dev.e404.sum())
chk("Live + VOD 404s", 217195, cont[cont.cut == "Live/VOD"].e404.sum())
chk("Content cut 404s", 217195, cont[cont.cut == "Content"].e404.sum())
chk("Live share of 404s (%)", 98.7, float(cont[(cont.cut == "Live/VOD") & (cont.value == "Live")].share_of_404s_correct.iloc[0]), 0.05)
chk("Repeat bands: viewers", 16586, rep.viewers.sum())
chk("Repeat bands: sessions", 217195, rep.sessions.sum())
chk("21+ band share of sessions (%)", 84.7, float(rep[rep.band == "21+"].session_share.iloc[0]), 0.05)
sc = pd.DataFrame(checks, columns=["check", "expected", "actual", "status"])
display(sc)
print("PASS %d / FAIL %d" % ((sc.status == "PASS").sum(), (sc.status == "FAIL").sum()))
