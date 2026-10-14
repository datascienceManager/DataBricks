# Databricks notebook source
# MAGIC %md
# MAGIC # No First Watch Analysis (PySpark)
# MAGIC
# MAGIC Finds **active D2C subscribers who have never watched more than `min_minutes` minutes**, then explains why:
# MAGIC cohort size, new vs winback, viewing attempts, content, errors, device, quality and device sharing, plus a year-on-year zero-minute comparison.
# MAGIC
# MAGIC **How to run:** set the widgets at the top (report date, threshold, paths), then *Run all*. Every analysis is a named DataFrame you can `display()` or write out.
# MAGIC
# MAGIC | Parameter | Meaning | Default |
# MAGIC |---|---|---|
# MAGIC | `report_date` | Subscriptions must be active on this date | 2026-10-10 |
# MAGIC | `history_start` | Viewing is summed from this date | 2022-01-01 |
# MAGIC | `min_minutes` | Cohort = lifetime minutes <= this value | 1 |
# MAGIC | `require_asset_id` | Drop viewing rows with a missing asset ID | false |
# MAGIC | `yoy_basis` | `original` or `as_of_each_year` (see YoY section) | original |
# MAGIC | `write_output` / `output_path` | Save result tables as CSV | false |

# COMMAND ----------

# DBTITLE 1,Parameters (widgets)
from pyspark.sql import functions as F, Window
import datetime as dt

dbutils.widgets.text("report_date", "2026-10-10", "Report date (YYYY-MM-DD)")
dbutils.widgets.text("history_start", "2022-01-01", "Viewing history start")
dbutils.widgets.text("min_minutes", "1", "Cohort threshold (minutes)")
dbutils.widgets.dropdown("require_asset_id", "false", ["false", "true"], "Require asset_id")
dbutils.widgets.dropdown("yoy_basis", "original", ["original", "as_of_each_year"], "YoY basis")
dbutils.widgets.text("viewing_path", "dbfs:/mnt/viewershipinfo/parquet/ALL_V2/EntertainmentMovies_SD", "Viewing parquet path")
dbutils.widgets.text("subscriber_path", "dbfs:/mnt/dimsubscriber/parquet/ALL/DIMSubscriber", "Subscriber parquet path")
dbutils.widgets.dropdown("write_output", "false", ["false", "true"], "Write CSV output")
dbutils.widgets.text("output_path", "/mnt/test2/NoFirstWatch/Final", "Output folder")

REPORT_DATE      = dbutils.widgets.get("report_date")
HISTORY_START    = dbutils.widgets.get("history_start")
MIN_MINUTES      = float(dbutils.widgets.get("min_minutes"))
REQUIRE_ASSET_ID = dbutils.widgets.get("require_asset_id") == "true"
YOY_BASIS        = dbutils.widgets.get("yoy_basis")
VIEWING_PATH     = dbutils.widgets.get("viewing_path")
SUBSCRIBER_PATH  = dbutils.widgets.get("subscriber_path")
WRITE_OUTPUT     = dbutils.widgets.get("write_output") == "true"
OUTPUT_PATH      = dbutils.widgets.get("output_path")

# Scale-aware tuning (as in the R version, sized for ~800M viewing rows)
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))

print(f"report_date={REPORT_DATE} | history_start={HISTORY_START} | min_minutes={MIN_MINUTES} | "
      f"require_asset_id={REQUIRE_ASSET_ID} | yoy_basis={YOY_BASIS}")

# COMMAND ----------

# DBTITLE 1,Helpers
def valid_str(c):
    """True when a string column is not null, empty or the text 'null'."""
    return F.col(c).isNotNull() & (F.trim(F.col(c)) != "") & (F.lower(F.trim(F.col(c))) != "null")

def save(df, name):
    """Optionally write a result table as a single CSV folder."""
    if WRITE_OUTPUT:
        (df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUTPUT_PATH}/{name}"))
        print(f"saved -> {OUTPUT_PATH}/{name}")

def is_err(c="errorlist"):
    return valid_str(c)

# COMMAND ----------

# DBTITLE 1,Load data (lazy, column-pruned)
VIEWING_COLS = [
    "viewerId", "country", "deviceos", "isp", "startdate", "playingunixtimems", "bufferingunixtimems",
    "bufferingtimems", "interrupts", "startuperror", "averagebitratekbps", "convivasessionid",
    "errorlist", "percentagecomplete", "asset_id", "assetname", "deviceid", "dvhwt",
    "contentType", "competitionName", "playingtimemin", "ContentType2",
]
viewing_raw = spark.read.parquet(VIEWING_PATH)
missing = [c for c in VIEWING_COLS if c not in viewing_raw.columns]
assert not missing, f"Viewing table is missing columns: {missing}"

viewing = (viewing_raw.select(*VIEWING_COLS)
           .withColumn("startdate", F.to_date("startdate"))
           .filter(F.col("startdate").between(F.lit(HISTORY_START), F.lit(REPORT_DATE)))
           .filter(valid_str("viewerId")))
if REQUIRE_ASSET_ID:
    viewing = viewing.filter(valid_str("asset_id"))

dim_raw = spark.read.parquet(SUBSCRIBER_PATH)
dim = (dim_raw
       .withColumn("Subscription_start_date", F.to_date("Subscription_start_date"))
       .withColumn("Expiry_date", F.to_date("Expiry_date")))

# Paying-subscriber base shared by every analysis (excludes promo / non-commercial / daily-weekly / product passes)
dim_commercial = (dim.filter(
    F.col("product_type").isNull()
    & (F.col("non_commercial") != 1) & (F.col("daily_weekly_flag") != 1) & (F.col("promo") != 1)
    & (F.col("Offer_type") == "Subscription")
    & F.col("Customer_External_ID").isNotNull())
    .filter(valid_str("Customer_External_ID")))

print("viewing columns:", len(VIEWING_COLS), "| subscriber columns:", len(dim.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cohort: active D2C subscribers with lifetime viewing <= threshold

# COMMAND ----------

# DBTITLE 1,Lifetime minutes per viewer
viewer_minutes = (viewing.groupBy("viewerId")
                  .agg(F.sum(F.col("playingunixtimems") / 60000.0).alias("TotalM")))

def active_on(df, as_of):
    return df.filter((F.col("Subscription_start_date") <= F.lit(as_of)) & (F.col("Expiry_date") >= F.lit(as_of)))

active_d2c = (active_on(dim_commercial.filter(F.col("Subscription_latest") == 1), REPORT_DATE)
              .filter(F.col("D2C_B2B") == "D2C")
              .select("Customer_External_ID", "Subscription_start_date", "Expiry_date", "D2C_B2B", "Subscription_type"))

# Left join keeps subscribers with no viewing row at all (TotalM is NULL)
cohort = (active_d2c.join(viewer_minutes, active_d2c.Customer_External_ID == viewer_minutes.viewerId, "left")
          .filter(F.col("TotalM").isNull() | (F.col("TotalM") <= MIN_MINUTES))
          .withColumn("TimeIntervalFlag",
                      F.when(F.col("TotalM").isNull(), "None")                 # no playback session on record
                       .when(F.col("TotalM") == 0, "NoViewingMins")            # sessions, but zero minutes
                       .otherwise("Greater0Mins_UpToThreshold"))
          .select("Customer_External_ID", "Subscription_start_date", "Expiry_date", "TotalM", "TimeIntervalFlag")
          .cache())

cohort_size = cohort.select("Customer_External_ID").distinct().count()
print(f"No-first-watch cohort (active D2C, <= {MIN_MINUTES} min lifetime): {cohort_size:,}")
save(cohort, "NoFirstWatch_Cohort")

# COMMAND ----------

# DBTITLE 1,Distribution of the cohort
cohort_distribution = (cohort.groupBy("TimeIntervalFlag")
                       .agg(F.countDistinct("Customer_External_ID").alias("TotalUniqueViewer"))
                       .withColumn("PctOfCohort", F.round(F.col("TotalUniqueViewer") / F.lit(cohort_size) * 100, 1)))
display(cohort_distribution)
save(cohort_distribution, "Cohort_Distribution")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. New vs winback (zero-minute subscribers)

# COMMAND ----------

zero_minute_ids = (cohort.filter(F.col("TimeIntervalFlag").isin("None", "NoViewingMins"))
                   .select("Customer_External_ID").distinct())

# All subscription rows (not only the latest) of these users; keep each user's latest-expiring subscription
w_latest = Window.partitionBy("Customer_External_ID").orderBy(
    F.col("Expiry_date").desc(), F.col("Subscription_start_date").desc(), F.col("Subscription_ID").desc())

new_winback = (dim_commercial.filter(F.col("Subscription_type").isin("new", "winback"))
               .join(zero_minute_ids, "Customer_External_ID", "inner")
               .select("Customer_External_ID", "Subscription_ID", "Subscription_type",
                       "Subscription_start_date", "Expiry_date", "Subscription_status")
               .withColumn("rn", F.row_number().over(w_latest))
               .filter("rn = 1").drop("rn").cache())

new_winback_summary = (new_winback.groupBy("Subscription_type")
                       .agg(F.countDistinct("Customer_External_ID").alias("TotalUnqSubs")))
display(new_winback_summary)
save(new_winback_summary, "New_Winback_Summary")

winback_users = new_winback.filter(F.col("Subscription_type") == "winback")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Session-level view of cohort users who have session records

# COMMAND ----------

# Users with a session record but <= threshold minutes (original: 'NoViewingMins' + '0-1 min' buckets)
users_with_sessions = (cohort.filter(F.col("TimeIntervalFlag") != "None")
                       .select(F.col("Customer_External_ID").alias("viewerId")).distinct())

SESSION_KEYS = ["viewerId", "deviceos", "country", "isp", "startdate", "bufferingunixtimems", "bufferingtimems",
                "interrupts", "startuperror", "averagebitratekbps", "convivasessionid", "errorlist",
                "percentagecomplete", "assetname", "deviceid", "dvhwt", "contentType", "competitionName", "ContentType2"]

sessions = (viewing.join(F.broadcast(users_with_sessions), "viewerId", "inner")
            .groupBy(*SESSION_KEYS)
            .agg(F.sum("playingtimemin").alias("TotalMins"))
            .withColumn("Customer_External_ID", F.col("viewerId"))
            # -1 is a placeholder for 'unknown' in percentagecomplete; exclude it from averages
            .withColumn("pct_complete_clean", F.when(F.col("percentagecomplete") >= 0, F.col("percentagecomplete")))
            .withColumn("startup_err", F.when(F.col("startuperror") > 0, 1).otherwise(0))
            .withColumn("has_error", F.when(is_err(), 1).otherwise(0))
            .cache())
print(f"sessions: {sessions.count():,} | users: {sessions.select('Customer_External_ID').distinct().count():,}")

# COMMAND ----------

# DBTITLE 1,Per-user session summary (cohort users with session records)
final_less_than_min_per_user = (sessions.groupBy("Customer_External_ID", "assetname", "startdate", "deviceid")
                                .agg(F.sum("TotalMins").alias("TotalMins")))
display(final_less_than_min_per_user.limit(1000))
save(final_less_than_min_per_user, "Sessions_PerUser_Asset_Day")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Winback users: did they try to watch?

# COMMAND ----------

winback_viewing = winback_users.join(sessions, "Customer_External_ID", "inner").cache()

total_winback = winback_users.select("Customer_External_ID").distinct().count()
with_attempts = winback_viewing.select("Customer_External_ID").distinct().count()
print("===== WINBACK NO-FIRST-WATCH OVERVIEW =====")
print(f"Winback zero-minute users              : {total_winback:,}")
print(f"... with viewing attempts (<= threshold): {with_attempts:,}")
print(f"... with zero attempts                  : {total_winback - with_attempts:,}")

# COMMAND ----------

winback_user_summary = (winback_viewing
    .groupBy("Customer_External_ID", "Subscription_start_date", "Expiry_date", "Subscription_status")
    .agg(F.count("*").alias("total_sessions"),
         F.sum("TotalMins").alias("total_playing_mins"),
         F.countDistinct("assetname").alias("unique_assets"),
         F.countDistinct("deviceid").alias("unique_devices"),
         F.sum("startup_err").alias("sessions_with_error"),
         F.avg("bufferingunixtimems").alias("avg_buffering_ms"),
         F.min("startdate").alias("first_attempt_date"),
         F.max("startdate").alias("last_attempt_date"))).cache()
display(winback_user_summary)
save(winback_user_summary, "Winback_User_Summary")

# COMMAND ----------

# DBTITLE 1,Monthly attempt trend
monthly_trends = (winback_viewing
    .withColumn("attempt_month", F.date_format("startdate", "yyyy-MM"))
    .groupBy("attempt_month")
    .agg(F.count("*").alias("total_sessions"),
         F.countDistinct("Customer_External_ID").alias("unique_users"),
         F.sum("startup_err").alias("sessions_with_err"),
         F.avg("TotalMins").alias("avg_playing_mins"),
         F.countDistinct("assetname").alias("unique_assets"))
    .orderBy("attempt_month"))
display(monthly_trends)
save(monthly_trends, "Winback_Monthly_Trends")

# COMMAND ----------

# DBTITLE 1,Days from subscription start to first attempt
days_to_attempt = (winback_user_summary
    .withColumn("days_sub_to_first_attempt", F.datediff("first_attempt_date", "Subscription_start_date"))
    .withColumn("days_sub_to_last_attempt", F.datediff("last_attempt_date", "Subscription_start_date"))
    .withColumn("days_active_window", F.datediff("last_attempt_date", "first_attempt_date")))

d = F.col("days_sub_to_first_attempt")
days_bucket_analysis = (days_to_attempt
    .withColumn("attempt_speed",
                F.when(d <= 0, "0_On_or_Before_Start")      # includes attempts from earlier subscriptions
                 .when(d <= 3, "1_Within_3_Days").when(d <= 7, "2_Within_1_Week")
                 .when(d <= 30, "3_Within_1_Month").when(d <= 90, "4_Within_3_Months")
                 .when(d > 90, "5_After_3_Months").otherwise("6_Unknown"))
    .groupBy("attempt_speed")
    .agg(F.count("*").alias("user_count"),
         F.avg("total_sessions").alias("avg_sessions"),
         F.avg(F.col("sessions_with_error") / F.col("total_sessions") * 100).alias("avg_error_pct"))
    .orderBy("attempt_speed"))
display(days_bucket_analysis)
save(days_bucket_analysis, "Winback_Days_To_First_Attempt")

# COMMAND ----------

# DBTITLE 1,What did they try to watch?
top_content = (winback_viewing
    .groupBy("assetname", "ContentType2", "competitionName")
    .agg(F.count("*").alias("session_count"),
         F.countDistinct("Customer_External_ID").alias("unique_users"),
         (F.avg("startup_err") * 100).alias("error_rate_pct"),
         F.avg("TotalMins").alias("avg_playing_mins"))
    .orderBy(F.desc("session_count")).limit(25))
display(top_content)
save(top_content, "Winback_Top_Content")

# COMMAND ----------

# DBTITLE 1,Winback error codes, device OS and subscription status
winback_errors = (winback_viewing.filter(is_err())
    .groupBy("errorlist")
    .agg(F.count("*").alias("session_count"), F.countDistinct("Customer_External_ID").alias("unique_users"))
    .orderBy(F.desc("session_count")).limit(15))
display(winback_errors)

winback_device = (winback_viewing.groupBy("deviceos")
    .agg(F.count("*").alias("total_sessions"), F.countDistinct("Customer_External_ID").alias("unique_users"),
         (F.avg("startup_err") * 100).alias("error_rate_pct"), F.avg("TotalMins").alias("avg_playing_mins"))
    .orderBy(F.desc("total_sessions")))
display(winback_device)

winback_sub_status = (winback_user_summary.groupBy("Subscription_status")
    .agg(F.count("*").alias("user_count"), F.avg("total_sessions").alias("avg_sessions"),
         F.avg("unique_assets").alias("avg_unique_assets"), F.avg("sessions_with_error").alias("avg_error_sessions"),
         F.avg("total_playing_mins").alias("avg_playing_mins"))
    .orderBy(F.desc("user_count")))
display(winback_sub_status)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Why did playback fail? (all cohort users with session records)

# COMMAND ----------

# DBTITLE 1,1a. Overall session quality
quality_summary = sessions.agg(
    F.count("*").alias("total_sessions"),
    F.countDistinct("Customer_External_ID").alias("unique_users"),
    F.sum("startup_err").alias("sessions_with_startup_error"),
    F.sum("has_error").alias("sessions_with_errors"),
    F.sum(F.when(F.col("interrupts") > 0, 1).otherwise(0)).alias("sessions_with_interrupts"),
    F.avg("TotalMins").alias("avg_playing_mins"),
    F.avg("bufferingunixtimems").alias("avg_buffering_ms"),
    F.avg("interrupts").alias("avg_interrupts"),
    F.avg("pct_complete_clean").alias("avg_pct_complete"),
    F.avg("averagebitratekbps").alias("avg_bitrate_kbps"))
display(quality_summary)
save(quality_summary, "Quality_Summary")

# COMMAND ----------

# DBTITLE 1,1b. Startup error impact
startup_error_analysis = (sessions
    .withColumn("startup_error_flag", F.when(F.col("startuperror") > 0, "Has_Startup_Error").otherwise("No_Startup_Error"))
    .groupBy("startup_error_flag")
    .agg(F.count("*").alias("total_sessions"), F.countDistinct("Customer_External_ID").alias("unique_users"),
         F.avg("TotalMins").alias("avg_playing_mins"), F.avg("bufferingunixtimems").alias("avg_buffering_ms"),
         F.avg("pct_complete_clean").alias("avg_pct_complete"), F.avg("averagebitratekbps").alias("avg_bitrate_kbps")))
display(startup_error_analysis)
save(startup_error_analysis, "Startup_Error_Impact")

# COMMAND ----------

# DBTITLE 1,1c. Top error codes, with a category column
e = F.lower(F.col("errorlist"))
error_category = (F.when(e.contains("not_entitled") | e.contains("do not have access") | e.contains("tod.tv"), "Entitlement")
                   .when(e.contains("403") | e.contains("-1102") | e.contains("not authorized")
                         | e.contains("could_not_load_manifest") | e.contains("1208"), "HTTP 403 / manifest / not authorised")
                   .otherwise("Other"))

error_distribution = (sessions.filter(is_err())
    .groupBy("errorlist")
    .agg(F.count("*").alias("session_count"), F.countDistinct("Customer_External_ID").alias("unique_users"),
         F.avg("TotalMins").alias("avg_playing_mins"))
    .withColumn("error_category", error_category)
    .orderBy(F.desc("session_count")).limit(20))
display(error_distribution)
save(error_distribution, "Top_Error_Codes")

error_category_summary = (sessions.filter(is_err())
    .withColumn("error_category", error_category)
    .groupBy("error_category").agg(F.count("*").alias("session_count"))
    .withColumn("pct_of_errored_sessions", F.round(F.col("session_count") / F.sum("session_count").over(Window.partitionBy()) * 100, 1))
    .orderBy(F.desc("session_count")))
display(error_category_summary)
save(error_category_summary, "Error_Category_Summary")

# COMMAND ----------

# DBTITLE 1,1d-1g. Buffering, interrupts, bitrate, percentage complete
def bucket_report(df, bucket_col, expr, aggs, name):
    out = (df.withColumn(bucket_col, expr).groupBy(bucket_col).agg(*aggs).orderBy(bucket_col))
    display(out); save(out, name); return out

common = [F.count("*").alias("total_sessions"), F.countDistinct("Customer_External_ID").alias("unique_users"),
          F.avg("TotalMins").alias("avg_playing_mins")]

b = F.col("bufferingunixtimems")
buffering_analysis = bucket_report(sessions, "buffering_bucket",
    F.when(b.isNull() | (b == 0), "0_No_Buffering").when(b <= 5000, "1_Under_5sec").when(b <= 30000, "2_5sec_to_30sec")
     .when(b <= 60000, "3_30sec_to_1min").otherwise("4_Over_1min"),
    common + [F.avg("pct_complete_clean").alias("avg_pct_complete")], "Buffering_Buckets")

i = F.col("interrupts")
interrupt_analysis = bucket_report(sessions, "interrupt_bucket",
    F.when(i.isNull() | (i == 0), "0_No_Interrupts").when(i <= 3, "1_Low_1to3").when(i <= 10, "2_Medium_4to10").otherwise("3_High_10plus"),
    common + [F.avg("bufferingunixtimems").alias("avg_buffering_ms")], "Interrupt_Buckets")

br = F.col("averagebitratekbps")
bitrate_analysis = bucket_report(sessions, "bitrate_quality",
    F.when(br.isNull() | (br == 0), "0_No_Bitrate").when(br <= 500, "1_Very_Low").when(br <= 2000, "2_Low_SD")
     .when(br <= 5000, "3_Medium_HD").otherwise("4_High_FullHD_Plus"),
    common + [F.avg("pct_complete_clean").alias("avg_pct_complete"), F.avg("interrupts").alias("avg_interrupts")], "Bitrate_Buckets")

p = F.col("percentagecomplete")
pct_complete_analysis = bucket_report(sessions, "pct_bucket",
    F.when(p.isNull() | (p < 0), "5_Unknown").when(p == 0, "0_Not_Started_0pct").when(p <= 10, "1_Barely_Watched_1to10pct")
     .when(p <= 50, "2_Partial_11to50pct").when(p <= 90, "3_Mostly_51to90pct").otherwise("4_Near_Complete_91to100pct"),
    common + [F.avg("bufferingunixtimems").alias("avg_buffering_ms"), F.avg("interrupts").alias("avg_interrupts")], "PctComplete_Buckets")

# COMMAND ----------

# DBTITLE 1,1h. Device OS impact
device_analysis = (sessions.groupBy("deviceos")
    .agg(F.count("*").alias("total_sessions"), F.countDistinct("Customer_External_ID").alias("unique_users"),
         F.avg("TotalMins").alias("avg_playing_mins"), F.avg("bufferingunixtimems").alias("avg_buffering_ms"),
         (F.avg("startup_err") * 100).alias("startup_error_pct"), (F.avg("has_error") * 100).alias("error_session_pct"),
         F.avg("pct_complete_clean").alias("avg_pct_complete"))
    .orderBy(F.desc("total_sessions")))
display(device_analysis)
save(device_analysis, "Device_OS_Impact")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Device sharing: same device ID across several customers

# COMMAND ----------

device_to_customer_mapping = (sessions.filter(valid_str("deviceid"))
    .groupBy("deviceid")
    .agg(F.countDistinct("Customer_External_ID").alias("unique_customers"), F.count("*").alias("total_sessions"))
    .filter("unique_customers > 1").orderBy(F.desc("unique_customers")))

device_sharing_summary = device_to_customer_mapping.agg(
    F.count("*").alias("total_shared_devices"), F.avg("unique_customers").alias("avg_customers_per_device"),
    F.max("unique_customers").alias("max_customers_per_device"), F.sum("total_sessions").alias("total_sessions_on_shared"))
display(device_sharing_summary)

uc = F.col("unique_customers")
device_sharing_distribution = (device_to_customer_mapping
    .withColumn("sharing_bucket", F.when(uc == 2, "2_customers").when(uc == 3, "3_customers")
                .when(uc <= 5, "4_to_5_customers").when(uc <= 10, "6_to_10_customers").otherwise("10plus_customers"))
    .groupBy("sharing_bucket").agg(F.count("*").alias("device_count"), F.sum("total_sessions").alias("total_sessions"))
    .orderBy("sharing_bucket"))
display(device_sharing_distribution)
display(device_to_customer_mapping.limit(20))
save(device_to_customer_mapping, "Shared_Devices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Year-on-year: zero-minute (inactive) subscribers, D2C vs B2B
# MAGIC
# MAGIC `yoy_basis = original` reproduces the R logic: last year's active subscribers are checked against viewing **up to the report date**.
# MAGIC That means a subscriber who was active in 2025 but started watching later in 2026 is *not* counted as inactive in 2025, so the prior-year count is a floor and growth is slightly overstated.
# MAGIC `yoy_basis = as_of_each_year` measures each year against viewing up to that year's own date, which is like-for-like.

# COMMAND ----------

def run_zero_minutes_analysis(report_date=REPORT_DATE, history_start=HISTORY_START, min_minutes=MIN_MINUTES,
                              require_asset_id=True, basis=YOY_BASIS):
    """Zero-minute active subscribers by D2C/B2B for report_date vs the same date a year earlier."""
    rd = F.lit(report_date).cast("date")
    prev_rd = F.add_months(rd, -12)
    prev_str = spark.range(1).select(prev_rd.alias("d")).first()["d"].strftime("%Y-%m-%d")

    def viewers_over(cutoff):
        v = (viewing_raw.select("viewerId", "startdate", "asset_id", "playingunixtimems")
             .withColumn("startdate", F.to_date("startdate"))
             .filter(F.col("startdate").between(F.lit(history_start), F.lit(cutoff))).filter(valid_str("viewerId")))
        if require_asset_id:
            v = v.filter(valid_str("asset_id"))
        return (v.groupBy("viewerId").agg(F.sum(F.col("playingunixtimems") / 60000.0).alias("TotalM"))
                .filter(F.col("TotalM") > min_minutes).select("viewerId"))

    viewers_now = viewers_over(report_date)
    viewers_prev = viewers_now if basis == "original" else viewers_over(prev_str)

    base = dim_commercial.filter(F.col("Subscription_latest") == 1)

    def inactive(as_of, viewers, label):
        a = active_on(base, as_of)
        return (a.join(viewers, a.Customer_External_ID == viewers.viewerId, "left_anti")
                .groupBy("D2C_B2B").agg(F.countDistinct("Customer_External_ID").alias(label)))

    cur = inactive(report_date, viewers_now, "Total_Inactive_User_Current")
    prv = inactive(prev_str, viewers_prev, "Total_Inactive_User_Previous")
    return (cur.join(prv, "D2C_B2B", "full")
            .na.fill(0, ["Total_Inactive_User_Current", "Total_Inactive_User_Previous"])
            .withColumn("Difference", F.col("Total_Inactive_User_Current") - F.col("Total_Inactive_User_Previous"))
            .withColumn("PercentageChange", F.round(F.col("Difference") / F.when(F.col("Total_Inactive_User_Previous") == 0, None)
                                                    .otherwise(F.col("Total_Inactive_User_Previous")) * 100, 2))
            .withColumn("PreviousDate", F.lit(prev_str)).withColumn("CurrentDate", F.lit(report_date)))

zero_minutes_yoy = run_zero_minutes_analysis()
display(zero_minutes_yoy)
save(zero_minutes_yoy, "ZeroMinutes_YoY")

# COMMAND ----------

# DBTITLE 1,Run summary
print("=========== RUN SUMMARY ===========")
print(f"Report date            : {REPORT_DATE}")
print(f"Cohort threshold       : <= {MIN_MINUTES} min lifetime viewing (since {HISTORY_START})")
print(f"No-first-watch cohort  : {cohort_size:,}")
print(f"Winback zero-minute    : {total_winback:,} (with attempts: {with_attempts:,})")
print("Result tables are Spark DataFrames in this notebook; set write_output=true to export CSVs.")
