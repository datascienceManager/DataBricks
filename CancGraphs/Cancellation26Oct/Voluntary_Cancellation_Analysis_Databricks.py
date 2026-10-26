# Databricks notebook source
# MAGIC %md
# MAGIC # Voluntary Cancellation Analysis (PySpark, start-status basis)
# MAGIC
# MAGIC Every subscription **starts** as New or Win-back and **ends** as Churned, so each cancelled subscription is classified by the
# MAGIC status it started with. That keeps the review window and history like for like.
# MAGIC
# MAGIC **Questions:** (1) What changed? (2) Is this normal behaviour, particularly for win-backs? (3) What happened after cancellation?
# MAGIC
# MAGIC | Widget | Meaning | Default |
# MAGIC |---|---|---|
# MAGIC | `FROM` / `TO` | Cancellation window under review | 2026-08-01 / 2026-09-30 |
# MAGIC | `HIST_FROM` | Start of history for the run rate | 2025-01-01 |
# MAGIC | `ASOF` | Latest date in the data | 2026-10-13 |
# MAGIC | `MATURE_DAYS` | Days after expiry before 'returned?' is read | 30 |
# MAGIC | `VIEW_TBL` | Viewing table / view name | EntertainmentMovies_SD |
# MAGIC | `SUFFIX_LEN` | Viewing `subscriptionid` = Subscription_ID + N-digit suffix | 6 |
# MAGIC | `OUT_DIR` | Folder for chart PNGs | /dbfs/FileStore/cancellation_charts |
# MAGIC | `SUB_TBL` / `TXN_TBL` | Subscriber and transaction table / view names | DIM_SubscribersInfo_SDF / DIM_Transactions_SD |
# MAGIC | `*_PATH` | Optional parquet paths, used only if the view does not exist yet | |
# MAGIC
# MAGIC Run all. Every result is a Spark or pandas DataFrame shown with `display()`; charts are saved to `OUT_DIR` in grey / light grey.

# COMMAND ----------

# DBTITLE 1,Parameters
from pyspark.sql import functions as F
import pandas as pd, numpy as np, os, re, datetime as dt

W = {
    "FROM": "2026-08-01", "TO": "2026-09-30", "HIST_FROM": "2025-01-01", "ASOF": "2026-10-13",
    "MATURE_DAYS": "30", "VIEW_TBL": "EntertainmentMovies_SD", "SUFFIX_LEN": "6",
    "OUT_DIR": "/dbfs/FileStore/cancellation_charts",
    "SUB_TBL": "DIM_SubscribersInfo_SDF", "TXN_TBL": "DIM_Transactions_SD",
    "VIEW_PATH": "dbfs:/mnt/viewershipinfo/parquet/ALL_V2/EntertainmentMovies_SD",
    "SUB_PATH": "dbfs:/mnt/dimsubscriber/parquet/ALL/DIMSubscriber",
    "TXN_PATH": "",
}
for k, v in W.items():
    dbutils.widgets.text(k, v, k)
P = {k: dbutils.widgets.get(k) for k in W}

spark.conf.set("spark.sql.shuffle.partitions", "400")
print(P)

# COMMAND ----------

# DBTITLE 1,Helpers: {PARAM} substitution, register, charts
def fill(sql, p=P):
    for k, v in p.items():
        sql = sql.replace("{" + k + "}", str(v))
    return sql

def q(sql):
    return spark.sql(fill(sql))

def reg(df, name):
    df.createOrReplaceTempView(name)
    return df

def ensure_view(name, path):
    """Register a parquet path as a view if the view is not already defined in this session."""
    if spark.catalog.tableExists(name):
        return
    if not path:
        raise ValueError(f"View {name} not found and no path given")
    spark.read.parquet(path).createOrReplaceTempView(name)
    print(f"registered {name} from {path}")

ensure_view(P["VIEW_TBL"], P["VIEW_PATH"])
ensure_view(P["SUB_TBL"], P["SUB_PATH"])
ensure_view(P["TXN_TBL"], P["TXN_PATH"])

# Charts: grey palette (New = light grey, Win-back = dark grey)
import matplotlib
import matplotlib.pyplot as plt
NEW_C, WB_C, MID, INK, MUT, GRID = "#C9CED8", "#6B7280", "#9AA1AF", "#222B3A", "#5B6577", "#E3E7EE"
COL = {"new": NEW_C, "winback": WB_C}
LAB = {"new": "New", "winback": "Win-back"}
os.makedirs(P["OUT_DIR"], exist_ok=True)

def new_ax(w=11, h=5.5, title="", subtitle=""):
    fig, ax = plt.subplots(figsize=(w, h), dpi=120)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.spines["left"].set_color(NEW_C); ax.spines["bottom"].set_color(NEW_C)
    ax.tick_params(length=0, labelcolor=MUT)
    fig.suptitle(title, x=0.01, ha="left", fontsize=14, fontweight="bold", color=INK)
    if subtitle:
        ax.set_title(subtitle, loc="left", fontsize=10, color=MUT)
    return fig, ax

def out_plot(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(P["OUT_DIR"], name + ".png"), dpi=200, facecolor="white")
    display(fig); plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Subscription lifecycle table (one row per subscription)

# COMMAND ----------

base_rows = reg(q("""
  SELECT Customer_ID, Customer_External_ID, Subscription_ID, Subscription_version, Subscription_latest,
         Subscription_type, Subscription_status, to_date(Subscription_start_date) AS start_date,
         to_date(Expiry_date) AS expiry_date, to_date(Cancellation_date) AS cancel_date,
         Cancellation_reason, churn_reason, Subscription_churn_type, Acquisition_channel, Product_name,
         Offer_period, Country, Source_system
  FROM {SUB_TBL}
  WHERE D2C_B2B = 'D2C' AND offer_type = 'Subscription' AND product_type IS NULL
    AND non_commercial <> 1 AND daily_weekly_flag <> 1
    AND Customer_External_ID IS NOT NULL AND TRIM(Customer_External_ID) <> '' AND TRIM(Customer_External_ID) <> 'null'
"""), "base_rows")

first_v = reg(q("""
  SELECT Subscription_ID, Subscription_type AS start_status FROM (
    SELECT Subscription_ID, Subscription_type,
           ROW_NUMBER() OVER (PARTITION BY Subscription_ID ORDER BY Subscription_version ASC) rn
    FROM base_rows WHERE Subscription_type IN ('new','winback')) WHERE rn = 1
"""), "first_v")

life = reg(q("""
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
         datediff(start_date, prev_paid_through) AS gap_since_prev_paid_through,
         datediff(next_start, expiry_date)  AS days_expiry_to_next_start,
         CASE WHEN cancel_date IS NULL THEN 'No cancel date'
              WHEN datediff(expiry_date, cancel_date) = 0 THEN 'Cancelled on expiry day'
              WHEN datediff(cancel_date, start_date) <= 1 THEN 'Cancelled on purchase day / next day'
              ELSE 'Cancelled mid-period' END AS cancel_type
  FROM o LEFT JOIN first_v f USING (Subscription_ID)
""").cache(), "life")

display(q("SELECT start_status = rule_status AS agree, COUNT(*) n FROM life GROUP BY 1"))
display(q("SELECT start_status, end_status, COUNT(*) subs FROM life GROUP BY 1,2 ORDER BY 1,2"))

cohort = reg(q("SELECT * FROM life WHERE cancel_date >= '{FROM}' AND cancel_date <= '{TO}'").cache(), "cohort")
n_subs = cohort.count(); n_cust = cohort.select("Customer_External_ID").distinct().count()
print(f"Cohort subscriptions: {n_subs:,} | customers: {n_cust:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Q1 What changed? (like for like on start status)

# COMMAND ----------

monthly = q("""
  SELECT date_format(cancel_date,'yyyy-MM') AS m, start_status, COUNT(DISTINCT Customer_External_ID) customers
  FROM life WHERE cancel_date >= '{HIST_FROM}' AND cancel_date <= '{TO}' GROUP BY 1,2""").toPandas()
tot = monthly.groupby("m", as_index=False)["customers"].sum()
base = tot[tot.m < P["FROM"][:7]]
last_m = P["TO"][:7]
last_v = int(tot.loc[tot.m == last_m, "customers"].iloc[0]) if (tot.m == last_m).any() else None
if last_v is not None and len(base) > 1:
    z = (last_v - base.customers.mean()) / base.customers.std()
    print(f"Run rate {base.m.min()}-{base.m.max()}: mean {base.customers.mean():,.0f}, sd {base.customers.std():,.0f}. "
          f"{last_m}: {last_v:,} (z = {z:.2f})")
display(monthly.pivot_table(index="m", columns="start_status", values="customers", fill_value=0).reset_index().sort_values("m"))

display(q("""SELECT date_format(cancel_date,'yyyy-MM') m, start_status, cancel_type, COUNT(*) subs
             FROM life WHERE cancel_date >= date_trunc('year', DATE'{TO}') AND cancel_date <= '{TO}' GROUP BY 1,2,3 ORDER BY 1,2,3"""))
display(q("""SELECT start_status, cancel_type, COUNT(*) subs, ROUND(AVG(paid_period_days),1) avg_paid_period
             FROM cohort GROUP BY 1,2 ORDER BY 1,2"""))
display(q("""
  SELECT CASE WHEN cancel_date >= '{FROM}' THEN 'Review window' ELSE 'History' END period, start_status, COUNT(*) subs,
         ROUND(AVG(CASE WHEN days_start_to_cancel <= 1  THEN 1.0 ELSE 0 END),3) within_1d,
         ROUND(AVG(CASE WHEN days_start_to_cancel <= 7  THEN 1.0 ELSE 0 END),3) within_7d,
         ROUND(AVG(CASE WHEN days_start_to_cancel <= 30 THEN 1.0 ELSE 0 END),3) within_30d,
         percentile_approx(days_start_to_cancel, 0.5) median_days
  FROM life WHERE cancel_date >= '{HIST_FROM}' AND cancel_date <= '{TO}' GROUP BY 1,2 ORDER BY 1,2"""))
display(q("""SELECT date_trunc('week', start_date) wk, start_status, COUNT(DISTINCT Customer_External_ID) starts
             FROM life WHERE start_date >= date_trunc('year', DATE'{TO}') AND start_date <= '{TO}' GROUP BY 1,2 ORDER BY 1,2"""))
display(q("""SELECT start_status, Subscription_churn_type, churn_reason, COUNT(*) subs
             FROM life WHERE end_status = 'churned' AND expiry_date >= '{FROM}' AND expiry_date <= '{TO}'
             GROUP BY 1,2,3 ORDER BY subs DESC"""))

# COMMAND ----------

# DBTITLE 1,Revenue: renewal value forgone (last charge on each cancelled subscription)
last_charge = reg(q("""
  SELECT Subscription_ID, Transaction_price_incl_TAX_USD AS last_charge_usd, Payment_type FROM (
    SELECT Subscription_ID, Transaction_price_incl_TAX_USD, Payment_type,
           ROW_NUMBER() OVER (PARTITION BY Subscription_ID ORDER BY Transaction_Date DESC) rn FROM {TXN_TBL}) WHERE rn = 1"""), "last_charge")
rev = q("""SELECT date_format(c.cancel_date,'yyyy-MM') m, c.start_status, COUNT(*) subs,
                  ROUND(SUM(l.last_charge_usd),0) renewal_value_forgone_usd, ROUND(AVG(l.last_charge_usd),2) avg_last_charge
           FROM life c LEFT JOIN last_charge l USING (Subscription_ID)
           WHERE c.cancel_date >= '{HIST_FROM}' AND c.cancel_date <= '{TO}' GROUP BY 1,2 ORDER BY 1,2""").toPandas()
display(rev)

# COMMAND ----------

# DBTITLE 1,Chart 1 and 1b: monthly cancellations and renewal value forgone
def stacked_monthly(df, value, title, subtitle, name, money=False):
    pv = df.pivot_table(index="m", columns="start_status", values=value, fill_value=0).sort_index()
    x = np.arange(len(pv)); review = [m >= P["FROM"][:7] for m in pv.index]
    fig, ax = new_ax(12, 5.5, title, subtitle)
    bottom = np.zeros(len(pv))
    for st in ("winback", "new"):
        if st in pv:
            for i in x:
                ax.bar(i, pv[st].iloc[i], bottom=bottom[i], color=COL[st], alpha=1 if review[i] else 0.55, width=0.75,
                       label=LAB[st] if i == 0 else None)
            bottom += pv[st].values
    for i in x:
        ax.text(i, bottom[i] * 1.01, (f"${bottom[i]/1000:,.0f}K" if money else f"{bottom[i]/1000:,.1f}K"), ha="center", va="bottom", fontsize=8, color=INK)
    if not money and len(base):
        ax.axhline(base.customers.mean(), ls="--", color=INK, lw=1)
    ax.set_xticks(x); ax.set_xticklabels(pv.index, rotation=45, ha="right"); ax.legend(frameon=False, loc="upper left")
    out_plot(fig, name)

stacked_monthly(monthly, "customers", "Monthly cancelling customers, by the status they started with",
                "Review window at full colour; dashed line = historical average", "01_monthly_cancellations_by_start_status")
stacked_monthly(rev, "renewal_value_forgone_usd", "Renewal value that will not be billed, by month of cancellation",
                "Last charge on each cancelled subscription (USD incl. tax). An estimate, not realised loss.", "02_renewal_value_forgone", money=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Viewing attributed to the cancelled subscription

# COMMAND ----------

v = reg(q("""
  SELECT viewerId, to_date(startdate) AS vdate, convivasessionid, subscriptionid,
         CAST(substr(CAST(subscriptionid AS STRING), 1, length(CAST(subscriptionid AS STRING)) - {SUFFIX_LEN}) AS BIGINT) AS sub_id_view,
         playingunixtimems / 60000.0 AS minutes, startuperror, errorlist, interrupts, bufferingunixtimems / 60000.0 AS buffer_min,
         competitionName, ContentType2, Live, to_date(match_date) AS match_date, deviceos, planname, asset_id,
         CASE WHEN errorlist IS NOT NULL OR startuperror = 1 THEN 1 ELSE 0 END AS has_error,
         CASE WHEN errorlist LIKE '%ntitlement%' THEN 1 ELSE 0 END AS entitlement_error
  FROM {VIEW_TBL}
  WHERE viewerId IN (SELECT DISTINCT Customer_External_ID FROM cohort)""").cache(), "v")
# playingunixtimems is used because playingtimemin can be NULL

sub_view = reg(q("""
  SELECT c.Subscription_ID, c.Customer_External_ID, c.start_status, c.cancel_type, c.start_date, c.cancel_date, c.expiry_date,
         c.Product_name, c.Country, c.Acquisition_channel, c.Cancellation_reason, c.churn_reason, c.days_start_to_cancel, c.next_start,
         COUNT(DISTINCT CASE WHEN v.sub_id_view = c.Subscription_ID THEN v.convivasessionid END) AS own_sessions,
         COUNT(DISTINCT CASE WHEN v.sub_id_view = c.Subscription_ID AND v.minutes >= 1 THEN v.convivasessionid END) AS own_played_sessions,
         COALESCE(SUM(CASE WHEN v.sub_id_view = c.Subscription_ID THEN v.minutes END),0) AS own_minutes,
         COUNT(DISTINCT CASE WHEN v.sub_id_view = c.Subscription_ID THEN v.vdate END) AS own_days,
         COUNT(DISTINCT CASE WHEN v.sub_id_view = c.Subscription_ID THEN v.asset_id END) AS own_assets,
         SUM(CASE WHEN v.sub_id_view = c.Subscription_ID THEN v.has_error ELSE 0 END) AS own_error_sessions,
         SUM(CASE WHEN v.sub_id_view = c.Subscription_ID THEN v.entitlement_error ELSE 0 END) AS own_entitlement_errors,
         COUNT(DISTINCT CASE WHEN v.vdate BETWEEN c.start_date AND COALESCE(c.cancel_date, c.expiry_date) THEN v.convivasessionid END) AS any_sessions_in_window,
         COALESCE(SUM(CASE WHEN v.vdate BETWEEN c.start_date AND COALESCE(c.cancel_date, c.expiry_date) THEN v.minutes END),0) AS any_minutes_in_window,
         COUNT(DISTINCT CASE WHEN v.vdate BETWEEN c.start_date AND COALESCE(c.cancel_date, c.expiry_date) THEN v.asset_id END) AS any_assets_in_window,
         COUNT(DISTINCT CASE WHEN v.vdate > c.cancel_date AND v.vdate <= c.expiry_date THEN v.convivasessionid END) AS sessions_cancel_to_expiry,
         COALESCE(SUM(CASE WHEN v.vdate > c.cancel_date AND v.vdate <= c.expiry_date THEN v.minutes END),0) AS minutes_cancel_to_expiry,
         COUNT(DISTINCT CASE WHEN v.vdate > date_sub(c.expiry_date,7) AND v.vdate <= c.expiry_date THEN v.vdate END) AS days_in_final_week,
         COALESCE(SUM(CASE WHEN v.vdate > c.expiry_date AND (c.next_start IS NULL OR v.vdate < c.next_start) THEN v.minutes END),0) AS minutes_after_expiry
  FROM cohort c LEFT JOIN v ON c.Customer_External_ID = v.viewerId
  GROUP BY c.Subscription_ID, c.Customer_External_ID, c.start_status, c.cancel_type, c.start_date, c.cancel_date, c.expiry_date,
           c.Product_name, c.Country, c.Acquisition_channel, c.Cancellation_reason, c.churn_reason, c.days_start_to_cancel, c.next_start"""), "sub_view")

tiers = reg(q("""
  SELECT *, CASE WHEN any_sessions_in_window = 0 THEN '0 no session in window'
                 WHEN any_minutes_in_window < 1 THEN '1 tried, under 1 minute'
                 WHEN any_minutes_in_window < 60 THEN '2 1-59 min'
                 WHEN any_minutes_in_window < 300 THEN '3 1-5 hours'
                 WHEN any_minutes_in_window < 1200 THEN '4 5-20 hours' ELSE '5 20+ hours' END AS mou_tier,
             CASE WHEN own_sessions = 0 AND any_sessions_in_window > 0 THEN 'watched only on another subscription / pass'
                  WHEN own_minutes >= 1 THEN 'watched this subscription' ELSE 'did not watch this subscription' END AS attribution
  FROM sub_view""").cache(), "tiers")
display(q("""SELECT start_status, mou_tier, attribution, COUNT(*) subs, ROUND(AVG(any_minutes_in_window),0) avg_min,
                    ROUND(AVG(any_assets_in_window),1) avg_assets FROM tiers GROUP BY 1,2,3 ORDER BY 1,2,3"""))

# COMMAND ----------

# DBTITLE 1,Charts: viewing depth, attribution, never watched
def stack100(df, cat_col, order, title, subtitle, name, colors):
    pv = df.pivot_table(index="start_status", columns=cat_col, values="subs", fill_value=0)
    pv = pv[[c for c in order if c in pv.columns]]
    sh = pv.div(pv.sum(axis=1), axis=0)
    fig, ax = new_ax(11, 4, title, subtitle)
    y = np.arange(len(sh))[::-1]; left = np.zeros(len(sh))
    for c, col in zip(sh.columns, colors):
        ax.barh(y, sh[c], left=left, color=col, height=0.55, label=re.sub(r"^\d ", "", c), edgecolor="white")
        for i, val in enumerate(sh[c]):
            if val >= 0.05:
                ax.text(left[i] + val / 2, y[i], f"{val:.0%}", ha="center", va="center", fontsize=10,
                        color="white" if col in ("#6B7280", "#3F4756", "#8E96A6") else INK)
        left += sh[c].values
    ax.set_yticks(y); ax.set_yticklabels([LAB.get(s, s) for s in sh.index]); ax.set_xlim(0, 1); ax.xaxis.set_visible(False)
    ax.legend(frameon=False, ncol=3, loc="upper center", bbox_to_anchor=(0.5, -0.02), fontsize=9)
    out_plot(fig, name)

G6 = ["#E6E9EF", "#D0D5DF", "#B3BAC7", "#8E96A6", "#6B7280", "#3F4756"]
dp = q("SELECT start_status, mou_tier, COUNT(*) subs FROM tiers GROUP BY 1,2").toPandas()
stack100(dp, "mou_tier", sorted(dp.mou_tier.unique()), "How much did cancellers watch before cancelling?",
         "Total minutes across any subscription / pass inside the subscription window", "06_viewing_depth_tiers", G6)
at = q("SELECT start_status, attribution, COUNT(*) subs FROM tiers GROUP BY 1,2").toPandas()
stack100(at, "attribution", ["watched this subscription", "watched only on another subscription / pass", "did not watch this subscription"],
         "Where did cancelling subscriptions' viewing take place?", "Viewing matched on subscription ID", "06b_viewing_attribution", [WB_C, MID, NEW_C])

nv = q("""SELECT start_status, COUNT(*) subs, SUM(CASE WHEN own_minutes < 1 AND any_minutes_in_window < 1 THEN 1 ELSE 0 END) never
          FROM tiers GROUP BY 1""").toPandas()
nv.loc[len(nv)] = ["All", nv.subs.sum(), nv.never.sum()]
nv["pct"] = 100 * nv.never / nv.subs
display(nv)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Q2 Non-watchers: never tried, or tried and hit a problem?

# COMMAND ----------

nw = reg(q("SELECT * FROM tiers WHERE own_minutes < 1 AND any_minutes_in_window < 1"), "nw")
display(q("""
  SELECT n.start_status,
         COUNT(DISTINCT n.Subscription_ID) non_watching_subs,
         COUNT(DISTINCT CASE WHEN v.convivasessionid IS NULL THEN n.Subscription_ID END) never_opened_player,
         COUNT(DISTINCT CASE WHEN v.has_error = 1 THEN n.Subscription_ID END) had_error_coded_session,
         COUNT(DISTINCT CASE WHEN v.entitlement_error = 1 THEN n.Subscription_ID END) had_entitlement_error,
         COUNT(DISTINCT CASE WHEN v.convivasessionid IS NOT NULL AND v.has_error = 0 AND v.minutes < 1 THEN n.Subscription_ID END) opened_no_error_no_play
  FROM nw n LEFT JOIN v ON v.viewerId = n.Customer_External_ID AND v.vdate BETWEEN n.start_date AND COALESCE(n.cancel_date, n.expiry_date)
  GROUP BY 1"""))
display(q("""SELECT regexp_extract(errorlist, '^(.{0,40})', 1) error_head, deviceos, COUNT(*) sessions
             FROM v WHERE has_error = 1 GROUP BY 1,2 ORDER BY sessions DESC LIMIT 50"""))
display(q("""SELECT start_status, CASE WHEN days_start_to_cancel <= 1 THEN '0-1d' WHEN days_start_to_cancel <= 7 THEN '2-7d'
                    WHEN days_start_to_cancel <= 30 THEN '8-30d' ELSE '30d+' END tenure, cancel_type, COUNT(*) subs
             FROM nw GROUP BY 1,2,3 ORDER BY 1,2,3"""))
display(q("""
  SELECT n.start_status, COUNT(DISTINCT n.Customer_External_ID) nonwatch_customers,
         COUNT(DISTINCT p.Customer_External_ID) with_earlier_subs,
         COUNT(DISTINCT CASE WHEN COALESCE(pv.m, 0) < 1 THEN p.Customer_External_ID END) with_an_earlier_unwatched_sub
  FROM nw n
  LEFT JOIN life p ON p.Customer_External_ID = n.Customer_External_ID AND p.start_date < n.start_date
  LEFT JOIN (SELECT sub_id_view, SUM(minutes) m FROM v GROUP BY 1) pv ON pv.sub_id_view = p.Subscription_ID
  GROUP BY 1"""))
display(q("SELECT start_status, Cancellation_reason, churn_reason, Acquisition_channel, COUNT(*) subs FROM nw GROUP BY 1,2,3,4 ORDER BY subs DESC"))
display(q("SELECT Product_name, Country, COUNT(*) subs FROM nw GROUP BY 1,2 HAVING COUNT(*) >= 10 ORDER BY subs DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Q2 Viewers, and cancellations around match days

# COMMAND ----------

display(q("""
  SELECT start_status,
         SUM(CASE WHEN own_days = 1 THEN 1 ELSE 0 END) one_day_on_this_sub,
         SUM(CASE WHEN own_days BETWEEN 2 AND 4 THEN 1 ELSE 0 END) two_to_four_days,
         SUM(CASE WHEN own_days >= 5 THEN 1 ELSE 0 END) five_plus_days,
         SUM(CASE WHEN sessions_cancel_to_expiry > 0 THEN 1 ELSE 0 END) kept_watching_after_cancel,
         SUM(CASE WHEN days_in_final_week > 0 THEN 1 ELSE 0 END) watched_in_final_week,
         SUM(CASE WHEN minutes_after_expiry > 0 THEN 1 ELSE 0 END) watched_after_expiry
  FROM tiers GROUP BY 1"""))
display(q("""SELECT t.start_status, COUNT(DISTINCT t.Subscription_ID) subs, SUM(v.has_error) error_sessions, SUM(v.entitlement_error) entitlement_errors,
                    ROUND(AVG(CASE WHEN v.interrupts > 0 THEN 1.0 ELSE 0 END),3) share_sessions_with_interrupts
             FROM tiers t JOIN v ON v.viewerId = t.Customer_External_ID AND v.vdate BETWEEN t.start_date AND COALESCE(t.cancel_date, t.expiry_date)
             GROUP BY 1"""))

# Fixture calendar from the viewing table (real match dates)
fixtures = reg(q("""SELECT to_date(match_date) AS match_date, competitionName, COUNT(DISTINCT viewerId) viewers FROM {VIEW_TBL}
                    WHERE ContentType2 = 'Football' AND to_date(match_date) >= '{HIST_FROM}' GROUP BY 1,2""").cache(), "fixtures")
peak_md = reg(q("""SELECT match_date FROM (SELECT match_date, SUM(viewers) v, percent_rank() OVER (ORDER BY SUM(viewers)) pr
                   FROM fixtures GROUP BY 1) WHERE pr >= 0.75"""), "peak_md")

# Calendar: is each day within 2 days of a peak match day?
peak_cal = reg(q("""
  WITH d AS (SELECT explode(sequence(DATE'{HIST_FROM}', DATE'{TO}', INTERVAL 1 DAY)) AS d)
  SELECT CAST(d.d AS STRING) AS d, MAX(CASE WHEN p.match_date IS NOT NULL THEN 1 ELSE 0 END) AS near_peak
  FROM d LEFT JOIN peak_md p ON abs(datediff(d.d, p.match_date)) <= 2 GROUP BY 1"""), "peak_cal")
display(q("""SELECT 'sign-up' event, l.start_status, COUNT(*) n, ROUND(AVG(p.near_peak),3) share_within_2d_of_peak
             FROM life l JOIN peak_cal p ON p.d = CAST(l.start_date AS STRING)
             WHERE l.start_date >= date_trunc('year', DATE'{TO}') AND l.start_date <= '{TO}' GROUP BY 1,2
             UNION ALL
             SELECT 'cancel', l.start_status, COUNT(*), ROUND(AVG(p.near_peak),3)
             FROM life l JOIN peak_cal p ON p.d = CAST(l.cancel_date AS STRING)
             WHERE l.cancel_date >= date_trunc('year', DATE'{TO}') AND l.cancel_date <= '{TO}' GROUP BY 1,2"""))
display(q("SELECT ROUND(AVG(near_peak),3) AS base_rate_share_of_days_near_peak FROM peak_cal WHERE d >= CAST(date_trunc('year', DATE'{TO}') AS DATE)"))

# Event study around peak match days
ev_study = q("""
  SELECT c.start_status, datediff(c.d, f.match_date) AS offset_days,
         SUM(c.n) / (SELECT COUNT(*) FROM peak_md WHERE match_date >= date_trunc('year', DATE'{TO}') AND match_date <= date_sub(DATE'{TO}', 7)) AS avg_cancels_per_peak_day
  FROM (SELECT cancel_date AS d, start_status, COUNT(*) n FROM life
        WHERE cancel_date >= date_trunc('year', DATE'{TO}') AND cancel_date <= '{TO}' GROUP BY 1,2) c
  JOIN (SELECT match_date FROM peak_md WHERE match_date >= date_trunc('year', DATE'{TO}') AND match_date <= date_sub(DATE'{TO}', 7)) f
    ON datediff(c.d, f.match_date) BETWEEN -3 AND 7
  GROUP BY 1,2 ORDER BY 1,2""").toPandas()
display(ev_study)

def event_lines(df, ycol, title, subtitle, name):
    fig, ax = new_ax(10, 5, title, subtitle)
    for st, g in df.sort_values("offset_days").groupby("start_status"):
        ax.plot(g.offset_days, g[ycol], marker="o", lw=2.2, color=(WB_C if st == "winback" else MID), label=LAB[st])
    ax.axvline(0, ls="--", color=INK, lw=1); ax.set_ylim(0, None); ax.set_xticks(range(-3, 8))
    ax.yaxis.grid(True, color=GRID); ax.set_xlabel("Days relative to match day (0 = match day)", color=MUT); ax.legend(frameon=False)
    out_plot(fig, name)

event_lines(ev_study, "avg_cancels_per_peak_day", "Cancellations around peak match days",
            "Average cancellations per peak match day (top quartile by viewers). Overlapping windows count twice.", "11_event_study_peak_match_days")

# COMMAND ----------

# DBTITLE 1,Event studies by competition, by match, and FIFA World Cup
YEAR_START = q("SELECT CAST(date_trunc('year', DATE'{TO}') AS DATE) d").first()["d"]
daily_c = reg(q(f"""SELECT cancel_date AS d, start_status, COUNT(*) n FROM life
                    WHERE cancel_date >= date_sub(DATE'{YEAR_START}', 7) AND cancel_date <= '{{TO}}' GROUP BY 1,2"""), "daily_c")

def uplift_table(ev, key, val):
    pre = ev[ev.offset_days < 0].groupby(key + ["start_status"])[val].mean().rename("pre_avg")
    day0 = ev[ev.offset_days == 0].groupby(key + ["start_status"])[val].sum().rename("match_day")
    post = ev[ev.offset_days > 0].groupby(key + ["start_status"])[val].mean().rename("post_avg")
    t = pd.concat([pre, day0, post], axis=1).reset_index()
    t["uplift_vs_pre_pct"] = (100 * (t.match_day / t.pre_avg - 1)).round(1)
    return t

# Top 10 competitions (excl. World Cup)
top_comp = reg(q(f"""SELECT competitionName, COUNT(DISTINCT match_date) match_days, SUM(viewers) viewers FROM fixtures
                     WHERE match_date >= '{YEAR_START}' AND match_date <= date_sub(DATE'{{TO}}', 7)
                       AND competitionName IS NOT NULL AND competitionName <> '' AND competitionName <> 'FIFA World Cup'
                     GROUP BY 1 ORDER BY viewers DESC LIMIT 10"""), "top_comp")
ev_comp = q(f"""
  WITH f AS (SELECT DISTINCT x.competitionName, x.match_date FROM fixtures x JOIN top_comp t USING (competitionName)
             WHERE x.match_date >= '{YEAR_START}' AND x.match_date <= date_sub(DATE'{{TO}}', 7)),
  fn AS (SELECT *, COUNT(*) OVER (PARTITION BY competitionName) AS n_days FROM f)
  SELECT fn.competitionName, c.start_status, datediff(c.d, fn.match_date) AS offset_days, SUM(c.n) / MAX(fn.n_days) AS avg_cancels_per_match_day
  FROM fn JOIN daily_c c ON datediff(c.d, fn.match_date) BETWEEN -3 AND 7 GROUP BY 1,2,3""").toPandas()
comp_tbl = uplift_table(ev_comp, ["competitionName"], "avg_cancels_per_match_day").merge(top_comp.toPandas(), on="competitionName").sort_values(["viewers", "start_status"], ascending=[False, True])
display(comp_tbl)

# Top 10 matches (excl. World Cup)
def top_matches_sql(where):
    return f"""SELECT match_opid, competitionName, home_team, away_team, to_date(match_date) AS match_date, COUNT(DISTINCT viewerId) AS viewers
               FROM {{VIEW_TBL}} WHERE ContentType2 = 'Football' AND match_opid IS NOT NULL AND {where}
                 AND to_date(match_date) >= '{YEAR_START}' AND to_date(match_date) <= date_sub(DATE'{{TO}}', 7)
               GROUP BY 1,2,3,4,5 ORDER BY viewers DESC LIMIT 10"""

def match_event(tm_view):
    ev = q(f"""SELECT m.match_opid, c.start_status, datediff(c.d, m.match_date) AS offset_days, SUM(c.n) AS cancels
               FROM {tm_view} m JOIN daily_c c ON datediff(c.d, m.match_date) BETWEEN -3 AND 7 GROUP BY 1,2,3""").toPandas()
    return uplift_table(ev, ["match_opid"], "cancels")

top_matches = reg(q(top_matches_sql("competitionName <> 'FIFA World Cup'")), "top_matches")
display(match_event("top_matches").merge(top_matches.toPandas(), on="match_opid").sort_values(["viewers", "start_status"], ascending=[False, True]))

# FIFA World Cup
wc_days = reg(q("""SELECT match_date, SUM(viewers) AS viewers FROM fixtures
                   WHERE competitionName = 'FIFA World Cup' AND match_date <= '{TO}' GROUP BY 1 HAVING SUM(viewers) >= 10000"""), "wc_days")
wc = wc_days.toPandas()
if len(wc):
    wc_from, wc_to = wc.match_date.min(), wc.match_date.max()
    print(f"FIFA World Cup: {len(wc)} match days, {wc_from} to {wc_to}")
    ev_wc = q("""SELECT c.start_status, datediff(c.d, w.match_date) AS offset_days, SUM(c.n) / (SELECT COUNT(*) FROM wc_days) AS avg_cancels_per_match_day
                 FROM wc_days w JOIN daily_c c ON datediff(c.d, w.match_date) BETWEEN -3 AND 7 GROUP BY 1,2""").toPandas()
    display(uplift_table(ev_wc.assign(k=1), ["k"], "avg_cancels_per_match_day").drop(columns="k"))
    wc_daily = q(f"""SELECT cancel_date AS d, start_status, COUNT(*) n FROM life
                     WHERE cancel_date >= '{wc_from - dt.timedelta(days=28)}' AND cancel_date <= '{wc_to + dt.timedelta(days=28)}' GROUP BY 1,2""").toPandas()
    wc_daily["period"] = np.where(wc_daily.d < wc_from, "1 Before", np.where(wc_daily.d <= wc_to, "2 During", "3 After"))
    display(wc_daily.groupby(["start_status", "period"], as_index=False)["n"].mean().round(0).rename(columns={"n": "avg_daily_cancels"}))
    wc_matches = reg(q(top_matches_sql("competitionName = 'FIFA World Cup'")), "wc_matches")
    display(match_event("wc_matches").merge(wc_matches.toPandas(), on="match_opid").sort_values(["viewers", "start_status"], ascending=[False, True]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Q2 Repeat cycling and duplicate accounts

# COMMAND ----------

display(q("""
  SELECT CASE WHEN n_subs >= 6 THEN '6+' WHEN n_subs >= 4 THEN '4-5' ELSE CAST(n_subs AS STRING) END bucket, COUNT(*) customers,
         ROUND(AVG(avg_gap),1) avg_gap_days, ROUND(AVG(one_day_subs),2) avg_one_day_subs, ROUND(AVG(overlaps),2) avg_overlapping_subs
  FROM (SELECT Customer_External_ID, COUNT(*) n_subs, AVG(CASE WHEN seq > 1 THEN gap_since_prev_paid_through END) avg_gap,
               SUM(CASE WHEN paid_period_days <= 2 THEN 1 ELSE 0 END) one_day_subs, SUM(CASE WHEN gap_since_prev_paid_through < 0 THEN 1 ELSE 0 END) overlaps
        FROM life WHERE Customer_External_ID IN (SELECT Customer_External_ID FROM cohort WHERE start_status = 'winback') GROUP BY 1)
  GROUP BY 1 ORDER BY 1"""))
display(q("""
  SELECT start_status, COUNT(DISTINCT Customer_External_ID) customers FROM (
    SELECT Customer_External_ID, start_status,
           COUNT(*) OVER (PARTITION BY Customer_External_ID ORDER BY day_no RANGE BETWEEN CURRENT ROW AND 7 FOLLOWING) n7
    FROM (SELECT *, datediff(start_date, DATE'2020-01-01') AS day_no FROM life WHERE start_date >= '{FROM}' AND start_date <= '{TO}'))
  WHERE n7 >= 3 GROUP BY 1"""))
display(q("SELECT n_ext, COUNT(*) customer_ids FROM (SELECT Customer_ID, COUNT(DISTINCT Customer_External_ID) n_ext FROM life GROUP BY 1) GROUP BY 1 ORDER BY 1"))
display(q("""
  SELECT date_format(c.cancel_date,'yyyy-MM') m, COUNT(DISTINCT c.Customer_External_ID) new_cancellers,
         COUNT(DISTINCT CASE WHEN e.Customer_ID IS NOT NULL THEN c.Customer_External_ID END) earlier_sub_on_other_account
  FROM cohort c LEFT JOIN life e ON e.Customer_ID = c.Customer_ID AND e.Customer_External_ID <> c.Customer_External_ID AND e.start_date < c.start_date
  WHERE c.start_status = 'new' GROUP BY 1 ORDER BY 1"""))
display(q("""SELECT start_status, COUNT(*) subs, SUM(CASE WHEN gap_since_prev_paid_through < 0 THEN 1 ELSE 0 END) started_while_still_paid_up
             FROM cohort GROUP BY 1"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Q3 What happened after cancellation?

# COMMAND ----------

reversed_ = reg(q("""SELECT DISTINCT a.Subscription_ID FROM base_rows a JOIN base_rows b ON a.Subscription_ID = b.Subscription_ID
                     WHERE a.cancel_date IS NOT NULL AND b.Subscription_version > a.Subscription_version AND b.cancel_date IS NULL"""), "reversed_subs")
renewed = reg(q("""SELECT DISTINCT c.Subscription_ID FROM cohort c JOIN {TXN_TBL} t ON t.Subscription_ID = c.Subscription_ID
                   WHERE to_date(t.Transaction_Date) > c.cancel_date AND t.Payment_type = 'recurring'"""), "renewed")
post = reg(q("""
  SELECT c.*, CASE WHEN expiry_date <= '{ASOF}' THEN 1 ELSE 0 END reached_expiry,
         CASE WHEN date_add(expiry_date, {MATURE_DAYS}) <= '{ASOF}' THEN 1 ELSE 0 END matured,
         CASE WHEN r.Subscription_ID IS NOT NULL THEN 1 ELSE 0 END cancel_reversed,
         CASE WHEN n.Subscription_ID IS NOT NULL THEN 1 ELSE 0 END charged_after_cancel
  FROM cohort c LEFT JOIN reversed_subs r USING (Subscription_ID) LEFT JOIN renewed n USING (Subscription_ID)""").cache(), "post")
display(q("""SELECT start_status, COUNT(*) subs, SUM(reached_expiry) reached_expiry, SUM(cancel_reversed) reversed,
                    SUM(charged_after_cancel) charged_after_cancel, SUM(CASE WHEN next_start <= expiry_date THEN 1 ELSE 0 END) new_sub_before_expiry
             FROM post GROUP BY 1"""))

# Customer-level outcome between cancelling and paid expiry (one outcome each)
bf = q("""
  WITH s AS (
    SELECT p.Customer_External_ID, p.start_status, p.cancel_date, p.Subscription_ID, p.reached_expiry,
           CASE WHEN p.next_start <= p.expiry_date THEN 1 ELSE 0 END AS new_sub_before_expiry,
           CASE WHEN t.sessions_cancel_to_expiry > 0 THEN 1 ELSE 0 END AS kept_watching
    FROM post p JOIN tiers t ON t.Subscription_ID = p.Subscription_ID),
  c AS (SELECT Customer_External_ID, MAX_BY(start_status, struct(cancel_date, Subscription_ID)) AS start_status,
               MAX(new_sub_before_expiry) AS new_sub, MIN(reached_expiry) AS all_expired, MAX(kept_watching) AS kept_watching
        FROM s GROUP BY 1)
  SELECT start_status,
         CASE WHEN new_sub = 1 THEN '1 Started a new subscription before expiry' WHEN all_expired = 0 THEN '2 Still in paid period'
              WHEN kept_watching = 1 THEN '3 Kept watching, then expired' ELSE '4 Stopped watching, then expired' END AS outcome,
         COUNT(*) subs
  FROM c GROUP BY 1,2""").toPandas()
bf["share_pct"] = (100 * bf.subs / bf.groupby("start_status").subs.transform("sum")).round(1)
display(bf.sort_values(["start_status", "outcome"]))
print("Customers in table:", int(bf.subs.sum()), "| distinct cancelling customers:", q("SELECT COUNT(DISTINCT Customer_External_ID) n FROM post").first()["n"])
stack100(bf, "outcome", sorted(bf.outcome.unique()), "Between cancelling and paid expiry: what did customers do?",
         f"One outcome per customer; status as of {P['ASOF']}", "12_before_expiry_outcomes", ["#3F4756", MID, "#8E96A6", NEW_C])

# COMMAND ----------

# DBTITLE 1,After expiry: returns, speed, channel, what they watched
ret = q("""
  SELECT start_status, COUNT(*) matured_subs,
         SUM(CASE WHEN days_expiry_to_next_start >= 0 THEN 1 ELSE 0 END) returned,
         SUM(CASE WHEN days_expiry_to_next_start BETWEEN 0 AND 7  THEN 1 ELSE 0 END) d0_7,
         SUM(CASE WHEN days_expiry_to_next_start BETWEEN 8 AND 14 THEN 1 ELSE 0 END) d8_14,
         SUM(CASE WHEN days_expiry_to_next_start BETWEEN 15 AND 30 THEN 1 ELSE 0 END) d15_30
  FROM post WHERE matured = 1 GROUP BY 1""").toPandas()
ret["later"] = ret.returned - ret[["d0_7", "d8_14", "d15_30"]].sum(axis=1)
ret["not_back"] = ret.matured_subs - ret.returned
display(ret)
display(q("SELECT start_status, next_channel, COUNT(*) returns FROM post WHERE days_expiry_to_next_start >= 0 GROUP BY 1,2 ORDER BY returns DESC"))
display(q("SELECT date_trunc('week', next_start) wk, start_status, COUNT(*) returns FROM post WHERE days_expiry_to_next_start >= 0 GROUP BY 1,2 ORDER BY 1,2"))
display(q("SELECT date_trunc('week', match_date) wk, SUM(viewers) match_viewers, COUNT(DISTINCT competitionName) competitions FROM fixtures WHERE match_date >= '{FROM}' GROUP BY 1 ORDER BY 1"))
display(q("""
  SELECT p.start_status, v.competitionName, COUNT(DISTINCT p.Customer_External_ID) returners, ROUND(SUM(v.minutes),0) minutes
  FROM post p JOIN v ON v.viewerId = p.Customer_External_ID AND v.vdate BETWEEN p.next_start AND date_add(p.next_start, 7)
  WHERE p.days_expiry_to_next_start >= 0 GROUP BY 1,2 HAVING COUNT(DISTINCT p.Customer_External_ID) >= 20 ORDER BY minutes DESC"""))

rt = ret.melt(id_vars="start_status", value_vars=["d0_7", "d8_14", "d15_30", "later", "not_back"], var_name="when", value_name="subs")
stack100(rt, "when", ["d0_7", "d8_14", "d15_30", "later", "not_back"], "After paid expiry: how soon did they come back?",
         f"Subscriptions whose expiry is at least {P['MATURE_DAYS']} days before {P['ASOF']}", "13_return_speed_after_expiry",
         ["#3F4756", "#6B7280", "#8E96A6", "#B3BAC7", "#E6E9EF"])
print("Charts saved to", P["OUT_DIR"])
