# Databricks notebook source
# MAGIC %md
# MAGIC # Person-level churn, winback and cancellation analysis (R + ggplot2)
# MAGIC **Inputs (from your schema notebook):** temp view `DIM_SubscribersInfo_SD` and the `sc` sparklyr connection.
# MAGIC
# MAGIC **Unit of analysis:** the *person* (`Customer_ID`). A person can hold several subscriptions over time. Consecutive subscriptions with no real gap (renewals, plan switches, overlaps) are merged into one **run** (episode). A new run that starts after a churn is a **winback**.
# MAGIC
# MAGIC **Sections:** 1) setup, 2) subscriptions -> runs -> persons, 3) person journeys, 4) person-level churn and retention, 5) winback, 6) cancellation, 7) key numbers.
# MAGIC
# MAGIC Heavy work runs in Spark; only small aggregated tables are collected into R.

# COMMAND ----------

# ---------------- 1. SETUP ----------------
pkgs <- c("ggplot2", "dplyr", "tidyr", "forcats", "scales", "lubridate")
new  <- pkgs[!pkgs %in% rownames(installed.packages())]
if (length(new)) install.packages(new, repos = "https://cran.r-project.org")
invisible(lapply(pkgs, library, character.only = TRUE))
library(sparklyr)

# ---- parameters you can change ----
ANALYSIS_START <- "2024-01-01"   # only persons whose FIRST subscription started on/after this date (full history kept)
RENEWAL_GAP    <- 1              # a new subscription starting < this many days after the previous one ended = continuation, not winback
TOP_N          <- 12             # top N products / partners / reasons in charts
MIN_N          <- 200            # hide groups smaller than this in rate charts
OUT_DIR <- "/dbfs/FileStore/person_level_analysis"   # use a Volume path if DBFS is not writable
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

DAY_GRID <- "array(0,7,14,30,45,60,90,120,150,180,270,365)"

q <- function(sql) {
  sdf_sql(sc, sql) %>% sdf_collect() %>% data.frame() %>%
    dplyr::mutate(across(where(bit64::is.integer64), as.numeric))
}
save_plot <- function(p, name, w = 11, h = 6) {
  print(p)
  ggsave(file.path(OUT_DIR, paste0(name, ".png")), p, width = w, height = h, dpi = 200, bg = "white")
}
theme_set(theme_minimal(base_size = 13) +
  theme(plot.title = element_text(face = "bold"),
        plot.subtitle = element_text(colour = "grey35"),
        legend.position = "bottom",
        panel.grid.minor = element_blank()))

# COMMAND ----------

# ---------------- 2. SUBSCRIPTIONS -> RUNS -> PERSONS ----------------
# 2a. one row per Subscription_ID (latest version), with run number per person.
#     Run logic: a subscription starts a NEW run if it is the person's first, or if every earlier
#     subscription has churned and it starts >= RENEWAL_GAP days after the latest earlier end date.
sql_subs <- "
CREATE OR REPLACE TEMP VIEW p_subs AS
WITH latest AS (
  SELECT *, row_number() OVER (PARTITION BY Subscription_ID
                               ORDER BY Subscription_version DESC, Latest_timestamp DESC) AS rn
  FROM DIM_SubscribersInfo_SD
  WHERE Customer_ID IS NOT NULL AND Subscription_start_date IS NOT NULL
),
s AS (
  SELECT Customer_ID, Subscription_ID, Country, D2C_B2B, Product_name, Offer_period, Offer_type,
         Partner_name, Payment_method, Subscription_status, Subscription_churn_type, Winback_type,
         to_date(Subscription_start_date) AS start_date,
         to_date(Expiry_date)             AS expiry_date,
         to_date(Cancellation_date)       AS cancel_date,
         coalesce(nullif(trim(churn_reason),''), nullif(trim(Cancellation_reason),''), 'unknown') AS reason,
         CASE WHEN lower(Subscription_status) = 'churned' THEN 1 ELSE 0 END AS is_churned
  FROM latest WHERE rn = 1
),
s2 AS (
  SELECT *,
         CASE WHEN is_churned = 1
              THEN coalesce(cancel_date, expiry_date, start_date)
              ELSE expiry_date END AS end_date
  FROM s
),
w AS (
  SELECT *,
    max(end_date)   OVER (PARTITION BY Customer_ID ORDER BY start_date, Subscription_ID
                          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max_end,
    min(is_churned) OVER (PARTITION BY Customer_ID ORDER BY start_date, Subscription_ID
                          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_all_churned
  FROM s2
),
n AS (
  SELECT *,
    CASE WHEN prev_all_churned IS NULL THEN 1
         WHEN prev_all_churned = 1 AND datediff(start_date, prev_max_end) >= @GAP@ THEN 1
         ELSE 0 END AS new_run
  FROM w
)
SELECT *,
  sum(new_run) OVER (PARTITION BY Customer_ID ORDER BY start_date, Subscription_ID
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run_no
FROM n
"
sdf_sql(sc, gsub("@GAP@", RENEWAL_GAP, sql_subs, fixed = TRUE))

# 2b. one row per run (episode)
sql_runs <- "
CREATE OR REPLACE TEMP VIEW p_runs0 AS
SELECT Customer_ID, run_no,
  min(start_date)                                   AS run_start,
  count(*)                                          AS n_subs,
  min(is_churned)                                   AS churned,
  CASE WHEN min(is_churned) = 1 THEN max(end_date) END AS run_end,
  min_by(Product_name,  struct(start_date, Subscription_ID)) AS first_product,
  min_by(Offer_period,  struct(start_date, Subscription_ID)) AS first_offer_period,
  min_by(Country,       struct(start_date, Subscription_ID)) AS country,
  min_by(D2C_B2B,       struct(start_date, Subscription_ID)) AS d2c_b2b,
  min_by(Partner_name,  struct(start_date, Subscription_ID)) AS partner,
  max_by(Product_name,  struct(end_date, Subscription_ID))   AS last_product,
  max_by(Subscription_churn_type, struct(end_date, Subscription_ID)) AS churn_type,
  max_by(reason,        struct(end_date, Subscription_ID))   AS reason,
  max_by(cancel_date,   struct(end_date, Subscription_ID))   AS run_cancel_date,
  max_by(expiry_date,   struct(end_date, Subscription_ID))   AS run_expiry_date,
  max(CASE WHEN trim(lower(coalesce(Winback_type,''))) NOT IN ('','null') THEN 1 ELSE 0 END) AS flagged_winback
FROM p_subs GROUP BY Customer_ID, run_no
"
sdf_sql(sc, sql_runs)

sdf_sql(sc, "
CREATE OR REPLACE TEMP VIEW p_runs AS
SELECT *,
  datediff(coalesce(run_end, current_date()), run_start) AS run_days,
  datediff(current_date(), run_start)                    AS observed_days,
  lead(run_start)     OVER (PARTITION BY Customer_ID ORDER BY run_no) AS next_start,
  lead(first_product) OVER (PARTITION BY Customer_ID ORDER BY run_no) AS next_product,
  CASE WHEN lead(run_start) OVER (PARTITION BY Customer_ID ORDER BY run_no) IS NOT NULL
       THEN 1 ELSE 0 END                                               AS returned,
  datediff(lead(run_start) OVER (PARTITION BY Customer_ID ORDER BY run_no), run_end) AS gap_to_return
FROM p_runs0
")

# 2c. one row per person (restricted to the acquisition window)
sdf_sql(sc, gsub("@START@", ANALYSIS_START, "
CREATE OR REPLACE TEMP VIEW p_persons AS
SELECT Customer_ID,
  sum(n_subs)                                   AS n_subs,
  count(*)                                      AS n_runs,
  count(*) - 1                                  AS n_returns,
  min(run_start)                                AS first_start,
  date_trunc('month', min(run_start))           AS first_month,
  max_by(churned, run_no)                       AS churned_now,
  max(churned)                                  AS ever_churned,
  sum(run_days)                                 AS total_days,
  min_by(first_product,       run_no)           AS first_product,
  min_by(first_offer_period,  run_no)           AS first_offer_period,
  max_by(last_product,        run_no)           AS last_product,
  min_by(country,             run_no)           AS country,
  min_by(d2c_b2b,             run_no)           AS d2c_b2b,
  min_by(partner,             run_no)           AS partner,
  max(flagged_winback)                          AS flagged_winback,
  CASE WHEN count(*) = 1 AND max_by(churned, run_no) = 0 THEN '1 Single run, active'
       WHEN count(*) = 1                                 THEN '2 Single run, churned'
       WHEN max_by(churned, run_no) = 0                  THEN '3 Returned, active now'
       ELSE                                                   '4 Returned, churned again' END AS journey
FROM p_runs
GROUP BY Customer_ID
HAVING min(run_start) >= '@START@'
", fixed = TRUE))

# restrict runs to those persons so every later chart uses the same population
sdf_sql(sc, "
CREATE OR REPLACE TEMP VIEW p_runs_f AS
SELECT r.* FROM p_runs r JOIN p_persons p ON r.Customer_ID = p.Customer_ID
")

# sanity checks (read these first!)
print(q("
SELECT count(*) AS persons, sum(n_subs) AS subscriptions,
       round(avg(n_subs), 2) AS avg_subs_per_person,
       round(avg(n_runs), 2) AS avg_runs_per_person,
       round(avg(CASE WHEN n_runs > 1 THEN 1 ELSE 0 END) * 100, 1) AS pct_persons_returned,
       round(avg(churned_now) * 100, 1) AS pct_churned_now
FROM p_persons"))

# does my derived 'returned' agree with the Winback_type flag in your data?
print(q("
SELECT run_no > 1 AS derived_winback_run, flagged_winback, count(*) AS runs
FROM p_runs_f GROUP BY 1,2 ORDER BY 1,2"))

# COMMAND ----------

# ---------------- 3. PERSON JOURNEYS ----------------
subs_pp <- q("SELECT least(n_subs, 8) AS n_subs, count(*) AS persons FROM p_persons GROUP BY 1 ORDER BY 1")
p1 <- ggplot(subs_pp, aes(factor(n_subs), persons)) +
  geom_col(fill = "#1f77b4") +
  geom_text(aes(label = label_comma()(persons)), vjust = -0.3, size = 3.5) +
  scale_y_continuous(labels = label_comma(), expand = expansion(mult = c(0, 0.12))) +
  scale_x_discrete(labels = function(x) ifelse(x == "8", "8+", x)) +
  labs(title = "How many subscriptions does one person hold over time?",
       x = "Subscriptions per person", y = "Persons")
save_plot(p1, "P01_subs_per_person", h = 5)

runs_pp <- q("SELECT least(n_runs, 6) AS n_runs, count(*) AS persons FROM p_persons GROUP BY 1 ORDER BY 1")
p2 <- ggplot(runs_pp, aes(factor(n_runs), persons)) +
  geom_col(fill = "#2ca02c") +
  geom_text(aes(label = label_comma()(persons)), vjust = -0.3, size = 3.5) +
  scale_y_continuous(labels = label_comma(), expand = expansion(mult = c(0, 0.12))) +
  scale_x_discrete(labels = function(x) ifelse(x == "6", "6+", x)) +
  labs(title = "Runs per person (1 = never came back after churning)",
       subtitle = "A run = continuous subscribed period; run 2+ means a winback happened",
       x = "Runs per person", y = "Persons")
save_plot(p2, "P02_runs_per_person", h = 5)

journey <- q("SELECT journey, count(*) AS persons FROM p_persons GROUP BY 1 ORDER BY 1") %>%
  mutate(share = persons / sum(persons))
p3 <- ggplot(journey, aes(journey, persons, fill = journey)) +
  geom_col(show.legend = FALSE) + coord_flip() +
  geom_text(aes(label = paste0(label_comma()(persons), "  (", label_percent(accuracy = 0.1)(share), ")")),
            hjust = -0.05, size = 3.6) +
  scale_y_continuous(labels = label_comma(), expand = expansion(mult = c(0, 0.35))) +
  scale_x_discrete(limits = rev) +
  scale_fill_manual(values = c("#2ca02c", "#d62728", "#1f77b4", "#ff7f0e")) +
  labs(title = "Person journey types", x = NULL, y = "Persons")
save_plot(p3, "P03_journey_types", h = 4.5)

# new persons vs winback persons per month (run starts)
starts <- q("
SELECT date_trunc('month', run_start) AS m,
       CASE WHEN run_no = 1 THEN 'New person' ELSE 'Winback (returning person)' END AS kind,
       count(*) AS persons
FROM p_runs_f GROUP BY 1,2 ORDER BY 1") %>% mutate(m = as.Date(m))
p4 <- ggplot(starts, aes(m, persons, fill = kind)) +
  geom_col() +
  scale_y_continuous(labels = label_comma()) +
  scale_x_date(date_breaks = "2 months", date_labels = "%b %y") +
  scale_fill_manual(values = c("New person" = "#1f77b4", "Winback (returning person)" = "#ff7f0e")) +
  labs(title = "Persons starting a run each month: new vs winback",
       subtitle = "Counted once per person-run, not per subscription", x = NULL, y = "Persons", fill = NULL) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
save_plot(p4, "P04_new_vs_winback_persons")

# cohort status mix: what became of each monthly acquisition cohort
cohort_status <- q("SELECT first_month, journey, count(*) AS persons FROM p_persons GROUP BY 1,2") %>%
  mutate(first_month = as.Date(first_month))
p5 <- ggplot(cohort_status, aes(first_month, persons, fill = journey)) +
  geom_col(position = "fill") +
  scale_y_continuous(labels = label_percent()) +
  scale_x_date(date_breaks = "2 months", date_labels = "%b %y") +
  scale_fill_manual(values = c("#2ca02c", "#d62728", "#1f77b4", "#ff7f0e")) +
  labs(title = "Where is each acquisition cohort today?",
       subtitle = "Cohort = month of the person's first-ever subscription", x = NULL, y = "Share of cohort", fill = NULL) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
save_plot(p5, "P05_cohort_journey_mix")

# lifetime days subscribed (all runs added up), by journey
life <- q("SELECT journey, total_days FROM p_persons WHERE total_days >= 0 ORDER BY rand() LIMIT 200000")
p6 <- ggplot(life, aes(journey, total_days + 1, fill = journey)) +
  geom_violin(show.legend = FALSE, alpha = 0.7, colour = NA) +
  geom_boxplot(width = 0.12, outlier.shape = NA, show.legend = FALSE, fill = "white") +
  scale_y_log10(labels = label_comma()) +
  scale_fill_manual(values = c("#2ca02c", "#d62728", "#1f77b4", "#ff7f0e")) +
  coord_flip() + scale_x_discrete(limits = rev) +
  labs(title = "Total days subscribed per person, by journey type (sample)",
       x = NULL, y = "Total subscribed days (log scale)")
save_plot(p6, "P06_lifetime_days_by_journey", h = 5)

# COMMAND ----------

# ---------------- 4. PERSON-LEVEL CHURN AND RETENTION ----------------
# 4a. retention of a run, censoring-aware (only persons observed long enough for each day d)
surv <- function(group_expr, where = "1=1") {
  q(paste0("
SELECT ", group_expr, " AS grp, d, count(*) AS at_risk,
       avg(CASE WHEN churned = 1 AND run_days < d THEN 0.0 ELSE 1.0 END) AS retention
FROM p_runs_f LATERAL VIEW explode(", DAY_GRID, ") g AS d
WHERE observed_days >= d AND ", where, "
GROUP BY 1, 2 HAVING count(*) >= ", MIN_N, " ORDER BY 1, 2"))
}

# retention of a person's FIRST run by first product
top_prod <- q(paste0("SELECT first_product FROM p_persons GROUP BY 1 ORDER BY count(*) DESC LIMIT ", TOP_N))$first_product
s_prod <- surv("first_product", "run_no = 1") %>% filter(grp %in% top_prod)
p7 <- ggplot(s_prod, aes(d, retention, colour = grp)) +
  geom_line(linewidth = 1) + geom_point(size = 1.4) +
  scale_y_continuous(labels = label_percent(), limits = c(0, 1)) +
  scale_x_continuous(breaks = c(0, 30, 60, 90, 180, 270, 365)) +
  labs(title = "Retention of a person's first run, by first product",
       subtitle = "Share still subscribed after N days (only persons observed that long)",
       x = "Days since first start", y = "Retention", colour = NULL)
save_plot(p7, "P07_first_run_retention_by_product")

# retention: 1st run vs winback runs
s_run <- surv("CASE WHEN run_no = 1 THEN '1st run' WHEN run_no = 2 THEN '2nd run (1st winback)' ELSE '3rd+ run' END")
p8 <- ggplot(s_run, aes(d, retention, colour = grp)) +
  geom_line(linewidth = 1.1) + geom_point(size = 1.6) +
  scale_y_continuous(labels = label_percent(), limits = c(0, 1)) +
  scale_x_continuous(breaks = c(0, 30, 60, 90, 180, 270, 365)) +
  labs(title = "Do returning persons stay longer or shorter than first-time persons?",
       x = "Days since run start", y = "Retention", colour = NULL)
save_plot(p8, "P08_retention_first_vs_winback_run")

# retention: D2C vs B2B
s_ch <- surv("d2c_b2b", "run_no = 1")
p9 <- ggplot(s_ch, aes(d, retention, colour = grp)) +
  geom_line(linewidth = 1.1) + geom_point(size = 1.6) +
  scale_y_continuous(labels = label_percent(), limits = c(0, 1)) +
  scale_x_continuous(breaks = c(0, 30, 60, 90, 180, 270, 365)) +
  labs(title = "First-run retention: D2C vs B2B", x = "Days since first start", y = "Retention", colour = NULL)
save_plot(p9, "P09_first_run_retention_d2c_b2b", h = 5)

# 4b. who is churned today? (person level) by first product x first offer period
seg <- q("
SELECT first_product, first_offer_period, count(*) AS persons, avg(churned_now) AS churned_now_rate,
       avg(CASE WHEN n_runs > 1 THEN 1.0 ELSE 0.0 END) AS returned_rate
FROM p_persons GROUP BY 1,2 HAVING count(*) >= 100") %>%
  filter(first_product %in% top_prod)
p10 <- ggplot(seg, aes(first_offer_period, fct_reorder(first_product, persons, sum), fill = churned_now_rate)) +
  geom_tile(colour = "white") +
  geom_text(aes(label = label_percent(accuracy = 1)(churned_now_rate)), size = 3.3) +
  scale_fill_gradient(low = "#e8f5e9", high = "#c62828", labels = label_percent()) +
  labs(title = "Share of persons not subscribed today",
       subtitle = "By the product and offer period of their first subscription", x = "Offer period", y = NULL, fill = "Churned today")
save_plot(p10, "P10_person_churn_product_offer", h = 6.5)

by_dim <- function(col, title, name, top = TOP_N, w = 10, h = 6) {
  d <- q(paste0("SELECT ", col, " AS grp, count(*) AS persons, avg(churned_now) AS churned_now_rate,
                        avg(CASE WHEN n_runs > 1 THEN 1.0 ELSE 0.0 END) AS returned_rate
                 FROM p_persons GROUP BY 1 HAVING count(*) >= ", MIN_N, " ORDER BY persons DESC LIMIT ", top))
  d <- d %>% pivot_longer(c(churned_now_rate, returned_rate), names_to = "metric", values_to = "v") %>%
    mutate(metric = recode(metric, churned_now_rate = "Not subscribed today", returned_rate = "Came back at least once"))
  p <- ggplot(d, aes(fct_reorder(grp, persons), v, fill = metric)) +
    geom_col(position = "dodge") + coord_flip() +
    scale_y_continuous(labels = label_percent()) +
    scale_fill_manual(values = c("#d62728", "#1f77b4")) +
    labs(title = title, x = NULL, y = "Share of persons", fill = NULL)
  save_plot(p, name, w = w, h = h)
}
by_dim("country",  "Person-level churn and return rate by country",  "P11_person_churn_country")
by_dim("partner",  "Person-level churn and return rate by partner",  "P12_person_churn_partner")
by_dim("d2c_b2b",  "Person-level churn and return rate: D2C vs B2B", "P13_person_churn_channel", h = 3.5)

# COMMAND ----------

# ---------------- 5. WINBACK ANALYSIS ----------------
# Population: each person's FIRST churn = run 1 that ended in churn.
sdf_sql(sc, "
CREATE OR REPLACE TEMP VIEW p_churn1 AS
SELECT *,
  date_trunc('month', run_end)                        AS churn_month,
  datediff(current_date(), run_end)                   AS obs_since_churn,
  CASE WHEN run_cancel_date IS NOT NULL THEN 'Cancelled (cancellation date present)'
       ELSE 'Expired / not renewed' END               AS cancel_type
FROM p_runs_f WHERE run_no = 1 AND churned = 1 AND run_end IS NOT NULL
")

print(q("
SELECT count(*) AS first_time_churners,
       round(avg(returned) * 100, 1) AS pct_ever_returned,
       round(percentile_approx(CASE WHEN returned = 1 THEN gap_to_return END, 0.5), 0) AS median_days_to_return
FROM p_churn1"))

# 5a. cumulative return curve (censoring-aware)
ret <- function(group_expr) {
  q(paste0("
SELECT ", group_expr, " AS grp, d, count(*) AS at_risk,
       avg(CASE WHEN returned = 1 AND gap_to_return <= d THEN 1.0 ELSE 0.0 END) AS returned_by_d
FROM p_churn1 LATERAL VIEW explode(", DAY_GRID, ") g AS d
WHERE obs_since_churn >= d
GROUP BY 1, 2 HAVING count(*) >= ", MIN_N, " ORDER BY 1, 2"))
}
r_all <- ret("'All first-time churners'")
r_ch  <- ret("d2c_b2b")
p14 <- ggplot(bind_rows(r_all, r_ch), aes(d, returned_by_d, colour = grp)) +
  geom_line(linewidth = 1.1) + geom_point(size = 1.6) +
  scale_y_continuous(labels = label_percent()) +
  scale_x_continuous(breaks = c(0, 30, 60, 90, 180, 270, 365)) +
  labs(title = "Winback curve: share of churned persons who have returned within N days",
       subtitle = "Only persons observed at least N days after churn are counted", x = "Days since churn", y = "Returned", colour = NULL)
save_plot(p14, "P14_winback_curve")

# 5b. how long do people stay away?
gap <- q("SELECT least(gap_to_return, 365) AS gap_days, count(*) AS persons FROM p_churn1 WHERE returned = 1 GROUP BY 1 ORDER BY 1")
p15 <- ggplot(gap, aes(gap_days, persons)) +
  geom_col(fill = "#ff7f0e", width = 1) +
  scale_y_continuous(labels = label_comma()) +
  scale_x_continuous(breaks = c(0, 30, 60, 90, 180, 270, 365), labels = c("0", "30", "60", "90", "180", "270", "365+")) +
  labs(title = "Days between churn and return (persons who came back)",
       subtitle = "A large spike at the left edge means quick re-subscribes (possible renewal gaps)", x = "Days away", y = "Persons")
save_plot(p15, "P15_days_away_distribution", h = 5)

# 5c. 90-day winback rate by churn-month cohort
wb_m <- q("
SELECT churn_month, count(*) AS churners, avg(CASE WHEN returned = 1 AND gap_to_return <= 90 THEN 1.0 ELSE 0.0 END) AS wb90
FROM p_churn1 WHERE obs_since_churn >= 90 GROUP BY 1 HAVING count(*) >= 50 ORDER BY 1") %>% mutate(churn_month = as.Date(churn_month))
p16 <- ggplot(wb_m, aes(churn_month, wb90)) +
  geom_line(colour = "#ff7f0e", linewidth = 1) + geom_point(colour = "#ff7f0e") +
  scale_y_continuous(labels = label_percent(), limits = c(0, NA)) +
  scale_x_date(date_breaks = "2 months", date_labels = "%b %y") +
  labs(title = "90-day winback rate by month of churn", x = "Month of churn", y = "Returned within 90 days") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
save_plot(p16, "P16_winback90_by_churn_month", h = 4.5)

# 5d. winback rate by segment (90 days)
wb_seg <- function(col, title, name, top = TOP_N, h = 6) {
  d <- q(paste0("SELECT ", col, " AS grp, count(*) AS churners,
                        avg(CASE WHEN returned = 1 AND gap_to_return <= 90 THEN 1.0 ELSE 0.0 END) AS wb90
                 FROM p_churn1 WHERE obs_since_churn >= 90
                 GROUP BY 1 HAVING count(*) >= ", MIN_N, " ORDER BY churners DESC LIMIT ", top))
  p <- ggplot(d, aes(fct_reorder(grp, wb90), wb90)) +
    geom_col(fill = "#ff7f0e") + coord_flip() +
    geom_text(aes(label = paste0(label_percent(accuracy = 0.1)(wb90), "  (n=", label_comma()(churners), ")")), hjust = -0.05, size = 3.4) +
    scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, 0.3))) +
    labs(title = title, x = NULL, y = "Returned within 90 days")
  save_plot(p, name, h = h)
}
wb_seg("first_product",  "90-day winback rate by product of the churned run", "P17_winback90_by_product")
wb_seg("country",        "90-day winback rate by country",                    "P18_winback90_by_country")
wb_seg("partner",        "90-day winback rate by partner",                    "P19_winback90_by_partner")
wb_seg("churn_type",     "90-day winback rate by churn type",                 "P20_winback90_by_churn_type", h = 4.5)
wb_seg("cancel_type",    "90-day winback rate: cancelled vs expired",         "P21_winback90_by_cancel_type", h = 3.5)
wb_seg("reason",         "90-day winback rate by churn / cancellation reason", "P22_winback90_by_reason", h = 6.5)
wb_seg("CASE WHEN run_days <= 7 THEN '0-7 days' WHEN run_days <= 30 THEN '8-30 days' WHEN run_days <= 90 THEN '31-90 days' WHEN run_days <= 180 THEN '91-180 days' WHEN run_days <= 365 THEN '181-365 days' ELSE '365+ days' END",
       "90-day winback rate by how long the person stayed before churning", "P23_winback90_by_tenure", h = 4.5)

# 5e. product movement on return: churned product -> returning product
mv <- q("
SELECT last_product AS from_product, next_product AS to_product, count(*) AS persons
FROM p_churn1 WHERE returned = 1 GROUP BY 1,2")
keep <- mv %>% group_by(from_product) %>% summarise(s = sum(persons)) %>% slice_max(s, n = 10) %>% pull(from_product)
keep2 <- mv %>% group_by(to_product) %>% summarise(s = sum(persons)) %>% slice_max(s, n = 10) %>% pull(to_product)
mv2 <- mv %>% filter(from_product %in% keep, to_product %in% keep2) %>%
  group_by(from_product) %>% mutate(share = persons / sum(persons)) %>% ungroup()
p24 <- ggplot(mv2, aes(to_product, from_product, fill = share)) +
  geom_tile(colour = "white") +
  geom_text(aes(label = label_percent(accuracy = 1)(share)), size = 3.2) +
  scale_fill_gradient(low = "#fff3e0", high = "#e65100", labels = label_percent()) +
  labs(title = "When people return, which product do they come back on?",
       subtitle = "Rows: product when they churned. Columns: product on return (row percentages)",
       x = "Product on return", y = "Product when churned", fill = "Share") +
  theme(axis.text.x = element_text(angle = 40, hjust = 1))
save_plot(p24, "P24_winback_product_movement", h = 6.5)

# 5f. repeat churners: how many runs do returning persons go through?
rep <- q("SELECT least(n_runs, 6) AS n_runs, count(*) AS persons, avg(churned_now) AS churned_now FROM p_persons WHERE n_runs > 1 GROUP BY 1 ORDER BY 1")
p25 <- ggplot(rep, aes(factor(n_runs), persons)) +
  geom_col(fill = "#ff7f0e") +
  geom_text(aes(label = paste0(label_comma()(persons), "\n", label_percent(accuracy = 0)(churned_now), " churned now")), vjust = -0.2, size = 3.3) +
  scale_y_continuous(labels = label_comma(), expand = expansion(mult = c(0, 0.2))) +
  scale_x_discrete(labels = function(x) ifelse(x == "6", "6+", x)) +
  labs(title = "Persons who returned: how many runs do they have?", x = "Runs per person", y = "Persons")
save_plot(p25, "P25_repeat_winbacks", h = 5)

# COMMAND ----------

# ---------------- 6. CANCELLATION ANALYSIS ----------------
# Population: every run that ended in churn (any run number), ended on/after ANALYSIS_START.
# 'Cancelled' = a Cancellation_date is present on the run's last subscription; otherwise it lapsed at expiry.
sdf_sql(sc, gsub("@START@", ANALYSIS_START, "
CREATE OR REPLACE TEMP VIEW p_ends AS
SELECT *,
  CASE WHEN run_cancel_date IS NOT NULL THEN 'Cancelled (cancellation date present)'
       ELSE 'Expired / not renewed' END                       AS cancel_type,
  date_trunc('month', run_end)                                AS end_month,
  datediff(run_expiry_date, run_cancel_date)                  AS cancel_lead_days,
  date_format(run_cancel_date, 'E')                           AS cancel_dow,
  CASE WHEN run_days <= 7 THEN '1: 0-7 days' WHEN run_days <= 30 THEN '2: 8-30 days'
       WHEN run_days <= 90 THEN '3: 31-90 days' WHEN run_days <= 180 THEN '4: 91-180 days'
       WHEN run_days <= 365 THEN '5: 181-365 days' ELSE '6: 365+ days' END AS tenure_bucket
FROM p_runs_f WHERE churned = 1 AND run_end IS NOT NULL AND run_end >= '@START@'
", fixed = TRUE))

# 6a. churn events per month: cancelled vs expired
ev <- q("SELECT end_month, cancel_type, count(*) AS runs_ended FROM p_ends GROUP BY 1,2 ORDER BY 1") %>%
  mutate(end_month = as.Date(end_month))
p26 <- ggplot(ev, aes(end_month, runs_ended, fill = cancel_type)) +
  geom_col() +
  scale_y_continuous(labels = label_comma()) +
  scale_x_date(date_breaks = "2 months", date_labels = "%b %y") +
  scale_fill_manual(values = c("#c62828", "#78909c")) +
  labs(title = "Churn events per month: cancelled vs expired",
       subtitle = "One event = one run of a person ending", x = NULL, y = "Runs ended", fill = NULL) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
save_plot(p26, "P26_churn_events_by_month")

# 6b. cancelled-vs-expired share by product and offer period
ct <- q("
SELECT first_product, first_offer_period, cancel_type, count(*) AS runs_ended
FROM p_ends GROUP BY 1,2,3") %>% filter(first_product %in% top_prod) %>%
  mutate(lbl = paste(first_product, first_offer_period, sep = " | "))
top_lbl <- ct %>% group_by(lbl) %>% summarise(s = sum(runs_ended)) %>% slice_max(s, n = 15) %>% pull(lbl)
p27 <- ct %>% filter(lbl %in% top_lbl) %>%
  ggplot(aes(fct_reorder(lbl, runs_ended, sum), runs_ended, fill = cancel_type)) +
  geom_col(position = "fill") + coord_flip() +
  scale_y_continuous(labels = label_percent()) +
  scale_fill_manual(values = c("#c62828", "#78909c")) +
  labs(title = "How do runs end? Cancelled vs expired, by product and offer period", x = NULL, y = "Share of ended runs", fill = NULL)
save_plot(p27, "P27_cancel_vs_expired_by_product", h = 7)

# 6c. top reasons and reason x product
rs <- q(paste0("SELECT reason, count(*) AS runs_ended FROM p_ends GROUP BY 1 ORDER BY 2 DESC LIMIT ", TOP_N * 2))
p28 <- ggplot(rs, aes(fct_reorder(reason, runs_ended), runs_ended)) +
  geom_col(fill = "#c62828") + coord_flip() +
  geom_text(aes(label = label_comma()(runs_ended)), hjust = -0.1, size = 3.4) +
  scale_y_continuous(labels = label_comma(), expand = expansion(mult = c(0, 0.15))) +
  labs(title = "Top churn / cancellation reasons", x = NULL, y = "Runs ended")
save_plot(p28, "P28_top_reasons", h = 7)

top_reason <- rs$reason[seq_len(min(10, nrow(rs)))]
rp <- q("SELECT first_product, reason, count(*) AS runs_ended FROM p_ends GROUP BY 1,2") %>%
  filter(first_product %in% top_prod, reason %in% top_reason) %>%
  group_by(first_product) %>% mutate(share = runs_ended / sum(runs_ended)) %>% ungroup()
p29 <- ggplot(rp, aes(reason, fct_reorder(first_product, runs_ended, sum), fill = share)) +
  geom_tile(colour = "white") +
  geom_text(aes(label = label_percent(accuracy = 1)(share)), size = 3.1) +
  scale_fill_gradient(low = "#ffebee", high = "#b71c1c", labels = label_percent()) +
  labs(title = "Reason mix by product (row percentages)", x = NULL, y = NULL, fill = "Share") +
  theme(axis.text.x = element_text(angle = 40, hjust = 1))
save_plot(p29, "P29_reason_by_product", h = 7)

# 6d. when do people leave? tenure bucket x cancel type
tb <- q("SELECT tenure_bucket, cancel_type, count(*) AS runs_ended FROM p_ends GROUP BY 1,2 ORDER BY 1")
p30 <- ggplot(tb, aes(tenure_bucket, runs_ended, fill = cancel_type)) +
  geom_col() +
  scale_y_continuous(labels = label_comma()) +
  scale_fill_manual(values = c("#c62828", "#78909c")) +
  labs(title = "How long had people been subscribed when the run ended?", x = "Run length", y = "Runs ended", fill = NULL)
save_plot(p30, "P30_tenure_at_end", h = 5)

# 6e. how early do cancellers cancel before expiry?
lead <- q("
SELECT least(greatest(cancel_lead_days, -30), 365) AS lead_days, count(*) AS runs_ended
FROM p_ends WHERE cancel_lead_days IS NOT NULL GROUP BY 1 ORDER BY 1")
p31 <- ggplot(lead, aes(lead_days, runs_ended)) +
  geom_col(fill = "#c62828", width = 1) +
  scale_y_continuous(labels = label_comma()) +
  labs(title = "Days between cancellation and the original expiry date",
       subtitle = "Zero or negative = cancelled at or after expiry; large values = cancelled long before expiry",
       x = "Days (clipped to -30..365)", y = "Runs ended")
save_plot(p31, "P31_cancel_lead_days", h = 5)

# 6f. weekday of cancellation
dow <- q("SELECT cancel_dow, count(*) AS runs_ended FROM p_ends WHERE cancel_dow IS NOT NULL GROUP BY 1") %>%
  mutate(cancel_dow = factor(cancel_dow, levels = c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")))
p32 <- ggplot(dow, aes(cancel_dow, runs_ended)) +
  geom_col(fill = "#c62828") +
  scale_y_continuous(labels = label_comma()) +
  labs(title = "Cancellations by weekday", x = NULL, y = "Runs ended")
save_plot(p32, "P32_cancel_weekday", h = 4.5)

# COMMAND ----------

# ---------------- 7. KEY NUMBERS ----------------
summary_tbl <- q("
SELECT
  (SELECT count(*) FROM p_persons)                                                      AS persons,
  (SELECT sum(n_subs) FROM p_persons)                                                   AS subscriptions,
  (SELECT round(avg(n_subs), 2) FROM p_persons)                                         AS subs_per_person,
  (SELECT round(avg(churned_now) * 100, 1) FROM p_persons)                              AS pct_not_subscribed_today,
  (SELECT round(avg(CASE WHEN n_runs > 1 THEN 1 ELSE 0 END) * 100, 1) FROM p_persons)  AS pct_persons_returned,
  (SELECT round(avg(returned) * 100, 1) FROM p_churn1)                                  AS pct_churners_ever_returned,
  (SELECT round(avg(CASE WHEN returned = 1 AND gap_to_return <= 90 THEN 1.0 ELSE 0.0 END) * 100, 1)
     FROM p_churn1 WHERE obs_since_churn >= 90)                                         AS pct_returned_within_90d,
  (SELECT percentile_approx(gap_to_return, 0.5) FROM p_churn1 WHERE returned = 1)       AS median_days_away,
  (SELECT round(avg(CASE WHEN cancel_type LIKE 'Cancelled%' THEN 1.0 ELSE 0.0 END) * 100, 1) FROM p_ends) AS pct_ends_cancelled
")
display(summary_tbl)

cat("PNG files saved in:", OUT_DIR, "\n")
print(list.files(OUT_DIR, pattern = "\\.png$"))
