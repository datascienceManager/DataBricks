# Databricks notebook source
# MAGIC %md
# MAGIC # Subscription Pattern Analysis (R + ggplot2)
# MAGIC **Inputs (already created in your schema notebook):** temp views `DIM_SubscribersInfo_SD` and `EntertainmentMovies_SD`, plus the `sc` sparklyr connection.
# MAGIC
# MAGIC **Sections:** 1) setup, 2) clean subscription base, 3) acquisition trend, 4) status and churn, 5) tenure and retention curves, 6) partner and country view, 7) churn reasons, 8) engagement link (viewing vs churn), 9) key-numbers summary.
# MAGIC
# MAGIC Heavy work runs in Spark; only small aggregated tables are collected into R for ggplot2.

# COMMAND ----------

# ---------------- 1. SETUP ----------------
pkgs <- c("ggplot2", "dplyr", "tidyr", "forcats", "scales", "lubridate")
new  <- pkgs[!pkgs %in% rownames(installed.packages())]
if (length(new)) install.packages(new, repos = "https://cran.r-project.org")
invisible(lapply(pkgs, library, character.only = TRUE))
library(sparklyr)

# ---- parameters you can change ----
ANALYSIS_START <- "2024-01-01"      # only subscriptions started on/after this date
TOP_N          <- 12                # top N partners / products in charts
# Output folder for PNGs (use a Unity Catalog Volume or DBFS path you can write to)
OUT_DIR <- "/dbfs/FileStore/subscription_analysis"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

# helper: run Spark SQL, collect to a plain R data.frame (integer64 -> numeric)
q <- function(sql) {
  sdf_sql(sc, sql) %>% sdf_collect() %>% data.frame() %>%
    dplyr::mutate(across(where(bit64::is.integer64), as.numeric))
}

# helper: show + save a plot
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

# ---------------- 2. CLEAN SUBSCRIPTION BASE ----------------
# Notes on the data (from your sample rows):
#  * Subscription_ID has multiple versions -> keep ONE row per Subscription_ID (highest version, latest timestamp)
#  * Subscription_type becomes 'churned' on churned rows, so winback is flagged from Winback_type instead
#  * PII columns (First_name, Last_name, Email) are deliberately NOT selected
sdf_sql(sc, sprintf("
CREATE OR REPLACE TEMP VIEW subs_base AS
SELECT
  Customer_ID, Subscription_ID, Subscription_key, Country, Dir_indir, D2C_B2B,
  Product_name, Offer_period, Offer_type, Tier, Payment_method,
  Partner_name, Acquisition_channel, Source_system, Campaign_name,
  Subscription_status, Subscription_churn_type, churn_reason,
  Subscription_start_date, Expiry_date, Cancellation_date,
  promo, non_commercial,
  CASE WHEN lower(Subscription_status) = 'churned' THEN 1 ELSE 0 END                         AS is_churned,
  CASE WHEN trim(lower(coalesce(Winback_type,''))) NOT IN ('','null') THEN 1 ELSE 0 END       AS is_winback,
  date_trunc('month', Subscription_start_date)                                                AS start_month,
  coalesce(Cancellation_date, Expiry_date)                                                    AS end_date,
  datediff(coalesce(Cancellation_date, Expiry_date), Subscription_start_date)                 AS tenure_days
FROM (
  SELECT *, row_number() OVER (PARTITION BY Subscription_ID
                               ORDER BY Subscription_version DESC, Latest_timestamp DESC) AS rn
  FROM DIM_SubscribersInfo_SD
  WHERE Subscription_start_date >= '%s'
) t
WHERE rn = 1 AND Subscription_start_date IS NOT NULL
", ANALYSIS_START))

base_check <- q("
SELECT count(*) AS subscriptions,
       count(DISTINCT Customer_ID) AS customers,
       min(Subscription_start_date) AS first_start,
       max(Subscription_start_date) AS last_start,
       round(avg(is_churned)*100,1) AS pct_churned,
       round(avg(is_winback)*100,1) AS pct_winback,
       round(avg(CASE WHEN tenure_days < 0 THEN 1 ELSE 0 END)*100,2) AS pct_negative_tenure
FROM subs_base")
print(base_check)

# COMMAND ----------

# ---------------- 3. ACQUISITION TREND ----------------
new_by_month <- q("
SELECT start_month, D2C_B2B, is_winback, count(*) AS subs
FROM subs_base GROUP BY 1,2,3 ORDER BY 1")

new_by_month <- new_by_month %>%
  mutate(start_month = as.Date(start_month),
         segment = paste(D2C_B2B, ifelse(is_winback == 1, "winback", "new"), sep = " | "))

p1 <- ggplot(new_by_month, aes(start_month, subs, fill = segment)) +
  geom_col() +
  scale_y_continuous(labels = label_comma()) +
  scale_x_date(date_breaks = "2 months", date_labels = "%b %y") +
  labs(title = "New subscriptions per month",
       subtitle = "Split by channel (D2C / B2B) and new vs winback",
       x = NULL, y = "Subscriptions", fill = NULL) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
save_plot(p1, "01_new_subs_by_month")

# Month-over-month growth of the total
mom <- new_by_month %>% group_by(start_month) %>% summarise(subs = sum(subs), .groups = "drop") %>%
  arrange(start_month) %>% mutate(mom_pct = (subs / lag(subs) - 1))

p1b <- ggplot(mom, aes(start_month, mom_pct)) +
  geom_hline(yintercept = 0, colour = "grey60") +
  geom_line(colour = "#1f77b4", linewidth = 1) + geom_point(colour = "#1f77b4") +
  scale_y_continuous(labels = label_percent()) +
  scale_x_date(date_breaks = "2 months", date_labels = "%b %y") +
  labs(title = "Month-over-month change in new subscriptions", x = NULL, y = "MoM change") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
save_plot(p1b, "01b_mom_growth", h = 4.5)

# COMMAND ----------

# ---------------- 4. STATUS AND CHURN ----------------
by_product <- q("
SELECT Product_name, Offer_period,
       count(*) AS subs,
       sum(is_churned) AS churned,
       round(avg(is_churned), 4) AS churn_rate,
       round(percentile_approx(tenure_days, 0.5), 0) AS median_tenure_days
FROM subs_base GROUP BY 1,2 HAVING count(*) >= 50 ORDER BY subs DESC")

top_products <- by_product %>% group_by(Product_name) %>% summarise(s = sum(subs)) %>%
  slice_max(s, n = TOP_N) %>% pull(Product_name)

p2 <- by_product %>% filter(Product_name %in% top_products) %>%
  ggplot(aes(reorder(Product_name, subs, sum), subs, fill = Offer_period)) +
  geom_col() + coord_flip() +
  scale_y_continuous(labels = label_comma()) +
  labs(title = "Subscriptions by product and offer period", x = NULL, y = "Subscriptions", fill = "Offer period")
save_plot(p2, "02_subs_by_product")

p3 <- by_product %>% filter(Product_name %in% top_products) %>%
  ggplot(aes(Offer_period, Product_name, fill = churn_rate)) +
  geom_tile(colour = "white") +
  geom_text(aes(label = percent(churn_rate, accuracy = 1)), size = 3.5) +
  scale_fill_gradient(low = "#e8f5e9", high = "#c62828", labels = label_percent()) +
  labs(title = "Churn rate: product x offer period",
       subtitle = "Share of subscriptions with status = churned (cells with 50+ subs)",
       x = NULL, y = NULL, fill = "Churn rate")
save_plot(p3, "03_churn_heatmap_product_period", h = 6.5)

# churn by B2B/D2C, tier, promo
seg <- q("
SELECT 'D2C / B2B' AS dim, D2C_B2B AS level, count(*) AS subs, avg(is_churned) AS churn_rate FROM subs_base GROUP BY 1,2
UNION ALL SELECT 'Tier', Tier, count(*), avg(is_churned) FROM subs_base GROUP BY 1,2
UNION ALL SELECT 'Promo', CAST(promo AS STRING), count(*), avg(is_churned) FROM subs_base GROUP BY 1,2
UNION ALL SELECT 'Acquisition channel', Acquisition_channel, count(*), avg(is_churned) FROM subs_base GROUP BY 1,2
UNION ALL SELECT 'Payment method', Payment_method, count(*), avg(is_churned) FROM subs_base GROUP BY 1,2") %>%
  filter(subs >= 100, !is.na(level))

p4 <- ggplot(seg, aes(churn_rate, fct_reorder(paste(level), churn_rate), size = subs, colour = dim)) +
  geom_point(alpha = 0.8) +
  facet_wrap(~dim, scales = "free_y") +
  scale_x_continuous(labels = label_percent()) +
  scale_size_continuous(range = c(2, 10), labels = label_comma()) +
  guides(colour = "none") +
  labs(title = "Churn rate by segment", subtitle = "Bubble size = number of subscriptions (segments with 100+ subs)",
       x = "Churn rate", y = NULL, size = "Subs")
save_plot(p4, "04_churn_by_segment", h = 8)

# COMMAND ----------

# ---------------- 5. TENURE AND RETENTION CURVES ----------------
# 5a. tenure distribution of ended subscriptions
tenure <- q("
SELECT Offer_period, tenure_days, count(*) AS subs
FROM subs_base
WHERE is_churned = 1 AND tenure_days BETWEEN 0 AND 400
GROUP BY 1,2")

p5 <- tenure %>% uncount(subs) %>%
  ggplot(aes(tenure_days, fill = Offer_period)) +
  geom_histogram(binwidth = 7, colour = "white") +
  facet_wrap(~Offer_period, scales = "free_y") +
  labs(title = "Tenure of churned subscriptions", subtitle = "Days from start to end; 7-day bins",
       x = "Tenure (days)", y = "Subscriptions") +
  guides(fill = "none")
save_plot(p5, "05_tenure_distribution")

# 5b. retention curve (censoring-aware): of subs old enough to be observed at day d,
#     what share was still not churned (or churned after d)?
ret <- q("
SELECT Product_name, d AS day,
       count(*) AS eligible,
       sum(CASE WHEN is_churned = 0 OR tenure_days >= d THEN 1 ELSE 0 END) AS retained
FROM subs_base
LATERAL VIEW explode(array(0,7,14,21,30,45,60,75,90,120,150,180)) x AS d
WHERE Subscription_start_date <= date_sub(current_date(), d)
GROUP BY 1,2")

ret <- ret %>% mutate(retention = retained / eligible)
keep <- ret %>% filter(day == 0) %>% slice_max(eligible, n = 6) %>% pull(Product_name)

p6 <- ret %>% filter(Product_name %in% keep, eligible >= 50) %>%
  ggplot(aes(day, retention, colour = Product_name)) +
  geom_line(linewidth = 1) + geom_point() +
  scale_y_continuous(labels = label_percent(), limits = c(0, 1)) +
  scale_x_continuous(breaks = c(0, 30, 60, 90, 120, 150, 180)) +
  labs(title = "Retention curve by product (top 6 by volume)",
       subtitle = "Only subscriptions old enough to be observed at each day are counted",
       x = "Days since subscription start", y = "Still retained", colour = NULL)
save_plot(p6, "06_retention_curves")

# 5c. cohort view: churn rate and median tenure by start-month cohort
cohort <- q("
SELECT start_month, count(*) AS subs, avg(is_churned) AS churn_rate,
       percentile_approx(CASE WHEN is_churned=1 THEN tenure_days END, 0.5) AS median_tenure
FROM subs_base GROUP BY 1 ORDER BY 1") %>% mutate(start_month = as.Date(start_month))

p7 <- ggplot(cohort, aes(start_month)) +
  geom_col(aes(y = subs / max(subs)), fill = "grey85") +
  geom_line(aes(y = churn_rate), colour = "#c62828", linewidth = 1.1) +
  geom_point(aes(y = churn_rate), colour = "#c62828") +
  scale_y_continuous(labels = label_percent(), name = "Churn rate (line)",
                     sec.axis = sec_axis(~ . * max(cohort$subs), name = "Cohort size (bars)", labels = label_comma())) +
  scale_x_date(date_breaks = "2 months", date_labels = "%b %y") +
  labs(title = "Churn rate by start-month cohort", x = NULL,
       subtitle = "Recent cohorts look better partly because they have had less time to churn") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
save_plot(p7, "07_cohort_churn")

# COMMAND ----------

# ---------------- 6. PARTNER AND COUNTRY VIEW ----------------
partner <- q("
SELECT Partner_name, count(*) AS subs, avg(is_churned) AS churn_rate,
       percentile_approx(tenure_days, 0.5) AS median_tenure
FROM subs_base WHERE Partner_name IS NOT NULL AND trim(Partner_name) <> ''
GROUP BY 1 HAVING count(*) >= 100")

p8 <- partner %>% slice_max(subs, n = 25) %>%
  ggplot(aes(subs, churn_rate, size = median_tenure, label = Partner_name)) +
  geom_point(colour = "#1f77b4", alpha = 0.7) +
  geom_text(vjust = -1.2, size = 3, check_overlap = TRUE) +
  scale_x_log10(labels = label_comma()) +
  scale_y_continuous(labels = label_percent()) +
  labs(title = "Partners: volume vs churn", subtitle = "Top 25 partners by volume; bubble = median tenure (days)",
       x = "Subscriptions (log scale)", y = "Churn rate", size = "Median tenure")
save_plot(p8, "08_partner_volume_vs_churn", h = 7)

country <- q("
SELECT Country, Product_name, count(*) AS subs, avg(is_churned) AS churn_rate
FROM subs_base WHERE Country IS NOT NULL GROUP BY 1,2")

top_c <- country %>% group_by(Country) %>% summarise(s = sum(subs)) %>% slice_max(s, n = 12) %>% pull(Country)
p9 <- country %>% filter(Country %in% top_c, Product_name %in% top_products) %>%
  ggplot(aes(Product_name, Country, fill = subs)) +
  geom_tile(colour = "white") +
  geom_text(aes(label = label_comma(accuracy = 1)(subs)), size = 3) +
  scale_fill_viridis_c(trans = "log10", labels = label_comma()) +
  labs(title = "Subscription volume: country x product", x = NULL, y = NULL, fill = "Subs (log)") +
  theme(axis.text.x = element_text(angle = 40, hjust = 1))
save_plot(p9, "09_country_product_volume", h = 6.5)

# COMMAND ----------

# ---------------- 7. CHURN TYPE / REASON ----------------
reasons <- q("
SELECT coalesce(Subscription_churn_type,'unknown') AS churn_type, count(*) AS subs
FROM subs_base WHERE is_churned = 1 GROUP BY 1 ORDER BY subs DESC")

p10 <- ggplot(reasons, aes(reorder(churn_type, subs), subs)) +
  geom_col(fill = "#c62828") + coord_flip() +
  geom_text(aes(label = label_comma()(subs)), hjust = -0.1, size = 3.5) +
  scale_y_continuous(labels = label_comma(), expand = expansion(mult = c(0, 0.15))) +
  labs(title = "Churn type (churned subscriptions)", x = NULL, y = "Subscriptions")
save_plot(p10, "10_churn_type", h = 4)

# COMMAND ----------

# ---------------- 8. ENGAGEMENT LINK: VIEWING vs CHURN ----------------
# Session table is large: aggregate to ONE row per subscription in Spark first.
# ASSUMPTION: sessions.subscriptionid matches DIM Subscription_key (15-digit key in both samples).
# The match-rate check below tells you if the join is right; if it is ~0%, try Subscription_ID instead.
sdf_sql(sc, "
CREATE OR REPLACE TEMP VIEW sess_agg AS
SELECT subscriptionid,
       sum(CAST(playingunixtimems AS DOUBLE)) / 60000                                   AS watch_min,
       sum(CASE WHEN Sports = 1 THEN CAST(playingunixtimems AS DOUBLE) ELSE 0 END)/60000 AS sports_min,
       count(DISTINCT convivasessionid)                                                 AS sessions,
       count(DISTINCT startdate)                                                        AS active_days
FROM EntertainmentMovies_SD
WHERE subscriptionid IS NOT NULL AND subscriptionid > 0
GROUP BY subscriptionid")

join_check <- q("
SELECT count(*) AS subs,
       sum(CASE WHEN s.subscriptionid IS NOT NULL THEN 1 ELSE 0 END) AS subs_with_viewing,
       round(avg(CASE WHEN s.subscriptionid IS NOT NULL THEN 1 ELSE 0 END)*100,1) AS match_pct
FROM subs_base b LEFT JOIN sess_agg s ON b.Subscription_key = s.subscriptionid")
print(join_check)

sdf_sql(sc, "
CREATE OR REPLACE TEMP VIEW subs_engage AS
SELECT b.*, coalesce(s.watch_min,0) AS watch_min, coalesce(s.sports_min,0) AS sports_min,
       coalesce(s.sessions,0) AS sessions, coalesce(s.active_days,0) AS active_days
FROM subs_base b LEFT JOIN sess_agg s ON b.Subscription_key = s.subscriptionid")

# 8a. churn rate by viewing-hours bucket
eng <- q("
SELECT CASE WHEN watch_min = 0 THEN '0  none'
            WHEN watch_min < 60 THEN '1  <1h'
            WHEN watch_min < 300 THEN '2  1-5h'
            WHEN watch_min < 1200 THEN '3  5-20h'
            ELSE '4  20h+' END AS watch_bucket,
       count(*) AS subs, avg(is_churned) AS churn_rate
FROM subs_engage GROUP BY 1 ORDER BY 1")

p11 <- ggplot(eng, aes(watch_bucket, churn_rate)) +
  geom_col(fill = "#6a1b9a") +
  geom_text(aes(label = paste0(percent(churn_rate, accuracy = 1), "\n(n=", label_comma()(subs), ")")), vjust = -0.2, size = 3.5) +
  scale_y_continuous(labels = label_percent(), expand = expansion(mult = c(0, 0.2))) +
  labs(title = "Churn rate by total viewing time in the analysis window",
       subtitle = "Caveat: viewing and churn overlap in time, so this shows association, not cause",
       x = "Total watch time per subscription", y = "Churn rate")
save_plot(p11, "11_churn_vs_engagement", h = 5.5)

# 8b. engagement profile by product and status
prof <- q("
SELECT Product_name, Subscription_status, count(*) AS subs,
       avg(watch_min)/60 AS mean_hours, percentile_approx(watch_min,0.5)/60 AS median_hours,
       sum(sports_min)/nullif(sum(watch_min),0) AS sports_share
FROM subs_engage GROUP BY 1,2 HAVING count(*) >= 50")

p12 <- prof %>% filter(Product_name %in% top_products) %>%
  ggplot(aes(reorder(Product_name, median_hours), median_hours, fill = Subscription_status)) +
  geom_col(position = "dodge") + coord_flip() +
  labs(title = "Median watch hours per subscription", subtitle = "By product and current status",
       x = NULL, y = "Median hours", fill = "Status")
save_plot(p12, "12_median_hours_product_status")

p13 <- prof %>% filter(Product_name %in% top_products) %>%
  ggplot(aes(reorder(Product_name, sports_share), sports_share, fill = Subscription_status)) +
  geom_col(position = "dodge") + coord_flip() +
  scale_y_continuous(labels = label_percent()) +
  labs(title = "Sports share of viewing time", subtitle = "By product and current status",
       x = NULL, y = "Sports share of minutes", fill = "Status")
save_plot(p13, "13_sports_share_product_status")

# COMMAND ----------

# ---------------- 9. KEY NUMBERS SUMMARY ----------------
overall <- q("
SELECT count(*) AS subscriptions, count(DISTINCT Customer_ID) AS customers,
       round(avg(is_churned)*100,1) AS churn_pct,
       round(avg(is_winback)*100,1) AS winback_pct,
       round(percentile_approx(CASE WHEN is_churned=1 THEN tenure_days END,0.5),0) AS median_tenure_churned_days,
       round(avg(CASE WHEN D2C_B2B='B2B' THEN 1 ELSE 0 END)*100,1) AS b2b_pct
FROM subs_base")
display(overall)

worst <- by_product %>% filter(subs >= 200) %>% arrange(desc(churn_rate)) %>% head(5)
best  <- by_product %>% filter(subs >= 200) %>% arrange(churn_rate) %>% head(5)
cat("Highest-churn product/period combos (200+ subs):\n"); print(worst)
cat("\nLowest-churn product/period combos (200+ subs):\n"); print(best)
cat("\nPNG charts saved to:", OUT_DIR, "\n")
