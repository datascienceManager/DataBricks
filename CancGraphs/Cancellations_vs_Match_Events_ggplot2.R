# =====================================================================
# Cancellations around key sports match events: ggplot2 with SAMPLE data
# Needs only: ggplot2, dplyr, tidyr, scales (all present in Databricks R runtimes).
# All numbers below are simulated so you can see the layout; section 5 shows
# exactly which real tables to swap in. Not run by the author (no R available there).
# =====================================================================
library(ggplot2); library(dplyr); library(tidyr); library(scales)

# Colour-blind-safe categorical pair (blue / orange) + neutral ink for events
COL <- c("New" = "#2a78d6", "Win-back" = "#eb6834")
INK <- "#0b0b0b"; INK2 <- "#52514e"; GRID <- "#e6e5e1"; AREA <- "#c9c8c2"

# ---------------------------------------------------------------------
# 1. SAMPLE DATA (replace in section 5)
# ---------------------------------------------------------------------
set.seed(42)
days <- seq(as.Date("2026-08-01"), as.Date("2026-09-30"), by = "day")

events <- tibble(
  date  = as.Date(c("2026-08-16", "2026-08-23", "2026-08-30", "2026-09-06",
                    "2026-09-13", "2026-09-16", "2026-09-20", "2026-09-26")),
  label = c("Community Shield", "PL opening round", "PL round 3", "Nations League",
            "Derby day", "UCL matchday 1", "PL round 6", "UCL matchday 2"),
  tier  = c("Major", "Regular", "Regular", "Major", "Major", "Major", "Regular", "Major")
)
w_tier <- c(Major = 1, Regular = 0.4)

# cancellations lift in the 1-3 days AFTER an event (people cancel auto-renew once the match is over)
after_lift <- function(d) {
  sum(vapply(seq_len(nrow(events)), function(i) {
    off <- as.numeric(d - events$date[i])
    w_tier[[events$tier[i]]] * (off >= 1 & off <= 3) * exp(-(off - 1) / 1.5)
  }, numeric(1)))
}
# viewing peaks ON event days
event_peak <- function(d) sum(vapply(seq_len(nrow(events)), function(i)
  w_tier[[events$tier[i]]] * dnorm(as.numeric(d - events$date[i]), 0, 0.8), numeric(1)))

daily <- tibble(date = days) %>%
  mutate(trend  = seq_along(date),
         lift   = vapply(date, after_lift, numeric(1)),
         peak   = vapply(date, event_peak, numeric(1)),
         New       = rpois(n(), 20 + 0.6 * trend + 55 * lift),
         `Win-back`= rpois(n(), 30 + 1.1 * trend + 120 * lift),
         fb_min_m  = pmax(0.2, 0.6 + 0.05 * trend + 4.5 * peak + rnorm(n(), 0, 0.15)))   # million minutes

# ---------------------------------------------------------------------
# 2. CHART 1: daily cancellations (top) and football viewing (bottom), with match markers
#    One y-scale per panel (no dual axis); both panels share the date axis and event lines.
# ---------------------------------------------------------------------
P1 <- "Cancellations per day"; P2 <- "Football viewing (M minutes)"
lv <- c(P1, P2)

canc_long <- daily %>% select(date, New, `Win-back`) %>%
  pivot_longer(-date, names_to = "type", values_to = "value") %>% mutate(panel = factor(P1, lv))
view_df   <- daily %>% transmute(date, value = fb_min_m, panel = factor(P2, lv))
ev_lab    <- events %>% mutate(panel = factor(P1, lv))
headroom  <- tibble(date = min(days), value = max(canc_long$value) * 1.45, panel = factor(P1, lv))

p1 <- ggplot() +
  # match-day markers, drawn in every panel
  geom_vline(data = events, aes(xintercept = date, linetype = tier), colour = INK2, linewidth = 0.4) +
  geom_area(data = view_df, aes(date, value), fill = AREA, alpha = 0.9) +
  geom_line(data = canc_long, aes(date, value, colour = type), linewidth = 0.9) +
  geom_blank(data = headroom, aes(date, value)) +
  geom_text(data = ev_lab, aes(x = date, y = Inf, label = label),
            angle = 90, hjust = 1.05, vjust = -0.5, size = 3.1, colour = INK2) +
  facet_grid(panel ~ ., scales = "free_y", switch = "y") +
  scale_colour_manual(values = COL, name = NULL) +
  scale_linetype_manual(values = c(Major = "solid", Regular = "dashed"), name = "Match tier") +
  scale_x_date(date_breaks = "1 week", date_labels = "%d %b", expand = expansion(mult = 0.01)) +
  scale_y_continuous(labels = label_number(big.mark = ","), expand = expansion(mult = c(0.02, 0.03))) +
  labs(title = "Cancellations jump in the 1-3 days after major matches",
       subtitle = "Sample data. Vertical lines are match days; grey area is football viewing on the same dates.",
       x = NULL, y = NULL) +
  theme_minimal(base_size = 12) +
  theme(panel.grid.minor = element_blank(), panel.grid.major.x = element_blank(),
        panel.grid.major.y = element_line(colour = GRID),
        strip.placement = "outside", strip.text.y.left = element_text(angle = 0, hjust = 1, colour = INK2),
        legend.position = "top", legend.justification = "left",
        plot.title = element_text(face = "bold", colour = INK), plot.subtitle = element_text(colour = INK2),
        axis.text = element_text(colour = INK2))
print(p1)
# ggsave("cancellations_vs_matches.png", p1, width = 11, height = 6, dpi = 200)

# ---------------------------------------------------------------------
# 3. CHART 2: event study. Average cancellations in the 7 days either side of a match,
#    indexed to the pre-match baseline (days -7 to -4 = 100), by match tier.
# ---------------------------------------------------------------------
offsets <- -7:7
es <- tidyr::crossing(events, offset = offsets) %>%
  mutate(d = date + offset) %>%
  inner_join(daily %>% select(date, New, `Win-back`) %>% pivot_longer(-date, names_to = "type", values_to = "n"),
             by = c("d" = "date")) %>%
  group_by(label, tier, type) %>%
  mutate(base = mean(n[offset >= -7 & offset <= -4]), idx = n / base * 100) %>%
  ungroup()

es_sum <- es %>% group_by(tier, type, offset) %>%
  summarise(mean_idx = mean(idx), lo = quantile(idx, 0.1), hi = quantile(idx, 0.9), .groups = "drop")

p2 <- ggplot(es_sum, aes(offset, mean_idx, colour = type, fill = type)) +
  geom_hline(yintercept = 100, colour = INK2, linewidth = 0.3) +
  geom_vline(xintercept = 0, colour = INK2, linetype = "dashed", linewidth = 0.4) +
  geom_ribbon(aes(ymin = lo, ymax = hi), alpha = 0.15, colour = NA) +
  geom_line(linewidth = 0.9) +
  geom_point(size = 2) +
  facet_wrap(~ tier, nrow = 1) +
  scale_colour_manual(values = COL, name = NULL) + scale_fill_manual(values = COL, name = NULL) +
  scale_x_continuous(breaks = seq(-7, 7, 1)) +
  labs(title = "Cancellations peak 1-2 days after the whistle, more so for major matches",
       subtitle = "Sample data. Index: pre-match baseline (days -7 to -4) = 100. Line = mean across matches, band = 10th-90th percentile.",
       x = "Days relative to match day (0 = match day)", y = "Cancellations index") +
  theme_minimal(base_size = 12) +
  theme(panel.grid.minor = element_blank(), legend.position = "top", legend.justification = "left",
        plot.title = element_text(face = "bold", colour = INK), plot.subtitle = element_text(colour = INK2),
        strip.text = element_text(face = "bold", colour = INK), axis.text = element_text(colour = INK2))
print(p2)

# ---------------------------------------------------------------------
# 4. CHART 3 (optional): per-match view, % lift in the 3 days after vs 3 days before
# ---------------------------------------------------------------------
lift_tbl <- es %>%
  group_by(label, date, tier, type) %>%
  summarise(before = mean(n[offset >= -3 & offset <= -1]), after = mean(n[offset >= 1 & offset <= 3]),
            lift = after / before - 1, .groups = "drop")

p3 <- ggplot(lift_tbl, aes(reorder(label, date), lift, fill = type)) +
  geom_col(position = position_dodge(width = 0.75), width = 0.65) +
  geom_hline(yintercept = 0, colour = INK2, linewidth = 0.3) +
  scale_fill_manual(values = COL, name = NULL) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  labs(title = "Post-match cancellation lift by match", subtitle = "Sample data. Average of days +1..+3 vs days -3..-1.",
       x = NULL, y = "Change in daily cancellations") +
  theme_minimal(base_size = 12) +
  theme(panel.grid.minor = element_blank(), panel.grid.major.x = element_blank(), legend.position = "top",
        legend.justification = "left", plot.title = element_text(face = "bold", colour = INK),
        plot.subtitle = element_text(colour = INK2), axis.text.x = element_text(angle = 30, hjust = 1, colour = INK2))
print(p3)

# ---------------------------------------------------------------------
# 5. SWAP IN REAL DATA (using the objects from Cancellation_DeepDive_sparklyr.R)
# ---------------------------------------------------------------------
# daily cancellations by like-for-like type  -> columns: date, New, Win-back
# daily <- sdf_sql(sc, "SELECT cancel_date AS date, signup_type, COUNT(DISTINCT Customer_External_ID) n
#                       FROM cohort GROUP BY 1,2") %>% collect() %>%
#          tidyr::pivot_wider(names_from = signup_type, values_from = n, values_fill = 0) %>%
#          mutate(date = as.Date(date))
# daily football viewing                      -> add column fb_min_m (million minutes)
# fb <- sdf_sql(sc, "SELECT vdate AS date, SUM(minutes)/1e6 AS fb_min_m FROM v_cohort
#                    WHERE ContentType2 = 'Football' GROUP BY 1") %>% collect() %>% mutate(date = as.Date(date))
# daily <- left_join(daily, fb, by = "date")
# events: your fixture list with columns date, label, tier ('Major' / 'Regular').
#   No fixture table yet? Take the biggest days by viewers in SportsEntertainmentMovies_SD:
#   events <- sdf_sql(sc, "SELECT vdate AS date, competitionName AS label, COUNT(DISTINCT viewerid) v
#                          FROM v_cohort WHERE ContentType2='Football' GROUP BY 1,2 ORDER BY v DESC LIMIT 12") %>% collect()
#   then set tier by competition (e.g. World Cup / UCL / derbies = 'Major').
# Then skip the sample-data block in section 1 (keep w_tier, after_lift not needed) and run sections 2-4.
