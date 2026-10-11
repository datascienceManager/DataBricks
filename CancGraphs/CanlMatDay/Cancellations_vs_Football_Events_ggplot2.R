# =====================================================================
# Cancellations vs football match events: ggplot2, using the REAL weekly figures from the
# Aug-Sep 2026 cancellation notebook (weekly cancellations by status + weekly football viewing).
# Needs: ggplot2, dplyr, tidyr, scales. Paste into a Databricks R cell.
# =====================================================================
library(ggplot2); library(dplyr); library(tidyr); library(scales); library(tibble)

BLUE <- "#2a78d6"; ORANGE <- "#eb6834"; GREY <- "#b8b7b0"; INK <- "#0b0b0b"; INK2 <- "#52514e"; GRID <- "#e6e5e1"

# ---- 1. Weekly data from the notebook (Weekly_Cancel_vs_Matches result) ----
weekly <- tribble(
  ~wk, ~start,        ~end,          ~total, ~new, ~winback, ~churned, ~view_min,
  31,  "2026-08-01",  "2026-08-02",   949,    19,   45,       890,       474993,
  32,  "2026-08-03",  "2026-08-09",  2687,    26,   43,      2620,      1627295,
  33,  "2026-08-10",  "2026-08-16",  3121,    48,   83,      2991,      7715974,
  34,  "2026-08-17",  "2026-08-23",  4879,    92,  133,      4654,     29142095,
  35,  "2026-08-24",  "2026-08-30",  4983,    63,  110,      4810,     37353014,
  36,  "2026-08-31",  "2026-09-06",  4751,    60,   91,      4602,     43170111,
  37,  "2026-09-07",  "2026-09-13",  5424,   345,  484,      4599,     65200188,
  38,  "2026-09-14",  "2026-09-20",  6359,   963, 1514,      3892,     58644368,
  39,  "2026-09-21",  "2026-09-27",  6079,   937, 1740,      3409,     22045682,
  40,  "2026-09-28",  "2026-09-30",  2010,   398,  705,       908,      9995484) %>%
  mutate(start = as.Date(start), end = as.Date(end), days = as.numeric(end - start) + 1,
         mid = start + days / 2)               # bar centre; bar width = days (so part-weeks are drawn at true width)

# ---- 2. Football events: first viewing date of each competition in the viewing data
#         (a proxy for kick-off; swap in your fixture list when you have one) ----
events <- tribble(
  ~date,        ~label,                                      ~ypos, ~hj,
  "2026-08-07", "League Cup starts",                          0.93,  0,
  "2026-08-12", "UEFA Super Cup",                             0.80,  0,
  "2026-08-16", "Community Shield / Trophee des Champions",   0.67,  0,
  "2026-09-14", "AFC Champions League Elite",                 0.93,  1,
  "2026-09-24", "Nations League + AFCON qualifiers",          0.80,  1) %>%
  mutate(date = as.Date(date), tx = date + ifelse(hj == 0, 0.4, -0.4))   # label sits beside its line

# ---- 3. Three panels on one date axis (one y-scale per panel, no dual axis) ----
lv <- c("Cancellations per day", "New + Win-back share of cancellations (%)", "Football viewing (million minutes per day)")
mk <- function(x) factor(x, levels = lv)

canc <- weekly %>% transmute(mid, days, `Churned status` = churned / days, New = new / days, `Win-back` = winback / days) %>%
  pivot_longer(c(`Churned status`, New, `Win-back`), names_to = "type", values_to = "v") %>%
  mutate(type = factor(type, levels = c("Churned status", "New", "Win-back")), panel = mk(lv[1]))
share <- weekly %>% transmute(mid, v = (new + winback) / total * 100, panel = mk(lv[2]))
view  <- weekly %>% transmute(mid, days, v = view_min / 1e6 / days, panel = mk(lv[3]))
top1 <- max(weekly$total / weekly$days) * 1.55
ev_lab   <- events %>% mutate(panel = mk(lv[1]), y = ypos * top1)
headroom <- tibble(mid = min(weekly$start), v = c(top1, 65), panel = mk(lv[1:2]))

p <- ggplot() +
  geom_vline(data = events, aes(xintercept = date), colour = INK2, linetype = "dashed", linewidth = .4) +
  geom_col(data = canc, aes(mid, v, fill = type, width = days), position = position_stack(reverse = TRUE), colour = "white", linewidth = .3) +
  geom_line(data = share, aes(mid, v), colour = INK, linewidth = .9) +
  geom_point(data = share, aes(mid, v), colour = INK, size = 2.4) +
  geom_text(data = share, aes(mid, v, label = sprintf("%.0f%%", v)), vjust = -1, size = 3.3, colour = INK) +
  geom_col(data = view, aes(mid, v, width = days), fill = "#c9c8c2") +
  geom_blank(data = headroom, aes(mid, v)) +
  geom_text(data = ev_lab, aes(tx, y, label = label, hjust = hj), size = 3.1, colour = INK2) +
  facet_wrap(~ panel, ncol = 1, scales = "free_y") +
  scale_fill_manual(values = c(`Churned status` = GREY, New = BLUE, `Win-back` = ORANGE), name = NULL) +
  scale_x_date(date_breaks = "1 week", date_labels = "%d %b", expand = expansion(mult = .01)) +
  scale_y_continuous(expand = expansion(mult = c(0, .12))) +
  labs(title = "Cancellations climbed with the football season, and the New + Win-back share jumped after mid-September",
       subtitle = paste0("Weekly totals divided by days in the week. Dashed lines = first viewing date of the competition (proxy for kick-off).\n",
                         "1-2 Aug and 28-30 Sep are part-weeks. Timing alone does not prove football caused the cancellations."),
       x = NULL, y = NULL) +
  theme_minimal(base_size = 12) +
  theme(panel.grid.minor = element_blank(), panel.grid.major.x = element_blank(), panel.grid.major.y = element_line(colour = GRID),
        strip.text = element_text(hjust = 0, face = "bold", colour = INK), legend.position = "top", legend.justification = "left",
        plot.title = element_text(face = "bold", colour = INK, size = 13), plot.title.position = "plot",
        plot.subtitle = element_text(colour = INK2, size = 10), axis.text = element_text(colour = INK2))
print(p)
ggsave("cancellations_vs_football_events.png", p, width = 11, height = 8, dpi = 200, bg = "white")
