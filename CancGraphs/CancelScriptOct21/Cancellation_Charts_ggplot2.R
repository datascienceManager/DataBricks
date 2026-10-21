# =====================================================================
# Cancellation analysis: the same 9 chart types in ggplot2 (R)
# Data = result tables from the Aug-Sep 2026 cancellation notebook, hard-coded so this runs
# stand-alone in a Databricks R cell. To use live data replace each tibble with
#   df <- sdf_sql(sc, "...") %>% collect()    (queries: Cancellation_DeepDive_sparklyr.R)
# Packages: ggplot2, dplyr, tidyr, scales (no extra installs). Not run by the author (no R there).
# Preview of the look: Cancellation_Chart_Gallery_preview.png (drawn with the Python version).
# =====================================================================
library(ggplot2); library(dplyr); library(tidyr); library(scales)

BLUE <- "#2a78d6"; ORANGE <- "#eb6834"; GREY <- "#b8b7b0"; INK <- "#0b0b0b"; INK2 <- "#52514e"; GRID <- "#e6e5e1"
COL <- c("New" = BLUE, "Win-back" = ORANGE, "Churned status" = GREY)
theme_cx <- function() theme_minimal(base_size = 12) + theme(
  panel.grid.minor = element_blank(), panel.grid.major.x = element_blank(),
  panel.grid.major.y = element_line(colour = GRID), plot.title = element_text(face = "bold", colour = INK),
  plot.title.position = "plot", axis.text = element_text(colour = INK2), axis.title = element_text(colour = INK2),
  legend.position = "top", legend.justification = "left")
types <- c("New", "Win-back", "Churned status")

# ---------------- 1. Run-rate bars ----------------
m <- tibble(
  month = factor(c("Apr 25","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan 26","Feb","Mar","Apr","May","Jun","Jul 26","Aug 26","Sep 26"),
                 levels = unique(c("Apr 25","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan 26","Feb","Mar","Apr","May","Jun","Jul 26","Aug 26","Sep 26"))),
  n = c(15107,20313,25519,20400,17395,25160,32273,31317,31375,30260,26370,25854,31693,43067,48369,16845,17293,23680)) %>%
  mutate(i = row_number(), grp = case_when(i == 16 ~ "Jul 26", i == 17 ~ "Aug 26", i == 18 ~ "Sep 26", TRUE ~ "History"))
avg <- mean(m$n[1:16])
# the month labels repeat (May, Jun...), so plot on the position index and relabel the axis
p1 <- ggplot(m, aes(i, n, fill = grp)) + geom_col(width = .7) +
  geom_hline(yintercept = avg, linetype = "dashed", colour = INK) +
  annotate("text", x = 0.6, y = avg + 1500, hjust = 0, size = 3.5, label = sprintf("Apr 25-Jul 26 average %.1fk", avg / 1000)) +
  scale_fill_manual(values = c(History = "#cfcfcf", `Jul 26` = GREY, `Aug 26` = BLUE, `Sep 26` = ORANGE), guide = "none") +
  scale_x_continuous(breaks = m$i, labels = as.character(m$month)) + scale_y_continuous(labels = label_number(big.mark = ",")) +
  labs(title = "Monthly cancelling customers vs run rate", x = NULL, y = "Unique customers") +
  theme_cx() + theme(axis.text.x = element_text(angle = 45, hjust = 1))
print(p1)

# ---------------- 2. Weekly stacked + New/Win-back share ----------------
w <- tibble(wk = factor(c("W31*","W32","W33","W34","W35","W36","W37","W38","W39","W40*"), levels = c("W31*","W32","W33","W34","W35","W36","W37","W38","W39","W40*")),
            `Churned status` = c(890,2620,2991,4654,4810,4602,4599,3892,3409,908),
            New = c(19,26,48,92,63,60,345,963,937,398), `Win-back` = c(45,43,83,133,110,91,484,1514,1740,705))
wl <- w %>% pivot_longer(-wk, names_to = "type", values_to = "n") %>% mutate(type = factor(type, levels = c("Churned status", "New", "Win-back")))
lab <- w %>% mutate(tot = `Churned status` + New + `Win-back`, share = (New + `Win-back`) / tot)
p2 <- ggplot(wl, aes(wk, n, fill = type)) + geom_col(width = .7, colour = "white", linewidth = .3) +
  geom_text(data = lab, aes(wk, tot + 150, label = percent(share, accuracy = 1)), inherit.aes = FALSE, fontface = "bold", size = 3.4) +
  scale_fill_manual(values = COL, name = NULL) + scale_y_continuous(labels = label_number(big.mark = ","), expand = expansion(mult = c(0, .08))) +
  labs(title = "Weekly cancellations by status (label = New + Win-back share)", x = NULL, y = NULL) + theme_cx()
print(p2)

# ---------------- 3. 100% stacked: days to expiry ----------------
e <- tibble(type = rep(types, each = 5), bucket = rep(c("Expired / 0 days","1-7 days","8-14 days","15-30 days","30+ days"), 3),
            n = c(0,0,40,2381,530,  0,0,53,4098,797,  2175,8969,4801,13108,4219)) %>%
  mutate(bucket = factor(bucket, levels = c("Expired / 0 days","1-7 days","8-14 days","15-30 days","30+ days")),
         type = factor(type, levels = rev(types))) %>% group_by(type) %>% mutate(share = n / sum(n)) %>% ungroup()
p3 <- ggplot(e, aes(share, type, fill = bucket)) + geom_col(width = .55, colour = "white", position = position_stack(reverse = TRUE)) +
  geom_text(data = filter(e, share > .05), aes(label = percent(share, accuracy = 1)), position = position_stack(vjust = .5, reverse = TRUE),
            size = 3.4, fontface = "bold", colour = ifelse(filter(e, share > .05)$bucket == "30+ days", "white", INK)) +
  scale_fill_manual(values = c("#c9c8c2", "#e3e2dd", "#9ec5f4", ORANGE, INK), name = NULL) + scale_x_continuous(labels = percent) +
  labs(title = "Days of paid access left at cancellation", x = NULL, y = NULL) + theme_cx() + theme(panel.grid.major.y = element_blank())
print(p3)

# ---------------- 4. Lollipop: non-watcher rate ----------------
nwr <- tibble(type = factor(c("New","Win-back","Churned status","All"), levels = rev(c("New","Win-back","Churned status","All"))),
              pct = c(16.4, 4.0, 6.3, 6.8), n = c(485, 200, 2080, 2765))
p4 <- ggplot(nwr, aes(pct, type, colour = type)) + geom_segment(aes(x = 0, xend = pct, yend = type), colour = "#cfcfcf", linewidth = 1.2) +
  geom_point(size = 5) + geom_text(aes(label = sprintf("%.1f%%  (%s customers)", pct, comma(n))), hjust = -.15, colour = INK, size = 3.6) +
  scale_colour_manual(values = c(New = BLUE, `Win-back` = ORANGE, `Churned status` = GREY, All = INK), guide = "none") +
  scale_x_continuous(limits = c(0, 24), labels = function(x) paste0(x, "%")) +
  labs(title = "Cancellers who never watched (>=1 min)", x = NULL, y = NULL) + theme_cx() + theme(panel.grid.major.y = element_blank())
print(p4)

# ---------------- 5. Heatmap: non-watchers product x country ----------------
hm <- expand_grid(product = c("TOD 4K","TOD Mobile","TOD HD","TOD Total"), country = c("UAE","Egypt","Jordan","Kuwait","Qatar","Iraq","Oman","Others")) %>%
  mutate(n = c(246,152,170,121,45,32,0,16,  209,152,86,63,52,56,18,68,  172,132,95,57,15,0,0,31,  101,13,12,15,15,0,0,0),
         product = factor(product, levels = rev(c("TOD 4K","TOD Mobile","TOD HD","TOD Total"))), country = factor(country, levels = unique(country)))
p5 <- ggplot(hm, aes(country, product, fill = n)) + geom_tile(colour = "white", linewidth = .8) +
  geom_text(aes(label = n, colour = n > 150), size = 3.6) + scale_colour_manual(values = c(`TRUE` = "white", `FALSE` = INK), guide = "none") +
  scale_fill_gradient(low = "#eff5fc", high = "#0d366b", guide = "none") +
  labs(title = "Non-watching cancellers: product x country", x = NULL, y = NULL) + theme_cx() + theme(panel.grid = element_blank())
print(p5)

# ---------------- 6. Grouped bars: viewing recency ----------------
rc <- tibble(when = factor(rep(c("Watched after cancel","Cancel day","1-7 days before","8-30 days before","30+ days before"), 3),
                           levels = c("Watched after cancel","Cancel day","1-7 days before","8-30 days before","30+ days before")),
             type = rep(types, each = 5),
             n = c(1231,300,384,253,298,  2066,375,656,431,1219,  15170,1366,3339,3445,7615)) %>%
  group_by(type) %>% mutate(share = n / sum(n)) %>% ungroup() %>% mutate(type = factor(type, levels = types))
p6 <- ggplot(rc, aes(when, share, fill = type)) + geom_col(position = position_dodge(width = .8), width = .75) +
  scale_fill_manual(values = COL, name = NULL) + scale_y_continuous(labels = percent) +
  labs(title = "When did watchers last watch, relative to cancelling?", x = NULL, y = NULL) + theme_cx()
print(p6)

# ---------------- 7. Distribution: win-back cycling ----------------
cy <- tibble(bucket = factor(c("1*","2","3","4-5","6+"), levels = c("1*","2","3","4-5","6+")), n = c(781,1537,879,907,843)) %>% mutate(share = n / sum(n))
p7 <- ggplot(cy, aes(bucket, n, fill = bucket)) + geom_col(width = .65) +
  geom_text(aes(label = paste0(comma(n), "\n", percent(share, accuracy = 1))), vjust = -.3, fontface = "bold", size = 3.5) +
  scale_fill_manual(values = c(GREY, ORANGE, ORANGE, ORANGE, INK), guide = "none") + scale_y_continuous(expand = expansion(mult = c(0, .15))) +
  labs(title = "Cancelling win-backs: how many cycles?", x = "Lifetime D2C subscriptions per customer", y = NULL) + theme_cx()
print(p7)

# ---------------- 8. Reach of competitions among cancellers ----------------
den <- c(New = 2951, `Win-back` = 4947, `Churned status` = 32873)
rch <- tibble(comp = rep(c("Premier League","UEFA Champions League","Primera Division","FIFA World Cup"), 3), type = rep(types, each = 4),
              n = c(1018,693,1131,247,  3244,3431,3393,2450,  19939,20773,20607,11445)) %>%
  mutate(share = n / den[type], comp = factor(comp, levels = rev(c("Premier League","UEFA Champions League","Primera Division","FIFA World Cup"))),
         type = factor(type, levels = types))
p8 <- ggplot(rch, aes(share, comp, fill = type)) + geom_col(position = position_dodge(width = .8), width = .75) +
  scale_fill_manual(values = COL, name = NULL) + scale_x_continuous(labels = percent) +
  labs(title = "Share of cancellers who watched each competition (before cancelling)", x = NULL, y = NULL) + theme_cx() +
  theme(panel.grid.major.y = element_blank(), panel.grid.major.x = element_line(colour = GRID))
print(p8)

# ---------------- 9. Small multiples: cancellations/day vs viewing (no dual axis) ----------------
wk <- c("W31","W32","W33","W34","W35","W36","W37","W38","W39","W40")
sm <- bind_rows(
  tibble(wk, value = c(474,384,446,697,712,679,775,908,868,670), panel = "Cancellations per day"),
  tibble(wk, value = c(.47,1.63,7.72,29.14,37.35,43.17,65.2,58.64,22.05,10.0), panel = "Football viewing (M minutes)")) %>%
  mutate(wk = factor(wk, levels = wk), panel = factor(panel, levels = c("Cancellations per day", "Football viewing (M minutes)")))
p9 <- ggplot(sm, aes(wk, value, group = 1)) +
  geom_col(data = filter(sm, panel == "Football viewing (M minutes)"), fill = "#c9c8c2", width = .65) +
  geom_line(data = filter(sm, panel == "Cancellations per day"), colour = BLUE, linewidth = 1) +
  geom_point(data = filter(sm, panel == "Cancellations per day"), colour = BLUE, size = 2.5) +
  facet_grid(panel ~ ., scales = "free_y", switch = "y") +
  labs(title = "Cancellations kept rising while viewing fell", x = NULL, y = NULL) + theme_cx() +
  theme(strip.placement = "outside", strip.text.y.left = element_text(angle = 0, hjust = 1, colour = INK2))
print(p9)

# Save any of them:  ggsave("chart.png", p2, width = 10, height = 4.5, dpi = 200)
# Interactive in R: plotly::ggplotly(p2) then htmlwidgets::saveWidget / displayHTML.
