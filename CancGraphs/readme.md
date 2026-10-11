I've made two separate scripts, one in Python and one in ggplot2, each producing the same nine charts. The preview image shows what they look like.

- **Python:** I ran the Python script and drew the preview from it. Paste it into a Databricks Python cell and each chart shows inline. It also has a commented-out plotly version of one chart for hover tooltips and zoom.
- **ggplot2:** The R script mirrors the Python one chart for chart. I couldn't run R, so the preview comes from the Python version. The ggplot2 charts should look very close but are unchecked, and may need a small fix on first run.

All data in both scripts comes from your notebook's result tables, hard-coded so they run on their own. To use live data, swap each one for a query result (the queries are in `Cancellation_DeepDive_sparklyr.R`).

| # | Chart type | What it shows |
|---|---|---|
| 1 | Bars against a run-rate line | Sep volume is inside the normal range |
| 2 | Weekly stacked bars with a share label | New and win-back go from 3% to 55% of cancellations |
| 3 | 100% stacked horizontal bars | 81–83% of new and win-back cancellations still have 15–30 days of access |
| 4 | Lollipop | Non-watcher rate: 16% of new customers, 4% of win-backs |
| 5 | Heatmap | Non-watchers by product and country (UAE 4K and Mobile, then Egypt) |
| 6 | Grouped bars | When watchers last watched; win-backs more often last watched 30+ days before cancelling |
| 7 | Distribution bars | How many subscribe–cancel cycles win-backs have had |
| 8 | Grouped horizontal bars | Half of cancelling win-backs watched the World Cup |
| 9 | Two stacked panels | Cancellations kept rising while viewing fell |

Further charts need data that isn't in this notebook yet:
- **Survival curves** of how long new and win-back subscribers last, from the subscription history.
- **Cohort retention heatmaps** by sign-up week.
- **Sankey or journey diagrams** of cancel, expiry and return, from post-cancellation history.
- **Event-study chart** around match days, in `Cancellations_vs_Match_Events_ggplot2.R`, which needs a fixture list.

Files are in /mnt/user-data/outputs:
- Cancellation_Chart_Gallery_preview.png
- Cancellation_Charts_Python.py
- Cancellation_Charts_ggplot2.R
