I'll build one Databricks notebook in your usual format: R/sparklyr cells do the calculations and save the result tables, then Python cells draw the charts and build the PowerPoint with python-pptx. First I'll check your notebook's export format and what I can test here.

All 20 slides built without errors. Rendering to check the layout.

The Python half works: 20 slides, all charts, and every title written from the numbers. A few touch-ups: remove the faint shape shadows, and stop titles saying "by +20%".

Now the R/sparklyr cells. I'm reusing your notebook's SQL, which already ran on your data, and adding a step that saves each summary table for the Python cells.

Both halves are written. One fix in the R chart helper first, so legends show readable labels instead of codes like `d0_7`. Then I'll re-test the Python half and assemble the notebook.

Python tests clean. Now assembling the Databricks notebook in the same .ipynb format as your exports: R as the default language, with `%python` cells.

I've built a single Databricks notebook that runs the whole cancellation analysis in R and then builds the PowerPoint in Python. I tested the Python half end to end, but I couldn't run the R half because there's no R or Spark here. Please run it on your cluster and tell me any error message.

**How it works**
- **R cells (sparklyr, dplyr, ggplot2).** They do all the calculations using your own SQL. Each summary table is saved as a small CSV file in `OUT_ROOT/data`.
- **Python cells (matplotlib, python-pptx).** They read those files, draw the charts in grey and light grey, and build the deck in the same gold-tab layout. The last cell shows a download link.
- **Settings.** Your parameters (`FROM`, `TO`, `HIST_FROM`, `ASOF`, `MATURE_DAYS`, `VIEW_TBL`, `SUFFIX_LEN`) are boxes at the top. I added `EVENT_FROM`, `OUT_ROOT`, the table names, and `CHART_SOURCE`.
- **Charts.** `CHART_SOURCE` controls who draws the charts in the deck. `python` (the default) uses matplotlib. `ggplot2` uses the R charts instead, which are drawn in the notebook either way.
- **Titles update.** Slide titles and cards are written from the results, so rerunning for another period gives correct titles. For example "ran below the average" becomes "above" if volumes go up.

**What I tested**
- I fed the Python half test tables in the same format the R cells save, using your notebook's real numbers. It produced a 20-slide deck that passed the file checks and matches the deck I made earlier.
- Four slides are new: cancel speed, non-watcher signals, win-back cycles and return speed. Your saved notebook had no results for these, so I tested them with made-up values. Your real run will fill them in.

**When you first run it**
- **python-pptx install.** The first Python cell installs it with pip. If your cluster blocks pip, add `python-pptx` as a cluster library instead.
- **Output folder.** If your cluster blocks `/dbfs/FileStore`, set `OUT_ROOT` to a Unity Catalog Volume path.
- **Missing tables.** If there's no transactions table, the revenue slide is skipped. If there are no World Cup match days in the window, the World Cup slide is skipped.

Files are in /mnt/user-data/outputs:
- VoluntaryCancellation_PPT_Generator.ipynb
