I'll read the new notebook first, then build the deck and Databricks notebook the same way as before.

Reading the rest of the new notebook, which holds the findings write-ups; deck and Databricks notebook come next.

The deck is done. Now the Databricks notebook: I'm porting the R/sparklyr logic to PySpark with the same parameters (FROM, TO, HIST_FROM, ASOF, MATURE_DAYS, VIEW_TBL, SUFFIX_LEN, OUT_DIR) as widgets.

The notebook takes your settings (`FROM`, `TO`, `HIST_FROM`, `ASOF`, `MATURE_DAYS`, `VIEW_TBL`, `SUFFIX_LEN`, `OUT_DIR`) as boxes at the top. I added boxes for the subscriber and transaction table names, which also take an optional parquet path in case those tables aren't loaded yet. Most of your SQL is carried over unchanged. It covers all three questions, including the match-day, competition, match and World Cup comparisons. Charts are saved in the same grey style to `OUT_DIR`.

Run it once on your cluster and check it reproduces 41,164 subscriptions, 40,032 customers, and 14,952 new against 26,212 win-back.

Files are in /mnt/user-data/outputs:
- Cancellations_Aug-Sep_2026.pptx
- Voluntary_Cancellation_Analysis_Databricks.py
