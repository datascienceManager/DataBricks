Yes, I wrote one (Cancellation_Lifecycle_Analysis.R), but I haven't tested it. I've now set up a local Spark, loading your sample subscription, transaction and viewing data into it, so I can run every query in the script and fix whatever breaks before you run it on Databricks.

Yes, `Cancellation_Lifecycle_Analysis.R` is the sparklyr + dplyr script. I've fixed the one query that failed, and I've only tested its SQL part, not the R code or Databricks itself.

**What I tested:** I ran all 45 SQL queries in the script against your sample Subscription, Transaction and viewership data in a local Spark 3.5.3. All 45 now run. The one that failed was the "3 or more sign-ups within 7 days" query, which Spark rejected because of how it handled dates. It now flags C2, who bought on 23, 24 and 25 Aug.

**What the sample gives back:** The 23 sample subscriptions split into 10 cancelled mid-period, 5 on expiry day, 4 on purchase day or the day after, and 4 with no cancel date. By start status, 6 are New and 17 are Win-back. Of those 17, 14 end as churned and 3 are still active. The viewing attribution also works as intended. For example, one subscription shows 0 own minutes but 4,784 minutes on another subscription or pass, so it counts as watching, just not on the cancelled subscription.

**What I did not test:**
- **R and sparklyr wrapper:** R isn't available here, so the `sdf_sql` / dplyr / `collect` / ggplot2 parts have not been run. Please run Cell 0 first on Databricks. If it errors, send me the message and I'll fix it.
- **Full data:** the sample has only 6 customers, and its viewing table is capped at 1,000 rows. Results will look different at full volume, and some queries returned zero rows on the sample simply because nothing matched.

**Questions about your data (marked `[CHECK]` in the script):**
- **Table names:** the script reads viewership from `EntertainmentMovies_SD`. Your earlier notebook used `SportsEntertainmentMovies_SD`, so change `VIEW_TBL` in Cell 0 to whichever table holds Entertainment and Sports together.
- **Status flip timing:** I assumed a cancelled subscription flips to churned at its paid expiry. That fits the sample but isn't confirmed. When does it actually flip?
- **Revenue:** it was 0.00 in your original notebook, so the script takes it from the Transaction table. Please check those figures.
- **Reversals:** the reversal logic relies on `Subscription_version` history, so please check it against a few IDs you know were reversed.
- **Not in the data:** login failures and complaints. The script uses playback and entitlement errors as proxies.
- **1-day paid periods:** these appear in the data and I can't explain them.

Once you've run Cells 1–2 on the full data, I can rebuild the management deck on the start-status basis (New vs Win-back, from each customer's first status).
