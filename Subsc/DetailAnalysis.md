I'll write this as a Databricks R notebook source file you can import directly.

I've built the subscription pattern analysis as a Databricks R notebook, `Subscription_Pattern_Analysis.R`. I haven't run it, because I have no Databricks access from here. Import it into your workspace as a notebook, attach it to the cluster where your schema notebook ran, and run it top to bottom.

**What it produces (ggplot2, saved as PNGs and shown inline):**
- **Acquisition:** new subscriptions per month, split D2C vs B2B and new vs winback, with month-over-month growth.
- **Mix and churn:** subscriptions by product and offer period, a churn heatmap, and churn rate by segment.
- **Tenure and retention:** a tenure histogram, retention curves that account for subscriptions still active, and churn by start-month cohort.
- **Partner and country:** partner volume against churn, and a country × product breakdown.
- **Churn type:** a breakdown of how subscriptions end.
- **Engagement vs churn:** churn by watch-time bucket, plus median hours watched and sports share by product and status.

**What it needs from your schema notebook:**
- The `sc` connection and the `DIM_SubscribersInfo_SD` and `EntertainmentMovies_SD` temp views must already exist.
- Set `OUT_DIR` near the top to a writable location, such as a Volume path. The default is `/dbfs/FileStore/subscription_analysis`, which may not be writable on your workspace.

**One assumption to verify:** the engagement section links viewing sessions to subscriptions with `Subscription_key = subscriptionid`. The notebook prints a match rate for this join. If it's close to 0%, switch the join to `Subscription_ID`, and the engagement plots (11–13) will then populate.

If you hit errors or want a specific business question answered, send me the output or error text. I can also turn the PNGs into a PPT deck.
