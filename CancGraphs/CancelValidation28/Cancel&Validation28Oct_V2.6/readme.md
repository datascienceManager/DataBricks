I'll build V2.6 as one Databricks source file, with validation in the last cells. To answer your question first: yes, "expected" is a model estimate of what cancellations would have been given sign-up volumes, so it's a kind of prediction. I'll remove it completely. Let me look at the two charts you flagged.

Updating the method note to match V2.6:

I've sent `VoluntaryCancellation_All_V2.6.r`, one Databricks source file with the analysis, the HTML report and the validation in its last four cells.

On your question: yes, "expected" was a prediction. The model estimated what cancellations should have been from sign-up volumes, so I've removed it completely: the cell, chart, tables, report section, insight and its settings. Aug–Sep is now compared only with actual history, as a percentage against the Jan 2025–Jul 2026 monthly average and against the same month last year.

**Changes in V2.6**
- **Chart 2 (football calendar):** each competition is now a numbered marker on its dashed line, with a key under the chart giving the full names and dates. No competition name gets cut off, including the Africa Cup of Nations.
- **Chart 6 (non-watchers by product and country):** the three panels are stacked vertically, and country names are wrapped onto two lines in a larger font, so every country is readable.
- **Report tables show every row.** The 50-row limit that cut Aug–Sep from the revenue table is gone.
- **One non-watcher definition:** under 1 minute in the whole paid period. Cell 4, Charts 4 and 6, the KPI and Cell 10 all use it now.
- **September headline:** reported as a percentage against the run rate and against Sep 2025. On your V2.5 numbers that's +33% and +59%, which the z-score was hiding.
- **Revenue run rate:** uses only months where at least 95% of cancelled subscriptions have a transaction. Mid-2025 months are marked * on Chart 1b, and a coverage table shows which months were left out.
- **Bad seasonal expiry dates:** paid periods over 5 years no longer affect the gap rule or episodes, and seasonal offers are capped at 12 months for revenue.
- **Validation (last four cells):**
  - all the V2.5 checks
  - a check that the non-watcher counts in Chart 4, Cell 10 and an independent recount all match
  - a check that the revenue in the report matches the transaction table
  - a check that the bad expiry dates and the seasonal cap were handled
  - a check that every report table includes Aug and Sep

All 107 SQL queries ran on your sample data in local Spark. The R chart code still hasn't run here, so please look at Charts 2 and 6 in particular when you run it in Databricks.

I've also updated the method note to match: the expected-volume section is removed, and it now covers the single non-watcher definition, the revenue coverage rule, the bad expiry dates and the V2.6 settings.
