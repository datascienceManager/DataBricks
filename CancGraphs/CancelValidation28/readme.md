I've sent `VoluntaryCancellation_All_V2.4.r`, the V2.4 notebook. Most of it is tested, but the R code in three cells hasn't run anywhere yet (details at the end).

**To import:** in Databricks go to Workspace → Import → File, choose the `.r` file, and it opens as an R notebook with titled cells. Attach it to the same cluster as V2.3 and use **Run all**.

**Before you run:**
- **Data loading:** Cell 0 ("Load data") is your own loading code from V2.3, unchanged. If the subscription table `DIM_SubscribersInfo_SDF` or the Transaction table `DIM_Transactions_SD` is registered somewhere else, keep that as before.
- **Parameters:** the new settings are in the "Cell 0: parameters and helpers" cell. `ASOF = "auto"` and `PEAK_PCT = "0.90"` drive the fixes. `REF_FROM`, `REF_TO` and `K_MAX` drive the expected-volume model. The defaults (2025 Jan–Sep sign-ups, 12 months) fit the data up to `TO = 2026-09-30`.
- **`display_many()`:** a fallback is defined in case yours isn't loaded in the session.

**What to look at first after the run:**
1. **Data end date:** Cell 1 prints the `ASOF` it picked and Cell 3 prints where the viewing data ends. If viewing ends earlier, it warns you, and viewing after that date isn't counted.
2. **Cell 4 and the playback table:** the error counts should now be far lower than V2.3's 590 / 622 and 1.37M / 2.02M. That's the error-flag fix working.
3. **Chart 10 summary table:** with peak days now the top 10%, the day-0 uplift may change.
4. **Cell 10:** in the monthly back-test, expected and actual should track each other before Aug 2026. If they don't, the reference months need changing before you rely on the Aug–Sep gap.

**Not yet tested:** the R parts of Cell 11 (non-watchers vs viewers), Cell 12 (returns as a share of expiries) and the HTML report. Their SQL ran on the sample data, but the R code that builds their tables and charts hasn't. If any of them errors, send me the message and I'll fix it.
