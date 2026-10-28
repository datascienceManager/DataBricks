I’ve sent the updated VoluntaryCancellation_All_V2.5.r. Revenue now uses Transaction_price_excl_TAX_USD instead of the price including tax.

What changed:

Monthly revenue lost: the last charge is now taken excluding tax. This feeds Chart 1b, the revenue table, the KPI card and the report insight.
Value back from returners (Cell 11): the first charge on the next subscription is also excluding tax.
Labels: the chart subtitle and the report’s definitions box now say “excl. tax”.

All 89 SQL queries still run on the sample data, which has the Transaction_price_excl_TAX_USD column.



---- Validation Code -----

Partly. Most of the old checks still run on V2.5, but four would mislead you, because V2.5 changed what they compare against:

- **V2:** its recount from the source doesn't apply the voluntary filter, so it won't match the V2.5 cohort.
- **V5:** it now only sees voluntary records, so the July break disappears from view.
- **V6:** the error fix it tests is already built into V2.5.
- **V9:** the expected revenue figures include tax.

The comments also quote V2.3 figures. I'll write a V2.5 version with those fixed and new checks for the V2.5 changes.

Use `Cancellation_Validation_Checks_V2.5.R`, not the old script. All of its SQL ran on your sample data in local Spark, and the sample gave the expected results (details at the end).

**How to run it:** paste it into new cells after Cell 7 of the V2.5 notebook (the "returns vs fixtures" cell), as you did before. It uses the same `display_many()` layout.

**What changed from the old checks:**
| Check | V2.5 version |
|---|---|
| V2 Cohort recount | Now applies the voluntary filter to the source, so "source, voluntary" should equal the V2.5 cohort exactly. It also shows how many were excluded. |
| V3 Voluntary filter | Confirms nothing involuntary is left in the cohort, and shows by month what was removed, including the Mar–Jun Apple block. |
| V5 Gap rule (new) | Lists any subscription whose New / Win-back / Quick re-subscribe label breaks the 7-day rule. **Expect no rows.** |
| V6 Episodes (new) | Recomputes episode numbers independently. **Expect 0 mismatches**, and never more episodes than subscriptions. |
| V7 Error flags | Confirms V2.5's fix is in place. **Expect `blank_flagged_as_error` = 0** and an error rate of a few percent, not ~90%. |
| V8 Non-watchers | The whole-paid-period count should equal the Non-watcher rows in Cell 11. |
| V9 Revenue | Recomputes revenue excluding tax and monthly-equivalent from the transaction table. It should match Cell 2's Aug–Sep total, which the cell prints next to it. |
| V10–V13 | Unchanged in purpose. The 5-customer trace now also shows the system label, the label used, the gap, the episode, the voluntary flag, months covered and the last charge. |

The comments now say what a correct result looks like rather than quoting V2.3 numbers, because V2.5 figures will differ.

On the sample data the checks came out as expected:
- the cohort recount matched
- no gap-rule breaks
- 0 episode mismatches
- no blank error text flagged as an error

Send me the output once you've run it on the full data and I'll go through it.
