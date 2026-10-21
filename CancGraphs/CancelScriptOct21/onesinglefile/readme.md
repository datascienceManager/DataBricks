Cell 9 is in the file, but the weekly-bar edit hasn't been applied. I'll read the file and then apply it.

Now I'll re-run the script in R with the saved sample query results standing in for Spark.

I've stopped. The run was declined again, so the charts are written but not yet shown running in R.

**Done:**
- `Cancellation_Lifecycle_Analysis.R` is now the single script.
- The new Cell 9 builds 14 ggplot2 charts from the script's own tables (`life`, `cohort`, `tiers`, `nw`, `post`, `fixtures`, `peak_md` and the collected results).
- All charts use the start-status split, with New in blue and Win-back in orange.
- PNGs save to `OUT_DIR`, which is set in Cell 0 (`/dbfs/FileStore/cancellation_charts`).
- The football-calendar chart (Chart 2) now draws its weekly bars in a way that works across ggplot2 versions. Its dashed event lines now show in all three panels.
- Revenue, the event study, return speed and returns vs fixtures are now saved as objects so Cell 9 can chart them.

**What I could and couldn't check:**
- All 57 SQL queries in the script ran in local Spark on your sample data.
- Before the Chart 2 changes, I ran the whole script in R against saved sample results. It ran end to end and saved the PNGs.
- I have not re-run it since those changes, and I haven't looked at the PNGs.
- The sample has only 6 customers, so the charts will look sparse until you run it on the full data in Databricks.

Would you like me to re-run it here and show you the PNGs, or hand it over as it is?
