I'll start by reading the attached notebook to see its parameters and logic.

Reading the notebook is done and the numbers reconcile. Next I'm building the deck in the TOD gold style from your earlier decks, then the Databricks notebook.

Now the deck build script.

The deck renders cleanly; I'm making two small layout fixes, then building the Databricks notebook.

Good. Delivering both files.

I've built a 15-slide deck and a Databricks notebook from your R notebook. The deck matches your earlier format only approximately. The notebook has not been run, because PySpark wouldn't install in my workspace. I checked its syntax only.

**Deck (`NoFirstWatch_W41.pptx`)**
- It tells the story that 6,635 paying D2C subscribers have never watched more than a minute, and that failures look like access errors rather than streaming problems.
- Every number comes from your notebook's saved outputs. Slide titles state the finding, and nothing in it refers to "the notebook" or a source.
- Charts are native PowerPoint charts, so you can edit them.
- I didn't have your earlier template file, only a description of it. I recreated the yellow header tab and the TOD gold-on-light look from that. If you attach the earlier deck, I'll rebuild on its exact master.

**Notebook (`NoFirstWatch_Analysis_Databricks.py`)**
- It is a PySpark port of your R logic that you can import into Databricks.
- Widgets at the top set the report date, viewing start date, minutes threshold, asset-ID filter, data paths and CSV output.
- Sections cover the cohort, new vs winback, winback attempts, content, errors, device, quality, device sharing, and the year-on-year zero-minute comparison.
- Two small additions: an error-category column (entitlement, HTTP 403, other), and `-1` values in `percentagecomplete` are excluded from averages.

Things I found in your notebook and handled:
- **Year-on-year growth is overstated.** Last year's active subscribers are checked against viewing up to today. Anyone who started watching after 2025 isn't counted as inactive in 2025, so the +226% (2,035 to 6,638) is likely too high. The notebook has a `yoy_basis` option, `as_of_each_year`, that measures like-for-like. The deck says this on slides 5 and 15.
- **"Same day" attempts include earlier history.** In the winback timing analysis, the "same day" bucket includes sessions from before the subscriber's latest re-subscription. I relabelled it "on or before subscription start".
- **Undefined variable.** Cell 8 uses `Final_GreaterThan1Mins_User_2026_UserID`, which is never defined. I used the cohort from cell 6 instead.
- **User counts don't match.** The cohort has 1,714 users with session records, but the session analysis covers 1,488. This is noted on slide 15 and I couldn't reconcile it from the outputs.
- **Error share is a minimum.** The "at least 85% access-denied" figure groups only the 20 most frequent error texts, so the true share is higher or equal.

Please run the notebook once on your cluster and compare its counts to the deck: 6,635 cohort, 4,556 new and 861 winback, 112 winback users with attempts.
