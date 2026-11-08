Working on it: the notebook will read the report's sparklyr/dplyr data frames (published as Spark views) and write every slide number, title and takeaway from them, with no typed-in figures. I'm going through the slide text now. This is a large rewrite, so it will take a while.

Still on it. The design: the notebook will take the report's two R data frames (raw1 and det, published to Spark with sparklyr) and also keep the direct Spark-view option. All slide figures, titles and takeaways will then be computed from the data. The Error 360 reference figures will move into one editable settings block, since they come from another document. I'm now writing the dynamic slide text.

Slides 2 to 12 now generate their text from the data. Next I'm doing the validation and Error 360 slides, then the two data inputs (R frames or Spark view), and then testing the whole notebook.

The dynamic slide text is done and matches the current deck almost word for word when I run it on the 4 Nov values. I'm now wiring the notebook: new input cell (R frames or Spark view), the settings cell, then a full test of both input modes.

The Spark-view mode runs end to end on test data. I'm fixing one variable clash that blocks the second test (R-frames mode), then I'll compare the two modes' output.

The generated deck renders cleanly on all 15 slides. The numbers look odd only because the test data is synthetic. Sending the notebook now.

The generator notebook is now fully data-driven. I tested it only on synthetic data, not on your real Databricks data or from R on a cluster.

**What changed**
- Slide titles, takeaways, KPI numbers, axis limits, month names and date ranges are now computed from the data.
- Examples are the dominant device, the spike window, the peak and quiet months, the top competitions, countries and assets, and the peak hours.
- The validation slide recomputes its checks on each run.
- It runs in either of two input modes, set in the config cell:
  - `report_frames` (for your sparklyr/dplyr case): the R report publishes its two frames as tables, and the notebook reads them. `raw1` holds the grouping-set aggregates and `det` holds one row per 404 session. A markdown cell in the notebook has the R snippet, using `spark_write_table` to `r404_raw1` and `r404_det`.
  - `spark_view`: the notebook runs the same SQL itself against your session view.

**What I tested**
- On synthetic Spark data, both modes give identical slide text. The deck has 15 slides and passes the PowerPoint validator. I rendered every slide and the layout is clean.

**What you should know before relying on it**
- **Wording:** the auto-generated text is slightly more generic than the delivered deck. I removed event attributions such as "World Cup", because the data can't prove them.
- **Still typed in the config cell:**
  - the Error 360 comparison values;
  - the alert levels (5,000 and 10,000, which are my proposals);
  - the focus country.
- **Outside-market count:** this appears only if you set `OUTSIDE_SOURCE_VIEW`. Otherwise the slide shows "not measured".
- **Validation slide:** it no longer quotes the report's earlier 32 checks or the 31/31 Databricks run, only the checks it recomputes on that run.
- **Hourly chart:** hours now come from `raw1` rather than the report's separate hour table.
- **Delivered PPTX:** it was built by the older hard-coded script, so a notebook run on your real data may differ slightly in wording.

Please run it once on your cluster and tell me about any column or schema mismatch. The notebook checks the required columns up front and names any that are missing.
