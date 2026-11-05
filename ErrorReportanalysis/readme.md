I'll first check the exact table shapes the deck needs, so the notebook can recreate each one from Spark.

I'm assembling the generator notebook from the tested pieces and testing it on a local Spark with synthetic data. The first step is to run the assembler.

The generated deck matches the delivered one on all 1,170 text lines except the device footer wording, and it passes the file validity check. Now I'll assemble the final notebook file.

I've written a Databricks notebook that builds the 15-slide Error 404 deck from your session data, using python-pptx with native, editable charts. I tested it two ways, but I couldn't run it on your Databricks data.

- **Local Spark test:** it runs end to end on a local Spark session with synthetic data. The output deck opens without errors.
- **Real-tables test:** fed your real report tables and the figures from your run, it produces the same 15 slides as the deck I sent you. The only differences are the device footnote wording, which I made dynamic, and the December cells, which now match.

**To use it**
1. Import the file into Databricks as a notebook and attach it to the cluster where `EntertainmentMovies_SD` exists, as in your validation run.
2. Upload `Error360_Nov2026_draft12_1.pptx` to a Volume. The notebook reuses its header, footer and logo so the new deck matches the Error 360 look.
3. In the config cell, set `TEMPLATE_PATH` and `OUTPUT_PATH`, and `END_DATE` if you want a fixed date. The default is yesterday, as in the report. Then run all cells.

**What it does**
- It rebuilds every table from Spark with the report's own definitions, including the standalone-404 rule, the spike line (median + 3 × MAD) and the 404 type rules. It also uses the Doha-time hour table.
- Charts, tables, KPI tiles and axis ranges all come from the current data.
- Totals and the date window inside the slide sentences are updated automatically.

**Limits**
- **Slide wording:** the sentences contain other numbers from 4 Nov 2026, such as "85.6% on tvOS" and "14 of the 15 worst days". They don't update on their own. The final check cell lists 31 quoted figures and marks any that moved as "REVIEW narrative", so you know which sentences to edit.
- **Validation slides:** the two VALIDATION slides were written for the 4–5 Nov data. Set `INCLUDE_VALIDATION_SLIDES = False` to leave them out of future runs.
- **Raw-message labels:** the shortened labels on the raw-message table cover the current top 10 messages. A message outside that list shows its raw text trimmed.
- **Source view:** it assumes the view has the same columns as in the report.

Files are in /home/claude:
- Error404_PPTX_Generator_Databricks.py
