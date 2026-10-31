The R script is attached as `overlapping_sessions_sparklyr.R`. It is the same code as before, with `dplyr::` on every dplyr function. Replace `"your_table"` with your Spark table name. If you are not on Databricks, you also need to create the `sc` connection with `spark_connect()`. I haven't run it on Spark.

The thresholds are two variables at the top (`min_concurrent` and `min_seconds`), so you can change them there. It also needs dbplyr 2.3.0 or later.
