I've built the chart from the weekly figures in your notebook and rendered a PNG. R isn't available in my workspace, so I drew the PNG with plotnine, a Python port of ggplot2, using the same layers and data as the R script. The ggplot2 code itself has not been run, so expect a small cosmetic tweak on first run, such as label positions.

The chart has three panels sharing one date axis:
- **Cancellations per day:** stacked by status, with churned in grey, new in blue and win-back in orange.
- **New + Win-back share of cancellations:** a line that goes from about 3% in early September to 15% (week of 7 Sep), 39%, 44% and 55% by the end of September.
- **Football viewing per day:** viewing peaks in the week of 7 Sep at about 9M minutes a day. It then drops to about 3M a day while cancellations stay high.

Dashed lines mark five football events. The notebook has no fixture list, so each line sits at the first date that competition appears in the viewing data. That is only a rough stand-in for kick-off. The events shown are:
- League Cup (7 Aug)
- UEFA Super Cup (12 Aug)
- Community Shield / Trophée des Champions (16 Aug)
- AFC Champions League Elite (14 Sep)
- Nations League and AFCON qualifiers (24 Sep)

The Premier League, Champions League and La Liga season starts aren't in the data. Their viewing already runs from 1 June, so I couldn't date them.

The data is weekly, so this shows trends across weeks, not the day-by-day jump after each match. For that, run the daily version in `Cancellations_vs_Match_Events_ggplot2.R` once you have a fixture list. The chart's subtitle says that timing alone doesn't prove football caused the cancellations.

Files are in /mnt/user-data/outputs:
- Cancellations_vs_Football_Events_preview.png
- Cancellations_vs_Football_Events_ggplot2.R
