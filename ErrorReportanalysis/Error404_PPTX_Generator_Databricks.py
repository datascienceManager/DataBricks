# Databricks notebook source
# MAGIC %md
# MAGIC # Error 404 deep-dive: generate the PowerPoint from Databricks
# MAGIC Builds the 15-slide Error 404 deck (same format as the Error 360 deck) straight from the session data, with python-pptx and native, editable charts.
# MAGIC
# MAGIC **Before you run**
# MAGIC 1. Run the report notebook (`Error_ReportJ_V7`, cell 1) in this session so the session-level view `EntertainmentMovies_SD` exists, or set `SOURCE_VIEW` to your own view with the same columns.
# MAGIC 2. Upload `Error360_Nov2026_draft12_1.pptx` to a Unity Catalog Volume (or DBFS) and set `TEMPLATE_PATH`. Its header, footer and logo are reused, so the output matches the Error 360 look.
# MAGIC 3. Set `OUTPUT_PATH`, then Run all.
# MAGIC
# MAGIC **What each part does**
# MAGIC | Cell | What it does |
# MAGIC |---|---|
# MAGIC | Config | paths, view name, end date |
# MAGIC | A. Data layer | rebuilds the report tables with the same definitions as the report (error session, standalone 404, spike line = median + 3 MAD, 404 type rules) |
# MAGIC | B. Derived objects | turns the tables into the numbers the slides use |
# MAGIC | C. Helpers | text, table and native chart helpers that reproduce the Error 360 styling (grey palette) |
# MAGIC | D. Build | the 15 slides and the file |
# MAGIC | E. Freshness check | the slide sentences quote 4 Nov 2026 figures; this lists every quoted figure that moved so you can edit that wording |
# MAGIC | F. Deliver | copies the deck to `OUTPUT_PATH` |
# MAGIC
# MAGIC Charts, tables, KPI tiles and axis ranges always come from the current data. Totals and the date window in the sentences are updated automatically; any other number quoted inside a sentence is listed by the freshness check.
# COMMAND ----------

# MAGIC %pip install python-pptx
# COMMAND ----------

dbutils.library.restartPython()
# COMMAND ----------

# ---- Configuration (edit these) --------------------------------------------------------------------------------------------
TEMPLATE_PATH = "/Volumes/<catalog>/<schema>/<volume>/Error360_Nov2026_draft12_1.pptx"   # the Error 360 deck: its slide chrome is reused
OUTPUT_PATH   = "/Volumes/<catalog>/<schema>/<volume>/Error404_DeepDive.pptx"            # where the finished deck is written
SOURCE_VIEW   = "EntertainmentMovies_SD"        # session-level view built in cell 1 of the report notebook (Error_ReportJ_V7)
END_DATE      = None                            # last day of data, 'YYYY-MM-DD'; None = yesterday (same rule as the report)
INCLUDE_VALIDATION_SLIDES = True                # the two VALIDATION slides were written for the 4-5 Nov 2026 data: review or switch off

import datetime as _dt, pandas as _pd
END_DATE = END_DATE or str(_dt.date.today() - _dt.timedelta(days=1))
_e = _pd.to_datetime(END_DATE)
END_LABEL = "%d %s" % (_e.day, _e.strftime("%b %Y"))          # e.g. 3 Nov 2026
LAST_MONTH_NAME, LAST_MONTH_DAYS = _e.strftime("%B %Y"), int(_e.day)
OUT_LOCAL = "/tmp/Error404_DeepDive.pptx"

# COMMAND ----------

# ---- A. Data layer: rebuild the Error 404 report tables from Spark (same definitions as Error_ReportJ_V7) ------------------
import pandas as pd, numpy as np, datetime as dt

COVERED = ['algeria', 'bahrain', 'egypt', 'iraq', 'jordan', 'kuwait', 'lebanon', 'libya', 'morocco', 'oman', 'qatar', 'saudi arabia', 'syria',
           'tunisia', 'united arab emirates', 'yemen', 'occupied palestinian territory', 'south sudan', 'mauritania', 'sudan', 'somalia', 'chad']
covered_sql = ",".join("'%s'" % c for c in COVERED)

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW err404_base AS
SELECT viewerId, startdate, year(startdate) AS yr,
       coalesce(hour(starttime), -1)             AS hr,
       coalesce(lower(trim(country)), 'unknown') AS country,
       coalesce(deviceos, 'Unknown')             AS deviceos,
       CASE WHEN Live = 1 THEN 'Live' ELSE 'VOD' END AS live_vod,
       coalesce(ContentType2, 'Unknown')         AS content_type,
       coalesce(cdn, 'Unknown')                  AS cdn,
       coalesce(appversion, 'Unknown')           AS appversion,
       coalesce(assetname, asset1, 'Unknown')    AS asset,
       coalesce(competitionName, 'Unknown')      AS competition,
       coalesce(playingunixtimems, 0) / 60000.0  AS play_min,
       CASE WHEN startuperror = '1' OR (errorlist IS NOT NULL AND trim(errorlist) <> '') THEN 1 ELSE 0 END AS is_err,
       CASE WHEN errorlist RLIKE '(?<![0-9])404(?![0-9])' THEN 1 ELSE 0 END AS is_404,
       CASE WHEN country IS NULL OR lower(trim(country)) IN ('', 'n/a', 'unknown', 'na') THEN 'Unknown'
            WHEN lower(trim(country)) IN ({covered_sql}) THEN 'Covered (MENA)'
            ELSE 'Outside coverage' END AS geo,
       errorlist
FROM {SOURCE_VIEW}
WHERE startdate BETWEEN DATE'2025-01-01' AND DATE'{END_DATE}'
  AND viewerId IS NOT NULL AND viewerId NOT LIKE '%.%'
""")

# Pass 1: all aggregates in one scan
raw1 = spark.sql("""
SELECT CASE WHEN grouping(startdate) = 0    THEN 'daily'
            WHEN grouping(country) = 0      THEN 'country'
            WHEN grouping(hr) = 0           THEN 'hour'
            WHEN grouping(content_type) = 0 THEN 'content'
            WHEN grouping(competition) = 0  THEN 'competition'
            ELSE 'cdn' END AS set_name,
       startdate, yr, geo, deviceos, live_vod, country, hr, content_type, cdn, competition,
       COUNT(*) AS sessions, SUM(is_err) AS err, SUM(is_404) AS e404,
       SUM(CASE WHEN is_err = 1 AND is_404 = 0 THEN 1 ELSE 0 END) AS eoth
FROM err404_base
GROUP BY GROUPING SETS ((startdate, geo, deviceos, live_vod), (yr, geo, country), (yr, hr), (yr, content_type), (yr, cdn), (yr, competition))
""").toPandas()
for c_ in ("sessions", "err", "e404", "eoth", "yr", "hr"):
    raw1[c_] = pd.to_numeric(raw1[c_])

# Pass 2: every Error 404 session (about 220K rows), with the 404 type and a masked raw message
det = spark.sql("""
SELECT startdate, yr, hr, geo, country, deviceos, live_vod, asset, competition, viewerId,
       CASE WHEN errorlist RLIKE '(?i)manifest' THEN 'Manifest 404 (DASH/HLS manifest not found)'
            WHEN errorlist RLIKE '(?i)playlist' THEN 'Playlist 404 (HLS playlist download failed)'
            WHEN errorlist RLIKE '(?i)(segment|fragment|chunk)' THEN 'Segment 404 (media segment missing)'
            WHEN errorlist RLIKE '(?i)(license|drm|widevine|fairplay)' THEN 'DRM / License 404'
            WHEN errorlist RLIKE '-12938' THEN 'Apple AVPlayer -12938 (File Not Found)'
            WHEN errorlist RLIKE '(?i)(BAD_HTTP_STATUS|unexpected HTTP status)' THEN 'Android/ExoPlayer bad HTTP status 404'
            ELSE 'Other 404' END AS err_type,
       substring(regexp_replace(errorlist, '[0-9]{10,}', '<ts>'), 1, 110) AS err_msg
FROM err404_base WHERE is_404 = 1
""").toPandas()
det["startdate"] = pd.to_datetime(det["startdate"]).dt.date
det["yr"] = pd.to_numeric(det["yr"])
det26, det25 = det[det.yr == 2026], det[det.yr == 2025]


# ---- helpers (formatting identical to the report tables) -----------------------------------------------------------------
def fmt_n(x):
    return "-" if pd.isna(x) else "{:,.0f}".format(x)


def fmt_p(x, d=2):
    return "-" if (pd.isna(x) or np.isinf(x)) else ("%." + str(d) + "f%%") % x


def fmt_d(x, d=2):
    return "-" if (pd.isna(x) or np.isinf(x)) else ("%." + str(d) + "f") % x


def top_of(v):
    v = list(v)
    if not v:
        return "-"
    cnt = pd.Series(v).value_counts()
    cnt = cnt.reset_index()
    cnt.columns = ["k", "n"]
    cnt = cnt.sort_values(["n", "k"], ascending=[False, True])
    return "%s (%d%%)" % (cnt.k.iloc[0], round(100 * cnt.n.iloc[0] / len(v)))


def sum_by(df, by):
    out = df.groupby(by, dropna=False)[["sessions", "err", "e404", "eoth"]].sum().reset_index()
    out["per10k"] = 1e4 * out.e404 / out.sessions
    out["oth_per10k"] = 1e4 * out.eoth / out.sessions
    out["pct_err"] = np.where(out.err > 0, 100 * out.e404 / out.err, np.nan)
    return out


def cmp_table(agg, key, label, top=12):
    a = agg[agg.yr == 2026].copy()
    b = agg[agg.yr == 2025][[key, "per10k", "pct_err"]].rename(columns={"per10k": "per10k_25", "pct_err": "pct_err_25"})
    a["share"] = 100 * a.e404 / a.e404.sum()
    a = a.merge(b, on=key, how="left").sort_values("e404", ascending=False, kind="stable").head(top)
    out = pd.DataFrame({label: a[key], "2026 Sessions": a.sessions.map(fmt_n), "2026 Error 404": a.e404.map(fmt_n),
                        "2026 Other errors (non-404)": a.eoth.map(fmt_n), "Share of 2026 404": a.share.map(lambda x: fmt_p(x, 1)),
                        "2026 404 per 10k sessions": a.per10k.map(fmt_d), "2026 Other errors per 10k sessions": a.oth_per10k.map(fmt_d),
                        "2026 404 % of errors": a.pct_err.map(fmt_p), "2025 404 per 10k sessions": a.per10k_25.map(fmt_d),
                        "2025 404 % of errors": a.pct_err_25.map(fmt_p)})
    return out.reset_index(drop=True)


# ---- daily and monthly ---------------------------------------------------------------------------------------------------
dd = raw1[raw1.set_name == "daily"].copy()
dd["startdate"] = pd.to_datetime(dd["startdate"])
dd["yr"] = dd.startdate.dt.year
dd["mn"] = dd.startdate.dt.month
day_tot = sum_by(dd, ["startdate", "yr", "mn"])
mon = (day_tot.groupby(["yr", "mn"]).agg(days=("startdate", "nunique"), sessions=("sessions", "sum"), err=("err", "sum"), e404=("e404", "sum"),
                                       eoth=("eoth", "sum"), avg_daily_oth=("eoth", "mean"), avg_daily_404=("e404", "mean"),
                                       avg_daily_pct=("pct_err", "mean")).reset_index())
yt = sum_by(dd, ["yr"])
y26, y25 = yt[yt.yr == 2026].iloc[0], yt[yt.yr == 2025].iloc[0]


def _mk(y):
    x = mon[mon.yr == y].set_index("mn")
    return x.reindex(range(1, 13))


w26, w25 = _mk(2026), _mk(2025)
T = {}
T["monthly"] = pd.DataFrame({
    "Month": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
    "2026 Error 404": w26.e404.map(fmt_n).values, "2026 Other errors (non-404)": w26.eoth.map(fmt_n).values,
    "2026 Avg daily 404": w26.avg_daily_404.map(lambda v: fmt_d(v, 1)).values, "2026 Avg daily Other errors": w26.avg_daily_oth.map(lambda v: fmt_d(v, 1)).values,
    "2026 Avg daily 404 % of errors": w26.avg_daily_pct.map(fmt_p).values,
    "2025 Error 404": w25.e404.map(fmt_n).values, "2025 Other errors (non-404)": w25.eoth.map(fmt_n).values,
    "2025 Avg daily 404": w25.avg_daily_404.map(lambda v: fmt_d(v, 1)).values, "2025 Avg daily Other errors": w25.avg_daily_oth.map(lambda v: fmt_d(v, 1)).values,
    "2025 Avg daily 404 % of errors": w25.avg_daily_pct.map(fmt_p).values,
    "YoY change in 404 share (pp)": (w26.avg_daily_pct - w25.avg_daily_pct).map(fmt_d).values})

# ---- spike days: threshold = median + 3 * MAD (R's mad: 1.4826 * median absolute deviation) ------------------------------
d26 = day_tot[day_tot.yr == 2026].sort_values("startdate").reset_index(drop=True)
MEDIAN = float(d26.e404.median())
MAD = 1.4826 * float((d26.e404 - MEDIAN).abs().median())
THR = MEDIAN + 3 * max(MAD, 1)
d26["spike"] = d26.e404 > THR
sp = d26.sort_values("e404", ascending=False, kind="stable").head(15)
rows = []
for r_ in sp.itertuples():
    x = det26[det26.startdate == r_.startdate.date()]
    rows.append([str(r_.startdate.date()), r_.startdate.strftime("%A"), "Yes" if r_.spike else "No", fmt_n(r_.sessions), fmt_n(r_.err),
                 fmt_n(r_.e404), fmt_p(r_.pct_err), fmt_d(r_.e404 / MEDIAN, 1) + "x", top_of(x.deviceos), top_of(x.err_type), top_of(x.asset),
                 fmt_p(100 * (x.geo == "Outside coverage").mean() if len(x) else np.nan, 1)])
T["top_days"] = pd.DataFrame(rows, columns=["Date", "Weekday", "Spike", "Sessions", "Errors", "Error 404", "404 % of errors", "vs median day",
                                           "Top device", "Top 404 type", "Top asset", "Outside-coverage share"])

# ---- bottlenecks ---------------------------------------------------------------------------------------------------------
dev_agg = sum_by(dd, ["yr", "deviceos"])
dev_agg["deviceos"] = dev_agg.deviceos.replace({"": "(not reported)", "Unknown": "(not reported)"})
dev_agg = dev_agg.groupby(["yr", "deviceos"], as_index=False)[["sessions", "err", "e404", "eoth"]].sum()
dev_agg["per10k"] = 1e4 * dev_agg.e404 / dev_agg.sessions
dev_agg["oth_per10k"] = 1e4 * dev_agg.eoth / dev_agg.sessions
dev_agg["pct_err"] = np.where(dev_agg.err > 0, 100 * dev_agg.e404 / dev_agg.err, np.nan)
T["device"] = cmp_table(dev_agg, "deviceos", "Device OS", top=15)
N_OS = int(dev_agg[dev_agg.yr == 2026].deviceos.nunique())
DEV_ALL = dev_agg[dev_agg.yr == 2026].sort_values("e404", ascending=False)

e6 = det26.groupby("err_type").size().rename("n_26")
e5 = det25.groupby("err_type").size().rename("n_25")
et = pd.concat([e6, e5], axis=1).fillna(0).sort_values("n_26", ascending=False)
T["type"] = pd.DataFrame({"Error 404 type": et.index, "2026 sessions": et.n_26.map(fmt_n).values, "2026 share": (100 * et.n_26 / et.n_26.sum()).map(lambda v: fmt_p(v, 1)).values,
                          "2025 sessions": et.n_25.map(fmt_n).values, "2025 share": (100 * et.n_25 / max(et.n_25.sum(), 1)).map(lambda v: fmt_p(v, 1)).values})
mg = det26.groupby("err_msg").agg(n=("viewerId", "size"), v=("viewerId", "nunique")).sort_values("n", ascending=False, kind="stable").head(10)
T["msgs"] = pd.DataFrame({"Raw error message 2026 (timestamps masked)": mg.index, "404 sessions": mg.n.map(fmt_n).values, "Viewers": mg.v.map(fmt_n).values,
                          "Top device": [top_of(det26[det26.err_msg == m_].deviceos) for m_ in mg.index]})

live = sum_by(dd, ["yr", "live_vod"]).rename(columns={"live_vod": "key"})
live["key"] = "Live/VOD: " + live["key"]
cont_raw = sum_by(raw1[raw1.set_name == "content"], ["yr", "content_type"]).rename(columns={"content_type": "key"})
cont_raw["key"] = "Content: " + cont_raw["key"]
con = pd.concat([live, cont_raw], ignore_index=True)
T["content"] = cmp_table(con, "key", "Content dimension", top=15)
T["cdn"] = cmp_table(sum_by(raw1[raw1.set_name == "cdn"], ["yr", "cdn"]), "cdn", "CDN", top=10)

comp = sum_by(raw1[raw1.set_name == "competition"], ["yr", "competition"])
comp = comp[~comp.competition.str.lower().str.strip().isin(["unknown", "", "n/a", "na", "null", "none"])]
T["comp"] = cmp_table(comp, "competition", "Competition", top=20)
T["comp"]["404 viewers (2026)"] = [fmt_n(det26[det26.competition == c_].viewerId.nunique()) for c_ in T["comp"].Competition]
T["comp"]["Top device for 404"] = [top_of(det26[det26.competition == c_].deviceos) for c_ in T["comp"].Competition]

hr = sum_by(raw1[(raw1.set_name == "hour") & (raw1.hr >= 0)], ["yr", "hr"])

g = det26.assign(key=det26.asset + " | " + det26.live_vod).groupby("key")
as_ = g.agg(n=("viewerId", "size"), v=("viewerId", "nunique"), days=("startdate", "nunique"), first=("startdate", "min"), last=("startdate", "max")).sort_values("n", ascending=False, kind="stable").head(15)
T["assets"] = pd.DataFrame({"Asset | Live/VOD": as_.index, "404 sessions": as_.n.map(fmt_n).values, "Viewers": as_.v.map(fmt_n).values, "Days with 404": as_.days.values,
                            "First 404": as_["first"].astype(str).values, "Last 404": as_["last"].astype(str).values,
                            "Top device": [top_of(det26[(det26.asset + " | " + det26.live_vod) == k_].deviceos) for k_ in as_.index],
                            "Top 404 type": [top_of(det26[(det26.asset + " | " + det26.live_vod) == k_].err_type) for k_ in as_.index]})

vc_ = det26.groupby("viewerId").size()
bands = pd.cut(vc_, [0, 1, 5, 20, np.inf], labels=["1 session", "2-5 sessions", "6-20 sessions", "21+ sessions"])
bn = bands.value_counts().reindex(bands.cat.categories)
bs = vc_.groupby(bands, observed=False).sum().reindex(bands.cat.categories)
T["repeat"] = pd.DataFrame({"404 sessions per viewer (2026)": bn.index.astype(str), "Viewers": bn.map(fmt_n).values, "Share of 404 viewers": (100 * bn / bn.sum()).map(lambda v: fmt_p(v, 1)).values,
                            "404 sessions": bs.map(fmt_n).values, "Share of 404 sessions": (100 * bs / bs.sum()).map(lambda v: fmt_p(v, 1)).values})

geo = sum_by(dd, ["yr", "geo"])
geo["s_share"] = 100 * geo.sessions / geo.groupby("yr").sessions.transform("sum")
geo["f_share"] = 100 * geo.e404 / geo.groupby("yr").e404.transform("sum")
geo = geo.sort_values(["yr", "geo"], ascending=[False, True])
T["geo"] = pd.DataFrame({"Year": geo.yr, "Geo": geo.geo, "Sessions": geo.sessions.map(fmt_n), "Share of sessions": geo.s_share.map(lambda v: fmt_p(v, 1)), "Errors": geo.err.map(fmt_n),
                         "Error 404": geo.e404.map(fmt_n), "Share of 404": geo.f_share.map(lambda v: fmt_p(v, 1)), "404 per 10k sessions": geo.per10k.map(fmt_d),
                         "404 % of errors": geo.pct_err.map(fmt_p)})
cty = sum_by(raw1[(raw1.set_name == "country") & (raw1.yr == 2026)], ["geo", "country"]).sort_values("e404", ascending=False, kind="stable").head(15)
T["countries"] = pd.DataFrame({"Country": cty.country.str.upper(), "Geo": cty.geo, "2026 Sessions": cty.sessions.map(fmt_n), "2026 Errors": cty.err.map(fmt_n),
                               "2026 Error 404": cty.e404.map(fmt_n), "Share of 2026 404": (100 * cty.e404 / y26.e404).map(lambda v: fmt_p(v, 1)),
                               "404 per 10k sessions": cty.per10k.map(fmt_d), "404 % of errors": cty.pct_err.map(fmt_p),
                               "404 viewers": [fmt_n(det26[det26.country == c_].viewerId.nunique()) for c_ in cty.country]}).reset_index(drop=True)

# ---- numbers used by the deck ---------------------------------------------------------------------------------------------
TOT_RAW = dict(sess26=int(y26.sessions), err26=int(y26.err), e404_26=int(y26.e404), oth26=int(y26.eoth), sess25=int(y25.sessions), err25=int(y25.err),
               e404_25=int(y25.e404), oth25=int(y25.eoth), viewers404_26=int(det26.viewerId.nunique()), days26=int(len(d26)))
HOUR26_RAW = [float(hr[(hr.yr == 2026) & (hr.hr == h_)].per10k.iloc[0]) if len(hr[(hr.yr == 2026) & (hr.hr == h_)]) else 0.0 for h_ in range(24)]
HOUR25_RAW = [float(hr[(hr.yr == 2025) & (hr.hr == h_)].per10k.iloc[0]) if len(hr[(hr.yr == 2025) & (hr.hr == h_)]) else 0.0 for h_ in range(24)]
HOUR_SESS26 = [int(hr[(hr.yr == 2026) & (hr.hr == h_)].sessions.sum()) for h_ in range(24)]
HOUR_E404_26 = [int(hr[(hr.yr == 2026) & (hr.hr == h_)].e404.sum()) for h_ in range(24)]
DAILY = d26[["startdate", "sessions", "err", "e404", "eoth", "pct_err", "spike", "mn"]].copy()
LAST_DAY = pd.to_datetime(END_DATE)
print("Data layer ready: %s sessions, %s Error 404 sessions in 2026; %d days; spike line %.1f (median %.0f)" %
      (format(TOT_RAW['sess26'], ','), format(TOT_RAW['e404_26'], ','), len(d26), THR, MEDIAN))

# COMMAND ----------

# ---- B. Derived objects: every number the slides use (same structures as the report tables) ----
import datetime as dt
import pandas as pd
def num(x):
    s = str(x).strip()
    if s in ('-', 'nan', '', 'None', 'NaN'):
        return None
    s = s.replace(',', '').replace('%', '').replace('x', '')
    try:
        return float(s)
    except ValueError:
        return None


MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
m = T['monthly']
M = {
    'e26': [num(v) for v in m['2026 Error 404']], 'o26': [num(v) for v in m['2026 Other errors (non-404)']],
    'd26': [num(v) for v in m['2026 Avg daily 404']], 'p26': [num(v) for v in m['2026 Avg daily 404 % of errors']],
    'od26': [num(v) for v in m['2026 Avg daily Other errors']],
    'e25': [num(v) for v in m['2025 Error 404']], 'o25': [num(v) for v in m['2025 Other errors (non-404)']],
    'd25': [num(v) for v in m['2025 Avg daily 404']], 'p25': [num(v) for v in m['2025 Avg daily 404 % of errors']],
    'yoy': [num(v) for v in m['YoY change in 404 share (pp)']],
}

TOT = dict(TOT_RAW)
TOT['per10k26'] = TOT['e404_26'] / TOT['sess26'] * 1e4
TOT['per10k25'] = TOT['e404_25'] / TOT['sess25'] * 1e4
TOT['share26'] = TOT['e404_26'] / TOT['err26'] * 100
TOT['share25'] = TOT['e404_25'] / TOT['err25'] * 100
AVG = TOT['per10k26']  # fleet average, 12.53 per 10K sessions

# --- spike days
td = T['top_days']
DAYS = []
for _, r in td.iterrows():
    d = dt.date.fromisoformat(str(r['Date']))
    DAYS.append(dict(date=d, wd=r['Weekday'], sessions=num(r['Sessions']), errors=num(r['Errors']), e404=int(num(r['Error 404'])),
                     pct=num(r['404 % of errors']), mult=num(r['vs median day']), dev=r['Top device'], typ=r['Top 404 type'],
                     asset=r['Top asset'], out=r['Outside-coverage share']))
MEDIAN_DAY = MEDIAN
SPIKE_LINE = THR
CRIT, HIGH = 10000, 5000

dev = T['device']
DEV = []
for _, r in dev.iterrows():
    name = 'Not reported' if str(r['Device OS']) in ('nan', '(not reported)', '') else r['Device OS']
    DEV.append(dict(name=name, sess=int(num(r['2026 Sessions'])), e404=int(num(r['2026 Error 404'])), share=num(r['Share of 2026 404']),
                    p10=num(r['2026 404 per 10k sessions']), p10_25=num(r['2025 404 per 10k sessions'])))

TYP = []
for _, r in T['type'].iterrows():
    TYP.append(dict(name=r['Error 404 type'], s26=int(num(r['2026 sessions'])), sh26=num(r['2026 share']), s25=int(num(r['2025 sessions'])), sh25=num(r['2025 share'])))

MSG = []
for _, r in T['msgs'].iterrows():
    MSG.append(dict(msg=r.iloc[0], s=int(num(r.iloc[1])), v=int(num(r.iloc[2])), dev=r.iloc[3]))

CONT = []
for _, r in T['content'].iterrows():
    CONT.append(dict(name=r['Content dimension'], sess=int(num(r['2026 Sessions'])), e404=int(num(r['2026 Error 404'])), p10=num(r['2026 404 per 10k sessions']),
                     p10_25=num(r['2025 404 per 10k sessions']), pct=num(r['2026 404 % of errors']), share_report=num(r['Share of 2026 404'])))
CDN = []
for _, r in T['cdn'].iterrows():
    CDN.append(dict(name=r['CDN'], sess=int(num(r['2026 Sessions'])), e404=int(num(r['2026 Error 404'])), p10=num(r['2026 404 per 10k sessions']), pct=num(r['2026 404 % of errors'])))

COMP20 = []
for _, r in T['comp'].iterrows():
    COMP20.append(dict(name=r['Competition'], sess=int(num(r['2026 Sessions'])), e404=int(num(r['2026 Error 404'])), share=num(r['Share of 2026 404']),
                       p10=num(r['2026 404 per 10k sessions']), pct=num(r['2026 404 % of errors']), viewers=int(num(r['404 viewers (2026)'])),
                       dev=r['Top device for 404']))
COMP = COMP20[:15]

AST = []
for _, r in T['assets'].iterrows():
    nm, lv = str(r['Asset | Live/VOD']).rsplit(' | ', 1)
    AST.append(dict(name=nm, lv=lv, s=int(num(r['404 sessions'])), v=int(num(r['Viewers'])), days=int(num(r['Days with 404'])),
                    first=dt.date.fromisoformat(str(r['First 404'])), last=dt.date.fromisoformat(str(r['Last 404'])), dev=r['Top device'], typ=r['Top 404 type']))

REP = []
for _, r in T['repeat'].iterrows():
    REP.append(dict(band=r.iloc[0], viewers=int(num(r.iloc[1])), vshare=num(r.iloc[2]), sess=int(num(r.iloc[3])), sshare=num(r.iloc[4])))

CTRY = []
for _, r in T['countries'].iterrows():
    CTRY.append(dict(name=str(r['Country']).title(), sess=int(num(r['2026 Sessions'])), err=int(num(r['2026 Errors'])), e404=int(num(r['2026 Error 404'])),
                     share=num(r['Share of 2026 404']), p10=num(r['404 per 10k sessions']), pct=num(r['404 % of errors']), viewers=int(num(r['404 viewers']))))

HOUR26 = [round(v, 2) for v in HOUR26_RAW]
HOUR25 = [round(v, 2) for v in HOUR25_RAW]

# COMMAND ----------

# ---- C. Helpers: reproduce the Error 360 styling with python-pptx ----
import copy
from lxml import etree
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.chart.data import CategoryChartData
from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION, XL_LABEL_POSITION, XL_TICK_LABEL_POSITION
from pptx.enum.shapes import MSO_CONNECTOR
from pptx.oxml.ns import qn

NS_C = 'http://schemas.openxmlformats.org/drawingml/2006/chart'
NS_A = 'http://schemas.openxmlformats.org/drawingml/2006/main'

# palette taken from the Error 360 deck
INK = '1F1F1F'; TXT = '404040'; MID = '595959'; GREY = '8C8C8C'; RULE = 'D9D9D9'
GOLD = 'FFBD00'; GOLD_DK = 'B07D12'; GOLD_LT = 'FDE08A'; DARK = '4D4D4D'
RED = 'C0392B'; BLUE = '86A2D4'; GREEN = '1E7B34'


def rgb(h):
    return RGBColor.from_string(h)


def cq(tag):
    return '{%s}%s' % (NS_C, tag)


def aq(tag):
    return '{%s}%s' % (NS_A, tag)


# ----------------------------------------------------------------------------- text
def _run(p, text, sz=8, b=False, i=False, color=INK, caps=False, spc=None):
    r = p.add_run()
    r.text = text
    f = r.font
    f.size = Pt(sz)
    f.bold = b
    f.italic = i
    f.name = 'Arial'
    f.color.rgb = rgb(color)
    rPr = r._r.get_or_add_rPr()
    if caps:
        rPr.set('cap', 'all')
    if spc is not None:
        rPr.set('spc', str(spc))
    return r


def add_text(slide, x, y, w, h, paras, name=None, anchor='t', wrap=True):
    """paras: list of dicts {runs:[(text, fmt)], align:'l|r|ctr', after:pts, line:pct}"""
    tb = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    if name:
        tb.name = name
    tf = tb.text_frame
    tf.word_wrap = wrap
    bp = tf._txBody.find(qn('a:bodyPr'))
    for k in ('lIns', 'tIns', 'rIns', 'bIns'):
        bp.set(k, '0')
    bp.set('anchor', anchor)
    for ch in list(bp):
        bp.remove(ch)
    etree.SubElement(bp, qn('a:noAutofit'))
    first = True
    for pd in paras:
        p = tf.paragraphs[0] if first else tf.add_paragraph()
        first = False
        al = pd.get('align', 'l')
        p.alignment = {'l': 1, 'ctr': 2, 'r': 3}[al]
        pPr = p._p.get_or_add_pPr()
        if pd.get('line'):
            ln = etree.SubElement(pPr, qn('a:lnSpc'))
            etree.SubElement(ln, qn('a:spcPct')).set('val', str(int(pd['line'] * 1000)))
        sb = etree.SubElement(pPr, qn('a:spcBef'))
        etree.SubElement(sb, qn('a:spcPts')).set('val', '0')
        sa = etree.SubElement(pPr, qn('a:spcAft'))
        etree.SubElement(sa, qn('a:spcPts')).set('val', str(int(pd.get('after', 0) * 100)))
        for text, fmt in pd['runs']:
            _run(p, text, **fmt)
    return tb


def P(text, sz=8, b=False, i=False, color=INK, caps=False, spc=None, align='l', after=0, line=None):
    return dict(runs=[(text, dict(sz=sz, b=b, i=i, color=color, caps=caps, spc=spc))], align=align, after=after, line=line)


def hline(slide, x1, y1, x2, y2, color=RULE, w=0.75, dash=None):
    c = slide.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, Inches(x1), Inches(y1), Inches(x2), Inches(y2))
    c.line.color.rgb = rgb(color)
    c.line.width = Pt(w)
    if dash:
        ln = c.line._get_or_add_ln()
        pd = etree.SubElement(ln, qn('a:prstDash'))
        pd.set('val', dash)
    # remove the theme style reference so no shadow/effect leaks in
    st = c._element.find(qn('p:style'))
    if st is not None:
        c._element.remove(st)
    return c


def rect(slide, x, y, w, h, fill, line=None, name=None):
    from pptx.enum.shapes import MSO_SHAPE
    s = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(x), Inches(y), Inches(w), Inches(h))
    s.fill.solid()
    s.fill.fore_color.rgb = rgb(fill)
    if line:
        s.line.color.rgb = rgb(line)
        s.line.width = Pt(0.75)
    else:
        s.line.fill.background()
    st = s._element.find(qn('p:style'))
    if st is not None:
        s._element.remove(st)
    s.shadow.inherit = False
    if name:
        s.name = name
    return s


# ----------------------------------------------------------------------------- reference-style blocks
def kpi_strip(slide, tiles, y=1.45):
    """tiles: list of (label, value, sub). Five tiles across 12.43in like the reference."""
    n = len(tiles)
    xs = [0.45 + i * 2.4825 for i in range(n)]
    for i, (lab, val, sub) in enumerate(tiles):
        x = xs[i]
        add_text(slide, x, y, 2.34, 0.16, [P(lab.upper(), sz=7, color=GREY, after=3)])
        add_text(slide, x, y + 0.17, 2.34, 0.3, [P(val, sz=16, b=True, color=INK, after=3)])
        add_text(slide, x, y + 0.47, 2.34, 0.3, [P(sub, sz=6.5, color=MID, after=3)])
        if i > 0:
            hline(slide, x - 0.12, y + 0.02, x - 0.12, y + 0.62)
    hline(slide, 0.45, 2.22, 12.88, 2.22)


def heading(slide, x, y, w, left, right=None):
    add_text(slide, x, y, w, 0.18, [P(left, sz=8, b=True)])
    if right:
        add_text(slide, x, y + 0.01, w, 0.18, [P(right, sz=6.5, color=GREY, align='r')])
    hline(slide, x, y + 0.24, x + w, y + 0.24)


def side_panel(slide, takeaways, next_step, how=None, top=2.35):
    hline(slide, 10.25, top, 10.25, 6.9)
    paras = [P('Key takeaways', sz=9, b=True, after=7)]
    for k, t in enumerate(takeaways, 1):
        paras.append(dict(runs=[('%d   ' % k, dict(sz=7.8, b=True, color=GOLD_DK)), (t, dict(sz=7.8, color=TXT))], align='l', after=7))
    add_text(slide, 10.43, top, 2.45, 3.5, paras, name='Takeaways')
    hline(slide, 10.43, 5.95, 12.88, 5.95)
    add_text(slide, 10.43, 6.03, 2.45, 0.85, [P('Next step', sz=8, b=True, after=3), P(next_step, sz=7.6, color=INK, after=3)], name='NextStep')


# ----------------------------------------------------------------------------- tables
TBL_STYLE = '{2D5ABB26-0587-4C30-8999-92F81FD0307C}'


def _ln(tag, w, color):
    el = etree.Element(aq(tag))
    if color is None:
        el.set('w', '0')
        etree.SubElement(el, aq('noFill'))
    else:
        el.set('w', str(w))
        sf = etree.SubElement(el, aq('solidFill'))
        etree.SubElement(sf, aq('srgbClr')).set('val', color)
    return el


def add_table(slide, x, y, widths, rows, aligns, row_h=0.183, head_h=0.24, sz=6.8, fills=None, bolds=None, colors=None, name=None, head_wrap=False):
    """rows[0] = header. fills/bolds/colors: dict {(r,c): value}"""
    fills = fills or {}
    bolds = bolds or {}
    colors = colors or {}
    nr, nc = len(rows), len(widths)
    gf = slide.shapes.add_table(nr, nc, Inches(x), Inches(y), Inches(sum(widths)), Inches(head_h + row_h * (nr - 1)))
    if name:
        gf.name = name
    tbl = gf.table
    tblPr = tbl._tbl.tblPr
    for k in list(tblPr.attrib):
        del tblPr.attrib[k]
    tblPr.set('firstRow', '0')
    tblPr.set('bandRow', '0')
    for ch in list(tblPr):
        tblPr.remove(ch)
    etree.SubElement(tblPr, aq('tableStyleId')).text = TBL_STYLE
    for j, w in enumerate(widths):
        tbl.columns[j].width = Inches(w)
    for i in range(nr):
        tbl.rows[i].height = Inches(head_h if i == 0 else row_h)
        for j in range(nc):
            cell = tbl.cell(i, j)
            txt = rows[i][j]
            tf = cell.text_frame
            tf.word_wrap = True
            p = tf.paragraphs[0]
            p.alignment = {'l': 1, 'r': 3, 'ctr': 2}[aligns[j]]
            isb = (i == 0) or bolds.get((i, j), False)
            col = colors.get((i, j), INK)
            _run(p, str(txt), sz=sz, b=isb, color=col)
            tc = cell._tc
            old = tc.find(qn('a:tcPr'))
            if old is not None:
                tc.remove(old)
            tcPr = etree.SubElement(tc, qn('a:tcPr'))
            for k, v in (('marL', 27432), ('marR', 27432), ('marT', 0), ('marB', 0)):
                tcPr.set(k, str(v))
            tcPr.set('anchor', 'b' if i == 0 else 'ctr')
            tcPr.append(_ln('lnL', 0, None))
            tcPr.append(_ln('lnR', 0, None))
            tcPr.append(_ln('lnT', 0, None))
            tcPr.append(_ln('lnB', 9525 if i == 0 else 6350, INK if i == 0 else RULE))
            f = fills.get((i, j))
            if f:
                sf = etree.SubElement(tcPr, aq('solidFill'))
                etree.SubElement(sf, aq('srgbClr')).set('val', f)
            else:
                etree.SubElement(tcPr, aq('noFill'))
    return gf


# ----------------------------------------------------------------------------- charts
def _txpr(sz=7, color=MID, bold=False, rot=None):
    t = etree.Element(cq('txPr'))
    bp = etree.SubElement(t, aq('bodyPr'))
    if rot is not None:
        bp.set('rot', str(rot))
        bp.set('vert', 'horz')
    etree.SubElement(t, aq('lstStyle'))
    p = etree.SubElement(t, aq('p'))
    pPr = etree.SubElement(p, aq('pPr'))
    d = etree.SubElement(pPr, aq('defRPr'))
    d.set('sz', str(int(sz * 100)))
    d.set('b', '1' if bold else '0')
    sf = etree.SubElement(d, aq('solidFill'))
    etree.SubElement(sf, aq('srgbClr')).set('val', color)
    etree.SubElement(d, aq('latin')).set('typeface', 'Arial')
    etree.SubElement(p, aq('endParaRPr')).set('lang', 'en-US')
    return t


def axis_title_xml(text, color=MID, sz=7, rot='-5400000'):
    t = etree.Element(cq('title'))
    tx = etree.SubElement(t, cq('tx'))
    rich = etree.SubElement(tx, cq('rich'))
    bp = etree.SubElement(rich, aq('bodyPr'))
    bp.set('rot', rot)
    bp.set('vert', 'horz')
    etree.SubElement(rich, aq('lstStyle'))
    p = etree.SubElement(rich, aq('p'))
    pPr = etree.SubElement(p, aq('pPr'))
    d = etree.SubElement(pPr, aq('defRPr'))
    d.set('sz', str(int(sz * 100)))
    d.set('b', '1')
    r = etree.SubElement(p, aq('r'))
    rPr = etree.SubElement(r, aq('rPr'))
    rPr.set('lang', 'en-US')
    rPr.set('sz', str(int(sz * 100)))
    rPr.set('b', '1')
    sf = etree.SubElement(rPr, aq('solidFill'))
    etree.SubElement(sf, aq('srgbClr')).set('val', color)
    etree.SubElement(rPr, aq('latin')).set('typeface', 'Arial')
    etree.SubElement(r, aq('t')).text = text
    etree.SubElement(t, cq('overlay')).set('val', '0')
    return t


def base_chart(slide, ctype, x, y, w, h, cats, series, legend=True, name=None):
    cd = CategoryChartData()
    cd.categories = cats
    for nm, vals in series:
        cd.add_series(nm, vals)
    gf = slide.shapes.add_chart(ctype, Inches(x), Inches(y), Inches(w), Inches(h), cd)
    if name:
        gf.name = name
    ch = gf.chart
    ch.font.size = Pt(7)
    ch.font.name = 'Arial'
    ch.font.color.rgb = rgb(MID)
    ch.has_title = False
    ch.has_legend = legend
    if legend:
        ch.legend.position = XL_LEGEND_POSITION.BOTTOM
        ch.legend.include_in_layout = False
        ch.legend.font.size = Pt(7)
        ch.legend.font.name = 'Arial'
    return gf, ch


def style_value_axis(ax, vmin=None, vmax=None, major=None, fmt=None, color=MID, grid=True, title=None, title_color=MID, log=False):
    if vmin is not None:
        ax.minimum_scale = vmin
    if vmax is not None:
        ax.maximum_scale = vmax
    if major is not None:
        ax.major_unit = major
    ax.has_major_gridlines = grid
    if grid:
        ax.major_gridlines.format.line.color.rgb = rgb(RULE)
        ax.major_gridlines.format.line.width = Pt(0.5)
    ax.format.line.fill.background()
    ax.tick_labels.font.size = Pt(7)
    ax.tick_labels.font.name = 'Arial'
    ax.tick_labels.font.color.rgb = rgb(color)
    if fmt:
        ax.tick_labels.number_format = fmt
        ax.tick_labels.number_format_is_linked = False
    if title:
        ax.has_title = True
        ax.axis_title.text_frame.text = title
        r = ax.axis_title.text_frame.paragraphs[0].runs[0]
        r.font.size = Pt(7)
        r.font.bold = True
        r.font.name = 'Arial'
        r.font.color.rgb = rgb(title_color)


def style_cat_axis(ax, sz=7, color=MID, rot=None):
    ax.format.line.color.rgb = rgb('BFBFBF')
    ax.has_major_gridlines = False
    ax.tick_labels.font.size = Pt(sz)
    ax.tick_labels.font.name = 'Arial'
    ax.tick_labels.font.color.rgb = rgb(color)
    from pptx.enum.chart import XL_TICK_MARK
    ax.major_tick_mark = XL_TICK_MARK.NONE
    if rot is not None:
        txPr = ax._element.find(cq('txPr'))
        bp = txPr.find(aq('bodyPr'))
        bp.set('rot', str(rot))
        bp.set('vert', 'horz')


def style_line_series(s, color, width=2.25, dash=None, marker=True, msize=6, smooth=False):
    s.format.line.color.rgb = rgb(color)
    s.format.line.width = Pt(width)
    if dash:
        ln = s.format.line._get_or_add_ln()
        for old in ln.findall(aq('prstDash')):
            ln.remove(old)
        pd = etree.SubElement(ln, aq('prstDash'))
        pd.set('val', dash)
        # prstDash must follow the fill element
        fill = ln.find(aq('solidFill'))
        if fill is not None:
            ln.remove(pd)
            fill.addnext(pd)
    if not hasattr(s, 'marker'):      # a bar series that will be moved into a lineChart
        return
    s.smooth = smooth
    from pptx.enum.chart import XL_MARKER_STYLE
    if marker:
        s.marker.style = XL_MARKER_STYLE.CIRCLE
        s.marker.size = msize
        s.marker.format.fill.solid()
        s.marker.format.fill.fore_color.rgb = rgb(color)
        s.marker.format.line.color.rgb = rgb('FFFFFF')
        s.marker.format.line.width = Pt(0.75)
    else:
        s.marker.style = XL_MARKER_STYLE.NONE


def bar_fill(s, color):
    s.format.fill.solid()
    s.format.fill.fore_color.rgb = rgb(color)
    s.format.line.fill.background()
    s.invert_if_negative = False


def series_labels(s, fmt='#,##0', pos=XL_LABEL_POSITION.OUTSIDE_END, sz=7, color=INK, bold=False):
    dl = s.data_labels
    dl.show_value = True
    dl.number_format = fmt
    dl.number_format_is_linked = False
    if pos is not None:
        dl.position = pos
    dl.font.size = Pt(sz)
    dl.font.name = 'Arial'
    dl.font.bold = bold
    dl.font.color.rgb = rgb(color)


def plot_elements(chart):
    pa = chart._chartSpace.find('.//' + cq('plotArea'))
    return pa


def move_series_to_line(chart, ser_positions, secondary=False, sec=None):
    """Move series (by index in the first chart-type element) into a new lineChart.
    secondary=True also creates a secondary value axis (hidden category axis).
    sec: dict(min,max,major,fmt,color,title)"""
    pa = plot_elements(chart)
    first = None
    for ch in pa:
        if ch.tag in (cq('barChart'), cq('lineChart')):
            first = ch
            break
    sers = first.findall(cq('ser'))
    moving = [sers[i] for i in ser_positions]
    ax_ids = [a.get('val') for a in first.findall(cq('axId'))]
    lc = etree.Element(cq('lineChart'))
    etree.SubElement(lc, cq('grouping')).set('val', 'standard')
    etree.SubElement(lc, cq('varyColors')).set('val', '0')
    for s in moving:
        first.remove(s)
        inv = s.find(cq('invertIfNegative'))
        if inv is not None:
            s.remove(inv)
        if s.find(cq('marker')) is None:
            mk = etree.Element(cq('marker'))
            etree.SubElement(mk, cq('symbol')).set('val', 'none')
            anchor = s.find(cq('spPr'))
            if anchor is None:
                anchor = s.find(cq('tx'))
            anchor.addnext(mk)
        if s.find(cq('smooth')) is None:
            etree.SubElement(s, cq('smooth')).set('val', '0')
        lc.append(s)
    etree.SubElement(lc, cq('marker')).set('val', '1')
    if secondary:
        cat_id, val_id = '50010', '50020'
        etree.SubElement(lc, cq('axId')).set('val', cat_id)
        etree.SubElement(lc, cq('axId')).set('val', val_id)
    else:
        for a in ax_ids:
            etree.SubElement(lc, cq('axId')).set('val', a)
    first.addnext(lc)
    if secondary:
        sec = sec or {}
        last_ax = [e for e in pa if e.tag in (cq('valAx'), cq('catAx'))][-1]
        va = etree.Element(cq('valAx'))
        etree.SubElement(va, cq('axId')).set('val', val_id)
        sc = etree.SubElement(va, cq('scaling'))
        etree.SubElement(sc, cq('orientation')).set('val', 'minMax')
        if 'max' in sec:
            etree.SubElement(sc, cq('max')).set('val', str(sec['max']))
        if 'min' in sec:
            etree.SubElement(sc, cq('min')).set('val', str(sec['min']))
        etree.SubElement(va, cq('delete')).set('val', '0')
        etree.SubElement(va, cq('axPos')).set('val', 'r')
        if sec.get('title'):
            va.append(axis_title_xml(sec['title'], color=sec.get('color', MID), rot='-5400000'))
        nf = etree.SubElement(va, cq('numFmt'))
        nf.set('formatCode', sec.get('fmt', 'General'))
        nf.set('sourceLinked', '0')
        etree.SubElement(va, cq('majorTickMark')).set('val', 'none')
        etree.SubElement(va, cq('minorTickMark')).set('val', 'none')
        etree.SubElement(va, cq('tickLblPos')).set('val', 'nextTo')
        sp = etree.SubElement(va, cq('spPr'))
        ln = etree.SubElement(sp, aq('ln'))
        etree.SubElement(ln, aq('noFill'))
        va.append(_txpr(7, sec.get('color', MID)))
        etree.SubElement(va, cq('crossAx')).set('val', cat_id)
        etree.SubElement(va, cq('crosses')).set('val', 'max')
        etree.SubElement(va, cq('crossBetween')).set('val', 'between')
        if 'major' in sec:
            etree.SubElement(va, cq('majorUnit')).set('val', str(sec['major']))
        ca = etree.Element(cq('catAx'))
        etree.SubElement(ca, cq('axId')).set('val', cat_id)
        sc2 = etree.SubElement(ca, cq('scaling'))
        etree.SubElement(sc2, cq('orientation')).set('val', 'minMax')
        etree.SubElement(ca, cq('delete')).set('val', '1')
        etree.SubElement(ca, cq('axPos')).set('val', 'b')
        etree.SubElement(ca, cq('majorTickMark')).set('val', 'none')
        etree.SubElement(ca, cq('minorTickMark')).set('val', 'none')
        etree.SubElement(ca, cq('tickLblPos')).set('val', 'nextTo')
        etree.SubElement(ca, cq('crossAx')).set('val', val_id)
        etree.SubElement(ca, cq('crosses')).set('val', 'autoZero')
        etree.SubElement(ca, cq('auto')).set('val', '1')
        etree.SubElement(ca, cq('lblAlgn')).set('val', 'ctr')
        etree.SubElement(ca, cq('lblOffset')).set('val', '100')
        etree.SubElement(ca, cq('noMultiLvlLbl')).set('val', '0')
        last_ax.addnext(ca)
        ca.addnext(va)
    return lc


def inner_layout(chart, x, y, w, h):
    """fix the inner plot area (fractions of chart space) so several charts line up"""
    pa = plot_elements(chart)
    lay = pa.find(cq('layout'))
    if lay is None:
        lay = etree.Element(cq('layout'))
        pa.insert(0, lay)
    for ch in list(lay):
        lay.remove(ch)
    ml = etree.SubElement(lay, cq('manualLayout'))
    etree.SubElement(ml, cq('layoutTarget')).set('val', 'inner')
    etree.SubElement(ml, cq('xMode')).set('val', 'edge')
    etree.SubElement(ml, cq('yMode')).set('val', 'edge')
    etree.SubElement(ml, cq('x')).set('val', str(x))
    etree.SubElement(ml, cq('y')).set('val', str(y))
    etree.SubElement(ml, cq('w')).set('val', str(w))
    etree.SubElement(ml, cq('h')).set('val', str(h))

# COMMAND ----------

# ---- D. Build the deck ----
import copy, datetime as dt
from pptx import Presentation
from pptx.enum.chart import XL_CHART_TYPE, XL_LABEL_POSITION
import math


def nice_axis(v, max_steps=6):
    """(axis maximum, major unit) with at most max_steps gridlines, about 8% head-room above v."""
    v = max(float(v) * 1.08, 1e-9)
    k0 = math.floor(math.log10(v)) - 2
    for k in range(k0, k0 + 6):
        for m in (1, 2, 2.5, 5):
            major = m * 10 ** k
            if math.ceil(v / major - 1e-9) <= max_steps:
                return round(math.ceil(v / major - 1e-9) * major, 6), round(major, 6)
    return v, v / 5


def replace_text(prs):
    """Final pass: keep the totals and the date window in the narrative in step with the data (no-op when unchanged)."""
    pairs = [('217,195', fmt_n(TOT['e404_26'])), ('173,282,794', fmt_n(TOT['sess26'])), ('6,356,227', fmt_n(TOT['err26'])),
             ('6,139,032', fmt_n(TOT['oth26'])), ('3,022', fmt_n(TOT['e404_25'])), ('16,586', fmt_n(TOT['viewers404_26'])),
             ('12.53', '%.2f' % AVG), ('3.42%', '%.2f%%' % TOT['share26']), ('173.3M', '%.1fM' % (TOT['sess26'] / 1e6)),
             ('3 Nov 2026', END_LABEL), ('November 2026 covers 3 days', '%s covers %d days' % (LAST_MONTH_NAME, LAST_MONTH_DAYS)),
             ('November 2026 = 3 days', '%s = %d days' % (LAST_MONTH_NAME, LAST_MONTH_DAYS)), ('(3 for November 2026)', '(%d for %s)' % (LAST_MONTH_DAYS, LAST_MONTH_NAME))]
    pairs = [(a_, b_) for a_, b_ in pairs if a_ != b_]
    n = 0

    def walk(shapes):
        nonlocal n
        for sh in shapes:
            if sh.shape_type == 6:
                walk(sh.shapes)
            tfs = []
            if sh.has_text_frame:
                tfs.append(sh.text_frame)
            if getattr(sh, 'has_table', False) and sh.has_table:
                tfs += [c_.text_frame for r_ in sh.table.rows for c_ in r_.cells]
            for tf in tfs:
                for p_ in tf.paragraphs:
                    for r_ in p_.runs:
                        t_ = r_.text
                        for a_, b_ in pairs:
                            if a_ in t_:
                                t_ = t_.replace(a_, b_)
                        if t_ != r_.text:
                            r_.text = t_
                            n += 1
    for sl in prs.slides:
        walk(sl.shapes)
    return n


prs = Presentation(TEMPLATE_PATH)
N_TEMPLATE = len(prs.slides)
layout = prs.slides[5].slide_layout
TPL = prs.slides[5]
CHROME = ['Rectangle 1', 'Group 2', 'Slide Number Placeholder 2', 'HdrDate', 'DataWindow', 'TextBox 56', 'TextBox 57', 'TextBox 87', 'TextBox 88']
chrome_els = {sh.name: sh._element for sh in TPL.shapes if sh.name in CHROME}

DATA_WIN = 'Data: Conviva sessions in the 22 TOD MENA markets, 1 Jan 2025 to 3 Nov 2026 (November 2026 = 3 days)'   # dates are updated in the final text pass
DEF = ('Error session = startuperror = 1 or a non-empty error list. Error 404 = the error list contains a standalone 404 (not part of a timestamp or ID). '
       'Per 10K = 404 sessions per 10,000 sessions; fleet average = %.2f per 10K in 2026. 22 covered MENA markets; data 1 Jan 2025 to 3 Nov 2026, November 2026 covers 3 days.' % AVG)


def set_texts(el, texts):
    ts = el.findall('.//' + aq('t'))
    for t, s in zip(ts, texts):
        t.text = s


def new_slide(left, section, subtitle, title, footer, datawin=DATA_WIN):
    s = prs.slides.add_slide(layout)
    for ph in list(s.placeholders):
        ph._element.getparent().remove(ph._element)
    tree = s.shapes._spTree
    for nm in CHROME:
        el = copy.deepcopy(chrome_els[nm])
        tree.append(el)
        if nm == 'Group 2':
            set_texts(el, [left, section])
        elif nm == 'DataWindow':
            set_texts(el, [datawin])
        elif nm == 'TextBox 56':
            set_texts(el, [subtitle])
        elif nm == 'TextBox 57':
            set_texts(el, [title])
        elif nm == 'TextBox 87':
            ts = el.findall('.//' + aq('t'))
            ts[0].text = 'MEASURED  ·  '
            ts[1].text = footer
    return s


def finalize(s):
    i = 2
    for el in s.shapes._spTree.iter('{http://schemas.openxmlformats.org/presentationml/2006/main}cNvPr'):
        el.set('id', str(i))
        i += 1


def fmt_n(v):
    return '{:,.0f}'.format(v)


def d_lab(d):
    return '%d %s' % (d.day, d.strftime('%b'))


def pct(v, n=1):
    return ('%.' + str(n) + 'f%%') % v


# ---- objects computed in the data layer
DEV_TOP_404 = sum(d_['e404'] for d_ in DEV)

# =============================================================== slide 2: summary
s = new_slide('SUMMARY', 'WHAT THE 404S SHOW',
              'What should TOD do about Error 404? · six decisions, each with the number behind it',
              'Error 404 grew from 3,022 sessions in 2025 to 217,195 in 2026, and most of it is one tvOS fault: 14 of the 15 worst days fall between 23 February and 23 March.',
              'Source: Conviva sessions in the 22 TOD MENA markets, Error 404 deep-dive report of 4 Nov 2026 (EntertainmentMovies_SD). ' + DEF + ' Every number is taken from the page named on its right, with its definitions there.')
rows = [
    ('217.2K', '404 sessions in 2026, against 3.0K in 2025',
     'Treat Error 404 as a monitored failure type, not background noise.',
     '12.53 404 sessions per 10K sessions in 2026 against 0.28 in 2025, 45 times the rate. 10 of 11 months run above the same month of 2025; only August is below.',
     'See “Monthly trend”'),
    ('3.42%', 'of error sessions are 404 (0.10% in 2025)',
     'Keep 404 in proportion: 96.6% of error sessions are something else.',
     '217,195 of 6.36M error sessions are 404 in 2026, against 3,022 of 3.11M in 2025. Judging playback on 404 alone would miss almost all errors.',
     'See “Share of errors”'),
    ('33.8%', 'of March error sessions were 404',
     'Fix the tvOS playlist failure behind the February to March spike.',
     'March averaged 4,764 404 sessions a day against a normal day of 14. 14 of the 15 worst days fall between 23 Feb and 23 Mar, all on tvOS; the worst, 20 Mar, reached 25,046.',
     'See “Spike days”'),
    ('85.6%', 'of 404s are on tvOS, 3.8% of sessions',
     'Make tvOS the first bottleneck to fix, then alert on any platform above the fleet average.',
     'tvOS runs at 282 404s per 10K sessions, 22.5 times the fleet average of 12.5; Android is next at 5.0. The HLS playlist download is 85.4% of 404 types in 2026, against 0.7% in 2025.',
     'See “Device and error type”'),
    ('84.7%', 'of 404 sessions come from 512 viewers',
     'Stop retry loops: a few viewers repeat the same failed request hundreds of times.',
     '512 viewers (3.1%) with 21 or more 404 sessions each account for 183,935 of 217,195 sessions. Nice vs Rennes alone logged 4,172 sessions from one viewer.',
     'See “Repeat viewers and hours”'),
    ('74.5%', 'of competition 404s: three leagues',
     'Start the fix and the monitor with the Champions League, Premier League and Primera División.',
     '78.8K, 57.7K and 24.8K 404 sessions, 96% to 98% on tvOS. The World Cup (21.5K) is Android and runs below the fleet average, so it is not the same fault.',
     'See “Competitions” and “Assets”'),
]
y0, pitch = 1.42, 0.86
for i, (val, lab, head, det, see) in enumerate(rows):
    y = y0 + i * pitch
    add_text(s, 0.45, y + 0.02, 0.3, 0.3, [P(str(i + 1), sz=14, b=True, color=GOLD)])
    add_text(s, 0.9, y + 0.1, 2.2, 0.28, [P(val, sz=12, b=True)])
    add_text(s, 0.9, y + 0.33, 2.2, 0.3, [P(lab, sz=6.5, color=MID)])
    add_text(s, 3.2, y + 0.08, 7.4, 0.2, [P(head, sz=8, b=True)])
    add_text(s, 3.2, y + 0.29, 7.4, 0.4, [P(det, sz=7, color=TXT, line=105)])
    add_text(s, 11.0, y + 0.08, 1.9, 0.2, [P(see, sz=6.5, color=GREY)])
    hline(s, 0.45, y + pitch - 0.08, 12.88, y + pitch - 0.08)
finalize(s)

# =============================================================== monthly trend charts (slides 3 and 4)
def month_table(slide, label_a, a26, label_b, a25, fmt, y=5.95):
    hdr = [''] + MONTHS
    r26 = [label_a] + [(fmt(v) if v is not None else '–') for v in a26]
    r25 = [label_b] + [(fmt(v) if v is not None else '–') for v in a25]
    widths = [1.2] + [0.7] * 12
    add_table(slide, 0.45, y, widths, [hdr, r26, r25], ['l'] + ['r'] * 12, row_h=0.19, head_h=0.2, sz=6.8,
              bolds={(1, j): True for j in range(13)}, name='MonthTable')


def dual_line(slide, x, y, w, h, v26, v25, n26, n25, l_max, l_major, l_fmt, r_max, r_major, r_fmt, ltitle, rtitle, lab_fmt26, lab_fmt25):
    gf, ch = base_chart(slide, XL_CHART_TYPE.LINE_MARKERS, x, y, w, h, MONTHS, [(n26, v26), (n25, v25)], name='MonthlyTrend')
    p = ch.plots[0]
    a, b = p.series[0], p.series[1]
    style_line_series(a, '404040', width=3.0, msize=7)
    style_line_series(b, 'A6A6A6', width=2.25, dash='dash', msize=6)
    series_labels(a, lab_fmt26, XL_LABEL_POSITION.ABOVE, sz=7, color='404040', bold=True)
    style_value_axis(ch.value_axis, 0, l_max, l_major, l_fmt, color='404040', title=ltitle, title_color='404040')
    style_cat_axis(ch.category_axis)
    move_series_to_line(ch, [1], secondary=True, sec=dict(min=0, max=r_max, major=r_major, fmt=r_fmt, color='7F7F7F', title=rtitle))
    return gf, ch


# ---- slide 3
s = new_slide('ERROR 404', 'MONTHLY TREND',
              'Is Error 404 growing? · average 404 sessions per day by month, 2026 against 2025',
              'Error 404 ran above 2025 in 10 of 11 months and peaked at 4,764 sessions a day in March, against a 2025 high of 27 a day in August.',
              'Source: Error 404 deep-dive report, section 1 (df_5_1). ' + DEF + ' Average per day = 404 sessions in the month divided by the days of data in it (3 for November 2026). Left axis is 2026, right axis is 2025: the scales differ by about 170 times.')
kpi_strip(s, [
    ('Peak month, 2026', '4,764 a day', 'March; 175 times the 2025 peak month (August, 27 a day)'),
    ('Second peak, 2026', '1,423 a day', 'February; 2025 had no 404 sessions in February'),
    ('World Cup months', '355 · 445', 'a day in June · July; 2025: 4.8 · 7.4 a day'),
    ('Quietest month, 2026', '10 a day', 'August, the only month below 2025 (27 a day)'),
    ('2026 to 3 Nov', '217,195', '404 sessions against 3,022 in all of 2025; 12.53 against 0.28 per 10K sessions'),
])
heading(s, 0.45, 2.35, 9.6, 'Average Error 404 sessions per day, by month', 'dark grey = 2026 (left axis) · light grey dashed = 2025 (right axis)')
v26 = [M['d26'][i] if i < 11 else None for i in range(12)]
v25 = [M['d25'][i] for i in range(12)]
dual_line(s, 0.45, 2.64, 9.6, 3.25, v26, v25, '2026 average per day (left axis)', '2025 average per day (right axis)',
          *nice_axis(max(v for v in v26 if v is not None)), '#,##0', *nice_axis(max(v25)), '0', '2026 sessions per day', '2025 sessions per day', '#,##0', '0.0')
month_table(s, '2026 per day', v26, '2025 per day', v25, lambda v: '{:,.1f}'.format(v))
side_panel(s, [
    'February and March carry the year: 1,423 and 4,764 a day, against 0 in both months of 2025. January started at 16 a day.',
    'From April the average drops to about 35 a day, then rises with the World Cup to 355 in June and 445 in July.',
    'August (10 a day) is the only month below its 2025 level (27 a day); the other 10 months are above.',
    'November covers 3 days only (155 a day); read it as provisional.',
], 'Report 404 sessions a day against the alert levels on the Spike days page, so a repeat of February to March is caught in days. Success: no day above 5,000.')
finalize(s)

# ---- slide 4
s = new_slide('ERROR 404', 'SHARE OF ERRORS',
              'How big is 404 among all errors? · Error 404 as % of all error sessions, monthly average of daily %, 2026 against 2025',
              'In March 33.8% of all error sessions were 404; from April to October the share is back to 0.2% to 0.6%, the same size as 2025.',
              'Source: Error 404 deep-dive report, section 1 (df_5_1). ' + DEF + ' Monthly figure = average of the daily 404 percentages of error sessions, so it differs from the share of monthly totals (March: 147,694 of 413,909 = 35.7%). Left axis is 2026, right axis is 2025.')
kpi_strip(s, [
    ('March peak', '33.79%', 'of error sessions; 2025 high was 0.55% (August)'),
    ('February', '10.37%', 'of error sessions; 2025: 0.00%'),
    ('April to October', '0.18% to 0.63%', '2025 same months: 0.00% to 0.55%'),
    ('November, 3 days', '1.26%', 'provisional; 2025: 0.11%'),
    ('2026 to 3 Nov', '3.42%', '217,195 of 6.36M error sessions; 2025: 0.10% (3,022 of 3.11M)'),
])
heading(s, 0.45, 2.35, 9.6, 'Error 404 as % of all errors, monthly average of daily %', 'dark grey = 2026 (left axis) · light grey dashed = 2025 (right axis)')
p26 = [M['p26'][i] if i < 11 else None for i in range(12)]
p25 = [M['p25'][i] for i in range(12)]
dual_line(s, 0.45, 2.64, 9.6, 3.25, p26, p25, '2026 % of errors (left axis)', '2025 % of errors (right axis)',
          *nice_axis(max(v for v in p26 if v is not None)), '0"%"', *nice_axis(max(p25)), '0.00"%"', '2026 % of errors', '2025 % of errors', '0.00"%"', '0.00"%"')
month_table(s, '2026 % of errors', p26, '2025 % of errors', p25, lambda v: '%.2f%%' % v)
side_panel(s, [
    'In March one error session in three was a 404 (33.79%); in February one in ten (10.37%). In 2025 the highest month was 0.55%.',
    'From April to October the share stays at 0.18% to 0.63%, the same size as 2025 (0.00% to 0.55%).',
    'June and July stay low (0.63%, 0.53%) although 404s rose, because other errors surged in the World Cup (62.4K and 43.3K a day).',
    'The monthly figure averages daily percentages, so it differs from the share of totals (March 35.7%). November is 3 days.',
], 'Alert on the daily 404 share as well as volume: a day above 10% of error sessions opens an incident (proposed level). Success: no such day outside a declared event.')
finalize(s)

# =============================================================== slide 4b: all 307 days
_d = DAILY.sort_values('startdate').reset_index(drop=True)
_d['mn'] = _d.startdate.dt.month
_crit = [int(v) if v >= CRIT else None for v in _d.e404]
_high = [int(v) if HIGH <= v < CRIT else None for v in _d.e404]
_watch = [int(v) if (v > THR and v < HIGH) else None for v in _d.e404]
_norm = [int(v) if v <= THR else None for v in _d.e404]
_cats = [(r_.startdate.strftime('%b') if r_.startdate.day == 1 else '') for r_ in _d.itertuples()]
_sp = [int(((_d.mn == m_) & (_d.e404 > THR)).sum()) if (_d.mn == m_).any() else None for m_ in range(1, 13)]
_hi = [int(((_d.mn == m_) & (_d.e404 >= HIGH)).sum()) if (_d.mn == m_).any() else None for m_ in range(1, 13)]
s = new_slide('ERROR 404', 'SPIKE DAYS: THE YEAR',
              'When does 404 spike? · every day of 2026 against the spike line and two alert levels',
              'Most days are quiet: 236 of 307 are at or below the spike line of 67, and the 16 days at 5,000 or more are 15 in 23 Feb to 23 Mar plus 4 Jul.',
              'Source: Error 404 deep-dive report (spike definition) and the Databricks validation run of 5 Nov 2026 (daily series of 307 days from EntertainmentMovies_SD). ' + DEF + ' Spike line = median day + 3 median absolute deviations = 67.4 404 sessions a day (report definition; median day = 14). Watch = above the line and below 5,000; high = 5,000 to 9,999; critical = 10,000 or more (high and critical levels proposed). The 67 line is too low to show on this scale; the monthly counts below give it.',
              datawin='Data: Conviva sessions in the 22 TOD MENA markets, 1 Jan to 3 Nov 2026 (307 days)')
kpi_strip(s, [
    ('Normal days', '236 of 307', 'at or below the spike line of 67; the median day is 14'),
    ('Watch days', '55', 'above 67 and below 5,000 404 sessions a day'),
    ('High days', '13', '5,000 to 9,999 (proposed level): 12 in Feb to Mar, 4 Jul'),
    ('Critical days', '3', '10,000 or more (proposed level): 26 Feb, 9 Mar, 20 Mar'),
    ('Apr to Nov spike days', '29', 'of the 71; the highest is 4 Jul at 5,088'),
])
heading(s, 0.45, 2.35, 9.6, '404 sessions per day, 2026', 'one bar per day · dashed lines = alert levels')
gf, ch = base_chart(s, XL_CHART_TYPE.COLUMN_STACKED, 0.45, 2.64, 9.6, 2.95, _cats,
                    [('Normal day (67 or fewer)', _norm), ('Watch day (68 to 4,999)', _watch), ('High day (5,000 to 9,999)', _high),
                     ('Critical day (10,000 or more)', _crit), ('Critical level: 10,000', [CRIT] * len(_d)), ('High level: 5,000', [HIGH] * len(_d))], name='SpikeYear')
p = ch.plots[0]
p.gap_width = 15
p.overlap = 100
bar_fill(p.series[0], 'D9D9D9')
bar_fill(p.series[1], 'A6A6A6')
bar_fill(p.series[2], '7F7F7F')
bar_fill(p.series[3], '404040')
style_line_series(p.series[4], '262626', width=1.5, dash='dash', marker=False)
style_line_series(p.series[5], '7F7F7F', width=1.5, dash='dash', marker=False)
style_value_axis(ch.value_axis, 0, *nice_axis(max(max(_d.e404), CRIT)), '#,##0', title='404 sessions in the day')
style_cat_axis(ch.category_axis)
move_series_to_line(ch, [4, 5])
_cx = ch.category_axis._element
for _nm in ('tickLblSkip', 'tickMarkSkip'):
    _e = etree.SubElement(_cx, cq(_nm))
    _e.set('val', '1')
    _nm_el = _cx.find(cq('noMultiLvlLbl'))
    if _nm_el is not None:
        _cx.remove(_e)
        _nm_el.addprevious(_e)
month_table(s, 'Days above 67', _sp, 'Days at 5,000+', _hi, lambda v: '%d' % v, y=5.72)
side_panel(s, [
    'Quiet is the norm: 236 of 307 days are at or below the spike line of 67, and the median day has 14 404 sessions.',
    'All 3 critical and 13 high days: 15 fall between 23 Feb and 23 Mar (tvOS playlist failures); the 16th is 4 Jul (Android, 5,088).',
    'From April to November there are 29 spike days but none reaches 5,100; June and July hold 15 of them.',
    'The line of 67 flags noise as well as incidents, which is why a high (5,000) and a critical (10,000) level are proposed above it.',
], 'Show the daily 404 count against the three levels in the weekly health report. Success: every day above 67 is classed watch, high or critical within a day.')
finalize(s)

# =============================================================== slide 5: spike days chart with thresholds
s = new_slide('ERROR 404', 'SPIKE DAYS',
              'When does 404 spike? · the 15 worst days of 2026 against the spike line and two alert levels',
              'The spike line of 67 a day was crossed on 71 days in 2026. The worst three days passed 10,000, and every one of the 15 worst passed 5,000.',
              'Source: Error 404 deep-dive report, section 2 (df_5_2). ' + DEF + ' Spike line = median day + 3 median absolute deviations = 67 404 sessions a day (report definition; median day = 14); 71 of 307 days are above it. The 5,000 (high) and 10,000 (critical) levels are proposed here. The daily series of all 307 days is on the previous page; this page charts the 15 worst.')
kpi_strip(s, [
    ('Spike days, 2026', '71 of 307', 'days above 67 404 sessions a day (a normal day is 14)'),
    ('Worst day', '25,046', '20 Mar (Fri); 1,789 times a normal day, 88% of that day’s errors'),
    ('Critical days, 10,000+', '3', '20 Mar · 9 Mar · 26 Feb'),
    ('Top 15 days', '61%', '132,774 of the 217,195 404 sessions in 2026'),
    ('Top 15 by device', '14 tvOS · 1 Android', 'tvOS: 23 Feb to 23 Mar; Android: 4 Jul (Australia vs Egypt)'),
])
heading(s, 0.45, 2.35, 9.6, 'Top 15 days by Error 404 sessions, 2026', 'ranked by 404 sessions in the day · dashed lines = alert levels')
cats = [d_lab(d['date']) for d in DAYS]
crit = [d['e404'] if d['e404'] >= CRIT else None for d in DAYS]
high = [d['e404'] if HIGH <= d['e404'] < CRIT else None for d in DAYS]
gf, ch = base_chart(s, XL_CHART_TYPE.COLUMN_STACKED, 0.45, 2.64, 9.6, 2.95, cats,
                    [('Critical day (10,000 or more)', crit), ('High day (5,000 to 9,999)', high),
                     ('Critical level: 10,000', [CRIT] * 15), ('High level: 5,000', [HIGH] * 15)], name='SpikeDays')
p = ch.plots[0]
p.gap_width = 45
p.overlap = 100
bar_fill(p.series[0], '404040')
bar_fill(p.series[1], '7F7F7F')
series_labels(p.series[0], '#,##0', XL_LABEL_POSITION.INSIDE_BASE, sz=7, color='FFFFFF', bold=True)
series_labels(p.series[1], '#,##0', XL_LABEL_POSITION.INSIDE_BASE, sz=7, color='FFFFFF', bold=True)
style_line_series(p.series[2], '262626', width=1.5, dash='dash', marker=False)
style_line_series(p.series[3], '7F7F7F', width=1.5, dash='dash', marker=False)
style_value_axis(ch.value_axis, 0, *nice_axis(max(max(d_['e404'] for d_ in DAYS), CRIT)), '#,##0', title='404 sessions in the day')
style_cat_axis(ch.category_axis)
move_series_to_line(ch, [2, 3])
# alert ladder
add_text(s, 0.45, 5.68, 9.6, 0.16, [P('Alert levels, in 404 sessions a day', sz=8, b=True)])
tiles = [
    ('EDEDED', INK, 'Normal day', 'about 14', 'median day of 2026'),
    ('BFBFBF', INK, 'Spike line: 67', '71 days above', 'report definition'),
    ('7F7F7F', 'FFFFFF', 'High: 5,000', '16 days at or above', 'proposed; all Top 15 days pass it'),
    ('404040', 'FFFFFF', 'Critical: 10,000', '3 days', 'proposed; 20 Mar, 9 Mar, 26 Feb'),
]
tw = 2.33
for i, (fill, tc, a, b_, c) in enumerate(tiles):
    x = 0.45 + i * (tw + 0.093)
    rect(s, x, 5.9, tw, 0.82, fill, name='Level%d' % i)
    add_text(s, x + 0.1, 5.96, tw - 0.2, 0.7, [P(a, sz=8.5, b=True, color=tc, after=2), P(b_, sz=7.5, b=True, color=tc, after=1), P(c, sz=6.5, color=tc)])
side_panel(s, [
    'Spikes are concentrated: the 15 worst days hold 61% of the year’s 404 sessions (132,774 of 217,195).',
    '14 of the 15 fall between 23 Feb and 23 Mar, on tvOS (98% to 100%), each time through HLS playlist download failures.',
    '4 Jul (5,088) is the only other day: an Android ExoPlayer HTTP 404 on Australia vs Egypt, a different cause.',
    'A normal day has 14 404 sessions, so the spike line of 67 is crossed on 71 days; 5,000 and 10,000 separate real incidents from noise.',
], 'Adopt three alert levels (proposed): watch above 67 a day, high above 5,000, critical above 10,000. Success: no critical day; any high day triaged within 24 hours.')
finalize(s)

# =============================================================== slide 6: Top 15 days table
s = new_slide('ERROR 404', 'SPIKE DAYS: DETAIL',
              'What was behind each spike? · Top 15 days by Error 404 sessions, with device, error type and asset',
              'The 15 worst days: 14 are tvOS playlist failures between 23 Feb and 23 Mar, each 383 to 1,789 times a normal day; the other is Android on 4 Jul.',
              'Source: Error 404 deep-dive report, section 2 (df_5_2). ' + DEF + ' Times a normal day = 404 sessions of the day divided by the median day (14). 404 % of errors = 404 sessions over all error sessions of the day. Level: critical = 10,000 or more, high = 5,000 to 9,999 (proposed levels). Top device and asset show their share of that day’s 404s.')
kpi_strip(s, [
    ('Worst day', '25,046', '20 Mar 2026; Liverpool vs Galatasaray 57% of the day'),
    ('Range of the 14 tvOS days', '5,363 to 25,046', '383 to 1,789 times a normal day'),
    ('Highest 404 % of errors', '88.19%', '20 Mar; the 15th day (4 Jul) is 4.68%'),
    ('Top device share', '98% to 100%', 'tvOS on the 14 tvOS days; Android 100% on 4 Jul'),
    ('Android day', '5,088', '4 Jul; ExoPlayer bad HTTP status 404, 100% Android'),
])
heading(s, 0.45, 2.35, 9.6, 'Top 15 days by Error 404 sessions, 2026', 'all 15 days are above the 67 a day spike line')
hdr = ['#', 'Date', 'Weekday', '404 sessions', 'Level', 'Times a normal day', '404 % of errors', 'Top device', 'Top asset (share of the day’s 404s)']
rows_ = [hdr]
fills = {}
colors = {}
for i, d in enumerate(DAYS, 1):
    lvl = 'Critical' if d['e404'] >= CRIT else 'High'
    rows_.append([str(i), d['date'].strftime('%d %b %Y'), d['wd'], fmt_n(d['e404']), lvl, '{:,.0f}x'.format(d['mult']),
                  '%.2f%%' % d['pct'], d['dev'], d['asset']])
    fills[(i, 4)] = '404040' if lvl == 'Critical' else 'BFBFBF'
    colors[(i, 4)] = 'FFFFFF' if lvl == 'Critical' else INK
add_table(s, 0.45, 2.7, [0.3, 0.85, 0.75, 0.8, 0.65, 0.95, 0.85, 0.85, 3.6], rows_, ['r', 'l', 'l', 'r', 'ctr', 'r', 'r', 'l', 'l'],
          row_h=0.245, head_h=0.3, sz=7.2, fills=fills, colors=colors, name='Top15Days')
side_panel(s, [
    'On the 14 tvOS days one match is 29% to 81% of the day’s 404s (Internazionale vs Bodø / Glimt 81% on 26 Feb).',
    '404s are 20% to 88% of that day’s error sessions on the 14 tvOS days; on 20 Mar, 88%.',
    'The busiest of the 15 days (22 Mar, 861,859 sessions) had 5,363 404s; the worst (20 Mar) had 170,079 sessions: 404s follow the broken stream, not traffic.',
    'The report counts only the 22 markets, so viewers elsewhere cannot explain these spikes; the device does (tvOS 98% to 100%).',
], 'Review every day above 5,000 404 sessions with the device, type and asset shown here. Success: root cause named within 24 hours.')
finalize(s)

# =============================================================== helper: ranked bar chart aligned with a table
def ranked_bar(slide, x, y, w, h, labels, values, fmt, colors_, vmax, label_frac, top_frac=0.075, h_frac=0.9, name='Ranked'):
    gf, ch = base_chart(slide, XL_CHART_TYPE.BAR_CLUSTERED, x, y, w, h, labels, [('v', values)], legend=False, name=name)
    p = ch.plots[0]
    p.gap_width = 35
    p.vary_by_categories = False
    ser = p.series[0]
    bar_fill(ser, '404040')
    for i, c in enumerate(colors_):
        pt = ser.points[i]
        pt.format.fill.solid()
        pt.format.fill.fore_color.rgb = rgb(c)
        pt.format.line.fill.background()
    series_labels(ser, fmt, XL_LABEL_POSITION.OUTSIDE_END, sz=7, color=INK, bold=True)
    ca = ch.category_axis
    ca.reverse_order = True
    style_cat_axis(ca, sz=7, color=INK)
    va = ch.value_axis
    va.minimum_scale = 0
    va.maximum_scale = vmax
    va.has_major_gridlines = False
    va.visible = False
    inner_layout(ch, label_frac, top_frac, 1 - label_frac - 0.02, h_frac)
    plot_top = y + top_frac * h
    plot_h = h_frac * h
    return gf, ch, dict(top=plot_top, h=plot_h, left=x + label_frac * w, w=(1 - label_frac - 0.02) * w, vmax=vmax)


def aligned_table(slide, x, geo, n, head, rows, widths, aligns, sz=7, fills=None, colors=None, bolds=None, name='Aligned'):
    rh = geo['h'] / n
    hh = 0.3
    allrows = [head] + rows
    return add_table(slide, x, geo['top'] - hh, widths, allrows, aligns, row_h=rh, head_h=hh, sz=sz, fills=fills, colors=colors, bolds=bolds, name=name)


# =============================================================== slide 7: device OS and error type
s = new_slide('ERROR 404', 'DEVICE AND ERROR TYPE',
              'Where does 404 land? · 404 rate by device OS and what was not found, 2026 against 2025',
              'tvOS is the bottleneck: 85.6% of 404s on 3.8% of sessions, and the request that fails is the HLS playlist download (85.4% of 404 types).',
              'Source: Error 404 deep-dive report, section 2 (df_5_3, df_5_4), device OS recomputed from the session data in the Databricks validation run of 5 Nov 2026. ' + DEF + ' Device OS as Conviva reports it: %d OS values cover all %.1fM sessions and all 217,195 404 sessions; the Top 15 by 404 sessions are shown and hold %s of the 217,195 (the other %d hold %d). Not reported = blank OS. Dark bar = above the fleet average. 404 type shares are %% of 404 sessions in the year.' % (N_OS, TOT['sess26'] / 1e6, fmt_n(DEV_TOP_404), max(N_OS - 15, 0), TOT['e404_26'] - DEV_TOP_404))
kpi_strip(s, [
    ('tvOS share of 404s', '85.6%', '185,945 of 217,195 on 3.8% of sessions'),
    ('tvOS rate', '282 per 10K', '22.5 times the fleet average of 12.53; 2025: 0.13'),
    ('Android rate', '5.03 per 10K', '12.1% of 404s on 30.1% of sessions; below the average'),
    ('HLS playlist 404', '85.4%', '185,463 sessions in 2026; 20 in 2025 (0.7%)'),
    ('Manifest 404', '2.5%', '5,379 sessions; was 82.9% of 404s in 2025'),
])
heading(s, 0.45, 2.35, 4.7, 'Error 404 per 10K sessions, by device OS', 'Top 15 of %d OS values by 404 sessions · 2026' % N_OS)
names = [d['name'] for d in DEV]
vals = [d['p10'] for d in DEV]
gf, ch = base_chart(s, XL_CHART_TYPE.COLUMN_CLUSTERED, 0.45, 2.64, 4.7, 4.2, names,
                    [('404 per 10K sessions, 2026', vals), ('Fleet average: %.2f' % AVG, [round(AVG, 2)] * len(names))], name='DeviceRate')
p = ch.plots[0]
p.gap_width = 40
bar_fill(p.series[0], '404040')
for i, v in enumerate(vals):
    pt = p.series[0].points[i]
    pt.format.fill.solid()
    pt.format.fill.fore_color.rgb = rgb('404040' if v > AVG else 'BFBFBF')
    pt.format.line.fill.background()
series_labels(p.series[0], '0.0', XL_LABEL_POSITION.OUTSIDE_END, sz=7, color=INK, bold=True)
style_line_series(p.series[1], '262626', width=1.75, dash='dash', marker=False)
style_value_axis(ch.value_axis, 0, *nice_axis(max(vals)), '0', title='404 sessions per 10K sessions')
style_cat_axis(ch.category_axis, sz=6.5, rot=-2700000)
move_series_to_line(ch, [1])
heading(s, 5.35, 2.35, 4.7, 'What was not found: 404 type', 'share of 404 sessions, 2025 against 2026')
short = {'Playlist 404 (HLS playlist download failed)': 'Playlist (HLS)', 'Android/ExoPlayer bad HTTP status 404': 'Android ExoPlayer',
         'Manifest 404 (DASH/HLS manifest not found)': 'Manifest (DASH/HLS)', 'Apple AVPlayer -12938 (File Not Found)': 'Apple AVPlayer',
         'DRM / License 404': 'DRM / licence'}
tn = [short.get(t['name'], t['name'][:22]) for t in TYP]
gf, ch = base_chart(s, XL_CHART_TYPE.COLUMN_CLUSTERED, 5.35, 2.64, 4.7, 4.2, tn,
                    [('2025', [t['sh25'] for t in TYP]), ('2026', [t['sh26'] for t in TYP])], name='TypeShare')
p = ch.plots[0]
p.gap_width = 60
p.overlap = -5
bar_fill(p.series[0], 'BFBFBF')
bar_fill(p.series[1], '404040')
series_labels(p.series[0], '0.0"%"', XL_LABEL_POSITION.OUTSIDE_END, sz=7, color=MID)
series_labels(p.series[1], '0.0"%"', XL_LABEL_POSITION.OUTSIDE_END, sz=7, color=INK, bold=True)
style_value_axis(ch.value_axis, 0, 100, 20, '0"%"', title='% of 404 sessions')
style_cat_axis(ch.category_axis, sz=6.5)
side_panel(s, [
    'tvOS carries 185,945 of 217,195 404 sessions (85.6%) on 3.8% of sessions: 282 per 10K, 22.5 times the fleet average of 12.5.',
    'Android is second (26,215; 12.1%) but at 5.03 per 10K it is below the average: ExoPlayer HTTP errors spread over 11,324 viewers.',
    'iOS and iPadOS are 32.2% of sessions (55.7M) with 1 404 session in total. Small platforms can read high on few sessions (Linux - Ubuntu 13.9 per 10K on 7): read rate with volume.',
    'The failing step changed: 82.9% of 2025’s 404s were manifest requests; in 2026, 85.4% are HLS playlist downloads.',
], 'Add a playlist-request start test to every tvOS release and alert when tvOS passes twice the fleet average (25 per 10K, proposed). Success: tvOS at or below 12.5.')
finalize(s)

# =============================================================== slide 8: content, CDN, raw messages
s = new_slide('ERROR 404', 'CONTENT, CDN AND MESSAGES',
              'What is failing? · live against on-demand, content type, CDN and the raw error messages behind 404',
              '404s sit on live football and one CDN: 98.7% are on live streams, 95.2% on football, and three raw messages explain 98.7%.',
              'Source: Error 404 deep-dive report, section 2 (df_5_6, df_5_7, df_5_5). ' + DEF + ' Live/VOD and Content are two cuts of the same 217,195 404 sessions; shares here are recomputed on 217,195 (the report’s share column divides by 434,390 and shows 49.4% for live). Raw messages: timestamps masked; top 10 shown (all that the report lists).')
kpi_strip(s, [
    ('Live share of 404s', '98.7%', '214,385 of 217,195 on 87.0% of sessions'),
    ('Live against VOD', '14.23 · 1.24', '404s per 10K sessions; live is 11.5 times higher'),
    ('Football', '95.2%', '206,824 404s; 15.77 per 10K, the highest content rate'),
    ('Akamai', '100%', 'of 404s; unknown CDN is 18.5% of sessions with 0 404s'),
    ('Top raw message', '85.4%', 'playlist download failed, tvOS, 2,441 viewers'),
])
heading(s, 0.45, 2.35, 4.7, 'Content cut and CDN, 2026', 'share of the 217,195 404 sessions')
CL = {'Live/VOD: Live': 'Live', 'Live/VOD: VOD': 'VOD', 'Content: Football': 'Football', 'Content: Non-Football': 'Non-football', 'Content: Entertainment': 'Entertainment'}
order = [k for nm_ in ['Live/VOD: Live', 'Live/VOD: VOD', 'Content: Football', 'Content: Non-Football', 'Content: Entertainment'] for k in range(len(CONT)) if CONT[k]['name'] == nm_]
rows_ = [['Cut', 'Sessions', '404 sessions', 'Share of 404s', '404 per 10K', '2025 per 10K', '404 % of errors']]
for k in order:
    c = CONT[k]
    rows_.append([CL[c['name']], fmt_n(c['sess']), fmt_n(c['e404']), '%.1f%%' % (c['e404'] / TOT['e404_26'] * 100), '%.2f' % c['p10'], '%.2f' % c['p10_25'], '%.2f%%' % c['pct']])
fl = {}
for r_ in (1, 3):
    for cc in range(7):
        fl[(r_, cc)] = 'EDEDED'
add_table(s, 0.45, 2.72, [0.95, 0.8, 0.65, 0.55, 0.55, 0.55, 0.65], rows_, ['l', 'r', 'r', 'r', 'r', 'r', 'r'], row_h=0.24, head_h=0.34, sz=6.8, fills=fl, name='ContentTable')
rows_ = [['CDN', 'Sessions', '404 sessions', 'Share of 404s', '404 per 10K', '404 % of errors']]
for c in CDN:
    rows_.append([c['name'].title(), fmt_n(c['sess']), fmt_n(c['e404']), '%.1f%%' % (c['e404'] / TOT['e404_26'] * 100), '%.2f' % c['p10'], '%.2f%%' % c['pct']])
add_table(s, 0.45, 4.45, [0.95, 0.9, 0.7, 0.7, 0.7, 0.75], rows_, ['l', 'r', 'r', 'r', 'r', 'r'], row_h=0.24, head_h=0.34, sz=6.8, name='CdnTable')
add_text(s, 0.45, 5.4, 4.7, 1.0, [
    P('How to read', sz=8, b=True, after=3),
    dict(runs=[('Shaded rows ', dict(sz=7, b=True)), ('are the two highest-risk cuts. Live and VOD add to 217,195; football, non-football and entertainment add to the same total.', dict(sz=7, color=TXT))], after=3),
    dict(runs=[('Unknown CDN ', dict(sz=7, b=True)), ('carries 32.0M sessions and no 404 at all, which may reflect how the CDN field is filled; the split cannot rank CDNs.', dict(sz=7, color=TXT))], after=0),
])
heading(s, 5.35, 2.35, 4.7, 'Most frequent raw 404 messages, 2026', 'top 10 (all listed in the report)')
MSG_LAB = ['HLS playlist download failed: 404 Not Found', 'Unexpected HTTP status 404 (Android)', 'DASH manifest load failed: 404 (statusCode 1208)',
           'ExoPlayer 2004: bad HTTP status 404', 'DASH manifest load failed: 404 Not Found (1208)', 'SOURCE_COULD_NOT_LOAD_MANIFEST 1208: 404 Not Found',
           'AVPlayer -12938: HTTP 404 File Not Found', 'SOURCE_COULD_NOT_LOAD_MANIFEST 1208: 404', 'Unexpected HTTP status 404 (Android, code 2203)',
           'DRM licence request failed: HTTP 404']
MSG_LABELS = dict(zip(['2224 Downloading the playlist has failed. Client error. HTTP Status Code: 404 Not Found.', 'HttpStatusCode - An unexpected HTTP status code was received: Response code: 404', 'Failed to load DASH manifest: 404 .. statusCode: 1208', '2004&[2004]: ERROR_CODE_IO_BAD_HTTP_STATUS - Response code: 404', 'Failed to load DASH manifest: 404 Not Found.. statusCode: 1208', 'Error code: 1208; Name: SOURCE_COULD_NOT_LOAD_MANIFEST; Message: Failed to load DASH manifest: 404 Not Found.;', '-12938 Playback failed. Networking Error. Received HTTP status code 404: File Not Found.', 'Error code: 1208; Name: SOURCE_COULD_NOT_LOAD_MANIFEST; Message: Failed to load DASH manifest: 404 .; Type: er', '2203 - An unexpected HTTP status code was received: Response code: 404', 'DrmRequestFailed - The DRM license request failed with HTTP status: Response code: 404'], MSG_LAB))
rows_ = [['#', 'Message (shortened)', '404 sessions', 'Viewers', 'Top device']]
for i_, mm in enumerate(MSG, 1):
    rows_.append([str(i_), MSG_LABELS.get(mm['msg'], mm['msg'][:46]), fmt_n(mm['s']), fmt_n(mm['v']), mm['dev']])
add_table(s, 5.35, 2.72, [0.25, 2.25, 0.7, 0.55, 0.95], rows_, ['r', 'l', 'r', 'r', 'l'], row_h=0.3, head_h=0.34, sz=6.8, name='MsgTable')
add_text(s, 5.35, 6.2, 4.7, 0.5, [P('Message text is shortened to fit; the full strings are in the report (df_5_5). The ten messages add to 217,181 of the 217,195 404 sessions.', sz=6.5, color=GREY)])
side_panel(s, [
    'Live streams are 87% of sessions but 98.7% of 404s: 14.23 per 10K against 1.24 on VOD, 11.5 times higher.',
    'Football is 95.2% of 404s at 15.77 per 10K; entertainment is almost untouched (429 sessions, 0.13 per 10K).',
    'All 217,195 404s sit on Akamai; 32.0M sessions with an unknown CDN show none, so this split cannot rank CDNs.',
    'Three raw messages cover 98.7% of 404 sessions; the top one (playlist download failed, tvOS) is 85.4%.',
], 'Log the CDN on every session so 404s can be split by CDN. Success: unknown CDN under 1% of sessions (now 18.5%).')
finalize(s)

# =============================================================== slide 9: competitions Top 15
s = new_slide('ERROR 404', 'COMPETITIONS',
              'Which competitions fail most? · Top 15 competitions by Error 404 sessions, 2026',
              'Three competitions hold 74.5% of competition 404s: Champions League, Premier League and Primera División, almost all on tvOS.',
              'Source: Error 404 deep-dive report, section 2 (df_5_17). ' + DEF + ' Top 15 of the 20 competitions in the report, ranked by 404 sessions; ranks 16 to 20 (Premier Padel 642, Süper Lig 521, NBA 470, UEFA Women’s U-19 458, Wimbledon 427) are left out. Share of 404s is the report’s, on sport 404s only (unknown and non-sport content excluded). Times the average = 404 per 10K divided by the fleet average.')
c3 = sum(c['e404'] for c in COMP[:3])
kpi_strip(s, [
    ('Champions League', '78,772', '36.4% of competition 404s; 39.94 per 10K; tvOS 98%'),
    ('Premier League', '57,680', '26.6%; 30.42 per 10K; tvOS 96%'),
    ('Primera División', '24,829', '11.5%; 15.27 per 10K; tvOS 97%'),
    ('FIFA World Cup', '21,518', '9.9%; 3.55 per 10K, below average; Android 100%'),
    ('Highest rate in the Top 15', '248.75 per 10K', 'UEFA Youth League: 746 sessions on 29,990'),
])
heading(s, 0.45, 2.35, 9.6, 'Top 15 competitions by Error 404 sessions, 2026', 'dark bar = rate above the fleet average of 12.53 per 10K')
sh = {'Miami Open presented by Itau': 'Miami Open', 'FIA F1 World Championship 2026': 'F1 World Championship 2026'}
labs = ['%d  %s' % (i, sh.get(c['name'], c['name'])) for i, c in enumerate(COMP, 1)]
gf, ch, geo = ranked_bar(s, 0.45, 2.64, 4.75, 4.25, labs, [c['e404'] for c in COMP], '#,##0', ['404040' if c['p10'] > AVG else 'BFBFBF' for c in COMP], nice_axis(max(c['e404'] for c in COMP), 5)[0], 0.50, name='CompBars')
rows_ = []
fills = {}
colors = {}
for i, c in enumerate(COMP, 1):
    mult = c['p10'] / AVG
    rows_.append(['%.1f%%' % c['share'], '%.2f' % c['p10'], '%.1fx' % mult, '%.2f%%' % c['pct'], fmt_n(c['viewers']), c['dev']])
    if c['p10'] > AVG:
        fills[(i, 3)] = 'EDEDED'
        fills[(i, 2)] = 'EDEDED'
aligned_table(s, 5.25, geo, 15, ['Share of 404s', '404 per 10K', 'Times the average', '404 % of errors', 'Viewers', 'Top device'], rows_,
              [0.6, 0.65, 0.75, 0.75, 0.65, 0.85], ['r', 'r', 'r', 'r', 'r', 'l'], sz=7, fills=fills, name='CompTable')
side_panel(s, [
    'Champions League, Premier League and Primera División hold 161,281 404 sessions (74.5% of competition 404s), 96% to 98% on tvOS.',
    '13 of the 15 run above the fleet average of 12.5 per 10K; the exceptions are the FIFA World Cup (3.55) and Friendlies (10.60).',
    'The World Cup is the largest event (60.6M sessions) but its 404s are Android, across 10,348 viewers: not the tvOS playlist fault.',
    'Small competitions show extreme rates on few sessions (Youth League 248.75 per 10K on 29,990 sessions): read rate with volume.',
], 'Add the three leagues to the tvOS start test and report 404 per 10K by competition weekly. Success: every competition under 25 per 10K (twice the average, proposed).')
finalize(s)

# =============================================================== slide 10: assets Top 15
s = new_slide('ERROR 404', 'ASSETS',
              'Which assets generate the most 404s? · Top 15 assets (matches and titles) by Error 404 sessions, 2026',
              'The 15 assets with the most 404s hold 55.0% of the year’s total; 13 are matches streamed between 21 Feb and 30 Mar.',
              'Source: Error 404 deep-dive report, section 2 (df_5_8). ' + DEF + ' Top 15 assets by 404 sessions (the report lists 15). Sessions per viewer = 404 sessions divided by viewers who met them for that asset. First and last = first and last day with a 404 on that asset. Days = days with at least one 404. Dark bar = more than 100 sessions per viewer (retry pattern).')
a5 = sum(a['s'] for a in AST[:5])
kpi_strip(s, [
    ('Top 15 assets', '119,526', '55.0% of the 217,195 404 sessions in 2026'),
    ('Top asset', '20,629', 'Liverpool vs Galatasaray; 134 viewers, 9 days'),
    ('Top 5 assets', '70,279', '32.4% of all 404 sessions'),
    ('Most repeated', '4,172 per viewer', 'Nice vs Rennes: 4,172 sessions from 1 viewer'),
    ('Longest run', '25 days', 'Arsenal vs Chelsea, 3 Feb to 6 Sep'),
])
heading(s, 0.45, 2.35, 9.6, 'Top 15 assets by Error 404 sessions, 2026', 'all 15 are live matches · top device tvOS (98% to 100%), HLS playlist 404')
labs = ['%d  %s' % (i, a['name']) for i, a in enumerate(AST, 1)]
gf, ch, geo = ranked_bar(s, 0.45, 2.64, 4.95, 4.25, labs, [a['s'] for a in AST], '#,##0', ['404040' if a['s'] / a['v'] > 100 else 'BFBFBF' for a in AST], nice_axis(max(a['s'] for a in AST), 5)[0], 0.58, name='AssetBars')
rows_ = []
for i, a in enumerate(AST, 1):
    rows_.append([fmt_n(a['v']), fmt_n(a['s'] / a['v']), str(a['days']), a['first'].strftime('%d %b'), a['last'].strftime('%d %b'), '%.1f%%' % (a['s'] / TOT['e404_26'] * 100)])
fl = {}
for i, a in enumerate(AST, 1):
    if a['s'] / a['v'] > 100:
        fl[(i, 1)] = 'EDEDED'
aligned_table(s, 5.45, geo, 15, ['Viewers', 'Sessions per viewer', 'Days with 404', 'First 404', 'Last 404', 'Share of all 404s'], rows_,
              [0.55, 0.7, 0.7, 0.7, 0.7, 0.6], ['r', 'r', 'r', 'r', 'r', 'r'], sz=7, fills=fl, name='AssetTable')
side_panel(s, [
    'The 15 assets hold 119,526 404 sessions (55.0% of 2026); the top five alone hold 70,279 (32.4%).',
    '13 of the 15 had all their 404s between 21 Feb and 30 Mar; 12 of the 15 ran 7 to 15 days with 404s.',
    'Few viewers, many retries: Nice vs Rennes shows 4,172 sessions from 1 viewer, Aston Villa vs Leeds 4,898 from 3.',
    'Arsenal vs Chelsea and Liverpool vs Tottenham still returned 404s months later (last seen 6 Sep and 15 Sep).',
], 'Check the playlists of the Top 15 assets and repair or remove any still failing. Success: no 404s on these assets in the 30 days after the fix.')
finalize(s)

# =============================================================== slide 11: repeat viewers and hours
s = new_slide('ERROR 404', 'REPEAT VIEWERS AND HOURS',
              'Who and when? · 404 sessions per viewer, and 404 per 10K sessions by hour of session start, 2026',
              'Few viewers, many retries: 3.1% of viewers produce 84.7% of 404 sessions, and 404s peak in the morning hours.',
              'Source: Error 404 deep-dive report (df_5_19, hour of session start in Doha time, UTC+3; the Databricks validation run of 5 Nov 2026 gives the same 24 values). ' + DEF + ' 2025 is below 0.6 per 10K in every hour. Morning 06:00 to 11:59 = 32,737 404 sessions on 10,473,327 sessions = 31.3 per 10K. Alert line = twice the fleet average, 25 per 10K (proposed).')
kpi_strip(s, [
    ('Viewers who met a 404', '16,586', 'in 2026; 1,772 in 2025'),
    ('Seen once', '55.2%', '9,159 viewers; 4.2% of 404 sessions'),
    ('21 or more sessions', '3.1%', '512 viewers; 183,935 sessions (84.7%), about 359 each'),
    ('Peak hour, 08:00 Doha', '39.6 per 10K', '3.2 times the fleet average of 12.53'),
    ('Evening, 19:00 to 23:00 Doha', '6 to 9 per 10K', 'below the fleet average'),
])
heading(s, 0.45, 2.35, 4.7, '404 sessions per viewer, 2026', 'share of viewers against share of 404 sessions')
bands = ['1 session', '2 to 5', '6 to 20', '21 or more']
gf, ch = base_chart(s, XL_CHART_TYPE.COLUMN_CLUSTERED, 0.45, 2.64, 4.7, 4.2, bands,
                    [('Share of viewers', [r_['vshare'] for r_ in REP]), ('Share of 404 sessions', [r_['sshare'] for r_ in REP])], name='RepeatBands')
p = ch.plots[0]
p.gap_width = 60
p.overlap = -5
bar_fill(p.series[0], 'BFBFBF')
bar_fill(p.series[1], '404040')
series_labels(p.series[0], '0.0"%"', XL_LABEL_POSITION.OUTSIDE_END, sz=7, color=MID)
series_labels(p.series[1], '0.0"%"', XL_LABEL_POSITION.OUTSIDE_END, sz=7, color=INK, bold=True)
style_value_axis(ch.value_axis, 0, 100, 20, '0"%"', title='% of total')
style_cat_axis(ch.category_axis, sz=7)
heading(s, 5.35, 2.35, 4.7, 'Error 404 per 10K sessions, by hour (Doha time)', '2026 against 2025 · UTC+3')
hrs = ['%02d' % h for h in range(24)]
gf, ch = base_chart(s, XL_CHART_TYPE.LINE_MARKERS, 5.35, 2.64, 4.7, 4.2, hrs,
                    [('2026 per 10K', HOUR26), ('2025 per 10K', HOUR25), ('Alert line: 25 (2 times average)', [round(2 * AVG, 1)] * 24), ('Fleet average: 12.53', [round(AVG, 2)] * 24)], name='HourRate')
p = ch.plots[0]
style_line_series(p.series[0], '404040', width=2.75, msize=5)
style_line_series(p.series[1], 'A6A6A6', width=2.25, dash='dash', msize=4)
style_line_series(p.series[2], '262626', width=1.5, dash='dash', marker=False)
style_line_series(p.series[3], '7F7F7F', width=1.5, dash='dash', marker=False)
style_value_axis(ch.value_axis, 0, *nice_axis(max(max(HOUR26), 2 * AVG)), '0', title='404 sessions per 10K sessions')
style_cat_axis(ch.category_axis, sz=6.5)
side_panel(s, [
    '55.2% of the 16,586 viewers who met a 404 saw it once; together they produce 4.2% of 404 sessions.',
    '512 viewers (3.1%) with 21 or more sessions produce 183,935 sessions (84.7%), about 359 each: it looks like automatic retry on a broken playlist.',
    '404s per 10K peak at 08:00 (39.6, 3.2 times the average) and stay at 29 to 40 from 06:00 to 10:00; evenings run at 6 to 9.',
    'From 06:00 to 11:59 Doha time the rate is 31.3 per 10K, 2.5 times the fleet average (Error 360 reads 2.4 times).',
], 'Cap player retries on playlist 404, and count unique viewers as well as sessions in the 404 alert. Success: no viewer above 20 404 sessions a day (proposed).')
finalize(s)

# =============================================================== slide 12: countries Top 15
s = new_slide('ERROR 404', 'COUNTRIES',
              'Where do 404s land? · Top 15 countries by Error 404 sessions, rate per 10K sessions against the fleet average, 2026',
              'UAE and Kuwait hold 74.5% of 404s; only four countries run above the fleet average of 12.5 404s per 10K sessions.',
              'Source: Error 404 deep-dive report, section 3 (df_5_10, df_5_11). ' + DEF + ' Top 15 countries ranked by 404 sessions (the report lists 15 of the 22 markets); they hold 217,118 of the 217,195 404 sessions. The dashed line marks the fleet average of 12.53 per 10K. Geo check: the report’s dataset holds only the 22 covered markets by design; the source system has 3,233 more 404 sessions (1.5% of the 2026 total) in other countries, which are outside this report.')
kpi_strip(s, [
    ('United Arab Emirates', '119,605', '55.1% of 404s; 26.67 per 10K on 44.8M sessions'),
    ('Kuwait', '42,170', '19.4% of 404s; 22.17 per 10K on 19.0M sessions'),
    ('Above the average', '4 countries', 'UAE 26.67, Kuwait 22.17, Qatar 14.78, Jordan 14.28'),
    ('Largest markets, low rate', '1.13 · 1.93', 'Egypt · Iraq per 10K, on 32.3M and 22.4M sessions'),
    ('Sessions outside the 22 markets', '0', 'in this dataset; 404s are 100% covered-market'),
])
heading(s, 0.45, 2.35, 9.6, 'Top 15 countries: Error 404 per 10K sessions, 2026', 'dark bar = rate above the fleet average of 12.53 per 10K')
labs = ['%d  %s' % (i, c['name'].replace('United Arab Emirates', 'UAE')) for i, c in enumerate(CTRY, 1)]
vmax = nice_axis(max(c['p10'] for c in CTRY), 5)[0]
gf, ch, geo = ranked_bar(s, 0.45, 2.64, 4.75, 4.25, labs, [c['p10'] for c in CTRY], '0.00', ['404040' if c['p10'] > AVG else 'BFBFBF' for c in CTRY], vmax, 0.30, name='CountryBars')
# fleet-average line drawn over the plot area (axis and inner layout are fixed)
xline = geo['left'] + geo['w'] * AVG / vmax
hline(s, xline, geo['top'] - 0.06, xline, geo['top'] + geo['h'], color='262626', w=1.25, dash='dash')
add_text(s, xline + 0.04, geo['top'] + geo['h'] - 0.2, 1.4, 0.15, [P('fleet average 12.53', sz=6.5, b=True, color='262626')])
rows_ = []
fl = {}
for i, c in enumerate(CTRY, 1):
    rows_.append([fmt_n(c['sess']), fmt_n(c['e404']), '%.1f%%' % c['share'], '%.1fx' % (c['p10'] / AVG), '%.2f%%' % c['pct'], fmt_n(c['viewers'])])
    if c['p10'] > AVG:
        fl[(i, 3)] = 'EDEDED'
aligned_table(s, 5.25, geo, 15, ['Sessions', '404 sessions', 'Share of 404s', 'Times the average', '404 % of errors', 'Viewers'], rows_,
              [0.8, 0.75, 0.65, 0.75, 0.75, 0.6], ['r', 'r', 'r', 'r', 'r', 'r'], sz=7, fills=fl, name='CountryTable')
side_panel(s, [
    'UAE (119,605) and Kuwait (42,170) hold 74.5% of 404s at 26.67 and 22.17 per 10K; Qatar (14.78) and Jordan (14.28) are also above the average.',
    'Egypt and Iraq are large markets with low rates (1.13 and 1.93 per 10K): 404 is not a market-wide issue.',
    'Saudi Arabia, a problem market in Error 360, has 734 404s (6.24 per 10K, 0.56% of its errors): its failures are other types.',
    'This dataset holds only the 22 covered markets, while Error 360 puts 2% of 404s outside them: the geo check is not independent.',
], 'Add the all-country session table to the 404 report so the outside-coverage test can be run for real. Success: outside-market 404s reported as a number.')
finalize(s)

if INCLUDE_VALIDATION_SLIDES:
    # =============================================================== slides 13 and 14: validation
    GREEN_T, AMB, REDT = GREEN, GOLD_DK, RED
    s = new_slide('VALIDATION', 'TIE-OUT OF THE HTML REPORT',
                  'Are the numbers right? · every total recomputed from the report tables, and what was corrected',
                  'Every total in the report ties out; two tables were corrected or completed here, and the 71 spike days now re-derive from the 307-day series.',
                  'Source: Error 404 deep-dive report tables (18 tables parsed from the HTML) and its notebook (Error_ReportJ_V7), recomputed in this deck build. The report’s own validation block lists 32 checks, all PASS. A Databricks validation notebook was run on 5 Nov 2026 on the same data: 31 of 31 checks pass. Result: Pass = recomputed and equal; Fixed here = corrected or completed in this deck from the session data.',
                  datawin='Checked: 4 Nov 2026 report, recomputed 5 Nov 2026 in Databricks')
    kpi_strip(s, [
        ('Report’s own checks', '32 of 32', 'PASS, each an independent recount from the source table'),
        ('Totals recomputed here', '12 of 12', 'monthly, yearly, per 10K, shares, repeat bands, Top 15 sums, spike days, hours'),
        ('Fixed here', '2', 'content shares on 217,195; full device list'),
        ('Databricks run', '31 of 31', 'validation notebook scorecard, all PASS'),
        ('Open', '0', 'the 71 spike days re-derive from the daily series'),
    ])
    heading(s, 0.45, 2.35, 12.43, 'Checks run on the Error 404 report', 'recomputed from the report tables unless stated')
    V = [
        ['Check', 'Report value', 'Recomputed here', 'Result'],
        ['2026 404 sessions: 11 months add to the year', '217,195', '217,195 (sum of monthly)', 'Pass'],
        ['2025 404 sessions: 12 months add to the year', '3,022', '3,022 (sum of monthly)', 'Pass'],
        ['Error sessions 2026: 404 + other = total', '6,356,227', '217,195 + 6,139,032 = 6,356,227', 'Pass'],
        ['404 per 10K sessions, 2026 and 2025', '12.53 and 0.28', '12.534 and 0.279', 'Pass'],
        ['404 share of error sessions, 2026 and 2025', '3.42% and 0.10%', '3.417% and 0.097%', 'Pass'],
        ['Average per day = month total ÷ days (23 months)', 'Jan 15.7, Mar 4,764.3, Aug 2025 27.3 ...', 'all 23 equal to 0.1', 'Pass'],
        ['Year-on-year change in share (pp)', 'Jul 0.31, Aug -0.38', 'Jul 0.30, Aug -0.37', 'Pass (rounding)'],
        ['Repeat bands: viewers and sessions', '16,586 and 217,195', '16,586 and 217,195', 'Pass'],
        ['Top 15 days / assets / countries add up', '–', '132,774 / 119,526 / 217,118 (below 217,195)', 'Pass'],
        ['Times a normal day: all 15 days', '1,789.0x to 363.4x', 'every day implies the same median day, 14.0', 'Pass'],
        ['Content table: share of 404s (live, football...)', 'Live 49.4%, football 47.6%', 'Live 98.7%, football 95.2%', 'Fixed here'],
        ['Device table coverage', '12 OS values (117.5M sessions)', '29 OS values: 173,282,794 sessions, 217,195 404s', 'Fixed here'],
        ['Hour-of-day values (Doha time)', 'df_5_19: peak 08:00, 39.55', 'validation run gives the same 24 values', 'Pass'],
        ['71 spike days, threshold 67, median 14', 'text and chart', '307 days: line 67.4, median 14, 71 days above, 16 at 5,000+', 'Pass'],
    ]
    fills = {}
    colors = {}
    for i in range(1, len(V)):
        r_ = V[i][3]
        colors[(i, 3)] = GREEN_T if r_.startswith('Pass') else (AMB if r_ in ('Caveat', 'Fixed here') else REDT)
    add_table(s, 0.45, 2.7, [4.0, 2.7, 4.2, 1.53], V, ['l', 'l', 'l', 'l'], row_h=0.26, head_h=0.3, sz=7.2, colors=colors,
              bolds={(i, 3): True for i in range(1, len(V))}, name='ValidationTable')
    finalize(s)

    s = new_slide('VALIDATION', 'AGAINST ERROR 360',
                  'Does it agree with Error 360 (draft 12)? · the same measures read from both documents',
                  'Against Error 360 the tvOS story and the market sizes agree; the gaps in totals come from scope (countries, viewers, dates), and none is a contradiction.',
                  'Source: Error 404 deep-dive report and Error 360, November 2026 draft 12. Error 360 counts every country and, on its market pages, identified viewers in the 22 markets, 1 Jan to 31 Oct 2026; the 404 report covers the 22 markets, all sessions, 1 Jan to 3 Nov 2026. Jan to Oct 404 sessions here = 217,195 less 465 in November = 216,730. Outside-market share checked in the source system by the Databricks validation run of 5 Nov 2026. Aligned = same within rounding or explained scope; Scope = different base, not a contradiction.',
                  datawin='Compared: Error 404 report (3 Nov 2026) with Error 360 draft 12 (31 Oct 2026)')
    kpi_strip(s, [
        ('Aligned', '6', '404 count, tvOS spike, tvOS share, UAE + Egypt, Saudi sessions, morning peak'),
        ('Scope difference', '5', '404 share, error sessions in 2026 and 2025, session base, outside-market share'),
        ('Conflict', '0', 'outside-market 404s are 1.5% in the source system, 2% in Error 360'),
        ('404 sessions, Jan to Oct', '216.7K | 219.7K', 'this deck | Error 360; 0.7% apart once its 2% outside is removed'),
        ('Next', '1 action', 'state the 22-market scope on the page; align period and viewer filter if one total is needed'),
    ])
    heading(s, 0.45, 2.35, 12.43, 'Same measure, both documents', 'Error 360 values from the slide named')
    E = [
        ['Measure', 'Error 404 report', 'Error 360', 'Result and reason'],
        ['404 sessions, Jan to Oct 2026', '216,730 (22 markets)', '219.7K, all countries (slide 4)', 'Aligned: 219.7K less 2% outside = 215.3K, within 0.7%'],
        ['404 share of error sessions', '3.42% of 6.36M', '2.7% of 8.10M (slide 4)', 'Scope: 360 counts every country; denominator differs'],
        ['Error sessions, Jan to Oct 2026', '6.33M (22 markets)', '8.10M, 19% outside = 6.56M inside', 'Scope: 0.23M (3.5%) apart, filter not identical'],
        ['Error sessions, Jan to Oct 2025', '2.07M', '2.70M (slide 4)', 'Scope: 360 includes outside markets and no-text starts'],
        ['Sessions 2026', '173.3M, all sessions to 3 Nov', '163.3M attempts, identified viewers, to 31 Oct', 'Scope: different viewer filter and period'],
        ['404 spike Feb to Mar', 'tvOS 14 of 15 worst days, first on 23 Feb', 'tvOS build 10274 first seen 19 Feb (slide 6)', 'Aligned: spike starts days after the build appears'],
        ['tvOS share of attempts', '3.8% of sessions', '3.9% (slide 6)', 'Aligned'],
        ['UAE + Egypt share of sessions', '44.5%', '45% (slide 8)', 'Aligned'],
        ['Saudi Arabia sessions', '1,175,430', '1.17M attempts (slide 8)', 'Aligned; Saudi problem is other errors, 404 is 0.56%'],
        ['Morning concentration of 404', '2.5 times average, 06:00 to 11:59 Doha (31.3 per 10K)', '2.4 times average 06:00 to 11:59 Doha (slide 9)', 'Aligned: 2.5 against 2.4 times, same Doha time zone'],
        ['404 outside the 22 markets', '1.5% of 404s, 17.6% of errors in the source system', '2% of 404s, 19% of all errors (slide 4)', 'Scope: same finding; this report keeps only the 22 markets, so none is in its data'],
    ]
    colors = {}
    for i in range(1, len(E)):
        r_ = E[i][3]
        colors[(i, 3)] = GREEN_T if r_.startswith('Aligned') else (AMB if r_.startswith('Scope') else REDT)
    add_table(s, 0.45, 2.7, [2.6, 2.9, 3.4, 3.53], E, ['l', 'l', 'l', 'l'], row_h=0.325, head_h=0.3, sz=7.2, colors=colors,
              bolds={(i, 3): False for i in range(1, len(E))}, name='CrossCheckTable')
    finalize(s)


# =============================================================== title slide edit
t = prs.slides[0]
for sh in t.shapes:
    if sh.name == 'TextBox 8':
        for r in sh.text_frame.paragraphs[0].runs:
            r.text = ''
        sh.text_frame.paragraphs[0].runs[0].text = 'Error 404 Deep-Dive'
    if sh.name == 'TextBox 15':
        ps = sh.text_frame.paragraphs
        ps[0].runs[0].text = 'Monthly trend  ·  Spikes  ·  Bottlenecks'
        for r in ps[0].runs[1:]:
            r.text = ''
        ps[1].runs[0].text = 'Error 404 sessions across TOD MENA on Conviva data, 1 January 2025 to 3 November 2026'
        for r in ps[1].runs[1:]:
            r.text = ''

# =============================================================== drop the original Error 360 slides (keep the title)
sldIdLst = prs.slides._sldIdLst
ids = list(sldIdLst)
n_old = N_TEMPLATE
for sid in ids[1:n_old]:
    prs.part.drop_rel(sid.rId)
    sldIdLst.remove(sid)
prs.core_properties.title = 'Error 404 Deep-Dive, November 2026'
REPL_DONE = replace_text(prs)
prs.save(OUT_LOCAL)
print('saved', OUT_LOCAL, len(prs.slides), 'slides;', REPL_DONE, 'text substitutions')

# COMMAND ----------

# ---- E. Freshness check: the narrative sentences were written for the 4 Nov 2026 data. Any line marked REVIEW has moved. -----
_mo = lambda n: DAILY[DAILY.mn == n]
_dev0 = DEV[0]
_morning_rate = sum(HOUR_E404_26[6:12]) / max(sum(HOUR_SESS26[6:12]), 1) * 1e4
_checks = [
    ("Sessions 2026", 173282794, TOT["sess26"]), ("Error sessions 2026", 6356227, TOT["err26"]), ("404 sessions 2026", 217195, TOT["e404_26"]),
    ("404 sessions 2025", 3022, TOT["e404_25"]), ("404 viewers 2026", 16586, TOT["viewers404_26"]),
    ("404 per 10K 2026", 12.53, round(AVG, 2)), ("404 % of errors 2026", 3.42, round(TOT["share26"], 2)),
    ("Days in the 2026 series", 307, len(DAILY)), ("Spike line", 67, round(THR)), ("Median day", 14, round(MEDIAN)),
    ("Spike days", 71, int((DAILY.e404 > THR).sum())), ("Days at 5,000 or more", 16, int((DAILY.e404 >= HIGH).sum())),
    ("Days at 10,000 or more", 3, int((DAILY.e404 >= CRIT).sum())), ("Worst day 404 sessions", 25046, DAYS[0]["e404"]),
    ("Top 15 days 404 sessions", 132774, sum(d_["e404"] for d_ in DAYS)), ("Top device by 404s", "tvOS", _dev0["name"]),
    ("Top device share of 404s (%)", 85.6, _dev0["share"]), ("Top device 404 per 10K", 282.21, _dev0["p10"]),
    ("Peak month avg per day (March)", 4764.3, float(M["d26"][2] or 0)), ("March 404 % of errors", 33.79, float(M["p26"][2] or 0)),
    ("Live share of 404s (%)", 98.7, round(next(c["e404"] for c in CONT if c["name"] == "Live/VOD: Live") / TOT["e404_26"] * 100, 1)),
    ("Football share of 404s (%)", 95.2, round(next(c["e404"] for c in CONT if c["name"] == "Content: Football") / TOT["e404_26"] * 100, 1)),
    ("Top 15 assets 404 sessions", 119526, sum(a_["s"] for a_ in AST)), ("Top 15 countries 404 sessions", 217118, sum(c_["e404"] for c_ in CTRY)),
    ("Top competition", "UEFA Champions League", COMP[0]["name"]), ("Top country", "United Arab Emirates", CTRY[0]["name"]),
    ("21+ band share of 404 sessions (%)", 84.7, REP[-1]["sshare"]), ("Peak hour", 8, int(np.argmax(HOUR26))),
    ("Peak hour 404 per 10K", 39.55, max(HOUR26)), ("06:00-11:59 times the average", 2.49, round(_morning_rate / AVG, 2)),
    ("Top 404 type", "Playlist 404 (HLS playlist download failed)", TYP[0]["name"]),
]
_rows = []
for nm_, exp_, act_ in _checks:
    ok_ = (exp_ == act_) if isinstance(exp_, str) else abs(float(exp_) - float(act_)) <= max(0.006, abs(float(exp_)) * 1e-9)
    _rows.append((nm_, exp_, act_, "ok" if ok_ else "REVIEW narrative"))
_fresh = pd.DataFrame(_rows, columns=["figure quoted in the deck text", "4 Nov 2026 baseline", "this run", "status"])
display(_fresh)
n_rev = int((_fresh.status != "ok").sum())
print("%d of %d quoted figures unchanged; %d to review in the slide text (charts and tables are always from this run)." % (len(_fresh) - n_rev, len(_fresh), n_rev))

# COMMAND ----------

# ---- F. Deliver ---------------------------------------------------------------------------------------------------------------
import shutil, os
try:
    shutil.copyfile(OUT_LOCAL, OUTPUT_PATH)
except Exception:
    dbutils.fs.cp("file:" + OUT_LOCAL, OUTPUT_PATH, True)      # fallback for DBFS paths
print("Deck written to", OUTPUT_PATH, "(%.1f MB)" % (os.path.getsize(OUT_LOCAL) / 1e6))
