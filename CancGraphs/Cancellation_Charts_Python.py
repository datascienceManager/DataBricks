# =====================================================================
# Cancellation analysis: 9 chart types in Python (matplotlib, + optional plotly)
# Data = the result tables from the Aug-Sep 2026 cancellation notebook, hard-coded so the
# file runs stand-alone in a Databricks Python cell. Replace each dict with
#   df = spark.sql("...").toPandas()   (queries are in Cancellation_DeepDive_sparklyr.R)
# In Databricks: leave SAVE_DIR = None and each chart shows inline (or use display(fig)).
# =====================================================================
import numpy as np, pandas as pd
import matplotlib; import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, PercentFormatter

SAVE_DIR = None            # e.g. "/dbfs/FileStore/charts" to write PNGs
BLUE, ORANGE, GREY, INK, INK2, GRID = "#2a78d6", "#eb6834", "#b8b7b0", "#0b0b0b", "#52514e", "#e6e5e1"
COL = {"New": BLUE, "Win-back": ORANGE, "Churned status": GREY}
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
    "axes.edgecolor": "#bbb", "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "axes.titleweight": "bold", "axes.titlesize": 13, "axes.titlelocation": "left", "axes.grid": False})
thousands = FuncFormatter(lambda v, p: f"{v:,.0f}")

def finish(fig, name):
    fig.tight_layout()
    if SAVE_DIR: fig.savefig(f"{SAVE_DIR}/{name}.png", dpi=200, bbox_inches="tight", facecolor="white")
    else: plt.show()
    plt.close(fig)

# ------------------------------------------------------------------ 1. Run-rate bars
months = ["Apr 25","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan 26","Feb","Mar","Apr","May","Jun","Jul 26","Aug 26","Sep 26"]
total = [15107,20313,25519,20400,17395,25160,32273,31317,31375,30260,26370,25854,31693,43067,48369,16845,17293,23680]
def chart1():
    fig, ax = plt.subplots(figsize=(10, 4.4)); avg = np.mean(total[:16])
    ax.bar(range(18), total, color=["#cfcfcf"]*15 + [GREY, BLUE, ORANGE], width=.7)
    ax.axhline(avg, color=INK, ls="--", lw=1); ax.text(-.4, avg+1000, f"Apr 25-Jul 26 average {avg/1000:.1f}k", fontsize=10)
    ax.set_xticks(range(18)); ax.set_xticklabels(months, rotation=45, ha="right"); ax.yaxis.set_major_formatter(thousands)
    ax.set_title("Monthly cancelling customers vs run rate"); ax.set_ylabel("Unique customers")
    finish(fig, "01_run_rate")

# ------------------------------------------------------------------ 2. Weekly stacked + share
wk = ["W31*","W32","W33","W34","W35","W36","W37","W38","W39","W40*"]
ch = [890,2620,2991,4654,4810,4602,4599,3892,3409,908]; nw = [19,26,48,92,63,60,345,963,937,398]; wb = [45,43,83,133,110,91,484,1514,1740,705]
def chart2():
    fig, ax = plt.subplots(figsize=(10, 4.4)); x = np.arange(10)
    ax.bar(x, ch, color=GREY, label="Churned status", width=.7)
    ax.bar(x, nw, bottom=ch, color=BLUE, label="New", width=.7)
    ax.bar(x, wb, bottom=np.array(ch)+nw, color=ORANGE, label="Win-back", width=.7)
    for i in x: ax.text(i, ch[i]+nw[i]+wb[i]+90, f"{(nw[i]+wb[i])/(ch[i]+nw[i]+wb[i]):.0%}", ha="center", fontweight="bold", fontsize=10)
    ax.set_xticks(x); ax.set_xticklabels(wk); ax.yaxis.set_major_formatter(thousands); ax.legend(frameon=False, ncol=3, loc="upper left")
    ax.set_title("Weekly cancellations by status (label = New + Win-back share)"); ax.set_ylim(0, 7600)
    finish(fig, "02_weekly_stacked")

# ------------------------------------------------------------------ 3. 100% stacked: days to expiry
def chart3():
    cats = ["Expired / 0 days","1-7 days","8-14 days","15-30 days","30+ days"]
    d = {"New":[0,0,40,2381,530], "Win-back":[0,0,53,4098,797], "Churned status":[2175,8969,4801,13108,4219]}
    cols = ["#c9c8c2","#e3e2dd","#9ec5f4",ORANGE,INK]
    fig, ax = plt.subplots(figsize=(10, 3.8)); left = np.zeros(3)
    for j, c in enumerate(cats):
        v = np.array([d[k][j]/sum(d[k]) for k in d]); ax.barh(list(d), v, left=left, color=cols[j], label=c, height=.55, edgecolor="white")
        for i, s in enumerate(v):
            if s > .05: ax.text(left[i]+s/2, i, f"{s:.0%}", ha="center", va="center", fontsize=10, fontweight="bold", color="white" if cols[j]==INK else INK)
        left += v
    ax.invert_yaxis(); ax.xaxis.set_major_formatter(PercentFormatter(1)); ax.legend(frameon=False, ncol=5, loc="upper center", bbox_to_anchor=(.5, 1.14), fontsize=9)
    ax.set_title("Days of paid access left at cancellation", pad=36)
    finish(fig, "03_expiry_100pct")

# ------------------------------------------------------------------ 4. Lollipop: non-watcher rate
def chart4():
    lab = ["New","Win-back","Churned status","All"]; v = [16.4, 4.0, 6.3, 6.8]; n = [485, 200, 2080, 2765]
    fig, ax = plt.subplots(figsize=(7, 3.8)); y = np.arange(4)[::-1]
    ax.hlines(y, 0, v, color="#cfcfcf", lw=2); ax.scatter(v, y, s=140, color=[BLUE, ORANGE, GREY, INK], zorder=3)
    for yi, vi, ni in zip(y, v, n): ax.text(vi+.5, yi, f"{vi}%  ({ni:,} customers)", va="center")
    ax.set_yticks(y); ax.set_yticklabels(lab); ax.set_xlim(0, 24); ax.xaxis.set_major_formatter(PercentFormatter(100))
    ax.set_title("Cancellers who never watched (>=1 min)"); finish(fig, "04_nonwatcher_lollipop")

# ------------------------------------------------------------------ 5. Heatmap: non-watchers product x country
def chart5():
    prod = ["TOD 4K","TOD Mobile","TOD HD","TOD Total"]; ctry = ["UAE","Egypt","Jordan","Kuwait","Qatar","Iraq","Oman","Others"]
    M = np.array([[246,152,170,121,45,32,0,16],[209,152,86,63,52,56,18,68],[172,132,95,57,15,0,0,31],[101,13,12,15,15,0,0,0]])
    fig, ax = plt.subplots(figsize=(8.5, 3.8)); im = ax.imshow(M, cmap="Blues", aspect="auto")
    for i in range(4):
        for j in range(8): ax.text(j, i, M[i, j], ha="center", va="center", fontsize=10, color="white" if M[i, j] > 150 else INK)
    ax.set_xticks(range(8)); ax.set_xticklabels(ctry); ax.set_yticks(range(4)); ax.set_yticklabels(prod)
    for s in ax.spines.values(): s.set_visible(False)
    ax.set_title("Non-watching cancellers: product x country"); finish(fig, "05_heatmap")

# ------------------------------------------------------------------ 6. Grouped bars: viewing recency
def chart6():
    lab = ["Watched after\ncancel","Cancel day","1-7 days\nbefore","8-30 days\nbefore","30+ days\nbefore"]
    d = {"New":[1231,300,384,253,298], "Win-back":[2066,375,656,431,1219], "Churned status":[15170,1366,3339,3445,7615]}
    fig, ax = plt.subplots(figsize=(10, 4.2)); x = np.arange(5); w = .27
    for i, (k, v) in enumerate(d.items()): ax.bar(x+(i-1)*w, np.array(v)/sum(v), w, color=COL[k], label=k)
    ax.set_xticks(x); ax.set_xticklabels(lab); ax.yaxis.set_major_formatter(PercentFormatter(1)); ax.legend(frameon=False)
    ax.set_title("When did watchers last watch, relative to cancelling?"); finish(fig, "06_recency")

# ------------------------------------------------------------------ 7. Distribution: win-back cycling
def chart7():
    lab = ["1*","2","3","4-5","6+"]; v = [781,1537,879,907,843]
    fig, ax = plt.subplots(figsize=(7, 4)); ax.bar(lab, v, color=[GREY, ORANGE, ORANGE, ORANGE, INK], width=.65)
    for i, s in enumerate(v): ax.text(i, s+25, f"{s:,}\n{s/sum(v):.0%}", ha="center", fontsize=10, fontweight="bold")
    ax.set_ylim(0, 1900); ax.set_xlabel("Lifetime D2C subscriptions per customer"); ax.set_title("Cancelling win-backs: how many cycles?")
    finish(fig, "07_cycling")

# ------------------------------------------------------------------ 8. Reach of competitions among cancellers
def chart8():
    comps = ["Premier League","UEFA Champions League","Primera Division","FIFA World Cup"]
    den = {"New":2951, "Win-back":4947, "Churned status":32873}
    n = {"New":[1018,693,1131,247], "Win-back":[3244,3431,3393,2450], "Churned status":[19939,20773,20607,11445]}
    fig, ax = plt.subplots(figsize=(8.5, 4)); y = np.arange(4); h = .26
    for i, k in enumerate(den): ax.barh(y+(1-i)*h, np.array(n[k])/den[k], h, color=COL[k], label=k)
    ax.set_yticks(y); ax.set_yticklabels(comps); ax.invert_yaxis(); ax.xaxis.set_major_formatter(PercentFormatter(1)); ax.legend(frameon=False, loc="lower right")
    ax.set_title("Share of cancellers who watched each competition (before cancelling)"); finish(fig, "08_competition_reach")

# ------------------------------------------------------------------ 9. Small multiples: cancels/day vs viewing
def chart9():
    wl = ["W31","W32","W33","W34","W35","W36","W37","W38","W39","W40"]
    perday = [474,384,446,697,712,679,775,908,868,670]; mins = [.47,1.63,7.72,29.14,37.35,43.17,65.2,58.64,22.05,10.0]
    fig, (a, b) = plt.subplots(2, 1, figsize=(10, 5.6), sharex=True, gridspec_kw={"height_ratios": [1, 1]})
    a.plot(wl, perday, color=BLUE, marker="o", lw=2.2); a.set_ylabel("Cancellations / day"); a.set_title("Cancellations kept rising while viewing fell")
    for i, s in enumerate(perday): a.text(i, s+30, s, ha="center", fontsize=9)
    b.bar(wl, mins, color="#c9c8c2", width=.65); b.set_ylabel("Football viewing (M min)")
    fig.subplots_adjust(hspace=.12); finish(fig, "09_small_multiples")

for f in (chart1, chart2, chart3, chart4, chart5, chart6, chart7, chart8, chart9): f()

# ------------------------------------------------------------------ Optional: interactive version (plotly)
# import plotly.graph_objects as go
# fig = go.Figure()
# for name, v, c in [("Churned status", ch, GREY), ("New", nw, BLUE), ("Win-back", wb, ORANGE)]:
#     fig.add_bar(x=wk, y=v, name=name, marker_color=c)
# fig.update_layout(barmode="stack", title="Weekly cancellations by status", template="simple_white")
# displayHTML(fig.to_html(include_plotlyjs="cdn"))      # hover tooltips, zoom, legend toggle
