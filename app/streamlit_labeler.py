import psycopg
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from datetime import date, timedelta

from pathlib import Path
import sys

_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from pipelines.features.load_from_pg import load_candles
from pipelines.features.indicators import add_indicators
from pipelines.common.settings import require, POSTGRES_DSN

st.set_page_config(layout="wide")
st.title("Binance Futures Candles — Explore & Label")

# Sidebar: market & data
market_type = st.sidebar.selectbox("Market type", ["um", "cm"], index=0 if require("MARKET_TYPE") == "um" else 1)
symbol = st.sidebar.text_input("Symbol", require("SYMBOL"))
interval = st.sidebar.selectbox("Interval", ["1m", "5m", "15m", "30m", "1h"], index=0)

# Load by date range (calendar) or last N days or raw candle count
CANDLES_PER_DAY = {"1m": 1440, "5m": 288, "15m": 96, "30m": 48, "1h": 24}
MAX_CANDLES = 30_000  # cap so Streamlit stays responsive
today = date.today()
default_end = today
default_start = today - timedelta(days=7)

load_mode = st.sidebar.radio("Load data by", ["Date range (calendar)", "Last N days", "Candle count"], horizontal=False)
use_date_range = False
limit = 5000

if load_mode == "Date range (calendar)":
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input("Start date", value=default_start, key="start_date")
    with col2:
        end_date = st.date_input("End date", value=default_end, key="end_date")
    if start_date > end_date:
        st.sidebar.warning("Start must be ≤ end. Using end as start.")
        start_date = end_date
    use_date_range = True
    estimated = (end_date - start_date).days * CANDLES_PER_DAY.get(interval, 24)
    if estimated > MAX_CANDLES:
        st.sidebar.caption(f"Range = {estimated:,} candles → capped at {MAX_CANDLES:,} (most recent in range)")
    else:
        st.sidebar.caption(f"→ ~{estimated:,} candles ({interval})")
elif load_mode == "Last N days":
    days = st.sidebar.slider("Days to load", 1, 365, 7, step=1)
    limit = min(days * CANDLES_PER_DAY.get(interval, 24), MAX_CANDLES)
    st.sidebar.caption(f"→ {limit:,} candles ({interval})")
else:
    limit = st.sidebar.slider("Candles to load", 200, MAX_CANDLES, 5000, step=100)

# Sidebar: overlay indicators (on main candlestick chart)
st.sidebar.subheader("Overlay on price")
show_ema20 = st.sidebar.checkbox("EMA 20", True)
show_ema50 = st.sidebar.checkbox("EMA 50", True)
show_ema200 = st.sidebar.checkbox("EMA 200", False)
show_bb = st.sidebar.checkbox("Bollinger Bands", True)

# Sidebar: subplot indicators (panels below)
st.sidebar.subheader("Subplots below")
show_rsi = st.sidebar.checkbox("RSI", True)
show_volume = st.sidebar.checkbox("Volume", True)
show_macd = st.sidebar.checkbox("MACD", True)

if not POSTGRES_DSN:
    st.error("POSTGRES_DSN env var is not set.")
    st.stop()

if use_date_range:
    df = load_candles(
        POSTGRES_DSN, market_type, symbol, interval,
        start_date=start_date, end_date=end_date, max_candles=MAX_CANDLES,
    )
else:
    df = load_candles(POSTGRES_DSN, market_type, symbol, interval, limit=limit)
if df.empty:
    st.warning("No data found for this range. Run the downloader or pick different dates.")
    st.stop()

feat = add_indicators(df).dropna().reset_index(drop=True)
feat["datetime"] = pd.to_datetime(feat["open_time"], utc=True)

# Show date range loaded
if len(feat) >= 2:
    start_dt = feat["datetime"].iloc[0]
    end_dt = feat["datetime"].iloc[-1]
    days_loaded = (end_dt - start_dt).total_seconds() / 86400
    st.sidebar.caption(f"Loaded: {start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')} ({days_loaded:.1f} days, {len(feat):,} candles)")

def label_to_int(x: str) -> int:
    return 1 if x == "BUY" else (-1 if x == "SELL" else 0)

# How many rows for subplots
rows = 1
if show_rsi: rows += 1
if show_volume: rows += 1
if show_macd: rows += 1

row_heights = [0.6] + [0.4 / max(1, rows - 1)] * (rows - 1) if rows > 1 else [1.0]
fig = make_subplots(
    rows=rows,
    cols=1,
    shared_xaxes=True,
    vertical_spacing=0.03,
    row_heights=row_heights,
    subplot_titles=(
        ["Price"] +
        (["RSI"] if show_rsi else []) +
        (["Volume"] if show_volume else []) +
        (["MACD"] if show_macd else [])
    ),
)

# Row 1: candlestick + overlays
r = 1
fig.add_trace(
    go.Candlestick(
        x=feat["datetime"],
        open=feat["open"],
        high=feat["high"],
        low=feat["low"],
        close=feat["close"],
        name="OHLC",
    ),
    row=r,
    col=1,
)
if show_ema20 and "ema_20" in feat.columns:
    fig.add_trace(go.Scatter(x=feat["datetime"], y=feat["ema_20"], name="EMA 20", line=dict(color="blue", width=1)), row=r, col=1)
if show_ema50 and "ema_50" in feat.columns:
    fig.add_trace(go.Scatter(x=feat["datetime"], y=feat["ema_50"], name="EMA 50", line=dict(color="orange", width=1)), row=r, col=1)
if show_ema200 and "ema_200" in feat.columns:
    fig.add_trace(go.Scatter(x=feat["datetime"], y=feat["ema_200"], name="EMA 200", line=dict(color="purple", width=1)), row=r, col=1)
if show_bb and "bb_upper" in feat.columns:
    fig.add_trace(go.Scatter(x=feat["datetime"], y=feat["bb_upper"], name="BB upper", line=dict(color="gray", width=1, dash="dash")), row=r, col=1)
if show_bb and "bb_lower" in feat.columns:
    fig.add_trace(go.Scatter(x=feat["datetime"], y=feat["bb_lower"], name="BB lower", line=dict(color="gray", width=1, dash="dash")), row=r, col=1)
if show_bb and "bb_ma20" in feat.columns:
    fig.add_trace(go.Scatter(x=feat["datetime"], y=feat["bb_ma20"], name="BB mid", line=dict(color="gray", width=1)), row=r, col=1)

# Subplots: RSI, Volume, MACD
colors = (feat["close"] >= feat["open"]).map({True: "#26a69a", False: "#ef5350"})
if show_rsi:
    r += 1
    fig.add_trace(
        go.Scatter(x=feat["datetime"], y=feat["rsi_14"], name="RSI", line=dict(color="blue", width=1.5)),
        row=r,
        col=1,
    )
    fig.add_hline(y=70, line_dash="dash", line_color="red", opacity=0.7, row=r, col=1)
    fig.add_hline(y=30, line_dash="dash", line_color="green", opacity=0.7, row=r, col=1)
    fig.update_yaxes(title_text="RSI", range=[0, 100], row=r, col=1)
if show_volume:
    r += 1
    fig.add_trace(
        go.Bar(
            x=feat["datetime"],
            y=feat["volume"],
            name="Volume",
            marker_color=colors,
            showlegend=False,
        ),
        row=r,
        col=1,
    )
    fig.update_yaxes(title_text="Volume", row=r, col=1)
if show_macd:
    r += 1
    macd_colors = (feat["macd_hist"] >= 0).map({True: "#26a69a", False: "#ef5350"})
    fig.add_trace(
        go.Scatter(x=feat["datetime"], y=feat["macd"], name="MACD", line=dict(color="blue", width=1)),
        row=r,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=feat["datetime"], y=feat["macd_signal"], name="Signal", line=dict(color="orange", width=1)),
        row=r,
        col=1,
    )
    fig.add_trace(
        go.Bar(x=feat["datetime"], y=feat["macd_hist"], name="Hist", marker_color=macd_colors, showlegend=False),
        row=r,
        col=1,
    )
    fig.update_yaxes(title_text="MACD", row=r, col=1)

fig.update_layout(
    height=250 + 180 * rows,
    xaxis_rangeslider_visible=False,
    template="plotly_white",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)
fig.update_xaxes(rangeslider_visible=False)

# Zoom and pan: use st.plotly_chart so zoom works (plotly_events would block it)
st.subheader("Chart (drag to zoom, double-click to reset)")
st.plotly_chart(
    fig,
    use_container_width=True,
    config={
        "scrollZoom": True,
        "displayModeBar": True,
        "modeBarButtonsToAdd": ["zoomIn2d", "zoomOut2d", "autoScale2d", "resetScale2d"],
    },
)

# Candle selection and labeling
st.subheader("Select candle to label")
st.caption("**How to label:** 1) Move the slider to the candle you want (exact timestamp below). 2) Choose BUY / SELL / HOLD. 3) Click **Save label to Postgres** — nothing is saved until you press that button.")
n = len(feat)
candle_idx = st.slider("Candle index (0 = oldest, drag or use arrows)", 0, max(0, n - 1), min(n // 2, n - 1) if n else 0, key="candle_idx")
clicked_row = None
if n > 0:
    clicked_row = feat.iloc[candle_idx].to_dict()
    sel_dt = pd.to_datetime(clicked_row["open_time"], utc=True)
    st.info(f"**Selected candle (this timestamp will be labeled):** {sel_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC — O {clicked_row['open']:.2f}  H {clicked_row['high']:.2f}  L {clicked_row['low']:.2f}  C {clicked_row['close']:.2f}")

label = st.radio("Label", ["BUY", "SELL", "HOLD"], horizontal=True)
note = st.text_input("Note (optional)", "")

# Draw SL/TP for this candle if BUY/SELL
if clicked_row and label in ("BUY", "SELL"):
    entry = float(clicked_row["close"])
    atr = float(clicked_row["atr_14"])
    if label == "BUY":
        sl, tp = entry - 1.0 * atr, entry + 1.5 * atr
    else:
        sl, tp = entry + 1.0 * atr, entry - 1.5 * atr
    st.info(f"**Entry** {entry:.2f} · **SL** {sl:.2f} (1×ATR) · **TP** {tp:.2f} (1.5×ATR)")

if clicked_row and st.button("Save label to Postgres"):
    open_time_val = clicked_row["open_time"]
    if hasattr(open_time_val, "to_pydatetime"):
        open_time_val = open_time_val.to_pydatetime()
    with psycopg.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO market.trade_labels (market_type, symbol, interval, open_time, label, note)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (market_type, symbol, interval, open_time)
                DO UPDATE SET label=EXCLUDED.label, note=EXCLUDED.note, created_at=now()
                """,
                (market_type, symbol, interval, open_time_val, label_to_int(label), note or None),
            )
        conn.commit()
    st.toast("Saved!", icon="✅")

if clicked_row:
    st.write("Row preview (OHLC + indicators):")
    preview = pd.DataFrame([clicked_row]).copy()
    preview["datetime"] = pd.to_datetime(preview["open_time"], utc=True)
    st.dataframe(preview)

# Saved labels
st.subheader("Saved labels")
with psycopg.connect(POSTGRES_DSN) as conn:
    labels = pd.read_sql(
        """
        SELECT open_time, label, note, created_at
        FROM market.trade_labels
        WHERE market_type=%s AND symbol=%s AND interval=%s
        ORDER BY open_time DESC
        LIMIT 200
        """,
        conn,
        params=[market_type, symbol, interval],
    )
if not labels.empty:
    labels["time"] = pd.to_datetime(labels["open_time"], utc=True)
    st.dataframe(labels[["time", "label", "note", "created_at"]])
else:
    st.info("No labels yet.")
