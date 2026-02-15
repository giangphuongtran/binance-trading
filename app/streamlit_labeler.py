import psycopg
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from streamlit_plotly_events import plotly_events

from pipelines.features.load_from_pg import load_candles
from pipelines.features.indicators import add_indicators
from pipelines.common.settings import require, POSTGRES_DSN

st.set_page_config(layout="wide")
st.title("Binance Futures Candles — Explore & Label")

# Sidebar controls
market_type = st.sidebar.selectbox("Market type", ["um", "cm"], index=0 if require("MARKET_TYPE") == "um" else 1)
symbol = st.sidebar.text_input("Symbol", require("SYMBOL"))
interval = st.sidebar.selectbox("Interval", ["1m", "5m", "15m", "30m", "1h"], index=0)
limit = st.sidebar.slider("Candles to load", 200, 5000, 1500, step=100)

if not POSTGRES_DSN:
    st.error("POSTGRES_DSN env var is not set.")
    st.stop()

# Load + indicators
df = load_candles(POSTGRES_DSN, market_type, symbol, interval, limit=limit)
if df.empty:
    st.warning("No data found. Run the downloader first.")
    st.stop()

feat = add_indicators(df).dropna().reset_index(drop=True)
# Use open_time (datetime from DB) for chart and matching
feat["datetime"] = pd.to_datetime(feat["open_time"], utc=True)

def label_to_int(x: str) -> int:
    return 1 if x == "BUY" else (-1 if x == "SELL" else 0)

# Radio and note above chart so we can use label when drawing SL/TP
label = st.radio("Label", ["BUY", "SELL", "HOLD"], horizontal=True)
note = st.text_input("Note (optional)", "")

# Build candlestick
fig = go.Figure(
    data=[
        go.Candlestick(
            x=feat["datetime"],
            open=feat["open"],
            high=feat["high"],
            low=feat["low"],
            close=feat["close"],
            name="candles",
        )
    ]
)
fig.update_layout(height=650, xaxis_rangeslider_visible=False)

# Persist last click + label so we can draw SL/TP on next run
if "last_clicked_open_time" not in st.session_state:
    st.session_state.last_clicked_open_time = None
if "last_label" not in st.session_state:
    st.session_state.last_label = None

# Draw SL/TP on chart when we have a stored BUY/SELL click (before showing chart)
last_open_time = st.session_state.last_clicked_open_time
last_lbl = st.session_state.last_label
if last_open_time is not None and last_lbl in ("BUY", "SELL"):
    # Match by datetime (normalize for comparison)
    last_ts = pd.Timestamp(last_open_time).tz_localize("UTC") if getattr(last_open_time, "tzinfo", None) is None else pd.Timestamp(last_open_time)
    feat_utc = pd.to_datetime(feat["open_time"], utc=True)
    idx = (feat_utc - last_ts).abs().idxmin()
    match = feat.loc[[idx]]
    if not match.empty:
        row = match.iloc[0]
        entry = float(row["close"])
        atr = float(row["atr_14"])
        if last_lbl == "BUY":
            sl = entry - 1.0 * atr
            tp = entry + 1.5 * atr
        else:
            sl = entry + 1.0 * atr
            tp = entry - 1.5 * atr
        entry_dt = row["datetime"]
        fig.add_hline(y=entry, line_dash="dot", line_color="blue", annotation_text="Entry")
        fig.add_hline(y=sl, line_dash="dash", line_color="red", annotation_text="SL (1×ATR)")
        fig.add_hline(y=tp, line_dash="dash", line_color="green", annotation_text="TP (1.5×ATR)")
        fig.add_vline(x=entry_dt, line_dash="dot", line_color="gray", opacity=0.7)

# Show chart and capture click
st.subheader("Click a candle to label it (BUY / SELL / HOLD)")
selected_points = plotly_events(fig, click_event=True, hover_event=False, select_event=False)

# Resolve clicked row and update session state
clicked_row = None
if selected_points:
    ts = selected_points[0].get("x")
    if ts:
        click_ts = pd.Timestamp(ts).tz_localize("UTC") if pd.Timestamp(ts).tzinfo is None else pd.Timestamp(ts)
        feat_utc = pd.to_datetime(feat["open_time"], utc=True)
        idx = (feat_utc - click_ts).abs().idxmin()
        clicked_row = feat.loc[idx].to_dict()
        st.session_state.last_clicked_open_time = clicked_row["open_time"]
        st.session_state.last_label = label

# Below chart: show selected candle and save
if clicked_row:
    sel_dt = pd.to_datetime(clicked_row["open_time"], utc=True)
    st.success(f"Selected candle: **{sel_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC**")
    preview = pd.DataFrame([clicked_row]).copy()
    preview["datetime"] = pd.to_datetime(preview["open_time"], utc=True)
    st.write("Row preview (OHLC + indicators):")
    st.dataframe(preview)

    if st.button("Save label to Postgres"):
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

# Show existing labels (open_time is already datetime in DB)
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
