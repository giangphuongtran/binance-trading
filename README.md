# Binance → Postgres → Streamlit Labeler

Pipeline: download Binance futures (or Spot) data into Postgres, compute indicators, and label BUY/SELL/HOLD in a Streamlit UI.

---

## What’s in the repo

- **scripts/download_btcusdt_futures_klines.py** — Bulk download Binance futures klines (1m, 5m, 15m, 1h) into `market.futures_candles`.
- **pipelines/ingestion/** — Binance Spot REST backfill and WebSocket live feed → `market.candles_raw`.
- **pipelines/features/** — Load candles from Postgres, add technical indicators (RSI, ATR, MACD, Bollinger, etc.).
- **app/streamlit_labeler.py** — Load candles + indicators, click candles to label BUY/SELL/HOLD with optional SL/TP (1× and 1.5× ATR); labels stored in `market.trade_labels`.
- **sql/001_create_market_tables.sql** — Schema for candles, metadata, and labels (all use `open_time` / `close_time` as `timestamptz`).

---

## Config

Set in `.env` (or export):

- **POSTGRES_DSN** — e.g. `postgresql://user:pass@localhost:5432/trading`
- **SYMBOLS** — e.g. `BTCUSDT,ETHUSDT` (used by download script and Spot ingestion)
- **INTERVALS** — e.g. `1h` (Spot only; futures script uses 1m, 5m, 15m, 1h)
- **MARKET_TYPE** — `um` or `cm` (for Streamlit / futures)
- **SYMBOL** — Default symbol for Streamlit (e.g. `BTCUSDT`)
- **BINANCE_USE_TESTNET** — `false` for production

---

## Getting started

1. **Postgres** (e.g. via Docker):
   ```bash
   make up
   # Set POSTGRES_DSN in .env to match (user, pass, db from docker-compose)
   ```

2. **Schema** (once):
   ```bash
   make schema
   # or: psql "$POSTGRES_DSN" -f sql/001_create_market_tables.sql
   ```

3. **Futures data:**
   ```bash
   make download
   ```

4. **Labeling UI:**
   ```bash
   make streamlit
   # or: streamlit run app/streamlit_labeler.py
   ```

Optional: Spot backfill `python -m pipelines.ingestion.binance_rest`; live Spot candles `python -m pipelines.ingestion.binance_ws`.
