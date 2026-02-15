CREATE SCHEMA IF NOT EXISTS market;

-- ---------------------------------------------------------------------------
-- Spot / generic ingestion (Binance REST + WebSocket)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.candles_raw (
  exchange        text        NOT NULL,
  symbol          text        NOT NULL,
  interval        text        NOT NULL,
  open_time       timestamptz NOT NULL,
  close_time      timestamptz,
  open            double precision NOT NULL,
  high            double precision NOT NULL,
  low             double precision NOT NULL,
  close           double precision NOT NULL,
  volume          double precision NOT NULL,
  is_final        boolean     NOT NULL DEFAULT true,
  ingested_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (exchange, symbol, interval, open_time)
);

CREATE TABLE IF NOT EXISTS market.api_metadata (
  exchange                    text NOT NULL,
  symbol                      text NOT NULL,
  interval                    text NOT NULL,
  last_final_candle_open_time    timestamptz,
  last_websocket_seen_at         timestamptz,
  status                        text NOT NULL DEFAULT 'ok',
  updated_at                     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (exchange, symbol, interval)
);

CREATE TABLE IF NOT EXISTS market.data_quality_issues (
  id              bigserial   PRIMARY KEY,
  exchange        text        NOT NULL,
  symbol          text        NOT NULL,
  interval        text        NOT NULL,
  open_time       timestamptz,
  issue_type      text        NOT NULL,
  details_json    jsonb,
  created_at      timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Futures candles (Binance bulk data)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market.futures_candles (
  market_type    text        NOT NULL,  -- 'um' or 'cm'
  symbol         text        NOT NULL,
  interval       text        NOT NULL,  -- '1m','5m','15m','30m',...
  open_time      timestamptz NOT NULL,
  open           double precision NOT NULL,
  high           double precision NOT NULL,
  low            double precision NOT NULL,
  close          double precision NOT NULL,
  volume         double precision NOT NULL,
  close_time     timestamptz,
  quote_volume   double precision,
  num_trades     bigint,
  taker_buy_base double precision,
  taker_buy_quote double precision,
  ingested_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (market_type, symbol, interval, open_time)
);

-- Metadata for how far we’ve backfilled
CREATE TABLE IF NOT EXISTS market.futures_ingestion_metadata (
  market_type   text NOT NULL,
  symbol        text NOT NULL,
  interval      text NOT NULL,
  last_open_time  timestamptz,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (market_type, symbol, interval)
);

-- Your manual labels (buy/sell/hold)
CREATE TABLE IF NOT EXISTS market.trade_labels (
  market_type   text NOT NULL,
  symbol        text NOT NULL,
  interval      text NOT NULL,
  open_time     timestamptz NOT NULL,
  label         smallint NOT NULL,  -- 1=buy, -1=sell, 0=hold
  note          text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (market_type, symbol, interval, open_time)
);
