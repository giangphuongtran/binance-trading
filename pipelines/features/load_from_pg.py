"""
Load candle data from PostgreSQL for feature computation and labeling UI.
Supports market.futures_candles (Binance futures bulk) and market.candles_raw (Spot REST/WS).
"""
from __future__ import annotations

import pandas as pd
import psycopg


def load_candles(
    dsn: str,
    market_type: str,
    symbol: str,
    interval: str,
    limit: int = 2000,
    *,
    table: str = "futures_candles",
) -> pd.DataFrame:
    """
    Load OHLCV candles from Postgres. Returns DataFrame with columns:
    open_time (datetime, UTC), open, high, low, close, volume (sorted by open_time).
    """
    if table == "futures_candles":
        sql = """
            SELECT open_time, open, high, low, close, volume
            FROM market.futures_candles
            WHERE market_type = %(market_type)s AND symbol = %(symbol)s AND interval = %(interval)s
            ORDER BY open_time DESC
            LIMIT %(limit)s
            """
        with psycopg.connect(dsn) as conn:
            df = pd.read_sql(
                sql,
                conn,
                params={"market_type": market_type, "symbol": symbol, "interval": interval, "limit": limit},
            )
    else:
        # market.candles_raw (exchange, symbol, interval)
        sql = """
            SELECT open_time, open, high, low, close, volume
            FROM market.candles_raw
            WHERE exchange = %(exchange)s AND symbol = %(symbol)s AND interval = %(interval)s
            ORDER BY open_time DESC
            LIMIT %(limit)s
            """
        with psycopg.connect(dsn) as conn:
            df = pd.read_sql(
                sql,
                conn,
                params={"exchange": market_type, "symbol": symbol, "interval": interval, "limit": limit},
            )

    if df.empty:
        return df
    # Ensure open_time is timezone-aware UTC
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    # Ascending order for indicators (oldest first)
    df = df.sort_values("open_time").reset_index(drop=True)
    return df
