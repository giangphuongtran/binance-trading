"""
Load candle data from PostgreSQL for feature computation and labeling UI.
Supports market.futures_candles (Binance futures bulk) and market.candles_raw (Spot REST/WS).
"""
from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import psycopg


def _normalize_open_time(dt) -> datetime:
    """Ensure we have a timezone-aware datetime in UTC for DB comparison."""
    if isinstance(dt, date) and not isinstance(dt, datetime):
        dt = datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=timezone.utc)
    elif getattr(dt, "tzinfo", None) is None:
        dt = pd.Timestamp(dt).tz_localize("UTC")
    return dt


def load_candles(
    dsn: str,
    market_type: str,
    symbol: str,
    interval: str,
    limit: int = 2000,
    *,
    table: str = "futures_candles",
    start_date: date | datetime | None = None,
    end_date: date | datetime | None = None,
    max_candles: int = 30_000,
) -> pd.DataFrame:
    """
    Load OHLCV candles from Postgres. Returns DataFrame with columns:
    open_time (datetime, UTC), open, high, low, close, volume (sorted by open_time).

    Either use limit (load latest N candles) or start_date/end_date (load range, capped at max_candles).
    """
    params = {"market_type": market_type, "symbol": symbol, "interval": interval}
    use_range = start_date is not None and end_date is not None
    if use_range:
        start_dt = _normalize_open_time(start_date)
        end_dt = _normalize_open_time(end_date)
        # Include full end day (so "end_date" includes all candles that day)
        if hasattr(end_dt, "date"):
            end_dt = datetime(
                end_dt.year, end_dt.month, end_dt.day, 23, 59, 59, 999_999, tzinfo=timezone.utc
            )
        params["start"] = start_dt
        params["end"] = end_dt
        params["max_candles"] = max_candles

    if table == "futures_candles":
        if use_range:
            sql = """
                SELECT open_time, open, high, low, close, volume
                FROM market.futures_candles
                WHERE market_type = %(market_type)s AND symbol = %(symbol)s AND interval = %(interval)s
                  AND open_time >= %(start)s AND open_time <= %(end)s
                ORDER BY open_time DESC
                LIMIT %(max_candles)s
                """
        else:
            sql = """
                SELECT open_time, open, high, low, close, volume
                FROM market.futures_candles
                WHERE market_type = %(market_type)s AND symbol = %(symbol)s AND interval = %(interval)s
                ORDER BY open_time DESC
                LIMIT %(limit)s
                """
            params["limit"] = limit
        with psycopg.connect(dsn) as conn:
            df = pd.read_sql(sql, conn, params=params)
    else:
        params["exchange"] = market_type
        if use_range:
            sql = """
                SELECT open_time, open, high, low, close, volume
                FROM market.candles_raw
                WHERE exchange = %(exchange)s AND symbol = %(symbol)s AND interval = %(interval)s
                  AND open_time >= %(start)s AND open_time <= %(end)s
                ORDER BY open_time DESC
                LIMIT %(max_candles)s
                """
        else:
            sql = """
                SELECT open_time, open, high, low, close, volume
                FROM market.candles_raw
                WHERE exchange = %(exchange)s AND symbol = %(symbol)s AND interval = %(interval)s
                ORDER BY open_time DESC
                LIMIT %(limit)s
                """
            params["exchange"] = market_type
            params["limit"] = limit
        with psycopg.connect(dsn) as conn:
            df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        return df
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    df = df.sort_values("open_time").reset_index(drop=True)
    return df
