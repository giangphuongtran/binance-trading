import io
import sys
import zipfile
from pathlib import Path
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

# Project root on path so "pipelines" can be imported when run as script
_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import pandas as pd
import requests
import psycopg

from pipelines.common.settings import require, SYMBOLS, INTERVALS, POSTGRES_DSN

# Intervals to download for futures bulk data
INTERVALS = ["1m", "5m", "15m", "1h"]

# Choose futures market type:
#   'um' = USDT-M Futures
#   'cm' = COIN-M Futures
MARKET_TYPE = "um"

BASE = "https://data.binance.vision"


@dataclass(frozen=True)
class KlinePath:
    market_type: str
    symbol: str
    interval: str

    def monthly_url(self, yyyy_mm: str) -> str:
        # Example:
        # https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2020-01.zip
        return f"{BASE}/data/futures/{self.market_type}/monthly/klines/{self.symbol}/{self.interval}/{self.symbol}-{self.interval}-{yyyy_mm}.zip"

    def daily_url(self, yyyy_mm_dd: str) -> str:
        # Example:
        # https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2020-01-01.zip
        return f"{BASE}/data/futures/{self.market_type}/daily/klines/{self.symbol}/{self.interval}/{self.symbol}-{self.interval}-{yyyy_mm_dd}.zip"


def _http_get(url: str) -> Optional[bytes]:
    r = requests.get(url, timeout=60)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.content


def _read_zip_csv(zip_bytes: bytes) -> pd.DataFrame:
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))
    # usually only one CSV inside
    name = z.namelist()[0]
    with z.open(name) as f:
        df = pd.read_csv(
            f,
            header=None,
            names=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_volume",
                "num_trades",
                "taker_buy_base",
                "taker_buy_quote",
                "ignore",
            ],
        )
    return df


def _normalize_timestamps_to_ms(df: pd.DataFrame) -> pd.DataFrame:
    # Some newer datasets may be microseconds; normalize to ms.
    # If open_time looks like 16 digits (>= 10^15), treat as microseconds.
    # If it's 13 digits (~10^12-10^13), it's ms already.
    for col in ["open_time", "close_time"]:
        if col in df.columns:
            v = df[col].dropna()
            if not v.empty and int(v.iloc[0]) >= 10**15:
                df[col] = (df[col] // 1000).astype("int64")
            else:
                df[col] = df[col].astype("int64")
    return df


def upsert_klines(conn: psycopg.Connection, market_type: str, symbol: str, interval: str, df: pd.DataFrame) -> int:
    df = _normalize_timestamps_to_ms(df)
    # Convert ms to datetime (UTC) for storage
    df["open_time_dt"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time_dt"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    rows = df[
        [
            "open_time_dt",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time_dt",
            "quote_volume",
            "num_trades",
            "taker_buy_base",
            "taker_buy_quote",
        ]
    ].to_records(index=False)

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO market.futures_candles
            (market_type, symbol, interval, open_time, open, high, low, close, volume,
             close_time, quote_volume, num_trades, taker_buy_base, taker_buy_quote)
            VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (market_type, symbol, interval, open_time)
            DO UPDATE SET
              open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
              volume=EXCLUDED.volume, close_time=EXCLUDED.close_time,
              quote_volume=EXCLUDED.quote_volume, num_trades=EXCLUDED.num_trades,
              taker_buy_base=EXCLUDED.taker_buy_base, taker_buy_quote=EXCLUDED.taker_buy_quote,
              ingested_at=now()
            """,
            [
                (
                    market_type,
                    symbol,
                    interval,
                    r.open_time_dt.to_pydatetime(),
                    float(r.open),
                    float(r.high),
                    float(r.low),
                    float(r.close),
                    float(r.volume),
                    r.close_time_dt.to_pydatetime() if pd.notna(r.close_time_dt) else None,
                    float(r.quote_volume) if pd.notna(r.quote_volume) else None,
                    int(r.num_trades) if pd.notna(r.num_trades) else None,
                    float(r.taker_buy_base) if pd.notna(r.taker_buy_base) else None,
                    float(r.taker_buy_quote) if pd.notna(r.taker_buy_quote) else None,
                )
                for r in rows
            ],
        )

        # update metadata (last_open_time as timestamptz)
        last_open_dt = df["open_time_dt"].max().to_pydatetime()
        cur.execute(
            """
            INSERT INTO market.futures_ingestion_metadata (market_type, symbol, interval, last_open_time)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (market_type, symbol, interval)
            DO UPDATE SET last_open_time=EXCLUDED.last_open_time, updated_at=now()
            """,
            (market_type, symbol, interval, last_open_dt),
        )

    conn.commit()
    return len(df)


def download_range(
    market_type: str,
    symbol: str,
    interval: str,
    start: date,
    end: date,
    prefer_monthly: bool = False,
):
    """
    Downloads klines for [start, end] inclusive.
    Strategy:
      - try monthly zips (fast) then fill missing days with daily zips
    """
    kp = KlinePath(market_type, symbol, interval)

    with psycopg.connect(POSTGRES_DSN) as conn:
        if prefer_monthly:
            # monthly loop
            cur = date(start.year, start.month, 1)
            while cur <= end:
                yyyy_mm = f"{cur.year:04d}-{cur.month:02d}"
                url = kp.monthly_url(yyyy_mm)
                blob = _http_get(url)
                if blob:
                    df = _read_zip_csv(blob)
                    n = upsert_klines(conn, market_type, symbol, interval, df)
                    print(f"[MONTHLY] {symbol} {interval} {yyyy_mm}: +{n}")
                cur = (date(cur.year + (cur.month // 12), (cur.month % 12) + 1, 1))

        # daily loop for exact coverage / missing months
        d = start
        while d <= end:
            yyyy_mm_dd = f"{d.year:04d}-{d.month:02d}-{d.day:02d}"
            url = kp.daily_url(yyyy_mm_dd)
            blob = _http_get(url)
            if blob:
                df = _read_zip_csv(blob)
                n = upsert_klines(conn, market_type, symbol, interval, df)
                print(f"[DAILY] {symbol} {interval} {yyyy_mm_dd}: +{n}")
            d += timedelta(days=1)


if __name__ == "__main__":
    # Set a wide range; it will auto-skip missing files
    start = date(2019, 1, 1)
    end = date.today() - timedelta(days=1)

    for symbol in SYMBOLS:
        for itv in INTERVALS:
            print(f"\n=== Downloading {symbol} {MARKET_TYPE} {itv} ===")
            download_range(MARKET_TYPE, symbol, itv, start, end, prefer_monthly=False)
