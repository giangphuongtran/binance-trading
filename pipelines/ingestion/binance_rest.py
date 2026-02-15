import time
import requests
from datetime import datetime, timezone

from pipelines.common.logging import get_logger
from pipelines.common.settings import BINANCE_BASE_URL, SYMBOLS, INTERVALS
from pipelines.ingestion.db import get_conn, upsert_candle, touch_metadata
from pipelines.ingestion.intervals import interval_to_ms

log = get_logger(__name__)

def fetch_klines(symbol: str, interval: str, start_ms: int | None, limit: int = 1000):
    url = f"{BINANCE_BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ms is not None:
        params["startTime"] = start_ms
    r = requests.get(url, params=params, timeout=30)
    if r.status_code == 429:
        # rate limit: back off
        time.sleep(2)
        r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def backfill_symbol_interval(symbol: str, interval: str, start_ms: int, end_ms: int | None = None):
    step = interval_to_ms(interval)
    cur = start_ms
    exchange = "binance"

    with get_conn() as conn:
        while True:
            klines = fetch_klines(symbol, interval, cur, limit=1000)
            if not klines:
                break

            last_open = None
            for k in klines:
                open_ms = int(k[0])
                close_ms = int(k[6])
                last_open = open_ms
                open_time = datetime.fromtimestamp(open_ms / 1000.0, tz=timezone.utc)
                close_time = datetime.fromtimestamp(close_ms / 1000.0, tz=timezone.utc)
                row = {
                    "exchange": exchange,
                    "symbol": symbol,
                    "interval": interval,
                    "open_time": open_time,
                    "close_time": close_time,
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                    "is_final": True,
                }
                upsert_candle(conn, row)
                touch_metadata(conn, exchange, symbol, interval, open_time=open_time, ws_seen=False)

            conn.commit()

            # Move forward: next candle after the last returned open_time
            if last_open is None:
                break
            cur = last_open + step

            if end_ms is not None and cur >= end_ms:
                break

            # small throttle to be nice to rate limits
            time.sleep(0.2)

def main():
    # Example: backfill last N days (simple approach).
    # Use your own start_ms as needed.
    now_ms = int(time.time() * 1000)
    days = 90
    start_ms = now_ms - days * 86_400_000

    for symbol in SYMBOLS:
        for interval in INTERVALS:
            log.info("Backfilling %s %s from %s", symbol, interval, start_ms)
            backfill_symbol_interval(symbol, interval, start_ms=start_ms)

if __name__ == "__main__":
    main()