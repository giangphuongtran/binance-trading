import asyncio
import json
import websockets
from datetime import datetime, timezone

from pipelines.common.logging import get_logger
from pipelines.common.settings import BINANCE_WS_URL, SYMBOLS, INTERVALS
from pipelines.ingestion.db import get_conn, upsert_candle, touch_metadata

log = get_logger(__name__)

def stream_name(symbol: str, interval: str) -> str:
    # Binance expects lowercase in stream names
    return f"{symbol.lower()}@kline_{interval}"

async def listen_one(symbol: str, interval: str):
    url = f"{BINANCE_WS_URL}/{stream_name(symbol, interval)}"
    exchange = "binance"

    while True:
        try:
            log.info("WS connect: %s", url)
            async with websockets.connect(url, ping_interval=20, ping_timeout=60) as ws:
                async for msg in ws:
                    data = json.loads(msg)
                    k = data.get("k", {})
                    is_final = bool(k.get("x", False))
                    if not is_final:
                        continue  # only store closed candles

                    open_ms = int(k["t"])
                    close_ms = int(k["T"])
                    open_time = datetime.fromtimestamp(open_ms / 1000.0, tz=timezone.utc)
                    close_time = datetime.fromtimestamp(close_ms / 1000.0, tz=timezone.utc)

                    row = {
                        "exchange": exchange,
                        "symbol": symbol,
                        "interval": interval,
                        "open_time": open_time,
                        "close_time": close_time,
                        "open": float(k["o"]),
                        "high": float(k["h"]),
                        "low": float(k["l"]),
                        "close": float(k["c"]),
                        "volume": float(k["v"]),
                        "is_final": True,
                    }

                    with get_conn() as conn:
                        upsert_candle(conn, row)
                        touch_metadata(conn, exchange, symbol, interval, open_time=open_time, ws_seen=True)
                        conn.commit()

        except Exception as e:
            log.warning("WS error for %s %s: %s. Reconnecting soon...", symbol, interval, e)
            await asyncio.sleep(5)

async def main():
    tasks = []
    for s in SYMBOLS:
        for itv in INTERVALS:
            tasks.append(asyncio.create_task(listen_one(s, itv)))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())