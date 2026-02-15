import json
from contextlib import contextmanager
import psycopg

from pipelines.common.settings import require

def pg_dsn() -> str:
    return require("POSTGRES_DSN")

@contextmanager
def get_conn():
    with psycopg.connect(pg_dsn()) as conn:
        yield conn

def upsert_candle(conn, row: dict) -> None:
    sql = """
    INSERT INTO market.candles_raw
    (exchange, symbol, interval, open_time, close_time, open, high, low, close, volume, is_final)
    VALUES
    (%(exchange)s, %(symbol)s, %(interval)s, %(open_time)s, %(close_time)s,
     %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(is_final)s)
    ON CONFLICT (exchange, symbol, interval, open_time)
    DO UPDATE SET
      close_time = EXCLUDED.close_time,
      open = EXCLUDED.open,
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      close = EXCLUDED.close,
      volume = EXCLUDED.volume,
      is_final = EXCLUDED.is_final,
      ingested_at = now();
    """
    conn.execute(sql, row)

def touch_metadata(conn, exchange: str, symbol: str, interval: str, open_time=None, ws_seen: bool = False):
    sql = """
    INSERT INTO market.api_metadata (exchange, symbol, interval, last_final_candle_open_time, last_websocket_seen_at, status)
    VALUES (%s, %s, %s, %s, CASE WHEN %s THEN now() ELSE NULL END, 'ok')
    ON CONFLICT (exchange, symbol, interval)
    DO UPDATE SET
      last_final_candle_open_time = COALESCE(EXCLUDED.last_final_candle_open_time, market.api_metadata.last_final_candle_open_time),
      last_websocket_seen_at = CASE WHEN %s THEN now() ELSE market.api_metadata.last_websocket_seen_at END,
      updated_at = now();
    """
    conn.execute(sql, (exchange, symbol, interval, open_time, ws_seen, ws_seen))

def log_quality_issue(conn, exchange: str, symbol: str, interval: str, issue_type: str, open_time=None, details: dict | None = None):
    conn.execute(
        """
        INSERT INTO market.data_quality_issues (exchange, symbol, interval, open_time, issue_type, details_json)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (exchange, symbol, interval, open_time, issue_type, json.dumps(details or {})),
    )
