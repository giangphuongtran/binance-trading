import numpy as np
import pandas as pd

def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    roll_up = up.ewm(alpha=1/period, adjust=False).mean()
    roll_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([(high-low), (high-prev_close).abs(), (low-prev_close).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False).mean()

def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()

def macd(close: pd.Series, fast: int=12, slow: int=26, signal: int=9):
    macd_line = ema(close, fast) - ema(close, slow)
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def bollinger(close: pd.Series, window: int=20, n_std: float=2.0):
    ma = close.rolling(window).mean()
    sd = close.rolling(window).std(ddof=0)
    upper = ma + n_std * sd
    lower = ma - n_std * sd
    width = (upper - lower) / ma.replace(0, np.nan)
    return ma, upper, lower, width

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expects columns: open_time (datetime), open, high, low, close, volume.
    """
    out = df.copy()

    out["log_return"] = np.log(out["close"]).diff()
    out["ret_1"] = out["close"].pct_change(1)
    out["ret_5"] = out["close"].pct_change(5)

    out["ema_20"] = ema(out["close"], 20)
    out["ema_50"] = ema(out["close"], 50)
    out["ema_200"] = ema(out["close"], 200)

    out["rsi_14"] = rsi(out["close"], 14)
    out["atr_14"] = atr(out["high"], out["low"], out["close"], 14)

    m, s, h = macd(out["close"])
    out["macd"] = m
    out["macd_signal"] = s
    out["macd_hist"] = h

    bb_ma, bb_up, bb_low, bb_w = bollinger(out["close"])
    out["bb_ma20"] = bb_ma
    out["bb_upper"] = bb_up
    out["bb_lower"] = bb_low
    out["bb_width"] = bb_w

    # Volume z-score (simple anomaly feature)
    vol_mean = out["volume"].rolling(50).mean()
    vol_std = out["volume"].rolling(50).std(ddof=0)
    out["vol_z50"] = (out["volume"] - vol_mean) / vol_std.replace(0, np.nan)

    return out
