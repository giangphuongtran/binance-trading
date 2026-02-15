import os
from dotenv import load_dotenv
from pathlib import Path

from pipelines.common.exceptions import ConfigError

BASE_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = BASE_DIR / ".env"

if ENV_PATH.exists():
    load_dotenv(str(ENV_PATH))


def _get(name: str, default: str | None = None) -> str | None:
    val = os.getenv(name, default)
    if val is None or val == "":
        return None
    return val


def require(var_name: str) -> str:
    val = _get(var_name)
    if val is None:
        raise ConfigError(f"Missing environment variable: {var_name}")
    return val


def as_int(var_name: str, default: int | None = None) -> int:
    raw = _get(var_name)
    if raw is None:
        if default is None:
            raise ConfigError(f"Missing environment variable: {var_name}")
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise ConfigError(f"Cannot convert {var_name}='{raw}' to an integer") from e


def as_float(var_name: str, default: float | None = None) -> float:
    raw = _get(var_name)
    if raw is None:
        if default is None:
            raise ConfigError(f"Missing environment variable: {var_name}")
        return default
    try:
        return float(raw)
    except ValueError as e:
        raise ConfigError(f"Cannot convert {var_name}='{raw}' to a float") from e


def as_bool(var_name: str, default: bool = False) -> bool:
    raw = _get(var_name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def as_list(var_name: str, default=None, sep=",") -> list[str]:
    raw = _get(var_name)
    if raw is None:
        return default if default is not None else []
    return [x.strip() for x in raw.split(sep) if x.strip()]


# Exchange / mode
EXCHANGE = _get("EXCHANGE", "binance")
MODE = _get("MODE", "paper")

SYMBOLS = as_list("SYMBOLS", default=["BTCUSDT", "ETHUSDT"])
INTERVALS = as_list("INTERVALS", default=["1h"])

# Binance
BINANCE_USE_TESTNET = as_bool("BINANCE_USE_TESTNET", False)
BINANCE_API_KEY = _get("BINANCE_API_KEY")
BINANCE_API_SECRET = _get("BINANCE_API_SECRET")
BINANCE_BASE_URL = _get(
    "BINANCE_BASE_URL",
    "https://testnet.binance.vision" if BINANCE_USE_TESTNET else "https://api.binance.com",
)
BINANCE_WS_URL = _get(
    "BINANCE_WS_URL",
    "wss://testnet.binance.vision/ws" if BINANCE_USE_TESTNET else "wss://stream.binance.com:9443/ws",
)

POSTGRES_DSN = _get("POSTGRES_DSN")