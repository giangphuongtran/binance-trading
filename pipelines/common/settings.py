import os
from dataclasses import dataclass
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

def as_int(var_name: str, default=None) -> int:
    raw = _get(var_name)
    if raw is None:
        if default is None:
            raise ConfigError(f"Missing environment variable: {var_name}")
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise ConfigError(f"Cannot convert {var_name}='{raw}' to an integer") from e

def as_float(var_name: str, default=None) -> float:
    raw = _get(var_name)
    if raw is None:
        if default is None:
            raise ConfigError(f"Missing environment variable: {var_name}")
        return default
    try:
        return float(raw)
    except ValueError as e:
        raise ConfigError(f"Cannot convert {var_name}='{raw}' to a float") from e

@dataclass(frozen=True)
class Topics:
    BARS_RAW: str = "polygon.bars.raw"
    NEWS_RAW: str = "polygon.news.raw"
    BARS_FILTERED: str = "market.bars.filtered"
    NEWS_FILTERED: str = "market.news.filtered"
    METRICS: str = "market.metrics"
    ALERTS: str = "market.alerts"

# Polygon API key
POLYGON_API_KEY=require("POLYGON_API_KEY")
POLYGON_BASE_URL=_get("POLYGON_BASE_URL", "")

# Kafka
KAFKA_BOOTSTRAP_SERVER=_get("KAFKA_BOOTSTRAP_SERVER", "kafka:9092")
KAFKA_SECURITY_PROTOCOL=_get("KAFKA_SECURITY_PROTOCOL","PLAINTEXT")
TOPICS=Topics()

# Snowflake
SNOWFLAKE_USER=require("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD=require("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT=require("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE=require("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE=require("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA_RAW=require("SNOWFLAKE_SCHEMA_RAW")
SNOWFLAKE_ROLE=_get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

# Alerts (typed + defaults)
ALERT_VOLUME_SPIKE_MULTIPLIER = as_float("ALERT_VOLUME_SPIKE_MULTIPLIER", 3.0)
ALERT_VOLUME_WINDOW = as_int("ALERT_VOLUME_WINDOW", 20)
ALERT_VOLATILITY_WINDOW = as_int("ALERT_VOLATILITY_WINDOW", 20)
ALERT_NEWS_WATCHLIST=[]
