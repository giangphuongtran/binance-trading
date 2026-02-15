import logging
import os
from typing import Optional

_LOGGING_INITIALIZED = False

def setup_logging(level: Optional[str] = None) -> None:
    """Initialize the logging module."""
    global _LOGGING_INITIALIZED
    if _LOGGING_INITIALIZED:
        return

    lvl = (level or os.getenv("LOGGING_LEVEL", "INFO") or os.getenv("AIRFLOW__CORE__LOGGING_LEVEL") or "INFO").upper()

    logging.basicConfig(
        level=getattr(logging, lvl, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    _LOGGING_INITIALIZED = True

def get_logger(name: str) -> logging.Logger:
    """Return a configured logger."""
    setup_logging()
    return logging.getLogger(name)