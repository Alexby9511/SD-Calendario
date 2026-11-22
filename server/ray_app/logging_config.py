"""Logging helpers with the project log format.

Every line carries an ISO 8601 UTC timestamp, the level, the component and
the identifiers of the operation when they are known.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

_CONFIGURED = False


class UTCFormatter(logging.Formatter):
    def formatTime(
        self, record: logging.LogRecord, datefmt: Optional[str] = None
    ) -> str:
        timestamp = datetime.fromtimestamp(record.created, timezone.utc)
        if datefmt:
            return timestamp.strftime(datefmt)
        return timestamp.isoformat()


def _formatter() -> logging.Formatter:
    return UTCFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


def get_logger(component: str, level: Optional[int] = None) -> logging.Logger:
    """Return a logger that emits the project log format."""
    global _CONFIGURED
    logger = logging.getLogger(f"sdcal.{component}")
    if not _CONFIGURED:
        handler = logging.StreamHandler()
        handler.setFormatter(_formatter())
        root = logging.getLogger("sdcal")
        if not root.handlers:
            root.addHandler(handler)
            root.setLevel(logging.INFO)
        _CONFIGURED = True
    if level is not None:
        logger.setLevel(level)
    return logger
