"""Logging helpers with the project log format.

Every line carries an ISO 8601 UTC timestamp, the level, the component and
the identifiers of the operation when they are known.
"""

import logging
import time
from typing import Optional

_CONFIGURED = False


def _formatter() -> logging.Formatter:
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    formatter.converter = time.gmtime
    return formatter


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
