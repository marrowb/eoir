"""Custom log handlers."""

import logging
from pathlib import Path
from eoir.settings import LOG_DIR


def setup_file_logging():
    """Setup file logging."""
    LOG_DIR.mkdir(exist_ok=True)
    handler = logging.FileHandler(LOG_DIR / "eoir.log")
    return handler
