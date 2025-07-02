"""Logging configuration."""

import structlog


def configure_logging():
    """Configure structured logging."""
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
    )
