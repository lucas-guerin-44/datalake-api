"""
Structured logging configuration for datalake-api.

Provides JSON-formatted logging for container/cloud compatibility with:
- Structured JSON output
- Correlation IDs for request tracing
- Configurable log levels via environment variable
- Performance timing
"""
import logging
import os
import sys
from contextvars import ContextVar
from typing import Any, Dict
from pythonjsonlogger import jsonlogger

# Context variable for correlation ID (thread-safe)
correlation_id_var: ContextVar[str] = ContextVar('correlation_id', default=None)


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """
    Custom JSON formatter that adds correlation_id and other context.
    """
    def add_fields(self, log_record: Dict[str, Any], record: logging.LogRecord, message_dict: Dict[str, Any]) -> None:
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)

        # Add standard fields
        log_record['timestamp'] = self.formatTime(record, self.datefmt)
        log_record['level'] = record.levelname
        log_record['logger'] = record.name
        log_record['module'] = record.module
        log_record['function'] = record.funcName
        log_record['line'] = record.lineno

        # Add correlation ID if available
        correlation_id = correlation_id_var.get()
        if correlation_id:
            log_record['correlation_id'] = correlation_id

        # Add exception info if present
        if record.exc_info:
            log_record['exc_info'] = self.formatException(record.exc_info)


def setup_logging(log_level: str = None) -> None:
    """
    Configure structured logging for the application.

    Args:
        log_level: Optional override for log level. If not provided, reads from LOG_LEVEL env var.
                   Defaults to INFO if not set.
    """
    # Determine log level
    if log_level is None:
        log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

    # Validate log level
    numeric_level = getattr(logging, log_level, None)
    if not isinstance(numeric_level, int):
        print(f'Invalid log level: {log_level}, defaulting to INFO', file=sys.stderr)
        numeric_level = logging.INFO

    # Create JSON formatter
    formatter = CustomJsonFormatter(
        '%(timestamp)s %(level)s %(name)s %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S%z'
    )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add console handler with JSON formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Set log levels for third-party libraries to reduce noise
    logging.getLogger('uvicorn').setLevel(logging.WARNING)
    logging.getLogger('uvicorn.access').setLevel(logging.WARNING)
    logging.getLogger('fastapi').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

    # Log that logging is configured
    logger = logging.getLogger(__name__)
    logger.info('Logging configured', extra={
        'log_level': log_level,
        'format': 'json'
    })


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a module.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def set_correlation_id(correlation_id: str) -> None:
    """
    Set the correlation ID for the current context.

    Args:
        correlation_id: Unique identifier for the request/operation
    """
    correlation_id_var.set(correlation_id)


def get_correlation_id() -> str:
    """
    Get the correlation ID for the current context.

    Returns:
        Correlation ID or None if not set
    """
    return correlation_id_var.get()


def clear_correlation_id() -> None:
    """
    Clear the correlation ID for the current context.
    """
    correlation_id_var.set(None)
