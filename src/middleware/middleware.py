"""
FastAPI middleware for request logging with correlation IDs.
"""
import time
import uuid
from typing import Callable
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from src.middleware.logging_config import get_logger, set_correlation_id, clear_correlation_id

logger = get_logger(__name__)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware that logs all incoming requests with timing and correlation IDs.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Generate correlation ID
        correlation_id = str(uuid.uuid4())
        set_correlation_id(correlation_id)

        # Start timing
        start_time = time.time()

        # Extract request info
        method = request.method
        path = request.url.path
        query_params = dict(request.query_params)
        client_host = request.client.host if request.client else None

        # Log incoming request
        logger.info(
            'Incoming request',
            extra={
                'method': method,
                'path': path,
                'query_params': query_params,
                'client_host': client_host,
                'correlation_id': correlation_id
            }
        )

        try:
            # Process request
            response = await call_next(request)

            # Calculate duration
            duration_ms = (time.time() - start_time) * 1000

            # Log response
            logger.info(
                'Request completed',
                extra={
                    'method': method,
                    'path': path,
                    'status_code': response.status_code,
                    'duration_ms': round(duration_ms, 2),
                    'correlation_id': correlation_id
                }
            )

            # Add correlation ID to response headers
            response.headers['X-Correlation-ID'] = correlation_id

            return response

        except Exception as e:
            # Calculate duration even on error
            duration_ms = (time.time() - start_time) * 1000

            # Log error
            logger.error(
                'Request failed',
                extra={
                    'method': method,
                    'path': path,
                    'duration_ms': round(duration_ms, 2),
                    'error': str(e),
                    'correlation_id': correlation_id
                },
                exc_info=True
            )

            # Re-raise the exception to be handled by FastAPI
            raise

        finally:
            # Clean up correlation ID
            clear_correlation_id()
