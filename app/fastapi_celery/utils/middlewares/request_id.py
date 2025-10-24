from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from typing import Callable, Awaitable
import uuid


class RequestIDMiddleware(BaseHTTPMiddleware):
    """
    Middleware to assign a unique UUID as a request ID for each incoming HTTP request.

    The request ID is added to the response headers as `X-Request-ID` to facilitate
    tracing and correlation of logs and requests across distributed systems.

    Usage:
        Add this middleware to your FastAPI or Starlette application to enable
        consistent request tracing.

    Example:
        app.add_middleware(RequestIDMiddleware)
    """

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response
