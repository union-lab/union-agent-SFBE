"""HTTP 请求访问日志中间件

自动记录每个请求的 method / path / status / duration_ms / client_ip，
输出到 union.access logger，跟随全局日志格式（JSON 或 text）。
"""
from __future__ import annotations

import logging
import re
import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response

logger = logging.getLogger("union.access")

SKIP_PATHS = {"/health", "/openapi.json", "/docs", "/redoc", "/favicon.ico"}

SLOW_THRESHOLD_MS = 1000

_SCANNER_RE = re.compile(
    r"/\.env|/\.git|phpinfo|\.php$|/_profiler|/_environment|/wp-login|/wp-admin"
    r"|/xmlrpc|/actuator|/\.aws|/\.docker|/\.kube|/\.ssh|/\.bash",
    re.IGNORECASE,
)


class AccessLogMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path

        if path in SKIP_PATHS:
            return await call_next(request)

        if _SCANNER_RE.search(path):
            return PlainTextResponse("", status_code=403)

        start = time.perf_counter()
        client_ip = request.client.host if request.client else "-"
        method = request.method

        status = 500
        error_msg: str | None = None

        try:
            response = await call_next(request)
            status = response.status_code
            return response
        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            duration_ms = round((time.perf_counter() - start) * 1000, 1)

            extra: dict = {
                "method": method,
                "path": path,
                "status": status,
                "duration_ms": duration_ms,
                "client_ip": client_ip,
            }

            query = str(request.url.query)
            if query:
                extra["query"] = query

            if duration_ms >= SLOW_THRESHOLD_MS:
                extra["slow"] = True

            if error_msg:
                extra["error"] = error_msg

            log_level = logging.WARNING if status >= 400 else logging.INFO
            logger.log(log_level, f"{method} {path} {status}", extra=extra)
