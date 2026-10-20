"""FastAPI application factory."""
from __future__ import annotations

import logging
import uuid

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError

from app.api import admin, dead_letters, feed
from app.api.errors import ApiError, api_error_handler, error_body
from app.api.routes import router

# 64 KiB payload + 8 KiB metadata + envelope. Rejecting on Content-Length avoids
# reading a huge body; chunked bodies without it are still bounded by the
# per-field validators after parsing.
logger = logging.getLogger(__name__)
MAX_REQUEST_BYTES = 80 * 1024
REQUEST_ID_HEADER = "X-Request-Id"


def create_app() -> FastAPI:
    app = FastAPI(title="Notification Platform", version="1.1.0")
    app.add_exception_handler(ApiError, api_error_handler)

    @app.exception_handler(OperationalError)
    async def database_unavailable(request: Request, exc: OperationalError) -> JSONResponse:
        # The database is down or unreachable: tell clients it is temporary and
        # when to retry (with the same Idempotency-Key), instead of a bare 500.
        logger.error("database_unavailable path=%s error=%s", request.url.path, type(exc.orig).__name__ if exc.orig else exc)
        return JSONResponse(status_code=503, headers={"Retry-After": "5"}, content=error_body("service_unavailable", "The service is temporarily unavailable; retry with the same Idempotency-Key.", request))

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        # Keep the documented error envelope even for bugs; details go to logs only.
        if isinstance(exc, DBAPIError) and exc.connection_invalidated:
            return await database_unavailable(request, exc)
        logger.exception("unhandled_error path=%s", request.url.path)
        return JSONResponse(status_code=500, content=error_body("internal_error", "Unexpected error; retry only with the same Idempotency-Key.", request))

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        details = [{"field": ".".join(str(part) for part in item["loc"] if part != "body"), "issue": item["msg"]} for item in exc.errors()]
        return JSONResponse(status_code=400, content=error_body("validation_error", "One or more fields are invalid.", request, details))

    @app.middleware("http")
    async def request_size_limit(request: Request, call_next):
        content_length = request.headers.get("content-length")
        try:
            too_large = content_length is not None and int(content_length) > MAX_REQUEST_BYTES
        except ValueError:
            return JSONResponse(status_code=400, content=error_body("validation_error", "Invalid Content-Length header.", request))
        if too_large:
            return JSONResponse(status_code=413, content=error_body("payload_too_large", "Request body exceeds the 64 KiB payload limit.", request))
        return await call_next(request)

    @app.middleware("http")
    async def request_id(request: Request, call_next):
        # Registered last so it runs first: every response, errors included, carries an ID.
        request.state.request_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid.uuid4())
        response = await call_next(request)
        response.headers[REQUEST_ID_HEADER] = request.state.request_id
        return response

    @app.get("/healthz", include_in_schema=False)
    def healthz() -> dict:
        """Liveness: the process is up. Never checks dependencies, so a database
        outage does not make the orchestrator restart every healthy API process."""
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    def readyz() -> JSONResponse:
        """Readiness: can this instance serve traffic? PostgreSQL is required;
        Redis and Kafka are reported but optional (the app degrades without them)."""
        from app.cache import get_cache
        from app.db.session import engine

        checks: dict[str, str] = {}
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            checks["postgres"] = "ok"
        except Exception as exc:  # noqa: BLE001 - any failure means not ready
            checks["postgres"] = f"unavailable: {type(exc).__name__}"
        cache = get_cache()
        if cache.client is None:
            checks["redis"] = "disabled"
        else:
            checks["redis"] = "ok" if cache.call("ping", lambda r: r.ping()) else "degraded"
        ready = checks["postgres"] == "ok"
        return JSONResponse(status_code=200 if ready else 503, content={"status": "ready" if ready else "not_ready", "checks": checks})

    app.include_router(router)
    app.include_router(feed.router)
    app.include_router(admin.router)
    app.include_router(dead_letters.router)
    return app


app = create_app()
