"""FastAPI application factory."""
from __future__ import annotations

import logging
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError

from app.api import admin, dead_letters, feed
from app.api.errors import ApiError, api_error_handler, error_body
from app.api.routes import router
from app.logging_config import configure_logging, request_id_var
from app.metrics import HTTP_DURATION, HTTP_REQUESTS, QueueCollector, api_registry

logger = logging.getLogger(__name__)
access_logger = logging.getLogger("app.access")
# 64 KiB payload + 8 KiB metadata + envelope. Rejecting on Content-Length avoids
# reading a huge body; chunked bodies without it are still bounded by the
# per-field validators after parsing.
MAX_REQUEST_BYTES = 80 * 1024
UNINSTRUMENTED_PATHS = {"/metrics"}
# Probes run every few seconds: counted in metrics, kept out of the access log.
UNLOGGED_PATHS = {"/metrics", "/healthz", "/readyz"}
_queue_collector_registered = False


def route_template(request: Request) -> str:
    """The matched route's template, never the raw path: /v1/events/{event_id}
    is one series, while raw paths (or 404 scans) would be unbounded."""
    route = request.scope.get("route")
    return getattr(route, "path", None) or "unmatched"


def metrics_response() -> Response:
    global _queue_collector_registered
    registry = api_registry()
    if registry is not None:  # multiprocess: build per scrape, add the DB gauges once
        registry.register(QueueCollector())
    else:
        registry = REGISTRY
        if not _queue_collector_registered:
            REGISTRY.register(QueueCollector())
            _queue_collector_registered = True
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)
REQUEST_ID_HEADER = "X-Request-Id"


def create_app() -> FastAPI:
    configure_logging("api")
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
    async def observe(request: Request, call_next):
        # Registered last so it runs first: every response, errors included, gets a
        # request id, a metrics sample, and one structured access-log line.
        request.state.request_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid.uuid4())
        token = request_id_var.set(request.state.request_id)
        started = time.perf_counter()
        status = 500  # if call_next raises, the outer error handler answers 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers[REQUEST_ID_HEADER] = request.state.request_id
            return response
        finally:
            elapsed = time.perf_counter() - started
            route = route_template(request)
            if request.url.path not in UNINSTRUMENTED_PATHS:
                HTTP_REQUESTS.labels(request.method, route, str(status)).inc()
                HTTP_DURATION.labels(request.method, route).observe(elapsed)
            if request.url.path not in UNLOGGED_PATHS:
                access_logger.info(
                    "request",
                    extra={"method": request.method, "route": route, "status": status, "duration_ms": round(elapsed * 1000, 1), "tenant_id": getattr(request.state, "tenant_id", None)},
                )
            request_id_var.reset(token)

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        return metrics_response()

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
