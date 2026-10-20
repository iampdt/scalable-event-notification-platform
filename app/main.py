"""FastAPI application factory."""
from __future__ import annotations

import uuid

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api import admin, feed
from app.api.errors import ApiError, api_error_handler, error_body
from app.api.routes import router

# 64 KiB payload + 8 KiB metadata + envelope. Rejecting on Content-Length avoids
# reading a huge body; chunked bodies without it are still bounded by the
# per-field validators after parsing.
MAX_REQUEST_BYTES = 80 * 1024
REQUEST_ID_HEADER = "X-Request-Id"


def create_app() -> FastAPI:
    app = FastAPI(title="Notification Platform", version="1.1.0")
    app.add_exception_handler(ApiError, api_error_handler)

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
        return {"status": "ok"}

    app.include_router(router)
    app.include_router(feed.router)
    app.include_router(admin.router)
    return app


app = create_app()
