"""FastAPI application factory."""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api.errors import ApiError, api_error_handler, error_body
from app.api.routes import router

MAX_REQUEST_BYTES = 80 * 1024


def create_app() -> FastAPI:
    app = FastAPI(title="Notification Platform", version="0.1.0")
    app.add_exception_handler(ApiError, api_error_handler)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        details = [{"field": ".".join(str(part) for part in item["loc"] if part != "body"), "issue": item["msg"]} for item in exc.errors()]
        return JSONResponse(status_code=400, content=error_body("validation_error", "One or more fields are invalid.", request, details))

    @app.middleware("http")
    async def request_size_limit(request: Request, call_next):
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > MAX_REQUEST_BYTES:
            return JSONResponse(status_code=413, content=error_body("payload_too_large", "Request body exceeds the 64 KiB payload limit.", request))
        return await call_next(request)

    app.include_router(router)
    return app


app = create_app()
