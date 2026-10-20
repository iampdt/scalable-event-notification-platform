"""Consistent public error responses."""
from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse


class ApiError(Exception):
    def __init__(self, status_code: int, code: str, message: str, details: list[dict[str, Any]] | None = None, headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.headers = headers
        self.code = code
        self.message = message
        self.details = details


def error_body(code: str, message: str, request: Request, details: list[dict[str, Any]] | None = None) -> dict:
    request_id = getattr(request.state, "request_id", None) or request.headers.get("X-Request-Id", "")
    error: dict[str, Any] = {"code": code, "message": message, "request_id": request_id}
    if details:
        error["details"] = details
    return {"error": error}


async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=error_body(exc.code, exc.message, request, exc.details), headers=exc.headers)
