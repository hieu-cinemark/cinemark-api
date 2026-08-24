"""Liveness check + a debug route that deliberately raises AppError, so the
error-handling wiring in main.py can be smoke-tested with a single curl
instead of needing a real failing resource lookup somewhere."""

from __future__ import annotations

from fastapi import APIRouter

from app.core.errors import NotFoundError

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, bool]:
    return {"ok": True}


@router.get("/health/error-demo")
async def error_demo() -> None:
    raise NotFoundError("This is what a custom AppError looks like as a response.")
