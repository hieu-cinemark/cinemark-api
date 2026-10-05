"""Cho dashboard kiểm tra khoá truy cập ở màn hình đăng nhập (xem app/core/auth.py)."""

from __future__ import annotations

from fastapi import APIRouter, Request

from app.core.auth import key_matches, provided_key
from app.core.config import settings

router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/check")
async def auth_check(request: Request) -> dict[str, bool]:
    expected = settings.api_auth_key or ""
    if not expected:
        return {"auth_required": False, "valid": True}
    return {"auth_required": True, "valid": key_matches(expected, provided_key(request.scope))}
