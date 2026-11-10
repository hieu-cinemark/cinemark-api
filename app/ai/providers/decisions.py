"""Model quyết định của Bee (`POST {base_url}/decisions`): đọc một đoạn văn bản và trả lời các câu hỏi lựa chọn kèm xác
suất.

Chỉ dùng cho ca khó, sau luật và model tại chỗ. Bình luận dùng `COMMENT_MODEL` (Jev), bài viết dùng `POST_MODEL`.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from app.ai.providers.client import load_provider
from app.core.config import settings
from app.services.ops import ops_alerts

COMMENT_MODEL_DEFAULT = "typesafe/jev-1.14"
POST_MODEL_DEFAULT = "openai/gpt-6-luna-decisions"

# Tổng token kể từ lúc tiến trình chạy (log, script đo chi phí).
usage_totals = {"requests": 0, "input_tokens": 0, "output_tokens": 0}

# Provider không có /decisions (404): tạm đánh dấu trong chừng này giây, bên gọi dùng chat cho ca khó.
_UNSUPPORTED_SECONDS = 1800
_unsupported_until = 0.0


class DecisionsUnsupportedError(RuntimeError):
    pass


def decisions_supported() -> bool:
    return time.monotonic() >= _unsupported_until


def comment_model() -> str:
    """Model mặc định cho bình luận khi dòng nhà cung cấp không ghi comment_model."""
    return (settings.bee_comment_model or "").strip() or COMMENT_MODEL_DEFAULT


def post_model() -> str:
    """Model mặc định cho bài viết khi dòng nhà cung cấp không ghi post_model."""
    return (settings.bee_post_model or "").strip() or POST_MODEL_DEFAULT


COMMENT = "comment"
POST = "post"


async def bee_endpoint() -> tuple[str, dict[str, str], dict[str, str]]:
    """(url /decisions, headers, {comment, post: model}) của nhà cung cấp đang chọn. Không có decisions_path (vd. Kira)
    -> DecisionsUnsupportedError (bên gọi dùng chat); chưa cấu hình -> RuntimeError (fail open)."""
    global _unsupported_until
    provider = await load_provider("bee")
    if provider is None:
        raise RuntimeError("ai_provider_not_configured")
    if not provider.decisions_path:
        _unsupported_until = time.monotonic() + _UNSUPPORTED_SECONDS
        raise DecisionsUnsupportedError(f"decisions_unsupported provider={provider.key}")
    models = {COMMENT: provider.comment_model or comment_model(), POST: provider.post_model or post_model()}
    url = f"{provider.base_url.rstrip('/')}/{provider.decisions_path.lstrip('/')}"
    return url, {"Authorization": f"Bearer {provider.api_key}"}, models


async def decide(
    kind: str,
    state: str,
    questions: dict[str, dict[str, Any]],
    *,
    client: httpx.AsyncClient | None = None,
) -> dict[str, Any]:
    """Một request tới model quyết định của loại `kind` (COMMENT / POST): `answers` theo tên câu hỏi. Thử lại 3 lần với 429/5xx/lỗi mạng rồi raise. 404 (nhà cung cấp không có
    /decisions) -> DecisionsUnsupportedError và decisions_supported() = False một lúc."""
    global _unsupported_until
    if not decisions_supported():
        raise DecisionsUnsupportedError("decisions_unsupported")
    url, headers, models = await bee_endpoint()
    model = models.get(kind) or kind
    own = client is None
    client = client or httpx.AsyncClient(timeout=httpx.Timeout(30.0))
    body = {"model": model, "state": state, "questions": questions}
    try:
        for attempt in range(3):
            try:
                resp = await client.post(url, json=body, headers=headers)
            except httpx.HTTPError:
                if attempt == 2:
                    raise
                await asyncio.sleep(1 + attempt)
                continue
            if resp.status_code in (404, 405):
                _unsupported_until = time.monotonic() + _UNSUPPORTED_SECONDS
                raise DecisionsUnsupportedError(f"decisions_unsupported HTTP {resp.status_code}")
            if resp.status_code == 429 and "do not exist" in resp.text:
                # 429 khi model cấu hình không phải model quyết định.
                raise RuntimeError(f"decision_model_invalid model={model} - phải là model quyết định (jev / luna)")
            await ops_alerts.note_jev_response(resp.status_code, resp.text if resp.status_code >= 400 else "")
            if (resp.status_code == 429 or resp.status_code >= 500) and attempt < 2:
                await asyncio.sleep(2 * (attempt + 1))
                continue
            resp.raise_for_status()
            data = resp.json() or {}
            usage = data.get("usage") or {}
            usage_totals["requests"] += 1
            usage_totals["input_tokens"] += int(usage.get("input_tokens") or 0)
            usage_totals["output_tokens"] += int(usage.get("output_tokens") or 0)
            await ops_alerts.note_jev_tokens(int(usage.get("input_tokens") or 0))  # chi phí ngày, cảnh báo
            return data.get("answers") or {}
        raise RuntimeError("decision_retries_exhausted")
    finally:
        if own:
            await client.aclose()
