"""Model quyết định của Bee (POST {bee base_url}/decisions): đọc một đoạn văn bản (`state`) và trả lời các câu hỏi có kiểu
(`choice` - kèm xác suất từng lựa chọn và độ tự tin). Thay cho việc gọi thẳng TypeSafe (api.typesafe.ai) từ 21/10/2026 -
mọi lời gọi AI đi qua Bee (một khoá, một nơi trả phí).

Chỉ dùng cho CA KHÓ: luật và model nhà trồng (ViSoBERT - app/ai/visobert.py) chạy trước, chỉ phần chúng không kết luận
được mới tới đây.
  - Bình luận (cảm xúc / giai đoạn / khía cạnh, giá trị bình luận): COMMENT_MODEL - typesafe/jev-1.14.
  - Bài viết (độ liên quan, cảm xúc nội dung bài): POST_MODEL - openai/gpt-6-luna-decisions.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from app.ai.client import load_provider
from app.core.config import settings
from app.services import ops_alerts

COMMENT_MODEL_DEFAULT = "typesafe/jev-1.14"
POST_MODEL_DEFAULT = "openai/gpt-6-luna-decisions"

# Tổng token kể từ lúc tiến trình chạy (log, script đo chi phí).
usage_totals = {"requests": 0, "input_tokens": 0, "output_tokens": 0}

# Nhà cung cấp của dòng "bee" không có /decisions (vd. tạm trỏ sang Kira khi Bee hết số dư - 404): đánh dấu trong chừng này
# giây; trong lúc đó bên gọi dùng chat (model của dòng bee, vd. qwen3.8-flash) cho ca khó thay vì model quyết định.
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
