"""Gọi Bee cho các job AI chuyển từ Worker: trả (JSON đã parse hoặc None, nội dung thô). Không raise - job tự quyết khi lỗi."""

from __future__ import annotations

from typing import Any

from app.ai.client import parse_json_response
from app.ai.runtime import call_llm
from app.core.logging import get_logger

logger = get_logger(__name__)


async def ask_json(*, system: str, user: str, max_tokens: int, task: str, temperature: float = 0.1) -> tuple[Any, str]:
    # force=True: các job này vốn chạy trên Worker theo cấu hình AI riêng, không theo công tắc tắt AI của bộ phân loại ingest.
    try:
        content = await call_llm(
            system_prompt=system,
            user_prompt=user,
            max_tokens=max_tokens,
            temperature=temperature,
            force=True,
            task=task,
        )
    except Exception as exc:  # noqa: BLE001 - lỗi provider: bên gọi coi như lượt hỏng
        logger.warning("ai_job_call_failed", task=task, error=str(exc)[:300])
        return None, ""
    try:
        return parse_json_response(content), content
    except ValueError:
        logger.warning("ai_job_unparsed", task=task, preview=content[:200])
        return None, content
