"""`call_bee`: gọi Bee qua client dùng chung với model chọn riêng (report). Các task khác đi qua `app.ai.runtime`."""

from __future__ import annotations

from app.ai.providers.client import call_ai, is_provider_configured, parse_json_response

__all__ = ["bee_is_configured", "call_bee", "parse_json_response"]


async def bee_is_configured() -> bool:
    return await is_provider_configured("bee")


async def call_bee(
    *,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int,
    temperature: float = 0.0,
    model: str | None = None,
    task: str = "chat",
) -> str:
    """Một lần chat completion qua Beeknoee. Raise khi lỗi (kể cả khi "chưa cấu hình") -
    bên gọi nên bắt rộng và bỏ qua lỗi (fail open), giống quy ước của mọi bộ phân loại."""
    return await call_ai(
        provider="bee",
        task=task,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
    )
