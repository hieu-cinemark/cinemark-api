"""Client LLM của Beeknoee (platform.beeknoee.com) - một proxy tương thích OpenAI mà
sản phẩm dùng cho Claude Sonnet 5. Là lớp mỏng, riêng cho provider, bọc
app.ai.client.call_ai(provider="bee", ...): HTTP client thực sự, retry/backoff và
giới hạn đồng thời đều nằm ở đó (cùng cấu trúc với app/ai/kira.py, nhưng Bee có
semaphore riêng, nên Bee sập hay bị rate limit cũng không làm Kira bị nghẽn, và
ngược lại).

Từ 2026-10-03 không còn dùng mặc định (hết số dư): report và mọi bộ phân loại đều
chạy trên Kira. Bee chỉ viết report social topic trở lại khi report provider trên
dashboard được đặt là "bee" (app/ai/tasks/report.py), và khi đó Kira làm dự phòng
nếu lời gọi Bee lỗi.
"""

from __future__ import annotations

from app.ai.client import call_ai, is_provider_configured, parse_json_response

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
    bên gọi nên bắt rộng và bỏ qua lỗi (fail open), giống quy ước của mọi bộ phân loại
    dùng Kira."""
    return await call_ai(
        provider="bee",
        task=task,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
    )
