from __future__ import annotations

import httpx

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"

TELEGRAM_BOT_TOKEN = settings.telegram_bot_token
TELEGRAM_CHAT_ID = settings.telegram_chat_id


def telegram_enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


async def send_telegram_message(text: str) -> None:
    """Async (httpx, giống mọi lời gọi ra ngoài khác trong service này - xem
    app/services/d1.py) thay vì `requests` đồng bộ mà trước đây gọi kèm `await` -
    `requests` thậm chí không phải dependency của project (httpx==0.28.1 mới là - xem
    pyproject.toml), nên trước đây lỗi ModuleNotFoundError ngay lúc import, và kể cả có
    cài thì cũng lỗi TypeError ở chỗ await (hàm đồng bộ trả về None, không phải
    awaitable)."""
    if not telegram_enabled():
        return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                TELEGRAM_API_URL.format(token=TELEGRAM_BOT_TOKEN),
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4096]},
            )
        if resp.status_code != 200:
            logger.warning("telegram_send_failed", status_code=resp.status_code, body=resp.text[:300])
    except httpx.HTTPError as exc:
        logger.warning("telegram_send_failed", error=str(exc))
