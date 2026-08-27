from __future__ import annotations

import httpx

from app.core.logging import get_logger
from app.core.config import settings

logger = get_logger(__name__)

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"

TELEGRAM_BOT_TOKEN = settings.telegram_bot_token
TELEGRAM_CHAT_ID = settings.telegram_chat_id


def telegram_enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


async def send_telegram_message(text: str) -> None:
    """Async (httpx, like every other outbound call in this service - see
    app/services/d1.py) rather than the sync `requests` this used to call
    with `await` on it - `requests` isn't even a project dependency
    (httpx==0.28.1 is - see pyproject.toml), so that raised
    ModuleNotFoundError at import time, and even installed it would have
    raised TypeError on the await (a sync function returns None, not an
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
