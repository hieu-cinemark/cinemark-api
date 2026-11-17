import asyncio

import httpx

from app.clients import telegram


def test_tests_never_send_real_telegram(monkeypatch):
    """Guard của conftest (_no_real_telegram): dù `.env` / môi trường có token, test không gửi được tin thật."""

    class Boom:
        def __init__(self, *args, **kwargs):
            raise AssertionError("test gửi Telegram thật")

    monkeypatch.setattr(httpx, "AsyncClient", Boom)
    assert not telegram.telegram_enabled()
    asyncio.run(telegram.send_telegram_message("cảnh báo thử"))  # không raise = không mở kết nối nào
