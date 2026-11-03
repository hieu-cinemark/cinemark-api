from unittest.mock import AsyncMock, Mock

from app.schemas.settings import AutoLoginSettings
from app.services.crawl import scheduler


async def test_cookie_checks_default_off_and_do_not_publish_or_read_redis(monkeypatch):
    cfg = AutoLoginSettings()
    assert cfg.cookie_check_enabled is False
    monkeypatch.setattr(scheduler, "resolve_auto_login_settings", AsyncMock(return_value=cfg))
    publish = AsyncMock()
    redis_client = Mock(side_effect=AssertionError("Should not read Redis when disabled"))
    monkeypatch.setattr(scheduler, "publish_cookie_check_request", publish)
    monkeypatch.setattr(scheduler, "get_redis_client", redis_client)
    await scheduler._cookie_check_tick()
    publish.assert_not_called()
    redis_client.assert_not_called()
