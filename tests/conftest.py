import pytest

from app.core.config import settings


@pytest.fixture(autouse=True)
def _no_live_jev(monkeypatch):
    """Không test nào được gọi Jev thật (JEV_API_KEY trong .env): mặc định gán nhãn comment bằng đường Kira (đã mock
    trong từng test). Test của Jev tự bật lại và mock HTTP."""
    monkeypatch.setattr(settings, "classifier", "kira")


@pytest.fixture(autouse=True)
def _no_ops_alert_writes(monkeypatch):
    """Hook cảnh báo vận hành trong app/ai (ghi mốc ví/token Jev vào Redis) tắt khi test - test riêng tự bật lại."""
    monkeypatch.setattr(settings, "ops_alerts_enabled", False)
