import pytest

from app.core.config import settings


@pytest.fixture(autouse=True)
def _no_live_jev(monkeypatch):
    """Không test nào được gọi Jev thật (JEV_API_KEY trong .env): mặc định gán nhãn comment bằng đường Kira (đã mock
    trong từng test). Test của Jev tự bật lại và mock HTTP."""
    monkeypatch.setattr(settings, "classifier", "kira")
