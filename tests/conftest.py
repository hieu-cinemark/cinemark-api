import pytest

from app.core.config import settings


@pytest.fixture(autouse=True)
def _no_live_jev(monkeypatch):
    """Không test nào được gọi Jev thật: mặc định gán nhãn comment bằng đường Kira (đã mock
    trong từng test). Test của Jev tự bật lại và mock HTTP."""
    monkeypatch.setattr(settings, "classifier", "kira")


@pytest.fixture(autouse=True)
def _no_ops_alert_writes(monkeypatch):
    """Hook cảnh báo vận hành trong app/ai (ghi mốc ví/token Jev vào Redis) tắt khi test - test riêng tự bật lại."""
    monkeypatch.setattr(settings, "ops_alerts_enabled", False)


@pytest.fixture(autouse=True)
def _no_visobert(monkeypatch):
    """Không nạp model ViSoBERT thật (374MB, có thể có sẵn ở máy dev) trong test - test riêng tự mock app.ai.local.visobert."""
    monkeypatch.setattr(settings, "visobert_enabled", False)
    monkeypatch.setattr(settings, "visobert_relevance_enabled", False)
    # Test sẵn có kiểm tra đường AI của bình luận; chế độ không-AI có test riêng (test_comment_home_only.py).
    monkeypatch.setattr(settings, "comment_ai_enabled", True)


@pytest.fixture(autouse=True)
def _fake_bee_decisions(monkeypatch):
    """Model quyết định của Bee (app/ai/decisions.py): không đọc ai_providers trên Supabase - test mock HTTP."""
    from app.ai import decisions

    async def endpoint():
        return (
            "https://bee.test/v1/decisions",
            {"Authorization": "Bearer k"},
            {"comment": "typesafe/jev-1.14", "post": "openai/gpt-6-luna-decisions"},
        )

    monkeypatch.setattr(decisions, "bee_endpoint", endpoint)
