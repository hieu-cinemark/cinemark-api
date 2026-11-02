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


@pytest.fixture(autouse=True)
def _no_real_redis(monkeypatch):
    """Không test nào được mở kết nối Redis thật: máy dev có sẵn Redis nên một test quên mock vẫn "pass" ở local rồi hỏng
    ở CI (hoặc âm thầm đọc/ghi state thật). Chặn ở tầng connect() để lộ ra ngay, kèm gợi ý mock. Test cần Redis giả thì dùng
    fakeredis hoặc monkeypatch lớp client - không bao giờ cần socket thật."""
    import redis.connection

    def _blocked(*args, **kwargs):
        raise RuntimeError(
            "Test mở kết nối Redis thật - mock get_redis_client / RedisCache (hoặc hàm gọi nó) trong test này."
        )

    monkeypatch.setattr(redis.connection.AbstractConnection, "connect", _blocked, raising=False)
    try:
        import redis.asyncio.connection as _aioconn

        monkeypatch.setattr(_aioconn.AbstractConnection, "connect", _blocked, raising=False)
    except ImportError:
        pass
