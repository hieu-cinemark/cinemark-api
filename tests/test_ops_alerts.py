from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from app.core.config import settings
from app.services.ops import ops_alerts

NOW = datetime(2026, 10, 13, 5, 0, tzinfo=UTC)  # 12:00 giờ VN


def test_omar_quota_threshold_and_daily_key():
    early = 8 * 3600  # còn 8 giờ mới reset: cạn sớm
    assert ops_alerts.omar_quota_alerts({"day": {"used": 899, "limit": 1000, "resets_in": early}}, NOW) == []
    alerts = ops_alerts.omar_quota_alerts({"day": {"used": 900, "limit": 1000, "resets_in": early}}, NOW)
    assert [a.key for a in alerts] == ["omar_quota:2026-10-13"]
    assert alerts[0].ttl >= 24 * 3600
    # Gần giờ reset (chế độ chạy liên tục cố ý dùng hết): không báo.
    assert ops_alerts.omar_quota_alerts({"day": {"used": 990, "limit": 1000, "resets_in": 2 * 3600}}, NOW) == []
    # Không có used: suy từ remaining.
    assert ops_alerts.omar_quota_alerts({"day": {"remaining": 50, "limit": 1000, "resets_in": early}}, NOW)
    assert ops_alerts.omar_quota_alerts({"day": {"limit": 0}}, NOW) == []
    assert ops_alerts.omar_quota_alerts(None, NOW) == []


@pytest.mark.parametrize(
    ("days_left", "key"),
    [
        (30, None),
        (2.5, "omar_expiry:3d:2026-10-13"),
        (0.5, "omar_expiry:1d:2026-10-13"),
        (-1, "omar_expiry:expired:2026-10-13"),
    ],
)
def test_omar_plan_expiry_alerts(days_left, key):
    usage = {"plan": "Pro", "expires_at": (NOW + timedelta(days=days_left)).isoformat()}
    alerts = ops_alerts.omar_quota_alerts(usage, NOW)
    assert [a.key for a in alerts] == ([key] if key else [])


def test_omar_error_rate():
    status = {"running": True, "started_at": "x", "stats": {"requests": 60, "failed": 40}}
    alert = ops_alerts.omar_error_alert(status, NOW)
    assert alert and alert.key == "omar_error_rate" and alert.ttl == int(settings.ops_alert_repeat_hours * 3600)
    # Dưới ngưỡng tỉ lệ / chưa đủ số lần gọi.
    assert ops_alerts.omar_error_alert({**status, "stats": {"requests": 80, "failed": 20}}, NOW) is None
    assert ops_alerts.omar_error_alert({**status, "stats": {"requests": 10, "failed": 30}}, NOW) is None
    # Lượt đã xong: chỉ xét nếu kết thúc trong 1 giờ qua.
    done = {**status, "running": False, "finished_at": (NOW - timedelta(minutes=30)).isoformat()}
    assert ops_alerts.omar_error_alert(done, NOW)
    old = {**done, "finished_at": (NOW - timedelta(hours=2)).isoformat()}
    assert ops_alerts.omar_error_alert(old, NOW) is None
    assert ops_alerts.omar_error_alert({"running": False, "finished_at": None, "stats": {}}, NOW) is None


def test_drought():
    fresh = (NOW - timedelta(hours=2)).isoformat()
    stale = (NOW - timedelta(hours=7)).isoformat()
    assert ops_alerts.drought_alert("posts_tiktok", "bài TikTok", fresh, NOW, 6) is None
    alert = ops_alerts.drought_alert("posts_tiktok", "bài TikTok", stale, NOW, 6)
    assert alert and alert.key == "drought:posts_tiktok"
    assert ops_alerts.drought_alert("comments", "comment", None, NOW, 6)  # bảng rỗng


def test_wallet_alert_window():
    now_ts = NOW.timestamp()
    assert ops_alerts.wallet_alert("bee", None, now_ts) is None
    assert ops_alerts.wallet_alert("bee", now_ts - 600, now_ts).key == "ai_wallet:bee"
    assert ops_alerts.wallet_alert("bee", now_ts - 2 * 3600, now_ts) is None


def test_wallet_error_detection():
    class Err(Exception):
        def __init__(self, status_code=None, code=None):
            super().__init__("boom")
            self.status_code, self.code = status_code, code

    assert ops_alerts.is_ai_wallet_error(Err(402))
    assert ops_alerts.is_ai_wallet_error(Err(429, "insufficient_quota"))
    assert not ops_alerts.is_ai_wallet_error(Err(500))


def test_vn_date_rolls_at_vn_midnight():
    assert ops_alerts.vn_date(datetime(2026, 10, 13, 17, 30, tzinfo=UTC)) == "2026-10-14"


@pytest.mark.asyncio
async def test_fire_dedupes(monkeypatch):
    redis = AsyncMock()
    redis.set.side_effect = [True, None]
    send = AsyncMock()
    monkeypatch.setattr(ops_alerts, "get_redis_client", lambda: redis)
    monkeypatch.setattr(ops_alerts, "send_telegram_message", send)
    alert = ops_alerts.Alert("drought:comments", 6 * 3600, "x")
    assert await ops_alerts._fire(alert) is True
    assert await ops_alerts._fire(alert) is False
    send.assert_awaited_once_with("x")
    key = redis.set.await_args_list[0]
    assert key.args[0].endswith("ops_alert:drought:comments")
    assert key.kwargs == {"nx": True, "ex": 6 * 3600}


@pytest.mark.asyncio
async def test_run_checks_drought_with_mocked_d1(monkeypatch):
    from app.repositories.d1 import sql as d1_sql

    stale = (datetime.now(tz=UTC) - timedelta(hours=8)).isoformat()
    monkeypatch.setattr(d1_sql, "d1_query", AsyncMock(return_value=[{"last": stale}]))
    alerts = await ops_alerts._drought_alerts(datetime.now(tz=UTC))
    assert [a.key for a in alerts] == ["drought:posts_tiktok", "drought:comments"]


@pytest.mark.asyncio
async def test_note_hooks_disabled_and_enabled(monkeypatch):
    redis = AsyncMock()
    monkeypatch.setattr(ops_alerts, "get_redis_client", lambda: redis)

    class Wallet(Exception):
        status_code = 402

    await ops_alerts.note_ai_wallet_error("bee", Wallet("payment required"))
    redis.set.assert_not_awaited()  # tắt trong conftest
    monkeypatch.setattr(settings, "ops_alerts_enabled", True)
    await ops_alerts.note_ai_wallet_error("bee", Exception("boom"))
    redis.set.assert_not_awaited()
    await ops_alerts.note_ai_wallet_error("bee", Wallet("payment required"))
    assert redis.set.await_args.args[0].endswith("ai_wallet_error:bee")
