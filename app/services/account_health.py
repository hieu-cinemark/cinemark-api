"""Tính sức khoẻ của một dòng platform_accounts từ các tín hiệu đã có sẵn - ở đây không
bao giờ gửi request mới nào tới Facebook/Threads/TikTok. Đây là lựa chọn có chủ đích
thay vì thăm dò "chủ động" (gọi thật tới nền tảng ngay lúc này): kiểm tra chủ động
cần một đường request riêng theo tài khoản (TikTokClient/bootstrap.py hiện tại đều
luôn tự xoay/chọn tài khoản, không bao giờ làm việc trên một dòng cụ thể), và sẽ tốn
một request thật vào chính nền tảng mà cả project này đang cố không bị chặn. Thụ
động thì có kết quả ngay, bấm "Check" bao nhiêu lần cũng an toàn, và dùng lại đúng
các tín hiệu mà logic tự tắt tài khoản của spider-hub vốn đã tin - xem:

  - TikTok: bộ đếm Redis tiktok_block_streak:<device_id> của client.py, tăng mỗi
    lần response rỗng (nhiều khả năng bị chặn), tài khoản bị tắt khi streak >= 3
    (xem disable_account bên đó).
  - Facebook/Threads: platform_token.get_token_status(), phản chiếu các key Redis
    <platform>:active_account / session_cache:<account> của spider-hub (xem
    docstring của module đó) - cùng tín hiệu mà TokenStatusBadge đang hiển thị trên
    dashboard.

Đánh đổi: kết quả chỉ mới tới mức hoạt động crawl/đăng nhập thật gần nhất đã chạm
vào các tín hiệu này. Một dòng không được dùng nhiều ngày sẽ báo trạng thái lần cuối
của nó, không phải "ngay lúc này"."""

from __future__ import annotations

from typing import Any

from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.services.platform_token import get_token_status


async def _check_tiktok(account_id: str) -> str:
    # Cùng key mà client.py bên spider-hub ghi (tiktok_block_streak:<device_id>, với
    # device_id được lưu trong platform_accounts.account_id khi platform='tiktok' - xem
    # update_tiktok_identity trong social_crawler/db/accounts.py về việc dùng lại cột đó).
    # Key chỉ tồn tại khi thực sự đã bị chặn (lệnh INCR đầu tiên của client.py tạo ra nó),
    # nên chỉ cần nó có mặt là đủ - không cần đọc chính con số đếm.
    client = get_redis_client()
    key = f"{REDIS_KEY_PREFIX}tiktok_block_streak:{account_id}"
    return "warning" if await client.get(key) else "ok"


async def _check_active_session(platform: str, account_id: str) -> str:
    active_account, ttl_seconds = await get_token_status(platform)
    if account_id.strip().lower() != (active_account or "").strip().lower():
        return "unknown"
    return "ok" if ttl_seconds is not None else "warning"


async def evaluate_account_health(account: dict[str, Any]) -> str:
    """Trả về trạng thái của một dòng platform_accounts: "disabled" | "ok" | "warning" |
    "unknown" - một cột chuỗi thường, không phải enum, để sau này một tín hiệu mới có
    thể thêm giá trị trạng thái mới mà không cần migrate."""
    if not account["enabled"]:
        return "disabled"

    platform = account["platform"]
    account_id = account["account_id"]
    if platform == "tiktok":
        return await _check_tiktok(account_id)
    if platform in ("facebook", "threads"):
        return await _check_active_session(platform, account_id)
    return "unknown"
