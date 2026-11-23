"""Kiểm tra và ghi nhận cookie khi nhập tài khoản (dùng chung cho form tài khoản và nhập hàng loạt)."""

from __future__ import annotations

from app.core.errors import CookieRefusedError
from app.services.accounts.config_db.accounts import record_account_event
from app.services.accounts.cookie_quality import assess_cookie
from app.services.accounts.user_agent import merge_user_agent


def _refuse_bad_cookie(platform: str, account_id: str, cookie: str) -> None:
    """Cookie Facebook được kiểm tra ngay khi nhập, trước khi có request nào lên Facebook (cookie_quality.py). Cookie trống vẫn
    lưu được: tài khoản có thể thêm trước, nhập phiên sau."""
    if platform != "facebook" or not cookie.strip():
        return
    # Thiếu mỗi User-Agent thì vẫn lưu (18/11/2026): tài khoản mua sẵn thường chỉ kèm cookie, UA nhập bổ sung sau ở form sửa.
    # Không mở được cửa nào: spider-hub (accounts/session_quality.py) vẫn chặn trước request đầu tiên cho tới khi có UA.
    problems = [p for p in assess_cookie(cookie, account_id).problems if p != "missing_user_agent"]
    if problems:
        raise CookieRefusedError(",".join(problems))


async def _record_cookie_saved(row: dict) -> None:
    if row.get("platform") == "facebook" and row.get("cookie"):
        await record_account_event(row, "cookie_saved", **assess_cookie(row["cookie"], row["account_id"]).facts)


def _with_user_agent(fields: dict) -> dict:
    """Ô User-Agent (form thêm tài khoản / import) -> ghép vào cookie dạng useragent=<base64> mà spider-hub đọc."""
    user_agent = str(fields.pop("user_agent", "") or "").strip()
    if user_agent:
        fields["cookie"] = merge_user_agent(fields.get("cookie") or "", user_agent)
    return fields
