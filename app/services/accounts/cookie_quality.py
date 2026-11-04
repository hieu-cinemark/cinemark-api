"""Kiểm tra cookie Facebook lúc nhập trên dashboard (04/11/2026). Chỉ đọc chuỗi cookie, không gửi gì lên Facebook.

Cùng quy tắc với spider-hub social_crawler/accounts/session_quality.py, nơi chặn lần cuối trước request đầu tiên: hai repo không
dùng chung package nên sửa một bên thì sửa cả bên kia.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote

from app.services.accounts.user_agent import split_user_agent

REQUIRED_COOKIES = ("c_user", "xs", "datr")
# Điểm xuất phát, chưa chứng minh là ngưỡng đúng: phiên 376 ngày đã bị Facebook đăng xuất ngay request đầu (04/11/2026).
MAX_XS_AGE_DAYS = int(os.getenv("FACEBOOK_COOKIE_MAX_AGE_DAYS", "180"))
FIRST_BUILD_OF_CHROME_100 = 4896
_CHROME_VERSION = re.compile(r"\bChrome/(\d+)\.\d+\.(\d+)\.\d+")


@dataclass
class CookieAssessment:
    problems: list[str] = field(default_factory=list)
    # Đưa vào dòng thời gian: không có giá trị cookie nào ở đây.
    facts: dict[str, Any] = field(default_factory=dict)


def _parse(cookie: str) -> dict[str, str] | None:
    """Header string hoặc JSON xuất từ extension. None khi là JSON nhưng không đọc được."""
    if cookie.lstrip().startswith(("[", "{")):
        try:
            parsed = json.loads(cookie)
        except ValueError:
            return None
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items()}
        if isinstance(parsed, list):
            return {str(c["name"]): str(c.get("value", "")) for c in parsed if isinstance(c, dict) and "name" in c}
        return None
    pairs = (part.partition("=") for part in cookie.split(";") if "=" in part)
    return {name.strip(): value.strip() for name, _, value in pairs}


def _xs_age_days(xs: str, now: float) -> float | None:
    fields = unquote(xs).split(":")
    if len(fields) < 4 or not re.fullmatch(r"\d{10}", fields[3]):
        return None
    return round((now - int(fields[3])) / 86400, 1)


def assess_cookie(cookie: str, account_id: str = "", *, now: float | None = None) -> CookieAssessment:
    base, user_agent = split_user_agent(cookie)
    cookies = _parse(base)
    if cookies is None:
        return CookieAssessment(problems=["cookie_unreadable"])
    problems = [f"missing_{name}" for name in REQUIRED_COOKIES if not cookies.get(name)]
    c_user = cookies.get("c_user", "")
    if c_user and not c_user.isdigit():
        problems.append("c_user_not_numeric")
    if account_id.isdigit() and c_user and c_user != account_id:
        problems.append("c_user_mismatch")
    age = _xs_age_days(cookies.get("xs", ""), now if now is not None else time.time())
    if age is not None and age > MAX_XS_AGE_DAYS:
        problems.append("xs_too_old")
    chrome = _CHROME_VERSION.search(user_agent)
    if not user_agent:
        problems.append("missing_user_agent")
    elif chrome and int(chrome[1]) >= 100 and int(chrome[2]) != 0 and int(chrome[2]) < FIRST_BUILD_OF_CHROME_100:
        problems.append("user_agent_impossible_chrome_build")
    return CookieAssessment(
        problems=problems,
        facts={
            "cookie_count": len(cookies),
            "has_sb": bool(cookies.get("sb")),
            "xs_age_days": age,
            "ua_chrome": f"{chrome[1]}.{chrome[2]}" if chrome else None,
        },
    )
