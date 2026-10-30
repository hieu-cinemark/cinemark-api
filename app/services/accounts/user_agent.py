"""User-Agent của trình duyệt đã tạo cookie một tài khoản Facebook/Threads (30/10/2026).

spider-hub đọc nó từ một cookie giả `useragent=<base64>` nằm trong cột platform_accounts.cookie (spider-hub
spiders/facebook/auth/cookies.extract_user_agent) - quy ước có sẵn của các chợ bán tài khoản. Dashboard có ô User-Agent
riêng; ở đây tách / ghép nó với chuỗi cookie để người vận hành không phải tự mã hoá base64.
"""

from __future__ import annotations

import base64
from urllib.parse import unquote

USER_AGENT_COOKIE_KEY = "useragent"


def _parts(cookie: str) -> list[str]:
    return [part.strip() for part in (cookie or "").split(";") if part.strip()]


def split_user_agent(cookie: str) -> tuple[str, str]:
    """(cookie không còn useragent=..., UA đã giải mã - "" khi không có / không giải mã được)."""
    kept: list[str] = []
    user_agent = ""
    for part in _parts(cookie):
        name, _, value = part.partition("=")
        if name.strip().lower() == USER_AGENT_COOKIE_KEY:
            try:
                user_agent = base64.b64decode(unquote(value.strip())).decode("utf-8").strip()
            except ValueError:
                user_agent = ""
            continue
        kept.append(part)
    return "; ".join(kept), user_agent


def merge_user_agent(cookie: str, user_agent: str) -> str:
    """Chuỗi cookie với useragent=<base64(UA)> thay cho mục cũ (nếu có)."""
    base, _ = split_user_agent(cookie)
    encoded = base64.b64encode(user_agent.strip().encode("utf-8")).decode("ascii")
    return "; ".join(part for part in (base, f"{USER_AGENT_COOKIE_KEY}={encoded}") if part)
