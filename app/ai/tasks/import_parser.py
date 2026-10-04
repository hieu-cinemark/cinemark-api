"""Import hàng loạt có AI hỗ trợ cho platform_accounts/platform_proxies - phục vụ
POST /settings/import/parse trong app/api/routes/settings.py. Cho phép người vận
hành dán một loạt dữ liệu tài khoản/proxy thô ở *bất kỳ* dạng nào (file xuất từ
bảng tính, file text lấy từ nơi mua tài khoản, mỗi dòng một tài khoản với các
trường theo thứ tự/ký tự phân cách tuỳ ý) kèm mô tả bằng lời về dạng đó, rồi nhận
lại các dòng có cấu trúc.

Cố ý làm hai bước, không bao giờ dán thẳng vào DB: parse_import() chỉ trả về các
dòng ứng viên để dashboard hiển thị thành bản xem trước sửa được - việc ghi thật
(platform_config_db.create_account/create_proxy) chỉ xảy ra khi người vận hành xác
nhận qua POST /settings/import/commit. Nếu không, một lỗi parse ở đây có thể âm
thầm ghi mật khẩu thật vào sai cột.

FORMAT nào vốn đã là dòng tiêu đề cột (ID|PASS|MAIL|COOKIE) sẽ được tách ngay tại
chỗ và không phải chờ Kira. Mô tả tự do vẫn gửi cho Kira (force=True để import ở
Settings vẫn chạy khi các bộ phân loại ingest đang tắt).
"""

from __future__ import annotations

from typing import Any, Literal

from app.ai.kira import call_kira, parse_json_response
from app.core.logging import get_logger

logger = get_logger(__name__)

ImportTarget = Literal["accounts", "proxies"]

ACCOUNT_FIELDS = ("account_id", "password", "totp_secret", "email", "email_password", "cookie", "token")
PROXY_FIELDS = ("proxy_url", "username", "password", "login_use_proxy")

_ACCOUNT_ALIASES = {
    "id": "account_id",
    "uid": "account_id",
    "user": "account_id",
    "userid": "account_id",
    "user_id": "account_id",
    "username": "account_id",
    "login": "account_id",
    "account": "account_id",
    "account_id": "account_id",
    "accountid": "account_id",
    "pass": "password",
    "pwd": "password",
    "passwd": "password",
    "password": "password",
    "mail": "email",
    "email": "email",
    "gmail": "email",
    "passmail": "email_password",
    "pass_mail": "email_password",
    "mailpass": "email_password",
    "mail_pass": "email_password",
    "emailpass": "email_password",
    "email_password": "email_password",
    "mailkp": "email_password",
    "totp": "totp_secret",
    "2fa": "totp_secret",
    "secret": "totp_secret",
    "totp_secret": "totp_secret",
    "cookie": "cookie",
    "cookies": "cookie",
    "token": "token",
    "refresh_token": "token",
    "refreshtoken": "token",
    "odin": "token",
    "odin_id": "token",
    "device": "token",
    "device_id": "token",
    "clientid": "token",
    "client_id": "token",
}

_PROXY_ALIASES = {
    "proxy": "proxy_url",
    "proxy_url": "proxy_url",
    "host": "proxy_url",
    "url": "proxy_url",
    "ip": "proxy_url",
    "user": "username",
    "username": "username",
    "pass": "password",
    "pwd": "password",
    "password": "password",
}

_ACCOUNT_SYSTEM_PROMPT = """
You are a data-extraction assistant for a social-media account pool importer.

The user provides a FORMAT description (their own words, may be Vietnamese
or English, may be a plain description or a sample row) and CONTENT (raw
pasted text containing many accounts, one per row/line/block, shaped
however FORMAT describes).

Extract every account in CONTENT and return a JSON array - one object per
account, each with EXACTLY these keys (string values; "" for any field not
present in the input - never omit a key, never invent a value not actually
present):

  account_id     - the login username/ID/phone/email used to log in
  password       - the login password
  totp_secret    - the persistent 2FA/TOTP secret (NOT a 6-digit one-time code)
  email          - recovery email, only if it's a genuinely separate field from account_id
  email_password - the recovery email's own password
  cookie         - a raw cookie header/string, if present
  token          - any other id/token field that doesn't fit the above (e.g. a device or odin id)

Skip a row entirely if you cannot identify at least an account_id for it.
Respond with ONLY the JSON array - no markdown code fence, no commentary,
no trailing explanation.
"""

_PROXY_SYSTEM_PROMPT = """
You are a data-extraction assistant for a proxy pool importer.

The user provides a FORMAT description (their own words, may be Vietnamese
or English, may be a plain description or a sample row) and CONTENT (raw
pasted text containing many proxies, one per row/line, shaped however
FORMAT describes - commonly host:port:username:password or similar).

Extract every proxy in CONTENT and return a JSON array - one object per
proxy, each with EXACTLY these keys:

  proxy_url        - "host:port" only, no scheme (strip "http://" etc if present)
  username          - proxy auth username, "" if none
  password          - proxy auth password, "" if none
  login_use_proxy   - boolean, true only if the FORMAT/CONTENT explicitly says
                       this proxy should also be used for browser login, false otherwise

Skip a row entirely if you cannot identify at least a proxy_url for it.
Respond with ONLY the JSON array - no markdown code fence, no commentary,
no trailing explanation.
"""

_SYSTEM_PROMPTS: dict[ImportTarget, str] = {
    "accounts": _ACCOUNT_SYSTEM_PROMPT,
    "proxies": _PROXY_SYSTEM_PROMPT,
}


def default_import_system_prompts() -> dict[str, str]:
    return dict(_SYSTEM_PROMPTS)


_FIELDS: dict[ImportTarget, tuple[str, ...]] = {
    "accounts": ACCOUNT_FIELDS,
    "proxies": PROXY_FIELDS,
}
_MAX_TOKENS = 4000


def _normalize_column(name: str) -> str:
    return "".join(ch for ch in name.strip().lower() if ch.isalnum() or ch == "_")


def _format_columns(format_hint: str, aliases: dict[str, str]) -> list[str | None] | None:
    """Dòng tiêu đề phân cách bằng pipe/tab/chấm phẩy như ID|PASS|MAIL|COOKIE. Trả về None
    nếu đây là văn bản mô tả tự do, vẫn cần Kira."""
    hint = format_hint.strip()
    separator = "|" if "|" in hint else ("\t" if "\t" in hint else (";" if ";" in hint else None))
    if separator is None:
        return None
    parts = [p.strip() for p in hint.split(separator)]
    if len(parts) < 2:
        return None
    mapped: list[str | None] = []
    seen: set[str] = set()
    known = 0
    for part in parts:
        field = aliases.get(_normalize_column(part))
        if field and field not in seen:
            mapped.append(field)
            seen.add(field)
            known += 1
        else:
            mapped.append(None)
    if known < 2:
        return None
    return mapped


def _line_separator(format_hint: str) -> str:
    if "|" in format_hint:
        return "|"
    if "\t" in format_hint:
        return "\t"
    return ";"


def parse_delimited(target: ImportTarget, format_hint: str, content: str) -> list[dict[str, Any]] | None:
    aliases = _ACCOUNT_ALIASES if target == "accounts" else _PROXY_ALIASES
    columns = _format_columns(format_hint, aliases)
    if columns is None:
        return None
    fields = _FIELDS[target]
    sep = _line_separator(format_hint)
    rows: list[dict[str, Any]] = []
    width = len(columns)
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        cells = line.split(sep, width - 1)
        if len(cells) < 2:
            continue
        row: dict[str, Any] = {f: "" for f in fields}
        if target == "proxies":
            row["login_use_proxy"] = False
        for i, field in enumerate(columns):
            if field is None or i >= len(cells):
                continue
            row[field] = cells[i].strip()
        key = "account_id" if target == "accounts" else "proxy_url"
        if not row.get(key):
            continue
        rows.append(row)
    return rows or None


async def parse_import(target: ImportTarget, format_hint: str, content: str) -> list[dict[str, Any]]:
    """Trả về các dòng ứng viên - xem docstring module để biết vì sao hàm này không bao
    giờ tự ghi vào DB. Raise với mọi thứ không parse được thành JSON hoặc không phải
    mảng JSON; bên gọi (app/api/routes/settings.py) đổi lỗi đó thành 4xx, yêu cầu
    người vận hành chỉnh FORMAT hoặc chia CONTENT thành lô nhỏ hơn, thay vì âm thầm
    trả về rỗng."""
    local = parse_delimited(target, format_hint, content)
    if local is not None:
        logger.info("import_parsed", target=target, row_count=len(local), source="delimited")
        return local

    system_prompt = _SYSTEM_PROMPTS[target]
    user_prompt = f"FORMAT:\n{format_hint.strip()}\n\nCONTENT:\n{content.strip()}"
    response = await call_kira(
        task=f"import_{target}",
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        max_tokens=_MAX_TOKENS,
        force=True,
    )
    parsed = parse_json_response(response)
    if not isinstance(parsed, list):
        raise ValueError(f"Expected a JSON array of rows, got {type(parsed).__name__}")

    fields = _FIELDS[target]
    rows: list[dict[str, Any]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        row: dict[str, Any] = {f: str(item.get(f) or "") for f in fields}
        if target == "proxies":
            row["login_use_proxy"] = bool(item.get("login_use_proxy", False))
        rows.append(row)

    logger.info("import_parsed", target=target, row_count=len(rows), source="kira")
    return rows
