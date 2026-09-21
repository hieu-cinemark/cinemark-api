"""AI-assisted bulk import for platform_accounts/platform_proxies - backs
app/api/routes/settings.py's POST /settings/import/parse. Lets an operator
paste a batch of raw account/proxy data in *any* shape (a spreadsheet
export, a text file from wherever the accounts were bought, one line per
account with fields in whatever order/separator) plus a plain-language
description of that shape, and get back structured rows.

Deliberately two-step, never paste-straight-to-DB: parse_import() only
returns candidate rows for the dashboard to show as an editable preview -
the actual write (platform_config_db.create_account/create_proxy) only
happens once the operator confirms it via POST /settings/import/commit. A
parsing mistake here would otherwise silently write a real password into
the wrong column.

A FORMAT that is already a column header (ID|PASS|MAIL|COOKIE) is split
locally and never waits on Kira. Free-form descriptions still go to Kira
(force=True so Settings import works while ingest classifiers stay off).
"""

from __future__ import annotations

from typing import Any, Literal

from app.core.logging import get_logger
from app.kira.client import call_kira, parse_json_response

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
    """Pipe/tab/semicolon header like ID|PASS|MAIL|COOKIE. None if this is
    free-form prose that still needs Kira."""
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
    """Returns candidate rows - see module docstring for why this never
    writes to the DB itself. Raises on anything that isn't parseable JSON
    or isn't a JSON array; the caller (app/api/routes/settings.py) turns
    that into a 4xx asking the operator to adjust FORMAT or split CONTENT
    into a smaller batch, rather than silently returning nothing."""
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
