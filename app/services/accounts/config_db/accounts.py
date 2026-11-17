"""Tài khoản nền tảng (platform_accounts) và proxy (platform_proxies): đọc/ghi từ trang Settings, ghim proxy, sự kiện tài khoản."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from app.core.errors import ConflictError, NotFoundError, ProxyPolicyError
from app.core.logging import get_logger

logger = get_logger(__name__)
from app.services.accounts.config_db.conn import _connect, _get_pool, _trusted_sql  # noqa: F401

ACCOUNT_COLUMNS = (
    "id, platform, account_id, password, totp_secret, cookie, token, email, email_password, "
    "enabled, created_at, updated_at, last_checked_at, last_check_status, last_check_note, "
    # Các cột pool / circuit-breaker + ghim proxy cố định - do services/pool.py & package
    # db/ của spider-hub ghi, phía này chỉ đọc (hệ thống ghi, không bao giờ nằm trong form
    # tài khoản) - xem ACCOUNT_CREATE_COLUMNS bên dưới, cố ý loại chúng ra. Cột gốc chỉ tên
    # là "status" - đặt alias thành pool_status để không lẫn với last_check_status ở trên
    # (một tín hiệu khác, kích hoạt bằng tay - xem app/services/accounts/account_health.py).
    "status AS pool_status, cooldown_until, consecutive_failures, last_used_at, assigned_proxy_id"
)
PROXY_COLUMNS = (
    "id, platform, proxy_url, username, password, login_use_proxy, ip_type, enabled, created_at, updated_at, "
    "status AS pool_status, cooldown_until, consecutive_failures, last_used_at, provider_key, provider_ref"
)

_pool_columns_ready = False


async def _ensure_pool_columns() -> None:
    """Thêm các cột pool/circuit-breaker + ghim proxy cố định (status, cooldown_until,
    consecutive_failures, last_used_at, assigned_proxy_id) nếu database Supabase này có
    từ trước services/pool.py của spider-hub - giống hệt các câu ALTER TABLE ... IF NOT
    EXISTS trong scripts/dev_db_schema.sql của spider-hub, để
    ACCOUNT_COLUMNS/PROXY_COLUMNS/_PROXY_LIST_COLUMNS (vốn SELECT các cột này không điều
    kiện) không làm cả trang Settings lỗi 500 với UndefinedColumn trên database chưa
    migrate. IF NOT EXISTS nên database đã migrate thì không làm gì."""
    global _pool_columns_ready
    if _pool_columns_ready:
        return
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            "ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active'"
        )
        await cur.execute("ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS cooldown_until timestamptz")
        await cur.execute(
            "ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS consecutive_failures int NOT NULL DEFAULT 0"
        )
        await cur.execute("ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS last_used_at timestamptz")
        await cur.execute("ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS assigned_proxy_id int")
        await cur.execute(
            "CREATE TABLE IF NOT EXISTS account_events (id bigserial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now(), "
            "platform text NOT NULL, account_key text NOT NULL, event text NOT NULL, "
            "details jsonb NOT NULL DEFAULT '{}'::jsonb)"
        )
        await cur.execute(
            "CREATE INDEX IF NOT EXISTS account_events_account_at ON account_events (platform, account_key, at DESC)"
        )
        await cur.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active'")
        await cur.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS cooldown_until timestamptz")
        await cur.execute(
            "ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS consecutive_failures int NOT NULL DEFAULT 0"
        )
        await cur.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS last_used_at timestamptz")
        await cur.execute(
            "ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS ip_type text NOT NULL DEFAULT 'unknown'"
        )
        # Gói nhà cung cấp nào sở hữu dòng proxy này (NULL = nhập tay/tĩnh). Trước đó code
        # phải đoán bằng cách so proxy_url với host:port trong token của từng gói.
        await cur.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS provider_key text")
        # Mã riêng của cổng trong hệ thống nhà cung cấp - điền vào placeholder {ref} của
        # link reset (vd proxy_id của webtui).
        await cur.execute("ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS provider_ref text")
        await conn.commit()
    _pool_columns_ready = True


_ACCOUNT_CREATE_COLUMNS = (
    "platform",
    "account_id",
    "password",
    "totp_secret",
    "cookie",
    "token",
    "email",
    "email_password",
    "enabled",
)
_PROXY_CREATE_COLUMNS = (
    "platform",
    "proxy_url",
    "username",
    "password",
    "login_use_proxy",
    "ip_type",
    "enabled",
    # Nguồn proxy sở hữu dòng này (NULL = nhập tay/tĩnh) - xem list_proxies.
    "provider_key",
    # Mã riêng của cổng trong hệ thống nhà cung cấp (placeholder {ref} của link reset).
    "provider_ref",
)


# --- tài khoản ---------------------------------------------------------


_ACCOUNT_SECRET_COLUMNS = ("password", "totp_secret", "cookie", "token", "email_password")

# Như ACCOUNT_COLUMNS nhưng không kéo giá trị bí mật về: mỗi cột bí mật thành chuỗi rỗng
# kèm cờ has_<cột>. Cookie/token của vài chục tài khoản là phần lớn payload từ Supabase
# (~0.3s mỗi lần tải danh sách) mà danh sách trên dashboard vốn che hết (AccountOut.masked).
_ACCOUNT_LIST_COLUMNS = ACCOUNT_COLUMNS
for _col in _ACCOUNT_SECRET_COLUMNS:
    _ACCOUNT_LIST_COLUMNS = _ACCOUNT_LIST_COLUMNS.replace(
        f" {_col},", f" '' AS {_col}, (COALESCE({_col}, '') <> '') AS has_{_col},", 1
    )


async def count_crawl_ready_accounts(platform: str) -> int:
    """Số tài khoản pool có thể giao cho crawler lúc này (giống get_accounts của spider-hub: bật, không checkpoint, không
    cooldown, không "chỉ nuôi") - nhịp đào bài cũ (crawl_lanes) xếp số job theo con số này. Chưa trừ tài khoản đang trong
    giai đoạn nuôi (đếm trong Redis của spider-hub) - xếp dư thì job thừa chỉ bị bỏ qua."""
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT count(*) AS n FROM platform_accounts WHERE platform = %s AND enabled = true "
            "AND status != 'checkpoint' AND (cooldown_until IS NULL OR cooldown_until <= now()) "
            "AND (platform = 'facebook' OR NOT coalesce(nurture_only, false))",
            (platform,),
        )
        row = await cur.fetchone()
    return int(row["n"]) if row else 0


async def record_account_event(account: dict[str, Any], event: str, **details: Any) -> None:
    """Dòng thời gian của tài khoản (spider-hub db/account_events.py). Ghi lỗi không làm hỏng thao tác của người vận hành."""
    key = str(account.get("email") or account.get("account_id") or "").strip().lower()
    try:
        await _ensure_pool_columns()
        async with _connect() as conn, conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO account_events (platform, account_key, event, details) VALUES (%s, %s, %s, %s)",
                (account["platform"], key, event, Jsonb(details)),
            )
            await conn.commit()
    except psycopg.Error as exc:
        logger.warning("account_event_write_failed", event_name=event, error_type=type(exc).__name__)


async def list_accounts(platform: str | None = None, *, with_secrets: bool = True) -> list[dict[str, Any]]:
    await _ensure_pool_columns()
    columns = ACCOUNT_COLUMNS if with_secrets else _ACCOUNT_LIST_COLUMNS
    async with _connect() as conn, conn.cursor() as cur:
        if platform:
            await cur.execute(f"SELECT {columns} FROM platform_accounts WHERE platform = %s ORDER BY id", (platform,))
        else:
            await cur.execute(f"SELECT {columns} FROM platform_accounts ORDER BY platform, id")
        return await cur.fetchall()


async def get_account(account_id: int) -> dict[str, Any] | None:
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(f"SELECT {ACCOUNT_COLUMNS} FROM platform_accounts WHERE id = %s", (account_id,))
        return await cur.fetchone()


async def create_account(fields: dict[str, Any]) -> dict[str, Any]:
    await _ensure_pool_columns()
    columns = [c for c in _ACCOUNT_CREATE_COLUMNS if c in fields]
    values = [fields[c] for c in columns]
    try:
        async with _connect() as conn, conn.cursor() as cur:
            await cur.execute(
                _trusted_sql(
                    f"INSERT INTO platform_accounts ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) "
                    f"RETURNING {ACCOUNT_COLUMNS}"
                ),
                values,
            )
            row = await cur.fetchone()
            await conn.commit()
            return row  # type: ignore[return-value]
    except psycopg.errors.UniqueViolation as exc:
        raise ConflictError(
            f"An account for {fields.get('platform')}/{fields.get('account_id')} already exists"
        ) from exc


async def update_account(account_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in _ACCOUNT_CREATE_COLUMNS if c in fields]
    if not columns:
        raise NotFoundError("No fields to update")
    set_clause = ", ".join(f"{c} = %s" for c in columns)
    values = [fields[c] for c in columns] + [account_id]
    # spider-hub coi updated_at mới hơn lần kiểm tra là "đã có cookie mới" và bỏ trạng thái dead. Sửa mật khẩu hay email không
    # cứu được phiên đã bị từ chối nên chỉ đổi cookie mới được chạm vào nó.
    touch = ", updated_at = now()" if "cookie" in columns else ""
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            _trusted_sql(f"UPDATE platform_accounts SET {set_clause}{touch} WHERE id = %s RETURNING {ACCOUNT_COLUMNS}"),
            values,
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


async def update_account_check_result(account_id: int, *, status: str) -> dict[str, Any]:
    """Ghi kết quả của một lần kiểm tra sức khoẻ (xem app/services/accounts/account_health.py) - cố
    ý tách khỏi update_account ở trên: các cột này do hệ thống ghi từ một lần kiểm tra tự
    động, người dùng không bao giờ sửa qua form tài khoản, nên chúng hoàn toàn không nằm
    trong _ACCOUNT_CREATE_COLUMNS."""
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        # Kiểm tra này thụ động, không biết gì về phiên: nó từng ghi đè 'dead' mỗi 6 giờ, nên cookie vừa bị Facebook từ chối lại
        # được dùng tiếp. 'dead' chỉ mất khi có cookie mới, cùng quy ước với spider-hub.
        await cur.execute(
            "UPDATE platform_accounts SET last_checked_at = now(), last_check_status = %s "
            "WHERE id = %s AND NOT (last_check_status IS NOT DISTINCT FROM 'dead' "
            "AND (last_checked_at IS NULL OR updated_at IS NULL OR last_checked_at >= updated_at)) "
            f"RETURNING {ACCOUNT_COLUMNS}",
            (status, account_id),
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        row = await get_account(account_id)
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


async def set_account_enabled(account_id: int, enabled: bool) -> dict[str, Any]:
    """Bật/tắt tay một tài khoản từ dashboard. Tắt: chỉ enabled = false. Bật: như reactivate_account của
    spider-hub (db/accounts.py) - gỡ cả status 'checkpoint', cooldown và chuỗi lỗi, vì pool (get_accounts) bỏ qua tài
    khoản checkpoint/cooldown dù enabled = true; chỉ bật enabled thì tài khoản vẫn nằm ngoài pool."""
    await _ensure_pool_columns()
    reset = ", status = 'active', consecutive_failures = 0, cooldown_until = NULL" if enabled else ""
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            _trusted_sql(f"UPDATE platform_accounts SET enabled = %s{reset} WHERE id = %s RETURNING {ACCOUNT_COLUMNS}"),
            (enabled, account_id),
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


def proxy_pin_refusal(target: dict[str, Any]) -> str | None:
    """Lý do tài khoản không được ghim vào proxy này (spider-hub db/proxies.meta_proxy_violation).

    Nguyên tắc từ 12/11/2026: MỘT proxy chỉ thuộc MỘT tài khoản - không kể nền tảng
    (Meta nối Facebook với Threads qua IP, và một proxy của TikTok cũng là một IP). Nên
    bất kỳ tài khoản nào khác đang giữ cổng này đều là vi phạm.

    Ngoài ra Facebook phải đúng nhãn static, Threads chỉ bị chặn proxy tự xoay (#17 đang
    chờ 2 tài khoản Threads và chưa có IP thay thế)."""
    platform, ip_type = target["platform"], target["ip_type"]
    # Một proxy chỉ thuộc một tài khoản - không kể nền tảng (Meta nối Facebook với
    # Threads qua IP, và một proxy của TikTok cũng là một IP).
    if int(target["others"]):
        return "shared_ip"
    if platform not in ("facebook", "threads"):
        return None
    if ip_type == "rotating":
        return "rotating_ip"
    if target["proxy_platform"] == "public":
        return "public_proxy"
    if platform == "facebook" and ip_type != "static":
        return "ip_type_not_static"
    return None


async def reset_account_proxy(account_id: int) -> dict[str, Any]:
    """Xoá ghim proxy cố định của một tài khoản (assigned_proxy_id -> NULL) - xem
    services/pool.acquire_proxy_for_account của spider-hub. Ở lượt crawl/bootstrap kế
    tiếp, tài khoản được ghim lại vào một proxy TRỐNG (proxy chưa có tài khoản nào khác -
    mỗi proxy thuộc riêng một tài khoản); dùng từ dashboard khi bỏ một proxy đang được
    ghim, hoặc để ghim lại sau khi thêm proxy mới vào pool."""
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            f"UPDATE platform_accounts SET assigned_proxy_id = NULL WHERE id = %s RETURNING {ACCOUNT_COLUMNS}",
            (account_id,),
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


async def set_account_proxy(account_id: int, proxy_id: int) -> dict[str, Any]:
    """Ghim tay một tài khoản vào một proxy cụ thể - phần tương ứng trên dashboard của logic
    tự ghim vào proxy ít tải nhất của spider-hub (xem
    services/pool.acquire_proxy_for_account), cho người vận hành muốn trực tiếp quyết
    định tài khoản nào nằm trên IP nào thay vì để pool tự cân bằng. Ràng buộc khoá ngoại
    trên assigned_proxy_id (xem scripts/dev_db_schema.sql của spider-hub) từ chối
    proxy_id không tồn tại - lỗi hiện ra ở đây là lỗi psycopg thường, không xử lý riêng,
    vì ô chọn proxy trên dashboard chỉ đưa ra id thật.

    Tài khoản Meta phải qua chính sách IP (proxy_pin_refusal), cùng quy tắc spider-hub áp lúc giao proxy."""
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT a.platform, a.account_id, a.email, p.platform AS proxy_platform, p.ip_type, "
            "(SELECT count(*) FROM platform_accounts o JOIN platform_proxies same ON same.id = o.assigned_proxy_id "
            # Mọi tài khoản khác đã ghim cổng này, kể cả đang tắt / checkpoint: tài khoản tắt tạm vẫn giữ IP của nó
            # (cùng luật với pin_account_to_free_proxy của spider-hub). Muốn lấy cổng thì bỏ ghim tài khoản kia trước.
            " WHERE same.proxy_url = p.proxy_url AND o.id != a.id) AS others "
            "FROM platform_accounts a JOIN platform_proxies p ON p.id = %s WHERE a.id = %s",
            (proxy_id, account_id),
        )
        target = await cur.fetchone()
        if target and (refusal := proxy_pin_refusal(target)):
            raise ProxyPolicyError(refusal)
        await cur.execute(
            f"UPDATE platform_accounts SET assigned_proxy_id = %s WHERE id = %s RETURNING {ACCOUNT_COLUMNS}",
            (proxy_id, account_id),
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Account {account_id} not found")
    return row


async def delete_account(account_id: int) -> None:
    account = await get_account(account_id)
    if account is not None:
        await record_account_event(account, "account_deleted", last_check_status=account.get("last_check_status"))
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM platform_accounts WHERE id = %s", (account_id,))
        deleted = cur.rowcount
        await conn.commit()
    if deleted == 0:
        raise NotFoundError(f"Account {account_id} not found")


_PROXY_LIST_COLUMNS = (
    "pp.id, pp.platform, pp.proxy_url, pp.username, pp.password, pp.login_use_proxy, pp.ip_type, pp.enabled, "
    "pp.created_at, pp.updated_at, pp.status AS pool_status, pp.cooldown_until, pp.consecutive_failures, "
    "pp.last_used_at, pp.provider_key, pp.provider_ref, "
    # Số dòng platform_accounts còn sống đang được ghim cố định vào proxy này (đang bật,
    # không bị checkpoint) - khớp với pin_account_to_free_proxy của spider-hub để con số "tài
    # khoản đã ghim" trên dashboard đúng bằng mức tải mà pool thực sự cân bằng theo. Các
    # ghim chết vẫn nằm trên assigned_proxy_id nhưng không được làm một IP trông như đã
    # đầy. Nguyên tắc mới: một proxy thuộc riêng một tài khoản - giá trị này luôn 0 hoặc 1.
    "(SELECT count(*) FROM platform_accounts pa WHERE pa.assigned_proxy_id = pp.id "
    "AND pa.enabled = true AND pa.status != 'checkpoint') AS assigned_account_count"
)


async def list_proxies(platform: str | None = None) -> list[dict[str, Any]]:
    await _ensure_pool_columns()
    from app.services.accounts.config_db.proxy_settings import (
        _ensure_proxy_settings_tables,  # vòng import: proxy_settings dùng accounts
    )

    await _ensure_proxy_settings_tables()
    async with _connect() as conn, conn.cursor() as cur:
        # LEFT JOIN proxy_providers để biết dòng proxy này thuộc nguồn nào và nguồn đó
        # được gọi lần cuối lúc nào - trước đó không có quan hệ này, code phải đoán
        # chủ quan bằng cách so proxy_url với token của từng nguồn.
        join = "LEFT JOIN proxy_providers pv ON pv.key = pp.provider_key"
        columns = (
            _PROXY_LIST_COLUMNS + ", pv.last_used_at AS provider_last_used_at, pv.last_status AS provider_last_status"
        )
        if platform:
            await cur.execute(
                f"SELECT {columns} FROM platform_proxies pp {join} WHERE pp.platform = %s ORDER BY pp.id",
                (platform,),
            )
        else:
            await cur.execute(f"SELECT {columns} FROM platform_proxies pp {join} ORDER BY pp.platform, pp.id")
        return await cur.fetchall()


async def create_proxy(fields: dict[str, Any]) -> dict[str, Any]:
    await _ensure_pool_columns()
    columns = [c for c in _PROXY_CREATE_COLUMNS if c in fields]
    values = [fields[c] for c in columns]
    try:
        async with _connect() as conn, conn.cursor() as cur:
            await cur.execute(
                _trusted_sql(
                    f"INSERT INTO platform_proxies ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) "
                    f"RETURNING {PROXY_COLUMNS}"
                ),
                values,
            )
            row = await cur.fetchone()
            await conn.commit()
            return row  # type: ignore[return-value]
    except psycopg.errors.UniqueViolation as exc:
        raise ConflictError(f"A proxy for {fields.get('platform')}/{fields.get('proxy_url')} already exists") from exc


async def update_proxy(proxy_id: int, fields: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in _PROXY_CREATE_COLUMNS if c in fields]
    if not columns:
        raise NotFoundError("No fields to update")
    set_clause = ", ".join(f"{c} = %s" for c in columns)
    # Sửa địa chỉ / thông tin đăng nhập hoặc bật lại: gỡ cờ degraded + cooldown + chuỗi lỗi do cấu hình cũ gây ra (16/11/2026:
    # username lưu thừa dấu ":" -> 407, sửa đúng rồi proxy vẫn bị khoá thêm vài giờ).
    if any(c in columns for c in ("proxy_url", "username", "password")) or fields.get("enabled") is True:
        set_clause += ", status = 'active', consecutive_failures = 0, cooldown_until = NULL"
    values = [fields[c] for c in columns] + [proxy_id]
    await _ensure_pool_columns()
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute(
            _trusted_sql(
                f"UPDATE platform_proxies SET {set_clause}, updated_at = now() WHERE id = %s RETURNING {PROXY_COLUMNS}"
            ),
            values,
        )
        row = await cur.fetchone()
        await conn.commit()
    if row is None:
        raise NotFoundError(f"Proxy {proxy_id} not found")
    return row


async def delete_proxy(proxy_id: int) -> None:
    async with _connect() as conn, conn.cursor() as cur:
        await cur.execute("DELETE FROM platform_proxies WHERE id = %s", (proxy_id,))
        deleted = cur.rowcount
        await conn.commit()
    if deleted == 0:
        raise NotFoundError(f"Proxy {proxy_id} not found")
