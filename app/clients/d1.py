"""Tầng truyền tải Cloudflare D1 - nơi duy nhất biết "database D1" nghĩa là HTTP query
API của Cloudflare hay một bản SQLite sao chép ở local (xem DB_MODE trong
app/core/config.py). Mọi repository (app/repositories/d1/*) và các hàm còn lại của
app/services/d1.py đều gọi d1_query() ở đây thay vì đụng thẳng vào sqlite3/httpx,
nên sau này có đổi (sang hẳn một kiểu lưu trữ khác, hoặc chỉ là API của D1 thay
đổi) thì chỉ có đúng một chỗ phải sửa.

Tách ra khỏi app/services/d1.py để các repository theo từng bảng có thể phụ thuộc
vào tầng truyền tải này mà không phụ thuộc vào chính d1.py (ngược lại, d1.py
re-export các hàm của repository cho những chỗ gọi vẫn dùng
`from app.services.d1 import persist_post` v.v.) - import d1.py từ một repository sẽ
thành import vòng."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path
from typing import Any

import httpx

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_BASE_URL = "https://api.cloudflare.com/client/v4"

_local_conn: sqlite3.Connection | None = None
_local_lock = threading.Lock()
_logged_db_target = False

# HTTP client dùng chung cho chế độ remote + giới hạn số request đồng thời. Tạo một
# AsyncClient mới cho mỗi query (bắt tay TLS với api.cloudflare.com) khi dashboard
# gọi dồn /stats/* cùng lúc với các lượt ghi của ingest sẽ làm nghẽn event loop, và đã
# từng treo cả API (kể cả /health) sau khi chuyển sang DB_MODE=remote.
_http_client: httpx.AsyncClient | None = None
_http_client_lock = asyncio.Lock()
_remote_sem = asyncio.Semaphore(8)


def _get_local_conn() -> sqlite3.Connection:
    global _local_conn
    if _local_conn is None:
        # timeout=30: mặc định của stdlib (5s) chính là cái đã hết hạn thành lỗi "database
        # is locked" (đã xác nhận thực tế 2026-09-17 - cứ khoảng 5s lại có một loạt lỗi này
        # trong lúc scripts/pull_local_db.py đang dựng lại chính file này song song) - 30s dư
        # sức chờ hết thời gian giữ khoá của một lệnh INSERT/UPDATE, không chỉ riêng lần dựng
        # lại vài phút thỉnh thoảng của script đó.
        #
        # Chế độ journal WAL: mặc định của stdlib (DELETE/rollback-journal) khoá độc quyền
        # *toàn bộ file* trong suốt một lần ghi, chặn cả những lượt đọc không liên quan - WAL
        # cho phép đọc tiếp trên snapshot đã commit gần nhất trong khi đang có lượt ghi, đúng
        # thứ mà các endpoint dashboard đọc nhiều của service này cần khi chạy song song với
        # các lượt ghi từ Kafka.
        conn = sqlite3.connect(settings.local_db_path, check_same_thread=False, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = sqlite3.Row
        _local_conn = conn
    return _local_conn


def _run_local_query(sql: str, params: list[Any] | None) -> list[dict[str, Any]]:
    """Chạy đồng bộ trên một worker thread (xem d1_query bên dưới) - sqlite3 của stdlib
    Python không có API async, và chặn thẳng event loop sẽ làm treo mọi request khác
    đang xử lý trong suốt thời gian chạy query."""
    conn = _get_local_conn()
    # Một connection dùng chung, nhiều worker asyncio.to_thread - nếu không khoá, SQLite
    # sẽ chạy xen kẽ execute/fetch và raise "bad parameter or other API misuse" /
    # IndexError ở dict(row).
    with _local_lock:
        cursor = conn.execute(sql, params or [])
        # HTTP API của D1 trả về [] (không phải lỗi) cho INSERT/UPDATE/DELETE thành công mà
        # không có dòng nào trả về - làm giống vậy ở đây để kiểm tra `is None` (thất bại) so
        # với `[]`/các dòng (thành công) của chỗ gọi chạy giống hệt nhau ở cả hai chế độ.
        rows = [] if cursor.description is None else [dict(row) for row in cursor.fetchall()]
        # Commit mỗi khi câu lệnh đã mở một transaction ghi - kể cả DML có trả về dòng
        # (DELETE/UPDATE ... RETURNING), nếu không nó sẽ giữ khoá ghi và bị rollback khi tiến
        # trình thoát.
        if conn.in_transaction:
            conn.commit()
        return rows


def _configured() -> bool:
    if settings.db_mode == "local":
        return Path(settings.local_db_path).exists()
    return bool(settings.cloudflare_account_id and settings.cloudflare_api_token and settings.cloudflare_d1_database_id)


async def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is not None and not _http_client.is_closed:
        return _http_client
    async with _http_client_lock:
        if _http_client is not None and not _http_client.is_closed:
            return _http_client
        # Timeout mặc định được ghi đè cho từng request bên dưới; limits giúp một lần refresh
        # dashboard không mở hàng chục phiên TLS tới Cloudflare.
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),
        )
        return _http_client


# Mã lỗi Cloudflare 7429 = "D1 DB storage operation exceeded timeout which caused
# object to be reset". HTTP API trả lỗi này dưới dạng 429 với body JSON có
# `errors[0].code` là 7429; storage object ở trạng thái từ chối 429 này khoảng 30-60s
# trong lúc Cloudflare dựng lại handle bên dưới. Một chỗ gọi đơn giản (hành vi hiện
# tại) chỉ trả về None ở lần 429 đầu tiên rồi dashboard / scheduler đi tiếp, nhưng
# một job theo lịch chạy đúng trong khoảng đó sẽ thất bại liên tục từng request một
# trong suốt thời gian backoff - đúng kiểu lỗi dây chuyền đã hiện ra thành
# scheduled_crawl_firing -> d1_request_failed (429) trong log của người vận hành. Thử
# lại với backoff tăng dần tới khoảng 60s là phủ hết khoảng backoff mà không giữ event
# loop lâu hơn mức một request dashboard bình thường nên chờ.
_D1_STORAGE_BACKOFF_MAX_RETRIES = 4
_D1_STORAGE_BACKOFF_BASE_SECONDS = 2.0
_D1_STORAGE_BACKOFF_CAP_SECONDS = 30.0


def _is_storage_backoff_response(status_code: int, body_text: str) -> bool:
    """True nếu lỗi 429 này là trường hợp backoff storage của Cloudflare có thể tự hồi
    phục (mã 7429) chứ không phải lỗi auth/quota vĩnh viễn. So khớp theo chuỗi con của
    mã lỗi - body có dạng '{"success":false,"errors":[{"code":7429,"message":"..."}]}'
    và không cần parse JSON chỉ để kiểm tra chừng này."""
    if status_code != 429:
        return False
    return "7429" in body_text


async def d1_query(
    sql: str,
    params: list[Any] | None = None,
    *,
    quiet: bool = False,
    timeout: float = 10.0,
    max_retries: int = _D1_STORAGE_BACKOFF_MAX_RETRIES,
) -> list[dict[str, Any]] | None:
    """Chạy một câu SQL trên database D1 đã cấu hình. Trả về các dòng kết quả, hoặc None
    nếu D1 chưa được cấu hình hoặc lời gọi thất bại. quiet=True bỏ qua log lỗi (dùng cho
    migration idempotent vốn chờ lỗi 'duplicate column' trên DB đã migrate rồi).

    Khi gặp 429 backoff storage của D1 Cloudflare (mã lỗi 7429), thử lại với backoff
    tăng dần tối đa max_retries lần - xem _D1_STORAGE_BACKOFF_MAX_RETRIES. Cả chuỗi thử
    lại bị giới hạn khoảng 60s tổng thời gian sleep để một request dashboard không bị
    treo vì một database hỏng hẳn; nếu lần thử nào cũng 429 thì hàm vẫn trả về None kèm
    một log cảnh báo cuối cùng để chỗ gọi tự quyết định làm gì."""
    if not _configured():
        return None

    global _logged_db_target
    if not _logged_db_target:
        _logged_db_target = True
        logger.info(
            "d1_target",
            mode=settings.db_mode,
            path=settings.local_db_path if settings.db_mode == "local" else None,
        )

    if settings.db_mode == "local":
        try:
            return await asyncio.to_thread(_run_local_query, sql, params)
        except (sqlite3.Error, IndexError) as exc:
            if not quiet:
                logger.warning("local_db_query_failed", error=str(exc), sql=sql[:200])
            return None

    url = (
        f"{_BASE_URL}/accounts/{settings.cloudflare_account_id}/d1/database/{settings.cloudflare_d1_database_id}/query"
    )
    headers = {"Authorization": f"Bearer {settings.cloudflare_api_token}"}

    attempt = 0
    while True:
        try:
            async with _remote_sem:
                client = await _get_http_client()
                resp = await client.post(
                    url,
                    headers=headers,
                    json={"sql": sql, "params": params or []},
                    timeout=timeout,
                )
        except httpx.HTTPError as exc:
            if not quiet:
                logger.warning(
                    "d1_request_failed",
                    error=str(exc) or repr(exc),
                    error_type=type(exc).__name__,
                    attempt=attempt,
                )
            return None

        # 429 do backoff storage: thử lại với backoff, không coi là lỗi hẳn. Đây là loại 429
        # DUY NHẤT được thử lại - rate-limit thật (body không có mã 7429) hoặc lỗi 4xx
        # auth/quota vẫn trả về None cho chỗ gọi để các nhánh xử lý lỗi hiện có không đổi.
        if resp.status_code == 429 and _is_storage_backoff_response(resp.status_code, resp.text):
            attempt += 1
            if attempt > max_retries:
                if not quiet:
                    logger.warning(
                        "d1_storage_backoff_retries_exhausted",
                        status=resp.status_code,
                        body=resp.text[:500],
                        attempts=attempt - 1,
                    )
                return None
            sleep_seconds = min(
                _D1_STORAGE_BACKOFF_CAP_SECONDS, _D1_STORAGE_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            )
            logger.warning(
                "d1_storage_backoff_retry",
                status=resp.status_code,
                body=resp.text[:200],
                attempt=attempt,
                sleep_seconds=sleep_seconds,
            )
            await asyncio.sleep(sleep_seconds)
            continue

        if resp.status_code >= 400:
            if not quiet:
                logger.warning("d1_request_failed", status=resp.status_code, body=resp.text[:500])
            return None

        data = resp.json()
        if not data.get("success"):
            if not quiet:
                logger.warning("d1_query_failed", errors=data.get("errors"))
            return None

        results = data.get("result") or []
        return results[0].get("results", []) if results else []


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except TypeError, ValueError:
        return 0
