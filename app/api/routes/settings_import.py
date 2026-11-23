"""Route nhập hàng loạt tài khoản/proxy trong Settings: đọc thử và ghi."""

from __future__ import annotations

from fastapi import APIRouter

from app.ai.tasks.import_parser import parse_import
from app.api.routes.settings_cookies import record_cookie_saved, refuse_bad_cookie, with_user_agent
from app.core.errors import ValidationError
from app.core.logging import get_logger
from app.schemas.settings import (
    ImportCommitRequest,
    ImportCommitResponse,
    ImportParseRequest,
    ImportParseResponse,
)
from app.services.accounts.config_db.accounts import create_account, create_proxy

logger = get_logger(__name__)
router = APIRouter(prefix="/settings", tags=["settings"])


@router.post("/import/parse", response_model=ImportParseResponse)
async def import_parse(payload: ImportParseRequest) -> ImportParseResponse:
    """Bước 1 của import hàng loạt có AI hỗ trợ (xem app/ai/tasks/import_parser.py) - biến
    dữ liệu tài khoản/proxy dán vào dạng tự do thành các dòng ứng viên có cấu trúc để
    dashboard hiển thị thành bản xem trước sửa được. Không bao giờ tự ghi vào DB; xem
    /import/commit bên dưới cho việc đó."""
    try:
        rows = await parse_import(payload.target, payload.format_hint, payload.content)
    except Exception as exc:
        raise ValidationError(
            f"Could not parse the pasted content ({exc}) - try describing the format more precisely, "
            "or pasting a smaller batch."
        ) from exc
    if not rows:
        raise ValidationError("No rows could be extracted from the pasted content - check the format description.")
    return ImportParseResponse(rows=rows)


@router.post("/import/commit", response_model=ImportCommitResponse)
async def import_commit(payload: ImportCommitRequest) -> ImportCommitResponse:
    """Bước 2 - người vận hành đã xem/sửa bản xem trước của /import/parse và đang xác
    nhận. Ghi từng dòng bằng create_account/create_proxy (cùng đường cột được cho phép
    mà form thêm tài khoản/proxy thông thường dùng, xem config_db/accounts.py) - dòng nào
    lỗi (ví dụ account_id trùng) thì bỏ qua, không làm hỏng phần còn lại của lô."""
    created = 0
    failed = 0
    for row in payload.rows:
        # Giữ "enabled" mà người vận hành đã đặt rõ khi sửa bản xem trước /import/parse (ví
        # dụ bỏ chọn một dòng đáng ngờ) - chỉ mặc định True khi dòng không có trường này.
        fields = {**row, "platform": payload.platform, "enabled": row.get("enabled", True)}
        try:
            if payload.target == "accounts":
                account_fields = with_user_agent(fields)
                refuse_bad_cookie(
                    payload.platform, str(account_fields.get("account_id") or ""), account_fields.get("cookie") or ""
                )
                await record_cookie_saved(await create_account(account_fields))
            else:
                await create_proxy(fields)
            created += 1
        except Exception as exc:  # noqa: BLE001 - một dòng nhập lỗi chỉ tính vào số dòng thất bại, các dòng khác vẫn lưu
            logger.warning("import_commit_row_failed", target=payload.target, error=exc)
            failed += 1
    return ImportCommitResponse(created=created, failed=failed)
