"""Kéo một bản sao SQLite local đầy đủ của database D1 thật (của cinemark-scraper, xem
cloudflare_d1_database_id trong app/core/config.py) để dùng DB_MODE=local khi
dev/test ở local mà không tốn quota đọc dòng hằng ngày của tài khoản D1 thật - xem
comment của setting đó để biết vì sao có script này (bản thay thế của repo này cho
script pull của repo anh em cinemark-be, vốn không còn trong workspace này).

Tạo lại đúng schema của mọi bảng (đọc mới từ sqlite_master của chính D1, nên không
thể lệch với thực tế) VÀ chép mọi dòng của mọi bảng - khác với một phiên bản trước,
hẹp hơn của script này chỉ tạo sẵn movies/keywords, đây là bản sao đầy đủ thật sự
(các trang Tổng quan/Bài viết của dashboard đọc qua cùng công tắc DB_MODE, nên một
bản sao thiếu khiến chúng trông như gần hết dữ liệu lịch sử đã biến mất). Lấy từng
bảng theo trang LIMIT/OFFSET (xem _PAGE_SIZE) thay vì một câu SELECT * khổng lồ, vì
vài bảng (posts, post_engagement_snapshots - mỗi bảng tới hàng chục nghìn dòng, có
dòng mang khối raw_json khá lớn) có nguy cơ vượt trần kích thước response mỗi query
của D1 nếu lấy trong một request không phân trang.

Luôn đọc từ D1 thật bất kể setting DB_MODE hiện tại (ép remote trong suốt script này)
- mục đích là chép *từ* remote *vào* local, nên tuyệt đối không được vô tình đọc file
local cũ mà nó sắp ghi đè.

Chạy lại riêng lẻ lúc nào cũng an toàn - mỗi lần đều xoá và tạo lại mọi bảng. KHÔNG
an toàn khi server của app.main (uvicorn) đang chạy và dùng DB_MODE=local trên cùng
đường dẫn: tiến trình đó giữ connection sqlite3 sống lâu của riêng nó (xem
_local_conn trong app/services/d1.py) mở trên inode hiện tại của file; unlink() ở đây
tách đường dẫn khỏi inode đó mà không đóng handle đang mở của ai khác, nên server đang
chạy cứ âm thầm đọc/ghi một bản mồ côi mà không ai còn thấy qua đường dẫn nữa, trong
khi mọi connection *mới* (kể cả của chính script này) lấy bản mới - đã xác nhận thực
tế (2026-09-17): chạy script này cùng lúc với server đang chạy sinh ra một loạt lỗi
"database is locked" do các lần ghi từ Kafka của server va với các lần commit của
script này trên cùng đường dẫn giữa lúc dựng lại, và lẽ ra còn âm thầm lệch tiếp sau
đó kể cả khi lỗi khoá đã hết. Dừng server trước, kéo dữ liệu, rồi khởi động lại -
chính việc khởi động lại mới làm nó mở connection mới trên file mới.

Kéo toàn bộ một database cỡ này mất vài phút và tốn quota đọc D1 thật (tỉ lệ với tổng
số dòng) - đó là chi phí một lần, một chiều để có bản sao local; sau đó không có gì
(dev/test local với DB_MODE=local) tốn thêm quota cho tới lần làm mới tiếp theo.

    python -m scripts.pull_local_db
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

# Số dòng mỗi trang D1 - thấp khá xa so với trần kích thước response của D1 kể cả với
# các dòng posts có raw_json, mà vẫn giữ tổng số request cho một bảng khoảng 12 nghìn
# dòng ở mức vài chục, không phải vài trăm.
_PAGE_SIZE = 500


async def _copy_table(d1_query, conn: sqlite3.Connection, table_name: str) -> None:
    offset = 0
    total = 0
    columns: list[str] | None = None
    while True:
        rows = await d1_query(f"SELECT * FROM {table_name} LIMIT {_PAGE_SIZE} OFFSET {offset}")
        if rows is None:
            raise RuntimeError(f"D1 read failed for table={table_name!r} at offset={offset} - see logged error above.")
        if not rows:
            break
        if columns is None:
            columns = list(rows[0].keys())
            placeholders = ", ".join("?" for _ in columns)
            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        conn.executemany(insert_sql, [[row[c] for c in columns] for row in rows])  # type: ignore[union-attr]
        conn.commit()
        total += len(rows)
        offset += _PAGE_SIZE
        if len(rows) < _PAGE_SIZE:
            break
    if total:
        logger.info("local_db_table_copied", table=table_name, rows=total)
    else:
        logger.info("local_db_table_empty", table=table_name)


async def pull() -> None:
    # Xem docstring module - chạy script này khi server của app.main đang chạy trên cùng
    # local_db_path gây ra vấn đề thật, đã xác nhận (tranh khoá trong lúc dựng lại, âm thầm
    # lệch sau đó).
    logger.warning(
        "local_db_pull_starting",
        path=settings.local_db_path,
        warning="stop any server using DB_MODE=local against this path first, and restart it after this finishes",
    )
    settings.db_mode = "remote"  # xem docstring module - không bao giờ đọc file local ở đây
    from app.services.d1 import d1_query  # import sau khi đã ép remote, không phải lúc nạp module

    if not (settings.cloudflare_account_id and settings.cloudflare_api_token and settings.cloudflare_d1_database_id):
        raise RuntimeError(
            "CLOUDFLARE_ACCOUNT_ID/CLOUDFLARE_API_TOKEN/CLOUDFLARE_D1_DATABASE_ID must be set (in .env) to pull from "
            "the real D1 - this script has nothing to copy without them."
        )

    tables = await d1_query(
        "SELECT name, sql FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' AND name NOT LIKE 'd1_%'"
    )
    if not tables:
        raise RuntimeError("Could not read the real D1's schema (empty/failed response) - check D1 credentials/quota.")

    local_path = Path(settings.local_db_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if local_path.exists():
        local_path.unlink()  # file mới mỗi lần chạy - xem docstring module

    conn = sqlite3.connect(local_path)
    try:
        for table in tables:
            conn.execute(table["sql"])
        conn.commit()
        logger.info("local_db_schema_created", tables=[t["name"] for t in tables], path=str(local_path))

        for table in tables:
            await _copy_table(d1_query, conn, table["name"])
    finally:
        conn.close()

    logger.info("local_db_pull_finished", path=str(local_path), telegram=True)


if __name__ == "__main__":
    asyncio.run(pull())
