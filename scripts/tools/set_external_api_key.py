"""Lưu key API của nguồn ngoài vào D1 config (bảng external_api_keys) - cinemark-api đọc key từ đây, .env chỉ là dự phòng.

    python -m scripts.tools.set_external_api_key trailerix <key>
    python -m scripts.tools.set_external_api_key trailerix ""      # xoá, quay về .env

Chỉ in 4 ký tự cuối của key."""

import asyncio
import sys

from app.services.accounts.config_db.d1_ops import get_external_api_key, set_external_api_key


async def main(name: str, key: str) -> None:
    await set_external_api_key(name, key.strip())
    saved = await get_external_api_key(name) or ""
    print(f"{name}: {'đã lưu ...' + saved[-4:] if saved else 'đã xoá (dùng .env)'}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("cách dùng: python -m scripts.tools.set_external_api_key <tên> <key>")
    asyncio.run(main(sys.argv[1], sys.argv[2]))
