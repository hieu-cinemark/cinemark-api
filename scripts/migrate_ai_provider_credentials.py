"""Migrate một lần: chép thông tin đăng nhập Kira/Bee từ .env
(KIRA_BASE_URL/KIRA_API_KEY, BEEKNOEE_BASE_URL/BEEKNOEE_API_KEY) cùng model đang có
hiệu lực vào bảng ai_providers mới trên Supabase (xem
app/services/platform_config_db.py), để app/ai/client.py nạp từ đó thay vì từ biến
env. Chạy lại an toàn - upsert theo key.

Đọc thẳng .env (không qua app.core.config.Settings, vốn không còn khai báo các trường
này - chúng đã bị xoá khỏi đó trên môi trường mà migration này chạy lần đầu) để vẫn
chạy được trên bất kỳ môi trường KHÁC nào (staging/.env của server khác) chưa
migrate. Xoá các dòng KIRA_*/BEEKNOEE_* khỏi .env của môi trường đó khi đã chạy thành
công ở đó.

Cách dùng: .venv/bin/python -m scripts.migrate_ai_provider_credentials
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from dotenv import dotenv_values

from app.services.platform_config_db import get_ai_provider, get_ai_settings, upsert_ai_provider

_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


def _env(name: str) -> str | None:
    # Env thật của tiến trình được ưu tiên (khớp thứ tự ưu tiên của pydantic-settings),
    # quay về giá trị còn trong .env.
    return os.getenv(name) or dotenv_values(_ENV_PATH).get(name) or None


async def main() -> None:
    kira_base_url, kira_api_key = _env("KIRA_BASE_URL"), _env("KIRA_API_KEY")
    if kira_base_url and kira_api_key:
        # Không có tên model trong code: KIRA_MODEL, nếu không thì lấy giá trị Supabase đang
        # giữ (dòng provider, rồi tới ai_settings.model cũ).
        existing = await get_ai_provider("kira") or {}
        legacy = await get_ai_settings()
        kira_model = (
            _env("KIRA_MODEL") or str(existing.get("model") or "").strip() or str(legacy.get("model") or "").strip()
        )
        await upsert_ai_provider("kira", base_url=kira_base_url, api_key=kira_api_key, model=kira_model)
        print("migrated kira provider credentials")
    else:
        print("skipped kira: KIRA_BASE_URL/KIRA_API_KEY not set in .env")

    bee_base_url, bee_api_key = _env("BEEKNOEE_BASE_URL"), _env("BEEKNOEE_API_KEY")
    if bee_base_url and bee_api_key:
        existing = await get_ai_provider("bee") or {}
        bee_model = _env("BEEKNOEE_MODEL") or str(existing.get("model") or "").strip()
        await upsert_ai_provider("bee", base_url=bee_base_url, api_key=bee_api_key, model=bee_model)
        print("migrated bee provider credentials")
    else:
        print("skipped bee: BEEKNOEE_BASE_URL/BEEKNOEE_API_KEY not set in .env")

    for key in ("kira", "bee"):
        row = await get_ai_provider(key)
        if row:
            print(
                f"verify {key}: base_url={row['base_url']!r} model={row['model']!r} api_key_set={bool(row['api_key'])}"
            )
            if not row["model"]:
                print(f"  -> {key} has no model yet: set it in the dashboard (Settings > AI providers)")
        else:
            print(f"verify {key}: no row")


if __name__ == "__main__":
    asyncio.run(main())
