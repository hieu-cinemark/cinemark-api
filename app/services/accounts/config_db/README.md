# app/services/accounts/config_db

Các bảng cấu hình trên Postgres (Supabase), tách từ `platform_config_db.py` (17/11/2026). `platform_config_db.py` còn là cửa ngõ re-export nên code cũ chạy như cũ; code mới import thẳng module con. Test monkeypatch phải patch module con (nơi hàm thật sự tra tên).

## Các file

| File | Việc nó làm |
|---|---|
| `conn.py` | Pool kết nối Postgres (Supabase) dùng chung cho mọi bảng cấu hình. |
| `accounts.py` | Tài khoản nền tảng (platform_accounts) và proxy (platform_proxies): đọc/ghi từ trang Settings, ghim proxy, sự kiện tài khoản. |
| `schedules.py` | Từ khoá lọc, lịch cào bài và lịch cào bình luận (giờ Việt Nam, sửa trên dashboard). |
| `ai_config.py` | Cấu hình AI: ai_settings (bật/tắt, prompt, provider đang dùng), ai_providers (base_url, key, model, reasoning_effort) và lịch sử đo độ chính xác. |
| `proxy_settings.py` | Cấu hình cào (proxy_settings) và nguồn proxy (proxy_providers). |
| `cleanup.py` | Cài đặt và lịch sử lượt dọn bài không liên quan (cleanup_settings, cleanup_run_history). |
| `omar.py` | Cài đặt khám phá tự động và Omar API (key, trần request/phút, tổng request mỗi ngày, số request đồng thời). |
