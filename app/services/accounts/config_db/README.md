# app/services/accounts/config_db

Các bảng cấu hình trên D1 `cinemark-config-db` (xem [docs/config-db-d1.md](../../../../docs/config-db-d1.md)). Mỗi file ở đây là lớp mỏng giữ tên hàm, việc truy vấn nằm ở `d1_store.py`, `d1_ops.py`, `d1_accounts.py`. Code import thẳng module con; test monkeypatch phải patch module con (nơi hàm thật sự tra tên).

## Các file

| File | Việc nó làm |
|---|---|
| `d1_store.py`, `d1_ops.py`, `d1_accounts.py` | Truy vấn D1 (SQLAlchemy Core -> `d1_exec`): AI + proxy settings / vận hành / tài khoản + proxy. |
| `accounts.py` | Tài khoản nền tảng (platform_accounts) và proxy (platform_proxies): đọc/ghi từ trang Settings, ghim proxy, sự kiện tài khoản. |
| `schedules.py` | Từ khoá lọc, lịch cào bài và lịch cào bình luận (giờ Việt Nam, sửa trên dashboard). |
| `ai_config.py` | Cấu hình AI: ai_settings (bật/tắt, prompt, provider đang dùng), ai_providers (base_url, key, model, reasoning_effort) và lịch sử đo độ chính xác. |
| `proxy_settings.py` | Cấu hình cào (proxy_settings) và nguồn proxy (proxy_providers). |
| `cleanup.py` | Cài đặt và lịch sử lượt dọn bài không liên quan (cleanup_settings, cleanup_run_history). |
| `omar.py` | Cài đặt khám phá tự động và Omar API (key, trần request/phút, tổng request mỗi ngày, số request đồng thời). |
