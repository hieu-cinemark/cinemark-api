# Bảng cấu hình trên D1 (`cinemark-config-db`)

Từ 23/11/2026 toàn bộ bảng cấu hình nằm ở D1 `cinemark-config-db`; Supabase/Postgres đã được gỡ khỏi code của cinemark-api và spider-hub.

## Bảng và nơi cài đặt

| Nhóm | Bảng | cinemark-api | spider-hub |
|---|---|---|---|
| AI + cào | `ai_settings`, `ai_providers`, `ai_accuracy_runs`, `proxy_settings` | `config_db/d1_store.py` | `db/config.py`, `db/proxy_settings.py` (đọc) |
| Vận hành | `filter_keywords`, `crawl_schedules`, `comment_crawl_schedules`, `cleanup_settings`, `cleanup_run_history`, `discovery_settings`, `omar_api_settings` | `config_db/d1_ops.py` | không đọc |
| Tài khoản/proxy | `platform_accounts`, `platform_proxies`, `proxy_providers`, `account_events` | `config_db/d1_accounts.py` | `db/d1_pool.py` (đọc + ghi, qua `accounts.py`, `proxies.py`, `account_events.py`, `proxy_settings.py`, `runtime/proxy_lease.py`) |

- cinemark-api dựng query bằng SQLAlchemy Core (dialect SQLite) rồi gửi qua `d1_exec(..., database="config")`. Các file `config_db/accounts.py`,
  `schedules.py`, `ai_config.py`, `cleanup.py`, `omar.py`, `proxy_settings.py` là lớp mỏng giữ tên hàm cũ cho nơi gọi.
- cinemark-api là nơi TẠO bảng (`ensure_tables`, `IF NOT EXISTS`, chạy ở lần dùng đầu và khi khởi động). spider-hub chỉ đọc/ghi dữ liệu, nên
  deploy cinemark-api trước khi chạy spider-hub trên một database trống.
- spider-hub không dùng SQLAlchemy: `db/d1.py` là client httpx đồng bộ, `db/d1_pool.py` viết SQL SQLite trực tiếp (thời gian tính ở Python).
  Script trong `scripts/` dùng `db/script_connect.py`.
- Định dạng thời gian trong D1: chuỗi ISO-8601 UTC độ rộng cố định (`2026-11-23T02:17:34.025795+00:00`) để so sánh chuỗi đúng thứ tự.
- Chọn-và-giữ chỗ tài khoản/proxy (`claim_account`, `claim_proxy`) là MỘT câu `UPDATE ... WHERE id = (SELECT ... LIMIT 1) RETURNING`; D1 chạy từng
  câu nguyên tử nên không cần khoá dòng.
- Cooldown luỹ thừa tính bằng `strftime('now', '+N minutes')` với số mũ chặn ở 20 và trần 2 giờ / `cooldown_max_minutes`.

## Biến môi trường

- cinemark-api: `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_D1_CONFIG_DATABASE_ID` (id của `cinemark-config-db`, xem `wrangler d1 list`).
- spider-hub: cùng ba biến (VPS và Mac). Token cần quyền D1 Edit.

## Lưu ý

- Worker cinemark vẫn đọc cấu hình AI qua bảng `ai_runtime` (đồng bộ từ `app/services/ops/ai_runtime_sync.py`).
- D1 không có transaction nhiều câu: các hàm chỉ ghi một dòng mỗi câu nên không cần; `delete_proxy_provider` kiểm tra rồi mới xoá (có khoảng
  hở rất nhỏ giữa hai bước, chấp nhận được với thao tác thủ công trên dashboard).
- Mỗi lượt cào gọi D1 qua mạng (vài chục đến vài trăm ms mỗi câu). Cài đặt vẫn cache 60 giây ở spider-hub.
- Schema D1 khai ở `d1_store.py`/`d1_ops.py`/`d1_accounts.py` (cinemark-api); `tests/test_d1_pool.py` của spider-hub có bản DDL chép tay để chạy SQL
  thật trên SQLite - đổi cột ở cinemark-api thì sửa cả hai.
- Sao lưu: Supabase cũ chưa bị đóng; giữ một bản `pg_dump` trước khi xoá project.
