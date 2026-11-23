# Chuyển bảng cấu hình từ Supabase sang D1

Giai đoạn 1 (23/11/2026): `ai_settings`, `ai_providers`, `ai_accuracy_runs`, `proxy_settings` có thể nằm ở D1 `cinemark-config-db`
thay vì Supabase. Mặc định vẫn là Supabase; chọn nơi lưu bằng `CONFIG_DB_BACKEND=postgres|d1` (cinemark-api và spider-hub).

## Cách hoạt động

| Phần | Việc |
|---|---|
| `app/services/accounts/config_db/d1_store.py` | Bản D1 của 4 bảng, dựng query bằng SQLAlchemy Core (dialect SQLite), gửi qua `d1_exec(..., database="config")`. Tự tạo bảng (IF NOT EXISTS). |
| `config_db/ai_config.py`, `config_db/proxy_settings.py` | Mỗi hàm có một nhánh `if d1_store.enabled()` ở đầu; chữ ký và dữ liệu trả về giữ nguyên (bool, dict, datetime). |
| `scripts/tools/copy_config_to_d1.py` | Chép dữ liệu từ Postgres sang D1 (mặc định chỉ xem trước, `--apply` mới ghi; chạy lại được). |
| spider-hub `social_crawler/db/d1.py` | Client D1 đồng bộ (httpx) để đọc `ai_settings`, `ai_providers`, `proxy_settings`; cache 60 giây như cũ. |

Chưa chuyển (giai đoạn sau): `proxy_providers` + `platform_proxies` (xoá nguồn proxy cần đếm proxy đang dùng nó, nên hai bảng đi cùng
nhau), `platform_accounts`, `account_events`, `crawl_schedules`, `comment_crawl_schedules`, `filter_keywords`, `cleanup_*`, `discovery_settings`,
`omar_api_settings`.

## Cutover (làm đúng thứ tự)

1. Thêm `CLOUDFLARE_D1_CONFIG_DATABASE_ID` (id của `cinemark-config-db`, xem `wrangler d1 list`) vào `.env` của cinemark-api và `CLOUDFLARE_ACCOUNT_ID`,
   `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_D1_CONFIG_DATABASE_ID` vào `.env` của spider-hub (VPS và Mac). Token cần quyền D1 Edit.
2. Deploy bản mới của cả hai repo (cờ còn là `postgres`, chưa đổi hành vi).
3. Ngừng sửa cấu hình trên dashboard, rồi chép lại dữ liệu mới nhất:
   `python -m scripts.tools.copy_config_to_d1` (xem trước) rồi `--apply`.
4. Đặt `CONFIG_DB_BACKEND=d1` ở cinemark-api (web + scheduler) và spider-hub (mọi tiến trình), restart. Hai bên phải cùng giá trị: bên còn
   `postgres` sẽ đọc dữ liệu cũ.
5. Mở dashboard, sửa thử một cài đặt AI/proxy, kiểm tra spider-hub nhận trong vòng một phút.
6. Quay lại: đặt `CONFIG_DB_BACKEND=postgres` ở cả hai bên và restart (dữ liệu sửa lúc ở D1 sẽ không tự về Postgres).

## Lưu ý

- Worker cinemark vẫn đọc cấu hình AI qua bảng `ai_runtime` (đồng bộ từ `app/ai/runtime.py`), không đổi.
- D1 không có transaction nhiều câu hay `SELECT ... FOR UPDATE`: các bảng giai đoạn 1 chỉ có ghi một dòng nên không cần; giai đoạn
  `platform_accounts` phải thiết kế lại chỗ chọn tài khoản rảnh.
- `proxy_settings` ~90 khoá lưu một dòng JSON (TEXT) - D1 giới hạn mỗi dòng 2 MB, hiện dùng chưa tới 10 KB.
