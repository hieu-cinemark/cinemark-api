# app/repositories/d1

SQL trên D1 `cinemark-scraper`.

| File | Bảng |
|---|---|
| `store.py` | phim, từ khoá, report, thống kê và các truy vấn dùng chung |
| `posts.py`, `comments.py` | bài viết, bình luận |
| `comment_rollup.py` | bảng tổng hợp bình luận theo phim x ngày |
| `topics.py` | chủ đề bình luận Worker đọc |
| `press_reviews.py` | tin báo chí sau công chiếu |
| `sql.py` | `compile_sql`, `d1_exec`, `d1_insert_many`, `json_values` |
| `tables.py` | bảng dùng chung (khai ở đây để dựng query) |

Lưu ý: D1 tính tiền và giới hạn theo số dòng đọc. Đọc theo phim nên đi qua index `(movie_id, posted_at)` hoặc chia lô
theo `post_id`; tránh `UPDATE ... WHERE sentiment IS NULL` quét toàn bảng.

## Viết query

Code mới dùng SQLAlchemy Core: bảng khai bằng `Table(..., metadata)` (bảng dùng chung ở `tables.py`, bảng riêng của
module khai ngay trong module đó), dựng câu bằng `select` / `insert` / `update` rồi gọi `d1_exec`. SQLAlchemy chỉ dựng
câu SQL, việc gửi, thử lại khi D1 quá tải và chọn database vẫn do `d1_query` làm. Bảng khai trong Python chỉ để dựng
query, schema thật do migrations bên cinemark quản lý; không gọi `metadata.create_all`.

Helper trong `sql.py`:

- `compile_sql(stmt)` -> `(sql, params)`: câu SQL và tham số theo thứ tự `?` (dialect SQLite).
- `d1_exec(stmt, **kwargs)`: chạy câu đã dựng; kwargs của `d1_query` (`database`, `timeout`, `quiet`).
- `d1_insert_many(table, rows, conflict=, update=)`: INSERT nhiều dòng, tự chia lô cho vừa giới hạn 100 tham số của D1.
- `json_values(ids)`: danh sách id cho `cột.in_(...)` đi qua MỘT tham số JSON (`json_each`) - dùng khi danh sách có thể
  dài hơn 90 phần tử, không dùng `.in_(list)` thường.

Viết SQL thuần với `d1_query` (kèm comment một dòng lý do) khi cần mẹo riêng của D1:

- DDL và `PRAGMA` (`CREATE TABLE/INDEX`, `ALTER TABLE ... ADD COLUMN`, `PRAGMA table_info`).
- Dấu `+` chặn planner dùng index (vd. `+sentiment IS NULL`, `+aspects IS NULL`) - không dựng được bằng SQLAlchemy Core.
- `INSERT ... SELECT ... FROM json_each` / `UNION` (ghi hàng loạt, upsert `channel_profiles` / `channel_niches`).

## Bẫy: biến trùng tên bảng

Tên bảng là tên biến Python rất dễ trùng (`posts`, `comments`, `movies`, `keywords`). Khi biến/tham số trùng tên bảng,
code dùng nhầm biến và lỗi bị `except` nuốt im lặng. Vì vậy import bảng dùng chung với tên khác
(`from app.repositories.d1.tables import posts as posts_table`). Có test `tests/test_no_table_name_shadowing.py` quét
AST toàn bộ `app/` để bắt trường hợp này.
