# app/repositories/d1

SQL trên D1 `cinemark-scraper`.

| File | Bảng |
|---|---|
| `store.py` | phim, từ khoá, report, thống kê và các truy vấn dùng chung |
| `posts.py`, `comments.py` | bài viết, bình luận |
| `comment_rollup.py` | bảng tổng hợp bình luận theo phim x ngày |
| `topics.py` | chủ đề bình luận Worker đọc |
| `press_reviews.py` | tin báo chí sau công chiếu |

Lưu ý: D1 tính tiền và giới hạn theo số dòng đọc. Đọc theo phim nên đi qua index `(movie_id, posted_at)` hoặc chia lô
theo `post_id`; tránh `UPDATE ... WHERE sentiment IS NULL` quét toàn bảng.
