# app/services/ai_jobs

Job AI chạy nền trong scheduler. Phần gọi AI dùng `app.ai`; ở đây chỉ điều phối D1, R2, Redis.

| File | Job | Nhịp |
|---|---|---|
| `author_classify.py` | Gắn tầng cho tác giả chưa phân loại (cinemark-db) | 2 phút |
| `channel_niches.py` | Gắn tệp khán giả cho kênh (`channel_profiles`) | 1 giờ |
| `review_angles.py` | Gom góc nhìn từ bài sau Premiere, ghi R2 cho Worker đọc | ~3 phút; 1 phút khi có phim ưu tiên |
| `channels.py` | Tách handle / kênh của bài (phải khớp bản TS ở Worker) | |

Cấu hình: `AI_JOBS_ENABLED`, `CLOUDFLARE_D1_APP_DATABASE_ID`, `CLOUDFLARE_D1_CONFIG_DATABASE_ID`.

Ưu tiên một phim cho Review Angles (đọc liên tục, ngân sách token ước lượng):

```bash
redis-cli SET social_crawler:review_angles:focus '{"movieId":"<movie_id>","tokenBudget":5000000}'
```
