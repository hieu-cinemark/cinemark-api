# app/services/omar

Cào TikTok qua Omar API (dev.omar-thing.site, thay ShortVid từ 18/11/2026). Chỉ dùng hai nhóm endpoint:

- **hashtag** (`omar_tiktok.py`): `hashtag_videos` + `hashtag_info`. Mỗi hashtag cào tới hết, cào lại từ đầu mỗi ngày (`omar_recrawl_days = 1`, song song với bình luận), con trỏ lưu ở Redis
  (`omar:hashtag_state`), video đi qua đúng đường ingest của spider (topic `raw_posts`).
- **bình luận + trả lời** (`omar_comments.py`): `comments` + `comment_replies` cho các video nhiều tương tác nhất của mỗi
  hashtag, xếp hàng mỗi ngày (hoặc mỗi tuần), cào hết bình luận rồi trả lời.

Tìm kiếm từ khoá, cào theo kênh, "video đang nổi" và chia ngân sách theo hiệu quả nguồn đã bỏ cùng đợt đó.

Gọi HTTP, ánh xạ dữ liệu và đọc hạn mức (`GET /usage`) ở `app/clients/omar.py`. Cấu hình ở dashboard (tab TikTok › Omar), lưu ở bảng
Supabase `omar_api_settings`: API key, tổng request mỗi ngày (`daily_requests`, 0 = theo gói), số request đồng thời
(`concurrency`, 0 = `OMAR_CONCURRENCY`) và trần request/phút. Lưu key mới thì server gọi `/usage` kiểm tra; key đúng thì lượt cào mở
ngay và chạy liên tục (`omar_tiktok.run_omar_round` dùng hết hạn mức ngày còn lại của gói, hoặc của số tự đặt nếu thấp hơn - xem
`plan_budget`).

`omar_seen.py` (dấu video đã thấy, đếm hashtag đi kèm) và `omar_pause.py` (tạm dừng chờ hạn mức / key mới, kết quả lượt gần nhất).
`omar_tiktok.py` vẫn re-export các tên đó; test patch `get_redis_client` / `get_omar_api_settings` ở `omar_pause`.

Còn hai nơi khác gọi Omar: `app/services/crawl/channel_profiles.py` (follower kênh qua `/profile`) và
`app/services/crawl/tracked_posts.py` (số liệu video booking qua `/video_info`).
