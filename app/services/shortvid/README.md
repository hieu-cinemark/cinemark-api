# app/services/shortvid

Cào TikTok qua ShortVid API (api.shortvidapi.com, thay Omar từ 16/11/2026): theo hashtag (`shortvid_tiktok.py`), từ khoá
(`shortvid_search.py`), kênh (`shortvid_channels.py`), bình luận (`shortvid_comments.py`), video đang nổi
(`shortvid_trending.py`) và hiệu quả từng nguồn (`shortvid_yield.py`).

Gọi HTTP, map dữ liệu và đếm hạn mức ở `app/clients/shortvid.py`. API key ở bảng Supabase `shortvid_api_settings` (nhập ở
dashboard › Settings). ShortVid tính tiền theo credit; hạn mức request mỗi ngày tự đặt ở `SHORTVID_DAILY_REQUESTS`.
Dữ liệu cũ của Omar đã được chuyển sang ShortVid (16/11/2026); script chuyển đã xoá, xem lịch sử git nếu cần.

Tách từ `shortvid_tiktok.py` (17/11/2026): `shortvid_seen.py` (dấu video đã thấy, đếm hashtag đi kèm) và `shortvid_pause.py` (tạm dừng chờ hạn mức/key mới,
kết quả lượt gần nhất). `shortvid_tiktok.py` vẫn re-export mọi tên; test patch `get_redis_client` / `get_shortvid_api_settings` ở `shortvid_pause`.
