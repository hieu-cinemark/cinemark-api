# app/services/shortvid

Cào TikTok qua ShortVid API (api.shortvidapi.com, thay Omar từ 16/11/2026): theo hashtag (`shortvid_tiktok.py`), từ khoá
(`shortvid_search.py`), kênh (`shortvid_channels.py`), bình luận (`shortvid_comments.py`), video đang nổi
(`shortvid_trending.py`) và hiệu quả từng nguồn (`shortvid_yield.py`).

Gọi HTTP, map dữ liệu và đếm hạn mức ở `app/clients/shortvid.py`. API key ở bảng Supabase `shortvid_api_settings` (nhập ở
dashboard › Settings). ShortVid tính tiền theo credit; hạn mức request mỗi ngày tự đặt ở `SHORTVID_DAILY_REQUESTS`.
Chuyển dữ liệu cũ của Omar (bảng D1 + khoá Redis): `scripts/tools/migrate_omar_to_shortvid.py`.
