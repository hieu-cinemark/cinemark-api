# app/services

Nghiệp vụ của service, gọi xuống `repositories`, `clients` và `ai`.

| Thư mục | Nội dung |
|---|---|
| `crawl/` | Lịch cào, làn Facebook, hàng đợi, bình luận, trang nguồn, bài booking |
| `ai_jobs/` | Job AI nền: nhãn tác giả, niche kênh, Review Angles |
| `discovery/` | AI tìm thêm hashtag và câu tìm kiếm |
| `omar/` | Cào TikTok qua Omar API |
| `accounts/` | Tài khoản, proxy, phiên đăng nhập, sức khoẻ tài khoản |
| `relevance/` | Luật độ liên quan, dọn bài không liên quan |
| `reports/` | Report Social Topic |
| `stats/` | Số liệu tổng hợp theo ngày |
| `topics/` | Gom chủ đề bình luận |
| `ops/` | Cảnh báo, số liệu vận hành, đồng bộ cấu hình AI |

Các file ở gốc (`google_news.py`, `news_articles.py`, `news_images.py`, `press_groups.py`, `post_release_news.py`)
thuộc tính năng tin báo chí sau công chiếu, mô tả bên dưới.

## Tin sau công chiếu (Google Search + News Light, lưu D1)

## Tin sau công chiếu (Google Search + News Light, lưu D1)

Cấu hình `SERPAPI_API_KEY` trong env hoặc `.env` rồi restart web/scheduler.
`SERPAPI_TIMEOUT_SECONDS` mặc định 120 giây. Không cần SDK mới.

- `POST /research/google-search`: tìm trực tiếp, body `{"q":"phim Út Lan 2 đánh giá","gl":"vn","hl":"vi"}`.
  Dùng engine `google`, trả `organic_results` và `top_stories` nếu có.
  Endpoint `/research/google-news` cũ vẫn giữ Google News Light cho client cũ.
- `GET /research/movies/{movie_id}/news`: chỉ đọc bản đã lưu từ D1, trả null khi chưa có.
  Không gọi SerpApi hay Bee khi đọc.
- `POST /research/movies/{movie_id}/news/refresh`: lấy tin đến hạn và lưu D1; không gọi Bee.

Scheduler kiểm tra mỗi giờ và lấy tin cho các phim đang theo dõi, đã khởi chiếu (giờ
Việt Nam), mỗi 72 giờ kể từ lần thành công. Tác vụ chạy nền, không chặn lịch crawl.
Dữ liệu lưu tại bảng `movie_press_reviews` trong D1 dùng chung với Cinemark; mỗi phim
một snapshot hiện tại, có ngày lấy tin/lịch lấy tiếp và trường phân tích Cinemark AI.
Mỗi lượt cập nhật gọi hai engine: Google Search cho organic_results và Google News
Light cho news_results. Giữ đầy đủ một trang mỗi nguồn cho hiển thị; sources chứa
tất cả nguồn duy nhất. analysis_sources chọn riêng tối đa 10 nguồn cân bằng giữa
hai nhóm cho trích xuất và AI. Không dùng top_stories cho tab báo chí.
Tin mới xoá phân tích của snapshot trước. Lỗi lấy tin giữ dữ liệu cũ, giãn retry 1 giờ;
Redis chỉ giữ khoá/cooldown, không còn chứa bản tin. Không tự theo trang kế tiếp. Hiển thị không cắt số bài trong trang API trả về; trafilatura trích nội dung, tác giả và
ngày xuất bản gốc, chuẩn bị sẵn analysis_input trong JSON trước khi gửi Bee. Nội dung
được giới hạn độ dài và đánh dấu fallback khi chỉ có snippet.

Cinemark Worker đọc D1 trực tiếp và gọi Bee từ cấu hình AI của Cinemark khi người dùng
bấm phân tích; cinemark-api không còn endpoint phân tích Bee cho tính năng này.
Bản tin/AI đã lưu không cần URL, key API VPS hoặc SerpApi để xem.

Chạy `cinemark-scheduler` trên VPS (hoặc cấu hình `scheduler_in_web` hiện có). API tự
`CREATE TABLE IF NOT EXISTS`; migration tương ứng ở repo Cinemark:
`scripts/migrations/20261102_movie_press_reviews.sql`.

Cấu hình lần đầu trên VPS:

```sh
cd /opt/cinemark-api
sudo nano .env
# Thêm SERPAPI_API_KEY=<key của bạn>, không có dấu ngoặc nhọn.
sudo systemctl restart cinemark-api-web cinemark-scheduler
```

Nếu chưa có key, đọc bản tin vẫn hoạt động; chưa có dữ liệu thì giao diện báo chờ lần
thu thập đầu tiên. Không commit key vào git.

Tài liệu provider: [Google News Light API](https://serpapi.com/google-news-light-api).
