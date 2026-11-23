# Đường dữ liệu từ đầu đến cuối (cả bốn repo)

File này nối các luồng riêng của từng repo thành MỘT đường: từ lúc scheduler quyết định cào, tới lúc người dùng thấy con số trên màn hình.
Đọc nó trước, rồi đi vào `FLOW.md` của repo bạn cần sửa:

- [FLOW.md](FLOW.md) - cinemark-api (lịch, ingest, job nền)
- [../../spider-hub/docs/FLOW.md](../../spider-hub/docs/FLOW.md) - spider-hub (cào)
- [../../cinemark/README.md](../../cinemark/README.md) - Worker + web (hiển thị)
- spider-hub-dashboard - bảng điều khiển vận hành (tab Omar, tài khoản, proxy, lịch cào)

## 1. Bốn repo, mỗi repo một việc

| Repo | Làm gì | KHÔNG làm gì | Chạy ở |
|---|---|---|---|
| **spider-hub** | Xin tài khoản + proxy, cào bài/bình luận, đẩy thô lên Kafka | Không phân loại, không ghi D1 chính | VPS (Facebook có tài khoản: Mac) |
| **cinemark-api** | Lên lịch cào, lọc liên quan, ghi D1, gắn nhãn, embedding, gom cụm, AI, bản chụp số liệu; API cho dashboard | Không phục vụ trang cho người dùng cuối | VPS |
| **cinemark** (Worker + web) | Đọc D1/R2, tính số hiển thị (Social Topic, Heat, báo cáo), vẽ giao diện | Không cào, không gắn nhãn | Cloudflare |
| **spider-hub-dashboard** | Giao diện vận hành: tài khoản, proxy, lịch, cài đặt cào, tab Omar. Gọi API của cinemark-api | Không có dữ liệu riêng | VPS |

## 2. Toàn cảnh

```
 (A) LẬP LỊCH            (B) CÀO                  (C) INGEST + LỌC          (D) GẮN NHÃN NỀN          (E) HIỂN THỊ
 cinemark-api            spider-hub               cinemark-api               cinemark-api (timer)      cinemark (Worker+web)

 scheduler ─Kafka crawl_requests──►  consumer ─► spider ─Kafka raw_posts────► ingest_consumer ─► D1 ──► ViSoBERT: cảm xúc, ─► Worker đọc D1/R2
   (TICKS)  ─Redis crawl_*  ──────►  (tài khoản                raw_comments      handle_post          khía cạnh, giai đoạn    Social Topic, Heat...
 Omar round ─(thẳng)─────────────────────────────────────────►                   handle_comment ─► ingest_decisions            e5 embedding ─► cụm ─► AI đặt tên
 (API trả phí, TikTok)               + proxy từ D1 config)                       └► ingest_dead_letters ─► R2 bronze           snapshot hashtag TikTok
```

Cùng lúc có hai kho cấu hình/trạng thái mà MỌI chặng dùng chung:

- **D1 `cinemark-config-db`**: tài khoản, proxy, nguồn proxy, lịch cào, cài đặt cào (`proxy_settings`), cấu hình AI. cinemark-api tạo bảng và là nguồn sự thật cho cài đặt; spider-hub đọc và ghi trạng thái tài khoản/proxy.
- **Redis** (tiền tố `social_crawler:`): hàng đợi việc, khoá, cache phiên đăng nhập, tập chống trùng, cờ "đã chạy hôm nay".

## 3. Từng chặng

### A. Lập lịch (cinemark-api, `app/services/crawl/scheduler/`)
- Một vòng lặp chạy các nhịp trong `TICKS` (`crawl`, `comments`, `public_pages`, `omar`, `omar_hashtag_stats`, `ai_jobs`, `stats_reconcile`, `comment_hourly`, `attack_detect`...). Mỗi nhịp có timeout; lỗi một nhịp không giết vòng lặp.
- Nhịp quyết định "cào gì, khi nào" bằng cách đọc D1 (phim, từ khoá, giờ trong `crawl_schedules`), rồi **giao việc** theo một trong ba đường:
  1. **Kafka `crawl_requests`** (và `crawl_requests.facebook_public_comments`): lệnh cào theo từ khoá, giữ thứ tự theo `nền tảng:keyword_id`.
  2. **Hàng đợi Redis** (xem bảng khoá ở mục 5): việc chen ngang, việc không cần tài khoản, page công khai, YouTube.
  3. **Gọi thẳng Omar API** (TikTok): không qua spider-hub, tự đẩy video lên `raw_posts`.

### B. Cào (spider-hub)
1. `consumer/loop.py` đọc Kafka mỗi nền tảng một vòng (Facebook nhiều làn song song); `gates.py` chặn lúc đang giờ yên lặng của Meta, hết hạn mức giờ, hết tài khoản; `queues.py` đọc các hàng đợi Redis.
2. `dispatch.py` chọn job theo `request["type"]`; job **mượn một tài khoản và một proxy** (D1 config), đảm bảo phiên đăng nhập, rồi chạy `scrapy crawl` ở tiến trình con (hoặc ngay trong tiến trình với YouTube, page công khai).
3. Spider đẩy bản ghi **thô** lên Kafka `raw_posts` / `raw_comments`, kèm tập chống trùng Redis. Mã thoát của tiến trình con báo kết cục (xong, hết proxy, bị huỷ, gửi Kafka lỗi...).

### C. Ingest và lọc liên quan (cinemark-api, `workers/ingest_consumer/`)
- **Bài** (`handle_post`): mapper theo nền tảng → tìm từ khoá/phim → cổng ngôn ngữ → cổng "phải nhắc phim" (hoặc chuyển sang phim khác được nhắc) → `resolve_relevance` (`related` / `uncertain` ẩn / `not_related` bị loại) → ghi D1 (`persist_post`). Mỗi bài một dòng quyết định gửi `ingest_decisions`.
- **Bình luận** (`handle_comment`): tìm bài cha (chưa có thì thử lại có giới hạn rồi vào `ingest_dead_letters`) → ghi D1 với `sentiment` còn trống.
- **Lake writer** lưu mọi message Kafka thô lên **R2** (`bronze/entity=.../platform=.../dt=...`), để phát lại khi luật sai.

### D. Gắn nhãn và phân tích nền (cinemark-api, timer systemd + nhịp scheduler)
| Việc | Chạy bằng | Đọc | Ghi |
|---|---|---|---|
| Cảm xúc (ViSoBERT, luật cho câu rỗng/emoji) | timer `cinemark-visobert` mỗi 3 giờ | bình luận 6 giờ gần nhất chưa có nhãn | `sentiment`, `sentiment_source` |
| Khía cạnh, giai đoạn khán giả | timer `cinemark-aspects` | bình luận chưa qua model | `aspects`, `audience_stage` |
| Embedding e5 | timer `cinemark-embed` mỗi giờ (phim đang chiếu, 2 giờ gần nhất) | bình luận có nội dung | SQLite cục bộ `data/comment_embeddings.sqlite` |
| Gom cụm → AI đặt tên/tổng kết → đưa lên D1 | timer `cinemark-clusters` 03:30 | embedding + nhãn cảm xúc | `comment_cluster_topics`, `comment_cluster_members` |
| Thống kê theo ngày/giờ | nhịp `stats_reconcile`, `comment_hourly` | D1 | `stats_*_daily`, bảng comment theo giờ |
| Attack Detection | nhịp `attack_detect` (mỗi giờ) | D1 | `attack_signals` |
| Ảnh bài và cảnh phim; báo cáo Social Topic | timer `cinemark-visuals` (`post_visuals.py` → `movie_scenes`), `cinemark-reports` (`social_topic_reports`) | D1 + R2 | `movie_scenes`, `social_topic_reports`, R2 |
| **Quy mô hashtag TikTok** | nhịp `omar_hashtag_stats` (từ 06:00 giờ VN, một lần mỗi ngày) | Omar `search_hashtags` | `tiktok_hashtag_stats` |
| Hỏi đáp (RAG) | nạp thủ công `load_qdrant` | embedding + D1 | Qdrant |

Nguyên tắc: **ingest ghi nhãn trống, việc nặng làm sau**. Mọi job chọn dòng "chưa có nhãn" nên chạy lại bao nhiêu lần cũng an toàn.

### E. Hiển thị (cinemark Worker + web)
- Worker đọc **thẳng D1 và R2** (không gọi cinemark-api): bài, bình luận, bảng tổng hợp theo ngày, nhóm bình luận, cảnh phim, bản chụp hashtag TikTok, bản tin báo chí...
- Phần nặng được cache (KV, thường khoảng một giờ); nút "Làm mới" bỏ qua cache. Web gọi `/api/...`, React Query giữ cache phía trình duyệt.
- Với các bảng do cinemark-api tạo ở lần chạy đầu (ví dụ `movie_scenes`, `tiktok_hashtag_stats`), Worker bắt lỗi khi bảng chưa có và coi như "chưa có số liệu" (khối ẩn) chứ không lỗi. Bảng khác có thể không được bọc như vậy.

## 4. Thứ nằm ở đâu

| Dữ liệu | Kho | Ai ghi | Ai đọc |
|---|---|---|---|
| Bài, bình luận, nhãn, thống kê | D1 chính | cinemark-api | Worker, cinemark-api |
| Tài khoản, proxy, lịch, cài đặt cào, cấu hình AI | D1 `cinemark-config-db` | cinemark-api (dashboard), spider-hub (trạng thái) | cả hai |
| Bản chụp hashtag TikTok | D1 chính (`tiktok_hashtag_stats`) | cinemark-api | Worker |
| Hàng đợi, khoá, cache phiên, cờ "đã chạy" | Redis | cả hai | cả hai |
| Message thô | Kafka → R2 `bronze/` | spider-hub, cinemark-api | lake writer, phát lại |
| Vector bình luận (cụm, RAG) | SQLite cục bộ `data/comment_embeddings.sqlite`; Qdrant (RAG) | cinemark-api | cinemark-api |
| Model ViSoBERT | thư mục `models/` (NGOÀI git) | huấn luyện tay | job gắn nhãn |

## 5. Những thứ PHẢI khớp giữa các repo

Sai một chỗ thì không có lỗi to, việc chỉ đứng yên. Có test đối chiếu cho cái ghi chú "test".

| Thứ | Hai phía | Hậu quả khi lệch |
|---|---|---|
| Tên khoá Redis (`crawl_priority:facebook`, `crawl_idle:facebook`, `crawl_public_pages:{facebook,threads}`, `crawl_youtube:*`) | cinemark-api `app/clients/crawl_queues.py` ↔ spider-hub `consumer/queues.py` (test: `test_crawl_queue_keys_match_spider_hub`) | việc xếp hàng mà không ai lấy |
| Topic Kafka (`crawl_requests`, `raw_posts`, `raw_comments`, `ingest_decisions`, `ingest_dead_letters`) | hai repo | mất dòng chảy |
| Trường của message cào (`type`, `keyword`, `run_id`...) | cinemark-api `publish_crawl_request` ↔ spider-hub `dispatch.py` | job bị bỏ qua |
| Khoá/mặc định của `proxy_settings` | cinemark-api `ProxySettings` ↔ spider-hub `get_setting("khoá")` | spider-hub báo thiếu khoá (cần deploy cinemark-api trước) |
| Cột/bảng D1 mà Worker đọc | cinemark-api (tạo) ↔ Worker SQL | khối ẩn hoặc lỗi SQL bị nuốt |
| Mẫu JSON trên R2 | cinemark-api ↔ Worker | Worker phải đọc được cả bản cũ |
| Định dạng thời gian trong D1 config (chuỗi ISO UTC cố định độ rộng) | cinemark-api ↔ spider-hub | so sánh chuỗi sai thứ tự |

## 6. Thứ tự deploy

1. **cinemark-api trước** (tạo bảng, ghi khoá cài đặt mới vào `proxy_settings`). CI chỉ tự deploy khi push **`main`** và biến repo `DEPLOY_ENABLED=true` (chạy `deploy/vps/deploy-repo.sh`, tự cài timer). Nhánh khác (ví dụ `attack-detection` nếu VPS đang theo nhánh đó) KHÔNG tự deploy: trên VPS chạy `sudo -u app git pull` rồi restart service.
2. **spider-hub sau** (cần khoá cài đặt mới đã có).
3. **dashboard** (cập nhật tay trên VPS: pull, build, restart).
4. **cinemark** (Worker + web): push `dev` là CI tự deploy. Worker phải đọc được cả dữ liệu cũ vì cinemark-api đã ghi trước.

Đổi cấu trúc dùng chung: thêm trước, bỏ sau (hai bản chạy song song một thời gian).

## 7. Khi dữ liệu không tới: dò từng chặng

| Triệu chứng | Kiểm tra theo thứ tự |
|---|---|
| Không có bài mới của một nền tảng | A: scheduler có đẩy lệnh không (`crawl_requests` có message?) → B: consumer có nhận, có tài khoản/proxy rảnh, có đang giờ yên lặng/hết hạn mức → `raw_posts` có message → C: `ingest_decisions` ghi lý do loại |
| Có bài nhưng không hiện ở web | C: nhãn `uncertain` (ẩn)? → D: thống kê theo ngày đã dựng chưa → E: cache KV (bấm "Làm mới") |
| Có bài nhưng thiếu cảm xúc/khía cạnh | D: timer `cinemark-visobert` / `cinemark-aspects` có chạy (`systemctl list-timers`), thư mục `models/` có đủ |
| Nhóm bình luận cũ | D: `cinemark-embed` rồi `cinemark-clusters` (03:30), cần embedding mới của phim |
| Thẻ "Quy mô TikTok" trống | D: `tiktok_hashtag_stats` có dòng cho phim; chạy `python -m scripts.jobs.snapshot_hashtag_stats`; Omar có đang tạm dừng vì hạn mức |
| Cào chậm hoặc dừng ở một tài khoản | `account_events` của tài khoản đó, `proxy_lease`, hạn mức giờ/ngày ở dashboard |
| Việc xếp hàng mà đứng yên | Khoá Redis có khớp hai bên không (mục 5); `...:processing` còn việc dở sau restart |

## 8. Cách đọc bản đồ này khi sửa

- Muốn đổi **hiển thị**: chỉ chặng E (và bảng D1 nếu cần số mới).
- Muốn đổi **luật lọc**: chặng C, `app/services/relevance/` (hàm thuần, có test); chạy lại nhãn bài cũ bằng script trong `scripts/tools/`.
- Muốn thêm **số liệu theo ngày** (như hashtag TikTok): job ghi bảng ở D (cinemark-api) + route/service đọc ở E (Worker) + migration SQL trong `cinemark/scripts/migrations/`.
- Muốn thêm **nền tảng**: spider (B) + mapper ở C + cấu hình nền tảng ở dashboard.
- Mọi đổi cấu trúc dùng chung giữa repo: xem mục 5 và 6.
