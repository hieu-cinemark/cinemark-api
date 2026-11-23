# Luồng của cinemark-api: cách đọc và cách sửa

Đọc file này TRƯỚC khi sửa code. Muốn thấy cả đường từ lịch cào tới màn hình (bốn repo) thì đọc [DATA_PATH.md](DATA_PATH.md) trước. Mỗi thư mục trong `app/` có README riêng; file này nối chúng thành một luồng. Phía cào dữ liệu nằm ở
spider-hub ([../../spider-hub/docs/FLOW.md](../../spider-hub/docs/FLOW.md)).

## 1. Bốn tiến trình + các timer

| Tiến trình | Lệnh | Việc |
|---|---|---|
| Web API | `uvicorn app.main:app` | Route HTTP cho dashboard spider-hub (`app/api/routes/`) |
| Scheduler | `python -m app.workers.scheduler.main` | Nguồn duy nhất quyết định khi nào cào và khi nào chạy job nền (`app/services/crawl/scheduler/`) |
| Ingest consumer | `python -m app.workers.ingest_consumer.main` | Nhận bài/bình luận từ Kafka, lọc, ghi D1 (`app/workers/ingest_consumer/`) |
| Lake writer | `app/workers/lake_writer/` | Lưu mọi message Kafka thô lên R2 (tầng bronze của data lake), theo ngày và topic |
| Timer systemd | `deploy/systemd/*.timer` | Việc nặng (model ViSoBERT, embedding, ảnh, báo cáo) chạy theo giờ - xem `deploy/systemd/README.md` |

Không có database riêng: dữ liệu ở **D1** (Cloudflare, gọi qua HTTP), cấu hình tài khoản/proxy/AI ở **D1 `cinemark-config-db`** (database riêng cho cấu hình), trạng thái ngắn hạn
ở **Redis**, dữ liệu thô và kết quả phân tích lớn ở **R2**. Worker `cinemark` đọc thẳng D1/R2 để hiển thị (cinemark-api không phục vụ trang
người dùng cuối).

## 2. Hai luồng chính

### 2.1 Từ lịch tới lệnh cào

```
scheduler/crawl_slots.crawl_tick  (giờ trong bảng crawl_schedules)
   -> đọc phim/từ khoá đang bật từ D1
   -> app/clients/kafka.publish_crawl_request()  --Kafka crawl_requests-->  spider-hub
        (hàm này cũng gọi task_queue.enqueue_published để job hiện trên trang Hàng đợi của dashboard)
```

Các nhịp khác (bình luận, trang nguồn, YouTube liên tục, Omar...) đi cùng kiểu: nhịp -> `publish_crawl_request` hoặc nạp hàng đợi Redis mà
spider-hub đọc. Danh sách nhịp và cách thêm nhịp: [../app/services/crawl/scheduler/README.md](../app/services/crawl/scheduler/README.md).

### 2.2 Từ bài thô tới D1

```
spider-hub --Kafka raw_posts / raw_comments--> workers/ingest_consumer/main.py
```

`handle_post` (bài):
1. **Mapper** theo nền tảng (`app/services/crawl/platforms.py`) đổi payload thô thành `PostDraft` chuẩn.
2. Tìm từ khoá/phim của bài; nhánh riêng cho bài theo dõi (booking) và bài từ trang nguồn.
3. **Cổng liên quan**: bài phải nêu tên phim/từ khoá (`relevance_rules`); bài nêu phim khác thì chuyển sang phim đó (`_reroute_to_other_film`).
4. **Quyết định** nhãn `related / not_related / uncertain` (`resolve_relevance`; AI chỉ khi bật `AI_INGEST_RELEVANCE`). Phim "chặt" (YouTube...)
   cần thêm tín hiệu gắn đúng phim, kể cả từ transcript.
5. `persist_post` (`app/repositories/d1/posts.py`) ghi D1 (upsert + snapshot tương tác); mỗi bài một dòng quyết định gửi `ingest_decisions`.

`handle_comment`: mapper -> tìm bài cha (chưa có thì thử lại có giới hạn rồi vào `ingest_dead_letters`) -> `persist_comment`. Nhãn cảm xúc,
khía cạnh, giai đoạn khán giả KHÔNG gán lúc ingest mà bởi các lượt quét nền (`sentiment_sweep.py`, timer ViSoBERT/aspects).

## 3. Job nền và model tại chỗ

| Loại | Ở đâu | Chạy bằng |
|---|---|---|
| Job AI gọi LLM (Bee): nhãn tác giả, niche kênh, Review Angles | `app/services/ai_jobs/` | nhịp `ai_jobs` trong scheduler |
| Gắn nhãn bằng model ViSoBERT tại chỗ (cảm xúc, khía cạnh, giai đoạn) | `scripts/jobs/*_label.py` | timer systemd `cinemark-visobert`, `cinemark-aspects` |
| Embedding bình luận (e5) | `scripts/jobs/embed_comments.py` | timer `cinemark-embed` |
| Gom nhóm bình luận + đặt tên + tổng kết (Social Topic) | `scripts/jobs/topic_clusters.py` | timer `cinemark-clusters` (03:30) |
| Ảnh bài, báo cáo Social Topic | `scripts/jobs/post_visuals.py`, `social_topic_reports.py` | timer `cinemark-visuals`, `cinemark-reports` |
| Số liệu tổng hợp theo ngày/giờ | `app/repositories/d1/comment_rollup.py`, `comment_hourly.py` | nhịp `stats_reconcile`, `comment_hourly` |
| Attack Detection | `app/services/attack/` | nhịp `attack_detect` (mỗi giờ) |

Mọi lời gọi LLM đi qua `app/ai/runtime.call_llm` / `call_llm_json` -> `app/ai/providers/client.call_ai`: nhà cung cấp, model và
`reasoning_effort` đọc từ bảng D1 `ai_providers` (sửa trên dashboard, cache 60 giây). Model suy luận (Meta Muse) tính phần "nghĩ"
vào `max_tokens`, nên client tự cộng thêm hạn mức khi provider có `reasoning_effort`.

## 4. Đọc code theo thứ tự nào

1. `app/workers/ingest_consumer/main.py` `handle_post` (thấy cả chuỗi lọc/ghi) -> `app/services/crawl/platforms.py`.
2. `app/services/crawl/scheduler/__init__.py` (`TICKS`) -> README thư mục đó.
3. `app/repositories/d1/README.md` + `tables.py` (các bảng) -> `sql.py` (cách gọi D1).
4. Một job AI tiêu biểu: `app/services/ai_jobs/review_angles.py` + `app/ai/tasks/review_angles.py` + `app/ai/prompts/review_angles.py`.
5. `app/api/routes/` khi cần biết dashboard gọi gì.

## 5. Quy ước dữ liệu (D1)

- Truy vấn dựng bằng **SQLAlchemy Core** rồi gửi qua `d1_exec` (`app/repositories/d1/sql.py`). Bảng khai ở `tables.py` (dùng chung) hoặc ngay trong
  module sở hữu nó (`Table(...)` + `CreateTable(..., if_not_exists=True)`, mẫu: `repositories/d1/transcripts.py`). SQL viết tay (`d1_query`) chỉ cho
  DDL, `PRAGMA`, mẹo index có dấu `+`, và `INSERT ... SELECT` tổng hợp lớn.
- **Tối đa 100 tham số mỗi câu**: danh sách dài dùng `json_values(...)` (một tham số JSON), chèn nhiều dòng dùng `d1_insert_many`, chèn từ một danh sách
  dict dùng `json_rows(...)` + `insert().from_select(...)`.
- **Tính phí theo số dòng đọc**: luôn lọc theo index (`movie_id, posted_at`; `post_id`); không quét cả bảng `comments`/`posts`. Job nặng ngoài giờ cao điểm,
  có `timeout` dài, và `d1_query` đã bắt lỗi 7429 (quá tải).
- **Bẫy**: biến/tham số đặt trùng tên bảng (`posts`, `comments`, `movies`) làm câu dùng nhầm biến, lỗi bị `except` nuốt im lặng - import bảng với tên khác
  (`posts as posts_table`).
- Đổi cấu trúc JSON trên R2 hoặc cột D1 mà Worker đọc: deploy cinemark-api trước, Worker sau, và Worker phải đọc được cả bản cũ.

## 6. Công thức sửa thường gặp

| Muốn | Làm |
|---|---|
| Thêm nhịp lịch | `async def x_tick()` + một dòng trong `TICKS` (README `scheduler/`) |
| Thêm job AI | file trong `app/services/ai_jobs/` (logic) + `app/ai/prompts/` (prompt) + `app/ai/tasks/` (parse, hàm thuần); gọi `call_llm_json`; gắn vào `ai_jobs_tick` |
| Thêm bảng D1 | `Table(...)` + `CreateTable(if_not_exists=True)` + hàm `ensure_table()`; test bằng SQLite trong bộ nhớ (mẫu `tests/test_comment_hourly.py`) |
| Thêm cài đặt chỉnh từ dashboard | Chỉ `ProxySettings` trong `app/schemas/settings.py` (khoá + mặc định + giới hạn) + form dashboard. Khi khởi động, cinemark-api ghi khoá mới vào dòng `proxy_settings`; spider-hub đọc bằng `get_setting("khoá")`, ở đây đọc bằng `await crawl_setting("khoá")`. Deploy cinemark-api TRƯỚC spider-hub |
| Thêm nền tảng | mapper bài/bình luận trong `app/services/crawl/platforms.py` + job bên spider-hub |
| Đổi luật liên quan | `app/services/relevance/relevance_rules.py` (hàm thuần, có test riêng); chạy lại nhãn bài cũ bằng script trong `scripts/tools/` |
| Thêm route cho dashboard | `app/api/routes/<tên>.py` + đăng ký trong `app/main.py` |
| Thêm timer nặng | cặp `.service` + `.timer` + `install_<tên>.sh` trong `deploy/systemd/` (mẫu `cinemark-aspects.*`) |

## 7. Kiểm thử, chạy local, deploy

```bash
TELEGRAM_BOT_TOKEN= TELEGRAM_CHAT_ID= pytest -q    # tests/conftest.py đã chặn Telegram thật, nhưng chạy tay thì nên xoá biến
ruff check app tests scripts && ruff format app tests scripts
```

- Hàm thuần có test riêng; test cần D1 thì monkeypatch `app.repositories.d1.sql.d1_query` hoặc chạy câu SQLAlchemy thật trên SQLite trong bộ nhớ.
- Có test bắt code thừa/trùng ở phía Worker (`cinemark`); phía này dùng `ruff` + bộ quét code chết (tên hàm cấp module chỉ xuất hiện một lần trong cả repo).
- Push `main` thì GitHub Actions deploy lên VPS (`deploy/vps/deploy-repo.sh`). Timer mới phải cài trên VPS: `bash deploy/systemd/install_all.sh` cài mọi timer một lần (chạy lại an toàn).

## 8. Log và cấu hình (từ 17/11/2026)

- **Log mặc định tiếng Việt.** `event` là mã snake_case cố định (dashboard, Telegram, test dựa vào nó); câu tiếng Việt nằm ở `app/core/log_messages.py`
  và được logger thêm vào mọi dòng (`msg` ở JSON, ngay sau mã sự kiện ở console). Thêm sự kiện mới = thêm một dòng vào `MESSAGES`;
  `tests/test_log_messages.py` quét mã nguồn và fail nếu thiếu hoặc mã không phải snake_case. Dữ liệu thay đổi đặt ở key=value, không ghép vào câu.
- **Cấu hình cào nằm ở D1 (`proxy_settings`), cinemark-api là nguồn sự thật.** Khoá/mặc định/giới hạn khai ở `ProxySettings`
  (`app/schemas/settings.py`); khi web/scheduler khởi động `ensure_proxy_settings_complete` ghi đủ mọi khoá vào dòng này; spider-hub chỉ đọc.
  Sửa trên dashboard (Settings › Proxy), có hiệu lực sau ≤ 60 giây, không cần `.env` hay restart. Deploy cinemark-api TRƯỚC spider-hub.
- **Deploy tự cài timer.** `deploy/vps/deploy-repo.sh` (CI gọi) tự cập nhật bản của chính nó, cài `requirements-ml.txt` khi cần, và đồng bộ/bật mọi
  cặp `deploy/systemd/*.timer`+`.service` - không còn bước cài tay sau deploy (`install_all.sh` chỉ còn để cài lần đầu).
- **Hàng đợi Redis giao việc cho spider-hub** khai một chỗ ở `app/clients/crawl_queues.py` (kèm bảng đối chiếu với phía đọc trong spider-hub, nơi dùng `runtime/job_queue.JobQueue`). Tên khoá phải khớp từng ký tự giữa hai repo.
