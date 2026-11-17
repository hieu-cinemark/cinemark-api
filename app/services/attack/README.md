# app/services/attack

Trả lời câu hỏi "lượng chê của phim này có dấu hiệu bất thường hoặc phối hợp không". KHÔNG kết luận "phim bị chơi xấu": kết quả luôn
kèm câu lưu ý và bằng chứng (link, giờ, tài khoản đã che tên). Trạng thái do quy tắc cố định quyết định (`verdict.py`), không do AI:
cùng dữ liệu luôn ra cùng kết quả, giải thích được với nhà sản xuất. Thiết kế đầy đủ và hướng dẫn làm tiếp:
[../../../docs/review-angles-va-attack.md](../../../docs/review-angles-va-attack.md) (mục 2 và 4).

## Các file

| File | Việc nó làm |
|---|---|
| `data.py` | Đọc dữ liệu cho Attack Detection: số theo giờ (movie_comment_hourly), mốc sự kiện, bình luận chê gần đây (D1) và embedding (file SQLite do scripts/jobs/embed_comments.py ghi trên cùng máy - VPS: timer cinemark-embed). |
| `signals.py` | Kiểu dữ liệu chung của các dấu hiệu. |
| `similar.py` | Dấu hiệu B: nhiều tài khoản đăng bình luận chê gần như giống nhau trong thời gian ngắn. |
| `spikes.py` | Dấu hiệu A: số bình luận chê của một giờ cao bất thường so với cùng giờ đó trong 14 ngày trước. |
| `store.py` | Bảng attack_signals (D1 scraper): mỗi lần tính một dòng, giữ KEEP_DAYS ngày (docs mục 2.6). |
| `verdict.py` | Quy tắc ra trạng thái (docs mục 2.5) - hàm thuần, cùng dữ liệu luôn ra cùng kết quả. |

## Luồng

```
comments (D1) --nhịp 10 phút--> movie_comment_hourly (repositories/d1/comment_hourly.py)
                                           |                               comment_embeddings.sqlite (timer cinemark-embed)
                                           v                                          |
   ai_jobs/attack_detect.detect() --data.py đọc--> spikes.py (A: chê dồn dập)  +  similar.py (B: chê giống nhau)
                                           |
                                           v
                       verdict.decide() --> store.save() --> bảng D1 attack_signals --> Worker cinemark --> tab Attack Detection
```

Trạng thái: `insufficient` (< 200 bình luận / 7 ngày) · `clear` · `suspicious` (có dấu hiệu MẠNH) · `coordinated` (>= 2 dấu hiệu mạnh
trùng khoảng giờ). Độ tin cậy (`high|medium|low`) theo độ phủ dữ liệu, không phải xác suất bị tấn công.

## Muốn sửa / thêm thì

- **Chạy thử một phim** (không ghi): `python -m app.services.ai_jobs.attack_detect --movie-id <id> --dry-run` (`--at <ISO>` thử mốc cũ).
- **Chỉnh ngưỡng**: hằng số đầu `spikes.py` / `similar.py` / `verdict.py`. Chưa thử trên dữ liệu cũ (Bước 4 trong tài liệu) nên ngưỡng
  hiện là đề xuất; chỉnh xong phải cập nhật test `tests/test_attack.py`.
- **Thêm dấu hiệu mới** (C nhóm tài khoản, D lan rộng, E khẳng định cần kiểm tra): file mới trả `Signal` (`signals.py`), thêm vào danh
  sách trong `ai_jobs/attack_detect.detect`, thêm câu giải thích trong `verdict._reason`, và kiểu `AttackSignal` bên Worker
  (`cinemark/packages/contracts/src/types/attackDetection.ts`).
- **Đổi cột `attack_signals`**: sửa `store.py` VÀ `cinemark/apps/api/src/services/socialTopic/attack.ts` - deploy cinemark-api trước, Worker sau.

## Lưu ý

Hàm trong `spikes.py`, `similar.py`, `verdict.py` là hàm thuần (không D1, không Redis) để test không cần hạ tầng. Phần có I/O chỉ ở
`data.py` và `store.py`. Embedding đọc từ file SQLite cùng máy với job; chưa có file thì dấu hiệu B chỉ gom câu trùng chữ.
