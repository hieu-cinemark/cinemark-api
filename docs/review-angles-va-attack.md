# Review Angles và Attack Detection: hướng dẫn tự làm tiếp

Tài liệu cho người tự làm tiếp hai mục còn dở của Social Topic. Mỗi phần gồm: hệ thống đang chạy thế nào, việc còn lại,
cách làm từng bước kèm khung code, cách kiểm tra.

- Review Angles: đã chạy tự động, kết quả còn vụn (Bò Sữa Bay 185 góc, 113 góc chỉ 1–2 bài).
- Attack Detection: chưa có code; mục 2 là thiết kế đã thống nhất và thứ tự làm.

Quy ước chung của repo xem mục 3 (giới hạn D1, cách viết truy vấn, test).

---

## 1. Review Angles

### 1.1 Đang chạy thế nào

**Câu hỏi:** từ ngày Premiere tới nay, các *bài đăng* về phim lặp lại những ý (góc) nào. Góc nhìn của *bình luận* là việc
của nhóm chủ đề AI, không phải mục này.

**Luồng:**

```
scheduler (_ai_jobs_tick, app/services/crawl/scheduler/)
  ├─ có focus (Redis social_crawler:review_angles:focus) -> run_focus mỗi ~60 giây, chỉ phim đó, tới khi hết tokenBudget
  └─ không focus -> run_tick mỗi ~200 giây: xoay vòng phim đã qua Premiere trong 60 ngày, trần 300 lô/ngày
        └─ analyze_next_batch(movie_id)          app/services/ai_jobs/review_angles.py
             1. _next_batch: 24 bài liên quan (FB/TikTok/Threads) đăng sau con trỏ posted_at;
                mỗi giờ quét lại 48 giờ trước con trỏ để nhặt bài về muộn
             2. gọi Bee: prompt REVIEW_ANGLES_SYSTEM (app/ai/prompts/review_angles.py)
                + catalog = MỌI góc đã có (id, name, aspect)
             3. parse_batch (app/ai/tasks/review_angles.py): kiểm quote nguyên văn, postId có thật, mỗi bài ≤ 2 góc
             4. gộp vào state: góc trùng id thì thêm refs, góc mới thì append
             5. ghi R2
Worker cinemark (apps/api/src/services/reviewAngles/*, routes/reviewAngles.ts) chỉ ĐỌC state.json
```

**Dữ liệu trên R2**, `analysis/review-angles/v2/<movieId>/`:

| File | Nội dung |
|---|---|
| `state.json` | `cursor`, `recent` (id bài đã đọc gần đây), `lateScanAt`, `pages`, `analyzed` (số bài theo nền tảng), `sources`, `updatedAt`, `model`, `failures`, `angles` |
| `page-N.json` | Bài của lô thứ N (để xem lại AI đã đọc gì) |

Mỗi góc trong `angles`:

```json
{
  "id": "c3dc4b62cf25edbd16939e83",
  "name": "Phi vụ đổi đời thành mớ bòng bong tình cảm",
  "description": "…",
  "aspect": "kich_ban",
  "refs": [
    {"id": "post_…", "page": 0, "platform": "tiktok", "source": "tiktok|khangverbluyd333",
     "interactions": 0, "views": 127, "quote": "…nguyên văn caption…"}
  ]
}
```

Khoá viết camelCase vì Worker đọc thẳng. Đổi cấu trúc thì sửa cả hai bên (cinemark-api ghi, cinemark đọc).

**Redis** (prefix `social_crawler:review_angles:`):

| Key | Ý nghĩa |
|---|---|
| `focus` | JSON `{"movieId": "...", "tokenBudget": 25000000}`: phim ưu tiên. Xoá key là về chế độ xoay vòng |
| `tokens:<movieId>` | Số token đã dùng cho phim (ước lượng ký tự / 3) |
| `active` | Cache 30 phút danh sách phim đang được xoay vòng |

**Điều khiển** (chạy trên VPS):

```bash
R=$(docker ps -qf name=redis)
docker exec $R redis-cli GET social_crawler:review_angles:focus
docker exec $R redis-cli GET social_crawler:review_angles:tokens:<movieId>
# đặt phim ưu tiên
docker exec $R redis-cli SET social_crawler:review_angles:focus '{"movieId":"<movieId>","tokenBudget":5000000}'
# bỏ ưu tiên
docker exec $R redis-cli DEL social_crawler:review_angles:focus
```

**Đọc nhanh state của một phim** (trên Mac, chỉ đọc R2):
 
```bash
cd cinemark-api && TELEGRAM_BOT_TOKEN= TELEGRAM_CHAT_ID= .venv/bin/python - <<'EOF'
import asyncio, json
from app.clients import lake
async def main(movie_id):
    st = json.loads(await lake.get_object(f"analysis/review-angles/v2/{movie_id}/state.json"))
    sizes = sorted(((len(a["refs"]), a["aspect"], a["name"]) for a in st["angles"]), reverse=True)
    print(len(sizes), "góc |", sum(n <= 2 for n, *_ in sizes), "góc ≤ 2 bài | đã đọc", st["analyzed"])
    for row in sizes[:15]: print(*row)
asyncio.run(main("<movieId>"))
EOF
```

Lưu ý: `cinemark/docs/review-angles.md` còn ghi job chạy trong cron Worker. Từ 10–11/11/2026 job đã chuyển sang
cinemark-api, Worker chỉ đọc. Nhớ sửa lại tài liệu đó.

### 1.2 Việc còn lại

> **Cập nhật 16/11/2026** (mục 1.1 mô tả bản cũ, phần này ghi đè):
> - Thứ tự đọc: bài mới trước (`head`), rồi đọc lùi khoảng thiếu `cursor..tail`; mỗi lượt đọc liên tiếp ~170 giây; trần
>   `REVIEW_ANGLES_DAILY_BATCHES` (600). Trang hiện khoảng chưa đọc (`unreadGap`).
> - Đọc thêm YouTube (tiêu đề + mô tả + transcript `post_transcripts`).
> - Mỗi ref có thêm `postedAt` và `stance` (`khen | che | trung_tinh`); trang hiện "x khen · y chê".
> - `parse_batch` bỏ riêng phần sai thay vì cả lô.
> - Việc A làm bằng AI thay vì embedding: `scripts/tools/review_angles_cleanup.py --apply` gộp khía cạnh > 8 góc và gắn
>   stance cho ref cũ (chạy tay khi thấy góc vụn lại). Trần 10 góc mỗi khía cạnh trong job chặn vụn từ 15/11.

| # | Việc | Vì sao | Độ khó |
|---|---|---|---|
| A | Gộp góc trùng ý | **Xong 16/11** (cleanup tool, xem trên). Muốn tự động: chạy tool theo lịch | — |
| B | Gửi AI catalog gọn | Một phần xong (catalog có mô tả + số bài, trần góc). Còn: giới hạn 60 góc lớn nhất | Nhỏ |
| C | Chia lượt công bằng giữa các phim | Một phần xong (đọc nhiều lô mỗi lượt). Còn: focus nhường 1/4 lượt | Nhỏ |
| D | So sánh giữa các lần đo | File Social Topic v4 yêu cầu góc tăng/giảm theo lịch đo | Vừa |
| E | So sánh nhiều phim | Đặt các phim cạnh nhau theo khía cạnh chung | Nhỏ (Worker) |
| F | Danh sách tên riêng do đội truyền thông duyệt | Bắt các góc hẹp (nhân vật, bài hát, câu thoại) | Vừa, cần đội truyền thông |

Làm theo thứ tự A → B → C, rồi D/E, cuối cùng F.

### 1.3 A: Gộp góc trùng ý

**Ý tưởng:** định kỳ embed `name + description` của mọi góc, nối các góc giống nhau, gộp mỗi nhóm thành một góc. Nhóm
gộp từ ≥ 2 góc thì nhờ AI đặt lại tên. Lưu bảng id cũ → id mới để lô sau vẫn dùng lại được id cũ.

**Bước làm:**

1. File mới `app/services/ai_jobs/review_angles_merge.py`.
2. Embed bằng `app.ai.local.embedding.embed_texts` (e5-small, đã chuẩn hoá vector, cosine = tích vô hướng). Văn bản:
   `f"{aspect}: {name}. {description}"`.
3. Nối cạnh khi cosine ≥ `MERGE_SIM` (bắt đầu 0,86) **và** cùng `aspect`. Riêng `tong_the` / `khuyen_xem` cho nối với
   nhau. Gom bằng union-find (thành phần liên thông).
4. Mỗi nhóm:
   - góc đại diện = góc nhiều `refs` nhất (giữ `id` của nó);
   - `refs` = hợp các refs, bỏ trùng theo `id` bài;
   - nhóm từ ≥ 2 góc: gọi Bee một lần cho mọi nhóm, xin tên và mô tả mới (prompt ngắn, JSON
     `[{"group": 1, "name": "...", "description": "..."}]`). AI lỗi thì giữ tên của góc đại diện.
5. Ghi vào state:
   - `angles`: danh sách sau gộp;
   - `merged`: `{id_cũ: id_đại_diện}`. Trong `analyze_next_batch`, khi AI trả id cũ thì đổi sang id đại diện trước
     khi gộp refs;
   - `mergedAt`: thời điểm gộp.
6. Ghi state bằng cách đọc lại ngay trước khi ghi. Nếu `updatedAt` đã đổi (lô mới vừa ghi) thì bỏ lượt gộp này, chạy
   lại lượt sau. Tránh ghi đè lô vừa phân tích.
7. Lịch chạy: trong `run_tick` / `run_focus`, gọi gộp khi `pages - pagesAtLastMerge ≥ 20` hoặc mỗi đêm.

**Khung code:**

```python
MERGE_SIM = 0.86
OPEN_ASPECTS = {"tong_the", "khuyen_xem"}


def merge_groups(angles: list[dict], vectors: np.ndarray) -> list[list[int]]:
    """Chỉ số các góc theo nhóm cần gộp (union-find trên cạnh cosine ≥ MERGE_SIM, cùng khía cạnh)."""
    parent = list(range(len(angles)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    sims = vectors @ vectors.T
    for i in range(len(angles)):
        for j in range(i + 1, len(angles)):
            same = angles[i]["aspect"] == angles[j]["aspect"] or {angles[i]["aspect"], angles[j]["aspect"]} <= OPEN_ASPECTS
            if same and sims[i, j] >= MERGE_SIM:
                parent[find(i)] = find(j)
    groups: dict[int, list[int]] = {}
    for i in range(len(angles)):
        groups.setdefault(find(i), []).append(i)
    return list(groups.values())
```

`merge_groups` là hàm thuần, viết test cho nó trong `tests/test_review_angles_merge.py`: 3 góc nhạc phim cùng ý phải
về một nhóm, góc "nhạc phim" và "diễn xuất" không gộp dù câu chữ gần nhau.

**Chọn ngưỡng:** chạy thử trên state của Bò Sữa Bay ở chế độ *dry-run* (in nhóm, không ghi), đọc tay 20 nhóm, tăng
hoặc giảm `MERGE_SIM` theo 0,02. Mục tiêu: các góc nhạc phim (Lỡ thương nhau / nhạc phim được khen / nhạc phim trước
khi xem) và hai góc Võ Điền Gia Huy về chung, còn "khen nhạc" và "chê nhạc" tách riêng.

**Hiển thị (Worker cinemark):** sắp góc theo số bài, hiện 15–20 góc đầu, phần còn lại vào "Khác (n góc nhỏ)" có thể
mở ra.

### 1.4 B: Gửi AI catalog gọn

Trong `analyze_next_batch`, thay vì gửi mọi góc:

- gửi tối đa 60 góc nhiều refs nhất, kèm `description` cắt 120 ký tự để AI nhận ra cùng ý;
- thêm vào `REVIEW_ANGLES_SYSTEM` một câu: "Ưu tiên dùng lại góc trong danh sách nếu cùng ý, kể cả khác cách diễn đạt.
  Chỉ tạo góc mới khi ý thật sự khác các góc đang có."

Kiểm tra: sau 20 lô, số góc mới mỗi lô phải giảm rõ so với trước (đếm trong log `review_angles_batch`).

### 1.5 C: Chia lượt công bằng

- Khi có focus: dành 1/4 số lượt cho vòng thường (ví dụ cứ 4 lượt focus thì chạy một lượt `run_tick`). Phim khác vẫn
  tiến, không đứng yên cả tuần.
- Vòng thường: chọn phim có `updatedAt` cũ nhất **và** còn bài chưa đọc, thay vì xoay vòng theo danh sách.
- Theo dõi bằng script đọc state ở mục 1.1: cột "đã đọc" của mọi phim phải tăng mỗi ngày.

### 1.6 D và E: So sánh giữa các lần đo và nhiều phim

Không cần đổi job: mỗi ref đã có `id` bài. Cần thêm `postedAt` vào ref (lấy từ `_load_posts`, đã có `posted_at`), với
các ref cũ thì bù bằng một lượt đọc lại D1 theo id.

- **Theo lần đo:** Worker lấy lịch đo của phim (Social Topic đã có), đếm ref theo khoảng giữa hai mốc. Góc tăng/giảm =
  số bài kỳ này so với kỳ trước, kèm mũi tên như các khối khác.
- **Nhiều phim:** gom góc của mỗi phim lên mức `aspect` (14 khía cạnh chung trong `REVIEW_ASPECTS`), so tỷ trọng từng
  khía cạnh giữa các phim. Góc riêng vẫn hiện khi bấm vào khía cạnh.
- Muốn số "tại thời điểm đo" không đổi về sau: khi tới mốc đo, ghi snapshot
  `analysis/review-angles/v2/<movieId>/snapshots/<measuredFor>.json` (góc + số bài tại mốc).

### 1.7 F: Danh sách tên riêng

1. Bảng D1 `review_entities (movie_id, kind, name, aliases_json, approved_by, approved_at)`. `kind` thuộc
   `character | actor | song | quote | scene`.
2. Trang nhập trên cinemark (Admin hoặc ngay trong tab) cho đội truyền thông thêm và duyệt.
3. Job đếm: với mỗi bài đã đọc (page-N.json), tìm tên và alias (so khớp không dấu, theo ranh giới từ), cộng số bài,
   tương tác và trích đoạn. Ghi vào `state.json` mục `entities`.
4. Trang hiện các tên riêng như một nhóm góc hẹp, xếp cạnh góc AI.

### 1.8 Kiểm tra trước khi bật

- `pytest -q` toàn bộ (`tests/test_ai_jobs.py` có fixture R2/Redis/D1 giả, dùng lại cho test gộp góc).
- Chạy dry-run trên state thật của 2 phim (Bò Sữa Bay, Chị Chị Em Em 3), đọc tay kết quả.
- Sao lưu `state.json` trước lần gộp thật đầu tiên:
  `wrangler r2 object get cinemark-lake/analysis/review-angles/v2/<movieId>/state.json --file backup.json --remote`.

---

## 2. Attack Detection

### 2.1 Mục tiêu và nguyên tắc

- Trả lời: **lượng chê này có dấu hiệu bất thường hoặc phối hợp không**. Không kết luận "phim bị chơi xấu".
- **Trạng thái do quy tắc cố định quyết định.** AI chỉ tóm tắt luận điểm và viết câu giải thích. Cùng dữ liệu phải ra
  cùng kết quả, giải thích được với nhà sản xuất.
- Mọi kết luận kèm bằng chứng: link, giờ đăng, số tài khoản.
- Luôn ghi: "Kết quả mang tính khuyến cáo, dựa trên dữ liệu thu được, không đầy đủ 100%".

**Khác nhóm chủ đề bình luận thế nào:** nhóm chủ đề gom *mọi* bình luận theo *nội dung* thành ~20 nhóm lớn để biết khán
giả nói gì. Attack gom *bình luận chê trong cửa sổ ngắn* thật chặt (gần trùng chữ) rồi đo *hành vi*: bao nhiêu tài
khoản, dồn trong mấy giờ, trên bao nhiêu bài. 300 người chê "kịch bản dở" rải cả tuần là phản hồi tự nhiên. 40 tài khoản
đăng gần như y hệt một câu trong 2 giờ mới là tín hiệu. Hai bên dùng chung embedding và nhãn cảm xúc.

### 2.2 Dữ liệu dùng (đã có)

| Cần | Ở đâu |
|---|---|
| Bình luận chê, giờ đăng, tác giả, bài | D1 `comments`: `sentiment`, `sentiment_source` (bỏ `rule`), `posted_at`, `first_seen_at`, `author_id`, `author_name`, `post_id`, `reactions_count` |
| Bài thuộc phim, nền tảng, nguồn | D1 `posts` (chỉ bài liên quan: `relevant_post(p)`), `trend_post` = 0 |
| Embedding | SQLite `data/comment_embeddings.sqlite`, bảng `comment_embeddings(comment_id, movie_id, embedding BLOB float16 384 chiều)`, ghi bởi `scripts/jobs/embed_comments.py` |
| Mốc sự kiện | `movie_measure_settings.premiere_date`, `movies.released_at`, lịch đo; thêm mốc tay (mục 2.9) |
| Đếm theo ngày | `movie_comment_daily` (`app/repositories/d1/comment_rollup.py`) |

### 2.3 Bước 0: Embedding chạy tự động trên VPS (làm trước)

Hiện `embed_comments.py` chạy tay trên Mac. Attack cần cập nhật hằng giờ nên phải chạy trên VPS.

1. Thêm tuỳ chọn `--showing --hours 6`: chỉ embed bình luận của phim đang chiếu (cùng điều kiện `showing_movies` trong
   `app/services/relevance/trend_posts.py`), thu trong 6 giờ qua. Con trỏ chung theo id hiện bỏ sót bình luận mới của
   phim cũ, nên dùng `--movie-id` cho từng phim.
2. Systemd timer mỗi giờ, theo mẫu `deploy/systemd/cinemark-aspects.*` (CPU 200%, `OMP_NUM_THREADS=2`, `Nice=10`).
3. Model e5-small trên CPU: đo trên VPS trước, mục tiêu ≥ 50 câu/giây. Nếu chậm hơn, giảm `--hours` hoặc chỉ embed bình
   luận chê (`sentiment = 'negative'`).
4. Chép file `data/comment_embeddings.sqlite` từ Mac lên VPS một lần để khỏi embed lại 2 triệu bình luận cũ.

Kiểm tra: sau 2 giờ, số dòng có `movie_id` của phim đang chiếu tăng; log không lỗi bộ nhớ (MemoryMax).

### 2.4 Năm dấu hiệu

Code đặt trong `app/services/attack/`, mỗi dấu hiệu một file, mỗi file có **một hàm thuần** nhận dữ liệu đã đọc và trả
kết quả, để test không cần D1.

```python
@dataclass
class Signal:
    key: str            # "spike" | "similar" | "accounts" | "spread" | "claims"
    fired: bool         # vượt ngưỡng
    strong: bool        # đủ mạnh để tự nâng trạng thái
    score: float        # số đo chính (bội số, số tài khoản…)
    window: tuple[str, str] | None   # khoảng thời gian của dấu hiệu (để đối chiếu trùng nhau)
    evidence: list[dict]             # [{"comment_id", "post_url", "posted_at", "text"}…] tối đa 10
    detail: dict                     # số đo phụ, hiện trên trang
```

#### A. Chê dồn dập (`spikes.py`)

1. Đếm theo **giờ đăng** (`posted_at`, giờ Việt Nam). Không dùng `first_seen_at`: dữ liệu thu bù sẽ tạo đợt tăng giả.
2. Cần bảng mới `movie_comment_hourly (movie_id, hour, total, negative, run)`: giữ 30 ngày của phim đang chiếu, mỗi
   10 phút tính lại 2 giờ gần nhất. Làm theo mẫu `comment_rollup.rebuild_day` (upsert theo `run` rồi xoá dòng cũ).
3. Mức thường của giờ `h`: trung vị và MAD của số chê cùng giờ-trong-ngày trong 14 ngày trước.
4. Bất thường khi đủ cả ba:
   - `negative ≥ 3 × trung vị` **và** `negative ≥ 30`;
   - tỷ lệ chê `negative / total` ≥ 1,5 × tỷ lệ chê thường. Chỉ tổng tăng (bài viral) thì không tính;
   - trong ±6 giờ không có mốc sự kiện. Có mốc thì vẫn ghi nhưng `strong = False`, kèm "trùng sự kiện".
5. Bằng chứng: chuỗi số theo giờ cho biểu đồ, 5 bình luận chê nhiều like nhất trong đợt.

```python
def find_spikes(hours: list[dict], events: list[str], *, min_count=30, ratio=3.0, share_ratio=1.5) -> list[dict]:
    """hours: [{"hour": iso, "total": int, "negative": int}] theo thứ tự thời gian, đủ 15 ngày."""
```

#### B. Nội dung chê giống nhau (`similar.py`)

1. Đầu vào: bình luận chê (bỏ `sentiment_source = 'rule'`) của phim trong 48 giờ, kèm embedding.
2. Trùng chữ (rẻ, chạy trước): chuẩn hoá (chữ thường, bỏ dấu, bỏ dấu câu/emoji/@tag) rồi so mã băm.
3. Trùng ý: cosine ≥ 0,92 là "gần như sao chép", 0,85–0,92 là "cùng ý". Nối các cặp ≥ 0,92 (union-find như mục 1.3).
   1.000 câu thì ma trận 1.000×1.000 vẫn nhẹ. Nhiều hơn 5.000 câu thì chia theo bài hoặc dùng mã băm trước.
4. Mỗi cụm đo: số bình luận, **số tài khoản khác nhau**, số bài, số nền tảng, **độ dồn** (% bình luận nằm trong 2 giờ
   dày nhất), độ giống trung bình.
5. Cụm đáng ngờ (`strong`): ≥ 15 tài khoản khác nhau, ≥ 70% trong 2 giờ, độ giống trung bình ≥ 0,92.
6. Bằng chứng: 10 câu của cụm (link bài, giờ, tên đã che bớt), tên cụm AI chứa nó nếu có.

```python
def similar_clusters(rows: list[dict], vectors: np.ndarray, *, sim=0.92) -> list[dict]:
    """rows: [{"id", "author_id", "post_id", "platform", "posted_at", "message"}], vectors cùng thứ tự."""
```

#### C. Nhóm tài khoản hành vi giống nhau (`accounts.py`)

Chỉ dùng được với nền tảng có `author_id` (Facebook, TikTok). Không có IP hay danh sách bạn bè nên không suy luận theo
những thứ đó.

- Một tài khoản chê phim này ở ≥ 5 bài khác nhau trong 24 giờ.
- Cặp tài khoản cùng bình luận dưới ≥ 3 bài chung, mỗi lần cách nhau ≤ 10 phút (Jaccard tập bài ≥ 0,5).
- Một nhóm tài khoản có mặt ở ≥ 2 cụm của dấu hiệu B.
- `strong` khi có nhóm ≥ 5 tài khoản thoả hai trong ba điều trên.
- Bằng chứng: danh sách tài khoản (che bớt tên: `Ng*** V** A`), số bài, giờ đăng.

#### D. Luận điểm lan rộng (`spread.py`)

Đo trên mỗi cụm B: số bài gốc, số trang/kênh (`channel_of_post` trong `app/services/ai_jobs/channels.py`), số nền
tảng, thời gian lan tới 10 bài. Phần lớn là chia sẻ lại cùng một bài gốc thì ghi "một làn sóng từ nguồn chung".
Dấu hiệu này không tự nâng trạng thái, chỉ thêm ngữ cảnh.

#### E. Khẳng định cần kiểm tra (`claims.py`, dùng AI)

- Chỉ chạy khi có cụm B đáng ngờ: lấy 3–5 cụm lớn nhất, mỗi cụm 10 câu.
- Gọi `call_llm_json` (Bee, `task="attack_claims"`), nhờ AI tách các **khẳng định về sự thật** ("phim đạo nhái phim X",
  "rạp ép suất"). Cảm nhận ("phim dở") không tính.
- Gắn nhãn "cần kiểm tra". Chỉ ghi "sai" khi đội truyền thông nhập nguồn đối chiếu.
- Không tự nâng trạng thái.

### 2.5 Quy tắc ra trạng thái (`verdict.py`)

| Trạng thái | Điều kiện |
|---|---|
| `insufficient` – Không đủ dữ liệu | < 200 bình luận trong 7 ngày, hoặc < 50% số bình luận nền tảng báo dưới bài đã được thu |
| `clear` – Không phát hiện bất thường | Không dấu hiệu A/B/C nào `fired` |
| `suspicious` – Có dấu hiệu nghi vấn | Có 1 dấu hiệu `strong` trong A, B, C |
| `coordinated` – Có dấu hiệu phối hợp mạnh | ≥ 2 dấu hiệu `strong` **có `window` chồng nhau** (ví dụ đợt tăng A chứa cụm B do nhóm C đăng) |

**Độ tin cậy** (`high | medium | low`) dựa trên độ phủ dữ liệu: tỷ lệ bình luận đã thu so với số nền tảng báo, số nền
tảng có dữ liệu, độ trễ thu. Không phải "xác suất bị tấn công".

`decide(signals, coverage) -> (status, confidence, reasons)` là hàm thuần. Test đủ 4 trạng thái và trường hợp hai dấu
hiệu mạnh nhưng không trùng thời gian thì chỉ là `suspicious`.

### 2.6 Lưu kết quả

Bảng D1 `attack_signals` (database scraper), mỗi lần tính một dòng, giữ lịch sử:

```sql
CREATE TABLE IF NOT EXISTS attack_signals (
  id TEXT PRIMARY KEY,
  movie_id TEXT NOT NULL,
  computed_at TEXT NOT NULL,
  window_start TEXT NOT NULL,
  window_end TEXT NOT NULL,
  status TEXT NOT NULL,          -- insufficient | clear | suspicious | coordinated
  confidence TEXT NOT NULL,      -- high | medium | low
  signals_json TEXT NOT NULL,    -- [Signal...] (đã cắt evidence ≤ 10 mỗi dấu hiệu)
  summary TEXT                   -- câu giải thích do AI viết, NULL khi clear/insufficient
);
CREATE INDEX IF NOT EXISTS idx_attack_signals_movie ON attack_signals (movie_id, computed_at);
```

Khai bảng bằng SQLAlchemy `Table` trong module và tạo bằng `CreateTable(..., if_not_exists=True)`, ghi bằng `d1_exec`
(mục 3). Giữ 60 ngày, xoá dòng cũ hơn trong cùng job.

### 2.7 Job và lịch chạy

```
app/services/attack/
  data.py      đọc dữ liệu: bình luận chê 48h + embedding, số theo giờ, mốc sự kiện, độ phủ
  spikes.py    A      similar.py  B      accounts.py  C      spread.py  D      claims.py  E (AI)
  verdict.py   quy tắc trạng thái
  store.py     ghi/đọc attack_signals
app/services/ai_jobs/attack_detect.py   run_tick(): mỗi giờ, cho từng phim đang chiếu
```

- Gắn vào `ai_jobs_tick` trong `app/services/crawl/scheduler/ai_jobs.py` như `channel_niches`
  (`AI_JOB_EVERY["attack_detect"] = 3600`, dùng `claim(...)` để chỉ một tiến trình chạy), chạy sau khi embedding cập
  nhật (phút 15 mỗi giờ nếu timer embed chạy phút 0).
- Chế độ `--dry-run` để chạy tay: in trạng thái và dấu hiệu, không ghi D1:
  `python -m app.services.ai_jobs.attack_detect --movie-id <id> --dry-run`.
- AI (dấu hiệu E và câu `summary`) chỉ gọi khi trạng thái là `suspicious` hoặc `coordinated`. Lượt bình thường không
  tốn token.

### 2.8 Hiển thị và cảnh báo

- Worker cinemark: endpoint `GET /api/social-topic/attack?movieId` đọc dòng mới nhất + 7 ngày lịch sử từ
  `attack_signals`. Thẻ trên Social Topic:
  - nhãn trạng thái + độ tin cậy;
  - biểu đồ chê theo giờ, đánh dấu đợt tăng và mốc sự kiện;
  - danh sách dấu hiệu nổi bật, mỗi dấu hiệu một câu giải thích và ví dụ thật (link + giờ). Ví dụ: *"Phát hiện 45 bình
    luận có nội dung tương tự trong 2 giờ, từ 38 tài khoản; chưa đủ cơ sở xác nhận có phối hợp có chủ đích."*
- Báo cáo gửi nhà sản xuất: thêm một mục vào báo cáo theo lịch đo, luôn kèm câu lưu ý ở mục 2.1.
- Cảnh báo nóng (Telegram) khi lên `coordinated`: quyết định sau khi thử ngưỡng (mục 2.9). Lúc đầu chỉ hiện trên trang.

### 2.9 Thử ngưỡng trên dữ liệu cũ (bắt buộc trước khi hiện cho đội truyền thông)

1. Chạy dry-run cho các phim đã chiếu, cửa sổ trượt mỗi 6 giờ qua toàn bộ thời gian chiếu. Ghi kết quả ra file
   `data/attack_backtest/<movieId>.jsonl`.
2. Chọn phim từng bị chê nhiều: Án Mạng Xém Hoàn Hảo, Bò Sữa Bay (Review Angles đã thấy các góc "Bị nghi ngờ được đẩy lên
   quá mức", "Bị đặt là minh chứng phim Việt ngày càng tệ"), cùng vài phim bình thường để đo báo động nhầm.
3. Đội truyền thông xác nhận 10–20 trường hợp: đúng là bất thường hay không. Chỉnh ngưỡng (`min_count`, `ratio`, `sim`,
   số tài khoản) cho tới khi phim bình thường gần như không bị gắn `suspicious`.
4. Ghi ngưỡng cuối và lý do vào đầu `verdict.py`.
5. Mốc sự kiện tay: thêm bảng hoặc trường để đội truyền thông nhập họp báo, drama diễn viên… Thiếu mốc thì dấu hiệu A dễ
   báo nhầm.

### 2.10 Thứ tự làm

> **Trạng thái 17/11/2026 (nhánh `attack-detection`):** Bước 0–3 đã có code + test, chưa chạy trên production.
> - Bước 0: `embed_comments.py --showing` + `deploy/systemd/cinemark-embed.*` + `install_embed.sh`.
> - Bước 1: `app/repositories/d1/comment_hourly.py` (SQLAlchemy), nhịp `comment_hourly` (scheduler/attack.py),
>   `scripts/tools/backfill_comment_hourly.py`; dấu hiệu A `app/services/attack/spikes.py`.
> - Bước 2: dấu hiệu B `app/services/attack/similar.py`.
> - Bước 3: `verdict.py`, `store.py` (bảng `attack_signals`, có thêm cột `reasons_json`), job
>   `app/services/ai_jobs/attack_detect.py` (chạy hằng giờ từ phút 15, nhịp `attack_detect` - không qua ai_jobs vì chưa
>   dùng AI). Worker: `GET /api/social-topic/attack?movieId=` (apps/api/src/services/socialTopic/attack.ts).
> - Khác thiết kế: `suspicious` cần ít nhất một dấu hiệu `strong`; dấu hiệu chỉ `fired` vẫn là `clear` nhưng ghi lý do.
> - Chưa làm: Bước 4 (thử ngưỡng), C, D, E, câu tóm tắt AI, mốc sự kiện tay, Telegram.

| Bước | Việc | Xong khi |
|---|---|---|
| 0 | Embedding tự động trên VPS (2.3) | Bình luận mới của phim đang chiếu có embedding trong ≤ 1 giờ |
| 1 | Bảng `movie_comment_hourly` + dấu hiệu A | Dry-run in được đợt tăng của phim cũ |
| 2 | Dấu hiệu B | Dry-run in được cụm và số tài khoản |
| 3 | `verdict.py` + `attack_signals` + job chạy hằng giờ (chưa hiện trang) | Bảng có dòng mỗi giờ cho phim đang chiếu |
| 4 | Thử ngưỡng (2.9) | Đội truyền thông đồng ý ngưỡng |
| 5 | Dấu hiệu C, D | |
| 6 | Dấu hiệu E + thẻ trên Social Topic + mục trong báo cáo | |

**Còn chờ chốt:**
- Xét cả bài đăng chê hay chỉ bình luận? (Đề xuất: cả hai, bài đăng chê lan nhanh hơn.)
- Có cho đội truyền thông nhập mốc sự kiện tay không?
- Có gửi Telegram khi lên `coordinated` không?

---

## 3. Quy ước chung của repo (áp dụng cho cả hai mục)

- **Truy vấn D1:** dựng bằng SQLAlchemy Core rồi gửi qua `d1_exec` (`app/repositories/d1/sql.py`). Bảng dùng chung ở
  `app/repositories/d1/tables.py`; điều kiện "bài liên quan" là `relevant_post(p)` trong `app/repositories/d1/posts.py`.
  Chỉ viết SQL thuần (`d1_query`) cho DDL, mẹo index có dấu `+`, và INSERT … SELECT tổng hợp lớn.
- **Giới hạn D1:**
  - tối đa 100 tham số mỗi câu: insert nhiều dòng dùng `d1_insert_many`, danh sách id dài dùng `json_values(...)`;
  - tính phí theo số dòng đọc: luôn lọc theo index (`movie_id, posted_at`; `post_id`), không quét cả bảng `comments`;
  - D1 chạy câu lần lượt: job nặng đặt ngoài giờ cao điểm, có `timeout` dài và bắt lỗi 7429 (đã có trong `d1_query`).
- **Bẫy đã gặp:** biến hoặc tham số trùng tên bảng (`posts`, `comments`, `movies`, `keywords`) làm truy vấn dùng nhầm
  biến, lỗi bị `except` nuốt im lặng. Khi trùng thì import bảng với tên khác (`posts as posts_table`).
- **Test:** hàm thuần có test riêng; test có D1 thì giả lập `app.repositories.d1.sql.d1_query` hoặc dùng SQLite trong bộ
  nhớ (xem `tests/test_crawl_reliability.py`). Chạy với `TELEGRAM_BOT_TOKEN= TELEGRAM_CHAT_ID=` để không gửi Telegram thật.
- **Deploy:** push `main` của cinemark-api là tự deploy lên VPS. Worker cinemark deploy khi push nhánh `dev`. Đổi cấu
  trúc JSON trên R2 thì deploy cinemark-api trước, Worker sau, và Worker phải đọc được cả bản cũ.
- **Không đọc lại toàn bộ dữ liệu cũ trên D1 giờ cao điểm:** backfill (bù `postedAt` cho ref, backtest Attack) chạy ban đêm,
  theo lô nhỏ, có con trỏ để dừng rồi chạy tiếp.

---

## 4. Thực hành: tự làm Attack Detection từng bước

Mục này là bài thực hành cho Bước 0 → 3 của bảng 2.10. Mỗi bước gồm: làm gì, file nào, khung code (đủ để chạy, chỗ
`TODO` là phần tự viết), cách chạy thử, cách biết là xong. Làm xong bước nào thì commit bước đó rồi mới sang bước sau.

### 4.0 Chuẩn bị (một lần)

```bash
cd cinemark-api
git pull && git checkout -b attack-detection        # làm trên nhánh riêng, xong mới gộp vào main
.venv/bin/python -m pytest -q                       # phải xanh hết trước khi bắt đầu
export TELEGRAM_BOT_TOKEN= TELEGRAM_CHAT_ID=        # chạy tay ở Mac không gửi Telegram thật
```

**Hai chỗ đặt lịch chạy** (cinemark-api không dùng crontab):

| Loại việc | Chỗ đặt | Ví dụ có sẵn |
|---|---|---|
| Nhẹ, gọi D1/Redis/AI, chạy thường xuyên | Thêm một dòng vào `TICKS` trong `app/services/crawl/scheduler/__init__.py` (chạy trong service `cinemark-scheduler`). Hàm tự quyết định tới giờ chưa bằng `claim(name, giây)` (`scheduler/core.py`) | `("trend_posts", refresh.trend_posts_tick, False)` |
| Nặng (model ML, nhiều CPU/RAM), hoặc cần đúng giờ | Cặp file `deploy/systemd/<tên>.service` + `.timer`, cài bằng `install_<tên>.sh` | `cinemark-aspects.*` |

Job AI hằng giờ (Attack) gắn vào `ai_jobs_tick` (`scheduler/ai_jobs.py`); embedding (nặng CPU) dùng systemd timer.

### 4.1 Bước 0: embedding bình luận tự động trên VPS

**Mục tiêu:** bình luận mới của phim đang chiếu có embedding trong ≤ 1 giờ, trên VPS.

**a) Thêm tuỳ chọn `--showing` cho `scripts/jobs/embed_comments.py`.** Hiện script có hai chế độ: con trỏ chung theo id
(bỏ sót bình luận mới vì id là uuid) và `--movie-id` (quét lại mọi bình luận của một phim). Thêm chế độ thứ ba: với mỗi
phim đang chiếu, chỉ lấy bình luận có `first_seen_at` trong N giờ qua.

```python
# scripts/jobs/embed_comments.py

async def _recent_pages(movie_id: str, hours: float):
    """--showing: bình luận về trong `hours` giờ qua của một phim (theo nhóm bài, qua index post_id như _movie_pages)."""
    since = (datetime.now(tz=UTC) - timedelta(hours=hours)).isoformat(timespec="seconds")
    # TODO: chép _movie_pages, thêm điều kiện c.c.first_seen_at >= since vào .where(...)
    ...

# trong main():
    if args.showing:
        from app.services.relevance.trend_posts import showing_movies
        for movie in await showing_movies(datetime.now(tz=UTC)):
            async for rows in _recent_pages(movie["id"], args.hours):
                ...  # giống vòng `async for rows in pages()` hiện có: lọc done, embed_texts, save_page

# argparse:
    parser.add_argument("--showing", action="store_true", help="chỉ phim đang chiếu, bình luận về trong --hours giờ qua")
    parser.add_argument("--hours", type=float, default=6)
```

Kiểm tra cột `first_seen_at` có trên bảng `comments` trước khi dùng
(`app/repositories/d1/tables.py`, hoặc `PRAGMA table_info(comments)`). Không có thì dùng `posted_at`.

Chạy thử ở Mac (chỉ đọc D1, ghi file SQLite local):

```bash
.venv/bin/python -m scripts.jobs.embed_comments --showing --hours 2
sqlite3 data/comment_embeddings.sqlite "SELECT movie_id, COUNT(*) FROM comment_embeddings GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
```

**b) Systemd timer.** Chép mẫu `cinemark-aspects`:

```ini
# deploy/systemd/cinemark-embed.service
[Unit]
Description=cinemark-api comment embeddings (e5-small) for showing films, comments of the last hours
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
WorkingDirectory=/opt/cinemark-api
EnvironmentFile=/opt/cinemark-api/.env
Environment=OMP_NUM_THREADS=2 MKL_NUM_THREADS=2 TOKENIZERS_PARALLELISM=false
ExecStart=/opt/cinemark-api/.venv/bin/python -m scripts.jobs.embed_comments --showing --hours 2
User=app
Group=app
Nice=10
CPUQuota=200%
MemoryMax=3G
TimeoutStartSec=50min
StandardOutput=journal
StandardError=journal
```

```ini
# deploy/systemd/cinemark-embed.timer
[Unit]
Description=Run cinemark-embed every hour

[Timer]
# mỗi giờ phút 0 (các timer khác: :20 ViSoBERT, :40 ảnh bìa, :50 aspects) - Attack đọc lúc phút 15
OnCalendar=*-*-* *:00:00
RandomizedDelaySec=120
Persistent=true

[Install]
WantedBy=timers.target
```

`deploy/systemd/install_embed.sh`: chép `install_aspects.sh`, đổi tên file, kiểm tra model e5 thay cho ViSoBERT
(`"$APP_DIR/.venv/bin/python" -c "from app.ai.local.embedding import embed_texts; print(embed_texts(['thử']).shape)"`).

**c) Chép dữ liệu cũ lên VPS một lần** (khỏi embed lại 2 triệu bình luận):

```bash
# trên Mac
scp -P 7076 data/comment_embeddings.sqlite root@<vps>:/opt/cinemark-api/data/
# trên VPS
sudo chown app:app /opt/cinemark-api/data/comment_embeddings.sqlite
```

**d) Cài và kiểm tra trên VPS:**

```bash
cd /opt/cinemark-api && sudo -u app git pull && bash deploy/systemd/install_embed.sh
sudo systemctl start cinemark-embed.service          # chạy ngay một lượt
journalctl -u cinemark-embed -n 30 --no-pager        # xem số dòng/giây, lỗi bộ nhớ
systemctl list-timers cinemark-embed.timer           # lượt kế tiếp
```

**Xong khi:** log báo ≥ 50 dòng/giây, không có `MemoryMax`/`Killed`, và sau 2 giờ số dòng của phim đang chiếu trong
`comment_embeddings` tăng.

### 4.2 Bước 1a: bảng đếm bình luận theo giờ

**Mục tiêu:** bảng `movie_comment_hourly (movie_id, hour, total, negative, run)`, cập nhật mỗi 10 phút cho 3 giờ gần
nhất.

Đọc kỹ `app/repositories/d1/comment_rollup.py` trước (`ensure_rollup_schema`, `rebuild_day`): cùng một mẫu
"upsert theo `run` rồi xoá dòng cũ", nên không lúc nào bảng trống.

```python
# app/repositories/d1/comment_hourly.py
"""Bình luận theo phim x giờ (giờ Việt Nam của posted_at) cho Attack Detection - dấu hiệu A (chê dồn dập)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from app.clients.d1 import d1_query
from app.repositories.d1.posts import RELEVANT_POST_SQL

_ready = False


async def ensure_schema() -> bool:
    global _ready
    if not _ready:
        ok = await d1_query(
            """CREATE TABLE IF NOT EXISTS movie_comment_hourly (
                movie_id TEXT NOT NULL, hour TEXT NOT NULL, total INTEGER NOT NULL, negative INTEGER NOT NULL,
                run TEXT NOT NULL, PRIMARY KEY (movie_id, hour))""",
            timeout=60.0,
        )
        _ready = ok is not None
    return _ready


async def rebuild_hours(start: datetime, end: datetime) -> bool:
    """Tính lại các giờ trong [start, end) (UTC) cho mọi phim. hour = 'YYYY-MM-DDTHH' giờ VN."""
    if not await ensure_schema():
        return False
    run = uuid.uuid4().hex[:12]
    s, e = start.astimezone(UTC).isoformat(timespec="seconds"), end.astimezone(UTC).isoformat(timespec="seconds")
    vn = lambda at: (at.astimezone(UTC) + timedelta(hours=7)).strftime("%Y-%m-%dT%H")  # noqa: E731
    upsert = await d1_query(
        f"""INSERT INTO movie_comment_hourly (movie_id, hour, total, negative, run)
            SELECT p.movie_id, strftime('%Y-%m-%dT%H', c.posted_at, '+7 hours'), COUNT(*),
                   SUM(c.sentiment = 'negative' AND COALESCE(c.sentiment_source, '') != 'rule'), ?3
            FROM comments c JOIN posts p ON p.id = c.post_id
            WHERE c.posted_at >= ?1 AND c.posted_at < ?2 AND p.movie_id IS NOT NULL AND {RELEVANT_POST_SQL}
            GROUP BY 1, 2
            ON CONFLICT (movie_id, hour) DO UPDATE SET total = excluded.total, negative = excluded.negative,
                run = excluded.run""",
        [s, e, run],
        timeout=120.0,
    )
    if upsert is None:
        return False
    # Giờ không còn bình luận nào (bài bị ẩn/xoá) thì xoá dòng cũ của giờ đó.
    await d1_query(
        "DELETE FROM movie_comment_hourly WHERE hour >= ?1 AND hour < ?2 AND run != ?3", [vn(start), vn(end), run]
    )
    return True
```

Trước khi chạy, kiểm tra giá trị thật của cột `sentiment` (có phải `'negative'` không):

```bash
.venv/bin/python - <<'EOF'
import asyncio
from app.clients.d1 import d1_query
print(asyncio.run(d1_query("SELECT sentiment, COUNT(*) n FROM comments WHERE posted_at >= datetime('now','-1 day') GROUP BY 1")))
EOF
```

**Gắn lịch** (nhẹ, chạy trong scheduler): thêm vào `app/services/crawl/scheduler/ops.py` hoặc file mới
`scheduler/attack.py`:

```python
async def comment_hourly_tick() -> None:
    """Mỗi 10 phút tính lại 3 giờ gần nhất (bình luận về muộn vẫn được đếm đúng giờ đăng)."""
    if not await claim("comment_hourly", 600):
        return
    now = datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0)
    await rebuild_hours(now - timedelta(hours=3), now + timedelta(hours=1))
```

rồi thêm một dòng vào `TICKS` trong `scheduler/__init__.py`: `("comment_hourly", attack.comment_hourly_tick, False),`.

**Dựng 30 ngày cũ một lần** (ban đêm, từng ngày một để D1 không quá tải):

```bash
.venv/bin/python - <<'EOF'
import asyncio
from datetime import UTC, datetime, timedelta
from app.repositories.d1.comment_hourly import rebuild_hours
async def main():
    end = datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0)
    for d in range(30, 0, -1):
        start = end - timedelta(days=d)
        print(start.date(), await rebuild_hours(start, start + timedelta(days=1)))
        await asyncio.sleep(5)
asyncio.run(main())
EOF
```

**Xong khi:** `SELECT hour, total, negative FROM movie_comment_hourly WHERE movie_id = ? ORDER BY hour DESC LIMIT 24`
ra 24 giờ có số, và số cộng theo ngày gần khớp `movie_comment_daily` của ngày đó.

### 4.3 Bước 1b: dấu hiệu A - chê dồn dập

**Mục tiêu:** hàm thuần `find_spikes` + test + lệnh dry-run in được đợt tăng của phim cũ.

```python
# app/services/attack/signals.py
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Signal:
    key: str
    fired: bool
    strong: bool
    score: float
    window: tuple[str, str] | None = None
    evidence: list[dict] = field(default_factory=list)
    detail: dict = field(default_factory=dict)
```

```python
# app/services/attack/spikes.py
"""Dấu hiệu A: số bình luận chê của một giờ cao bất thường so với cùng giờ đó trong 14 ngày trước."""

from __future__ import annotations

from datetime import datetime, timedelta
from statistics import median

BASELINE_DAYS = 14
EVENT_WINDOW = timedelta(hours=6)


def find_spikes(
    hours: list[dict], events: list[str], *, min_count: int = 30, ratio: float = 3.0, share_ratio: float = 1.5
) -> list[dict]:
    """hours: [{"hour": "YYYY-MM-DDTHH", "total", "negative"}] liên tục từng giờ (giờ trống = 0), cũ -> mới.
    events: các mốc 'YYYY-MM-DDTHH' (premiere, khởi chiếu, mốc tay)."""
    marks = [datetime.fromisoformat(e) for e in events]
    spikes = []
    for i in range(BASELINE_DAYS * 24, len(hours)):
        now = hours[i]
        past = [hours[i - 24 * k] for k in range(1, BASELINE_DAYS + 1)]  # cùng giờ-trong-ngày
        base = median(h["negative"] for h in past)
        base_share = sum(h["negative"] for h in past) / max(1, sum(h["total"] for h in past))
        share = now["negative"] / max(1, now["total"])
        if now["negative"] < min_count or now["negative"] < ratio * max(base, 1):
            continue
        if share < share_ratio * base_share:  # chỉ tổng tăng (bài viral) - không tính
            continue
        at = datetime.fromisoformat(now["hour"])
        near = any(abs(at - mark) <= EVENT_WINDOW for mark in marks)
        spikes.append({**now, "baseline": base, "share": share, "base_share": base_share, "near_event": near,
                       "strong": not near})
    return spikes
```

`fromisoformat("2026-11-16T08")` chạy được từ Python 3.11. Test (`tests/test_attack_spikes.py`), tự viết thêm ca:

```python
from app.services.attack.spikes import find_spikes


def _hours(days, spike_at=None, spike=0):
    rows = []
    for d in range(days):
        for h in range(24):
            hour = f"2026-11-{1 + d:02d}T{h:02d}"
            rows.append({"hour": hour, "total": 100, "negative": 10 + (spike if hour == spike_at else 0)})
    return rows


def test_spike_found_and_flagged_strong():
    spikes = find_spikes(_hours(16, "2026-11-16T20", 60), [])
    assert [s["hour"] for s in spikes] == ["2026-11-16T20"] and spikes[0]["strong"]


def test_spike_near_event_is_not_strong():
    spikes = find_spikes(_hours(16, "2026-11-16T20", 60), ["2026-11-16T18"])
    assert spikes and not spikes[0]["strong"]

# TODO tự viết: ca tổng tăng cùng tỷ lệ (total 1000, negative 100) -> không phải spike; ca dưới min_count.
```

**Đọc dữ liệu** (`app/services/attack/data.py`): một hàm `async def load_hours(movie_id, days=16)` đọc
`movie_comment_hourly` của phim, rồi **điền giờ trống bằng 0** (vòng từ giờ đầu tới giờ cuối, `timedelta(hours=1)`).
Thiếu bước điền thì `hours[i - 24*k]` lệch giờ.

**Dry-run** (`app/services/ai_jobs/attack_detect.py`):

```python
async def dry_run(movie_id: str) -> None:
    hours = await load_hours(movie_id, days=60)
    events = await load_events(movie_id)     # premiere_date / released_at -> 'YYYY-MM-DDT00'
    for spike in find_spikes(hours, events):
        print(spike["hour"], spike["negative"], "vs", spike["baseline"], "sự kiện" if spike["near_event"] else "")

if __name__ == "__main__":
    import argparse, asyncio
    parser = argparse.ArgumentParser()
    parser.add_argument("--movie-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(dry_run(args.movie_id))
```

```bash
.venv/bin/python -m app.services.ai_jobs.attack_detect --movie-id <id Bò Sữa Bay> --dry-run
```

**Xong khi:** test xanh; dry-run trên Bò Sữa Bay / Án Mạng Xém Hoàn Hảo in vài giờ, và khi mở các bình luận của giờ đó
thấy đúng là lúc chê dồn (đọc tay 5 giờ).

### 4.4 Bước 2: dấu hiệu B - bình luận chê giống nhau

**Mục tiêu:** hàm thuần `similar_clusters` + dry-run in cụm và số tài khoản.

Đọc embedding từ SQLite (cùng file với Bước 0):

```python
import sqlite3
import numpy as np
from scripts.jobs.embed_comments import DB_PATH

def load_vectors(comment_ids: list[str]) -> dict[str, np.ndarray]:
    conn = sqlite3.connect(DB_PATH)
    out = {}
    for i in range(0, len(comment_ids), 500):   # SQLite giới hạn số tham số
        chunk = comment_ids[i : i + 500]
        marks = ",".join("?" * len(chunk))
        for cid, blob in conn.execute(f"SELECT comment_id, embedding FROM comment_embeddings WHERE comment_id IN ({marks})", chunk):
            out[cid] = np.frombuffer(blob, dtype=np.float16).astype(np.float32)
    return out
```

Khung hàm cụm (union-find giống mục 1.3):

```python
def similar_clusters(rows: list[dict], vectors: np.ndarray, *, sim: float = 0.92, min_size: int = 5) -> list[dict]:
    """rows: [{"id","author_id","post_id","platform","posted_at","message"}], vectors (n, 384) đã chuẩn hoá, cùng thứ tự."""
    n = len(rows)
    parent = list(range(n))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    sims = vectors @ vectors.T
    for i, j in zip(*np.where(np.triu(sims, 1) >= sim)):
        parent[find(i)] = find(j)
    groups: dict[int, list[int]] = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(i)
    clusters = []
    for members in groups.values():
        if len(members) < min_size:
            continue
        items = [rows[i] for i in members]
        # TODO: số tài khoản khác nhau (author_id), số bài, số nền tảng,
        #       độ dồn = tỷ lệ bình luận nằm trong cửa sổ 2 giờ dày nhất (sắp posted_at, cửa sổ trượt),
        #       độ giống trung bình = trung bình sims[i, j] trong cụm
        clusters.append({"size": len(items), "items": items})
    return sorted(clusters, key=lambda c: -c["size"])
```

Trước khi gọi `similar_clusters`, gộp trước các câu trùng chữ (chuẩn hoá bằng `app.ai.tasks.review_angles.fold`, bỏ
dấu câu/emoji/@tag) để ma trận nhỏ lại. Hơn 5.000 câu thì chia theo bài.

Test: 6 câu gần như y hệt từ 6 tài khoản → 1 cụm, `accounts == 6`; 6 câu khác ý → không cụm nào. Vector giả: tạo
một vector ngẫu nhiên, cộng nhiễu nhỏ cho 6 câu giống, chuẩn hoá lại (`v / np.linalg.norm(v)`).

**Xong khi:** dry-run in được cụm lớn nhất của Bò Sữa Bay kèm số tài khoản, độ dồn; đọc tay 3 cụm thấy đúng là câu
giống nhau.

### 4.5 Bước 3: trạng thái + lưu + chạy hằng giờ

1. `app/services/attack/verdict.py`: `decide(signals, coverage) -> (status, confidence, reasons)` theo bảng 2.5. Viết test
   trước cho 4 trạng thái và ca "hai dấu hiệu mạnh nhưng không chồng thời gian → `suspicious`" (so `window` bằng
   `a_start < b_end and b_start < a_end`).
2. `app/services/attack/store.py`: tạo bảng `attack_signals` (mục 2.6) như `ensure_schema` ở 4.2; `save(row)` bằng
   `insert(...)` + `d1_exec`; xoá dòng cũ hơn 60 ngày.
3. `app/services/ai_jobs/attack_detect.py` thêm `async def run_tick()`: với mỗi phim từ `showing_movies(now)`, đọc dữ
   liệu → A, B → `decide` → `save`. Chưa gọi AI ở bước này.
4. Gắn lịch trong `app/services/crawl/scheduler/ai_jobs.py`:

   ```python
   from app.services.ai_jobs import attack_detect
   AI_JOB_EVERY = {..., "attack_detect": 3600}

   # trong ai_jobs_tick():
       if datetime.now(tz=UTC).minute >= 15 and await claim("attack_detect", AI_JOB_EVERY["attack_detect"]):
           _run("attack_detect", attack_detect.run_tick)
   ```

5. Chưa hiện trên trang. Để chạy vài ngày, đọc bảng:
   `SELECT movie_id, computed_at, status, confidence FROM attack_signals ORDER BY computed_at DESC LIMIT 20`.

**Xong khi:** mỗi giờ có một dòng cho mỗi phim đang chiếu; phần lớn là `clear`; các dòng `suspicious` đọc tay có lý.
Sau đó làm Bước 4 (thử ngưỡng, mục 2.9) trước khi làm giao diện.

### 4.6 Khi bị kẹt

| Hiện tượng | Xem gì |
|---|---|
| D1 báo `7429 overloaded` | Câu đang quét quá rộng. Thêm điều kiện theo index (`posted_at`, `post_id`, `movie_id`), chia nhỏ khoảng thời gian |
| Truy vấn trả `None` im lặng | `d1_query` trả None khi lỗi - đọc log `d1_*` ngay trên dòng đó; kiểm tra biến trùng tên bảng (mục 3) |
| Tick không chạy | Log scheduler: `journalctl -u cinemark-scheduler -n 100 --no-pager \| grep <tên tick>`; khoá `claim` còn sống trong Redis: `redis-cli TTL social_crawler:scheduler:<tên>` |
| Timer không chạy | `systemctl list-timers`, `systemctl status <tên>.service`, `journalctl -u <tên> -n 50` |
| Test đỏ sau khi sửa | Chạy riêng file: `.venv/bin/python -m pytest -q tests/test_attack_spikes.py -x` |
| Muốn hỏi lại thiết kế | Mục 2.4-2.5 là quy tắc đã chốt; ngưỡng chỉ đổi sau khi thử trên dữ liệu cũ (2.9) |
