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
scheduler (_ai_jobs_tick, app/services/crawl/scheduler.py)
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

| # | Việc | Vì sao | Độ khó |
|---|---|---|---|
| A | Gộp góc trùng ý | 185 góc vụn, trang không dùng được | Vừa |
| B | Gửi AI catalog gọn | Catalog càng dài thì AI càng ít dùng lại id, prompt càng tốn token | Nhỏ |
| C | Chia lượt công bằng giữa các phim | Focus dồn lượt cho một phim, phim khác mới đọc ~100 bài | Nhỏ |
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
6. Bằng chứng: 10 câu của cụm (link bài, giờ, tên đã che bớt), tên chủ đề AI chứa cụm nếu có
   (`data/topics.sqlite`, bảng `comment_topics`).

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

- Gắn vào `_ai_jobs_tick` trong `scheduler.py` như `review_angles` (`_AI_JOB_EVERY["attack_detect"] = 3600`), chạy
  sau khi embedding cập nhật (phút 15 mỗi giờ nếu timer embed chạy phút 0).
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
