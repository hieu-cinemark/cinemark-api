# Review Angles + Attack Detection: phần còn thiếu, hướng dẫn tự làm

Viết 18/11/2026, đối chiếu tài liệu "Social Topic - Bản 3 (26/10)" với code lúc đó. Mỗi việc có: đang có gì, làm gì, ở file nào,
khung code, test, cách kiểm tra. Làm theo thứ tự trong mục 0.3.

---

## 0. Trước khi bắt đầu

### 0.1 Bản đồ code

| Việc | Repo | File chính |
|---|---|---|
| Job đọc bài + gom góc (ghi R2) | cinemark-api | `app/services/ai_jobs/review_angles.py` |
| Worker đọc góc (R2 → API) | cinemark | `apps/api/src/services/reviewAngles/filmReviewAngles.ts`, route `apps/api/src/routes/reviewAngles.ts` |
| Kiểu dữ liệu góc | cinemark | `packages/contracts/src/types/filmReviewAngles.ts` |
| Giao diện góc | cinemark | `apps/web/src/features/social-topic/components/ReviewAngles.tsx`, `review-angles/AngleList.tsx`, `AngleRow.tsx` |
| Lịch đo (mốc đo) | cinemark | `apps/api/src/services/socialTopic/measurePlan.ts` (`loadMeasurePlan`, `measurementSlots`) |
| Job Attack Detection (mỗi giờ) | cinemark-api | `app/services/ai_jobs/attack_detect.py` |
| Các dấu hiệu | cinemark-api | `app/services/attack/{data,spikes,similar,verdict,store,signals}.py` |
| Worker đọc Attack | cinemark | `apps/api/src/services/socialTopic/attack.ts` |
| Kiểu dữ liệu Attack | cinemark | `packages/contracts/src/types/attackDetection.ts` |
| Giao diện Attack | cinemark | `apps/web/src/features/social-topic/components/AttackDetection.tsx`, `attack-detection/*` |
| Bảng giai đoạn (mẫu để thêm bảng mốc sự kiện) | cinemark | `apps/api/src/schema/ensureAppTables.ts::ensureMoviePlanTables` |

### 0.2 Quy ước bắt buộc

- **D1:** đọc theo phim, chia `post_id` thành lô 40; không `post_id IN (subquery)` kèm `ORDER BY`; job nặng thì `await asyncio.sleep`
  giữa các lô (D1 báo 7429 khi quá tải).
- **R2 state của Review Angles:** khoá camelCase vì Worker đọc thẳng. Đổi cấu trúc thì sửa cả hai repo cùng lúc.
- **Log cinemark-api:** mỗi event mới phải có câu tiếng Việt trong `app/core/log_messages.py` (test `test_log_messages.py` chặn).
- **Test:**
  - cinemark-api: `.venv/bin/python -m pytest -q`
  - cinemark: `pnpm typecheck && pnpm test`
- **Push:** cinemark-api `main` (tự deploy qua GitHub Actions) trước, cinemark `dev` sau (không bao giờ `main`).
  Sao lưu `state.json` trước khi chạy script sửa state thật:
  `npx wrangler r2 object get cinemark-lake/analysis/review-angles/v2/<movieId>/state.json --file backup-<movieId>.json --remote`

### 0.3 Thứ tự đề nghị

1. Review Angles R1 (bù `postedAt`) → R2 (thay đổi giữa lần đo) → R3 (so sánh nhiều phim).
2. Attack A0 (chuẩn bị) → A1 (dấu hiệu C) → A2 (D) → A3 (E không AI) → A4 (verdict) → A5 (mốc sự kiện tay) → A6 (giao diện).
3. Cuối cùng A7: thử ngưỡng trên dữ liệu cũ.

---

## PHẦN R — Review Angles (mục 4.4 của tài liệu)

Đang có: góc xếp theo số bài, khen/chê từng góc, dẫn chứng có nguồn, tính từ Premiere.
Thiếu: (1) thay đổi của từng góc giữa các lần đo, (2) so sánh nhiều phim cạnh nhau.

### R1. Bù `postedAt` cho các ref cũ

**Vì sao:** muốn đếm "góc này có bao nhiêu bài trong khoảng giữa 2 mốc đo" thì mỗi ref (một lần bài nhắc góc) phải có ngày đăng.
Ref ghi từ khoảng 16/11 đã có `postedAt` (`review_angles.py`, đoạn `angle["refs"].append({... "postedAt": post["postedAt"] ...})`).
Ref cũ hơn thì không.

**Kiểm tra trước:** đếm ref thiếu `postedAt` của từng phim.

```python
# scripts/tools/review_angles_posted_at.py
"""Bù postedAt cho ref cũ trong state.json của Review Angles (đọc D1 theo id, lô 40). --dry-run chỉ đếm."""
import argparse, asyncio
from sqlalchemy import select
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import posts
from app.services.ai_jobs.review_angles import _read_json, _write_json, _root, _active_movies

async def fix(movie_id: str, dry_run: bool) -> tuple[int, int]:
    key = f"{_root(movie_id)}/state.json"
    state = await _read_json(key)
    if not state:
        return 0, 0
    missing = sorted({r["id"] for a in state["angles"] for r in a["refs"] if not r.get("postedAt")})
    if dry_run or not missing:
        return len(missing), 0
    found: dict[str, str] = {}
    for i in range(0, len(missing), 40):
        rows = await d1_exec(select(posts.c.id, posts.c.posted_at).where(posts.c.id.in_(missing[i:i + 40]))) or []
        found.update({r["id"]: r["posted_at"] for r in rows if r.get("posted_at")})
        await asyncio.sleep(0.5)
    for angle in state["angles"]:
        for ref in angle["refs"]:
            if not ref.get("postedAt") and ref["id"] in found:
                ref["postedAt"] = found[ref["id"]]
    await _write_json(key, state)
    return len(missing), len(found)
```

- `_root`, `_read_json`, `_write_json` có sẵn trong `review_angles.py`. `_active_movies` trả phim đang xoay vòng; muốn
  chạy mọi phim từng có state thì liệt kê prefix trên R2.
- **Lưu ý race:** job `review_angles` có thể ghi `state.json` cùng lúc. Chạy script lúc job tạm dừng, hoặc lấy khoá Redis
  mà job dùng (`lock_key` trong `analyze_next_batch`) trước khi đọc/ghi.
- Ref không tìm thấy bài (bài đã xoá) thì để trống; Worker coi như không thuộc khoảng nào.

**Kiểm tra:** chạy `--dry-run` cho Bò Sữa Bay và Chị Chị Em Em 3 thấy số thiếu; chạy thật rồi `--dry-run` lại phải ra 0
(hoặc chỉ còn số bài đã xoá).

### R2. Thay đổi của từng góc giữa các lần đo

**Ý tưởng:** lấy 2 mốc đo gần nhất **đã qua** của phim (lịch đo có sẵn). Kỳ này = `[mốc trước, mốc gần nhất)`,
kỳ trước = `[mốc trước nữa, mốc trước)`. Đếm ref của mỗi góc rơi vào từng kỳ.

**Worker — `filmReviewAngles.ts`:**

1. Import `loadMeasurePlan` từ `../socialTopic/measurePlan`.
2. Hàm thuần chọn 2 kỳ (để test được):

```ts
export interface AnglePeriod { from: string; to: string }   // ISO UTC
/*** Kỳ này + kỳ trước từ các mốc đo đã qua; chưa đủ 3 mốc thì dùng 2 khoảng 7 ngày gần nhất. ***/
export const anglePeriods = (slots: { at: string }[], now: number): { current: AnglePeriod; previous: AnglePeriod; basis: 'measure' | 'week' } => {
  const past = slots.map((s) => s.at).filter((at) => Date.parse(at) <= now).sort();
  if (past.length >= 3) {
    const [a, b, c] = past.slice(-3);
    return { previous: { from: a, to: b }, current: { from: b, to: c }, basis: 'measure' };
  }
  const end = new Date(now).toISOString();
  const mid = new Date(now - 7 * 86_400_000).toISOString();
  const start = new Date(now - 14 * 86_400_000).toISOString();
  return { previous: { from: start, to: mid }, current: { from: mid, to: end }, basis: 'week' };
};
```

   - Ghi chú: mốc đo trong 10 ngày Premiere cách nhau 12 giờ, nên kỳ này/kỳ trước là 12 giờ; sau đó là theo giai đoạn/thứ Hai.
     Hiện thêm nhãn khoảng thời gian trên giao diện để người đọc biết đang so cái gì.
   - So `postedAt` (chuỗi ISO) với `from/to`: đổi về `Date.parse` để tránh lệch định dạng (`+00:00` và `Z`).

3. Trong `publicAngle`, thêm:

```ts
const inPeriod = (ref: StoredRef, p: AnglePeriod) => {
  const t = ref.postedAt ? Date.parse(ref.postedAt) : NaN;
  return Number.isFinite(t) && t >= Date.parse(p.from) && t < Date.parse(p.to);
};
// trong object trả về:
trend: {
  current: refs.filter((r) => inPeriod(r, periods.current)).length,
  previous: refs.filter((r) => inPeriod(r, periods.previous)).length,
},
```

   `publicAngle` cần nhận thêm `periods`. Kiểu `StoredAngle` (`filmReviewParse.ts`) thêm `postedAt?: string | null` cho ref.

4. `loadFilmReviewAngles`: gọi `loadMeasurePlan(env, movieId)` (song song với đọc state), tính `periods`, trả thêm
   `periods` ở cấp kết quả.
   - `loadMeasurePlan` đọc D1 (bảng giai đoạn) mỗi lần: bọc `swrCache` 10 phút giống `loadMovie` để không tăng tải D1.

**Contracts — `filmReviewAngles.ts`:**

```ts
export interface FilmReviewAngle { ...; trend: { current: number; previous: number } }
export interface FilmReviewAnglesResult { ...; periods: { current: { from: string; to: string }; previous: { from: string; to: string }; basis: 'measure' | 'week' } }
```

**Giao diện:**

- `AngleRow.tsx`: cạnh số bài, hiện `▲ +n` (xanh) / `▼ −n` (đỏ) / `—`. Góc mới (`previous = 0`, `current > 0`) hiện nhãn "Mới".
- `ReviewAnglesHeader.tsx` (hoặc `ReviewAnglesStats.tsx`): một dòng "So sánh: dd/mm HH:mm–dd/mm HH:mm với kỳ trước", ghi
  "theo mốc đo" hoặc "theo 7 ngày" theo `basis`.
- Thêm cách sắp xếp "Tăng nhiều nhất" (theo `current - previous`) bên cạnh "Nhiều bài nhất".

**Test (cinemark `tests/`, node --test):**
- `anglePeriods` với 5 mốc đã qua → đúng 3 mốc cuối; với 1 mốc → chế độ `week`.
- `inPeriod`: ref đúng mốc `from` tính vào kỳ, đúng mốc `to` thì không; ref thiếu `postedAt` không tính.

**Tuỳ chọn — số "tại lúc đo" không đổi về sau:** job `review_angles` mỗi lượt kiểm: mốc đo gần nhất đã qua mà chưa có
`snapshots/<at>.json` thì ghi `{ at, angles: [{ id, posts }] }`. Worker ưu tiên snapshot khi có. Không bắt buộc cho bản đầu.

### R3. So sánh nhiều phim

**Ý tưởng:** góc riêng mỗi phim khác nhau nên không so trực tiếp được. Gom lên **14 khía cạnh chung** (`REVIEW_ASPECTS`, trường
`aspect` có sẵn trên mỗi góc), so **tỷ trọng** từng khía cạnh giữa các phim. Bấm vào một khía cạnh thì xem các góc riêng của
từng phim trong khía cạnh đó.

**Worker — hàm mới `compareFilmReviewAngles(env, movieIds, platform)`** trong `filmReviewAngles.ts`:

```ts
export const compareFilmReviewAngles = async (env: Env, movieIds: string[], platform: string) => {
  const films = await Promise.all(movieIds.map(async (movieId) => {
    const result = await loadFilmReviewAngles(env, movieId, platform);   // dùng lại, đã lọc nền tảng
    const total = result.analysis.angles.reduce((s, a) => s + a.posts, 0);
    const byAspect: Record<string, { posts: number; share: number; khen: number; che: number; top: { id: string; name: string; posts: number }[] }> = {};
    for (const a of result.analysis.angles) {
      const row = (byAspect[a.aspect] ??= { posts: 0, share: 0, khen: 0, che: 0, top: [] });
      row.posts += a.posts; row.khen += a.stance.khen; row.che += a.stance.che;
      row.top.push({ id: a.id, name: a.name, posts: a.posts });
    }
    for (const row of Object.values(byAspect)) { row.share = total ? row.posts / total : 0; row.top = row.top.sort((x, y) => y.posts - x.posts).slice(0, 3); }
    return { movieId, title: result.movieTitle, available: result.available, analyzedPosts: result.analyzedPosts, byAspect };
  }));
  return { platform, aspects: Object.keys(REVIEW_ASPECTS), films };
};
```

- Một bài có thể nằm ở 2 góc (tối đa 2 góc/bài) nên tổng `posts` của các góc > số bài đã đọc. Tỷ trọng tính trên tổng ref
  (như trên) là nhất quán giữa các phim; ghi rõ trên giao diện "tỷ trọng lượt nhắc".
- Không đọc D1 bài viết, chỉ R2 (mỗi phim 1 file `state.json`).

**Route — `routes/reviewAngles.ts`:** `GET /compare?movieIds=a,b,c&platform=`:
- Tách `movieIds`, giới hạn tối đa 6 phim, kiểm quyền bằng `filterAccessibleMovieIds` (như `readMovie`) cho cả danh sách,
  thêm schema vào `@cinemark/contracts/api/content` cạnh `reviewAnglesQuerySchema`.

**Contracts:** `FilmReviewCompareResult` theo đúng kiểu trả về ở trên.

**Giao diện:**
- `SocialTopic.tsx::ReviewAnglesTab`: khi `movies.length > 1`, thêm `Segmented` "So sánh các phim | Chi tiết từng phim".
  "Chi tiết" giữ nguyên `MovieSwitcher` + `ReviewAngles` như hiện tại.
- Component mới `review-angles/AngleCompare.tsx`:
  - Bảng: hàng = khía cạnh, cột = phim, ô = tỷ trọng % + thanh nhỏ; tô đậm phim cao nhất mỗi hàng; cột phụ khen/chê.
  - Hoặc biểu đồ cột nhóm (recharts `BarChart`, mỗi phim một màu) cho 6–8 khía cạnh lớn nhất.
  - Bấm một hàng: xổ 3 góc riêng lớn nhất của từng phim (`top`), bấm góc thì chuyển sang "Chi tiết" đúng phim đó.
- Phim chưa tới Premiere (`available = false`): hiện cột xám "Chưa tới Premiere".

**Test:** `compareFilmReviewAngles` với state giả của 2 phim → tỷ trọng đúng, khía cạnh không có góc = 0.

---

## PHẦN A — Attack Detection (mục 3.2 của tài liệu)

Đang có: A (chê dồn dập, `spikes.py`), B (bình luận chê giống nhau, `similar.py`), 4 trạng thái (`verdict.py`), job mỗi giờ,
tab trên trang. Thiếu: C (tài khoản hành vi giống nhau), D (luận điểm lan rộng), E (khẳng định cần kiểm tra), mốc sự kiện
nhập tay, so với phim tương đồng, thử ngưỡng.

### A0. Chuẩn bị

1. **`similar.py::similar_clusters`:** mỗi cụm thêm `"member_ids": [rows[i]["id"] for i in members]`.
   `similar_signal` bỏ `member_ids` khỏi `detail` giống cách đang bỏ `evidence`:
   `{k: v for k, v in c.items() if k not in ("evidence", "member_ids")}`.
2. **`similar_signal(rows, vectors, clusters=None)`:** nhận cụm đã tính sẵn (nếu `None` thì tự tính như cũ) để C/D dùng chung.
3. **`data.py::load_negative_comments`:** select thêm `p.c.author.label("post_author")` (cần cho D).
4. **`attack_detect.py::detect`:**

```python
clusters = similar_clusters(negatives, vectors)
signals = [
    spike_signal(hours, events, since_hour=data.vn_hour(start)),
    similar_signal(negatives, vectors, clusters=clusters),
    account_signal(negatives, clusters),
    spread_signal(negatives, clusters),
    claims_signal(negatives),
]
```

5. **`signals.py`:** cập nhật comment của `Signal.key`: `"spike" | "similar" | "accounts" | "spread" | "claims"`.

### A1. Dấu hiệu C — tài khoản hành vi giống nhau (`app/services/attack/accounts.py`)

**Phạm vi:** chỉ bình luận có `author_id` (Facebook, TikTok). Threads/YouTube thiếu id ổn định thì bỏ qua. Không có IP, bạn bè
nên không suy luận theo những thứ đó.

**Ba luật cho mỗi tài khoản (trong 48 giờ dữ liệu đã đọc):**

| Luật | Điều kiện | Hằng số |
|---|---|---|
| L1 nhiều bài | chê ở ≥ 5 bài khác nhau trong một khoảng 24 giờ bất kỳ (cửa sổ trượt) | `MANY_POSTS = 5`, `MANY_POSTS_WINDOW = 24h` |
| L2 đi cặp | có tài khoản khác cùng bình luận dưới ≥ 3 bài chung, mỗi lần cách nhau ≤ 10 phút, và Jaccard tập bài ≥ 0,5 | `PAIR_POSTS = 3`, `PAIR_GAP = 10min`, `PAIR_JACCARD = 0.5` |
| L3 nhiều cụm | có mặt trong ≥ 2 cụm của dấu hiệu B | `MULTI_CLUSTER = 2` |

- Tài khoản **đạt** khi thoả ≥ 2 luật.
- **Ghép nhóm** (union-find như `similar.py`): nối 2 tài khoản khi là cặp L2, hoặc cùng nằm trong một cụm B và cả hai thoả L3.
- `fired` khi có nhóm có ≥ 2 tài khoản đạt; `strong` khi có nhóm có ≥ `STRONG_ACCOUNTS = 5` tài khoản đạt.
- `window` = giờ VN của bình luận sớm nhất và muộn nhất của các tài khoản đạt trong nhóm (dạng `YYYY-MM-DDTHH`, dùng
  `similar._vn_hour`), để `verdict` đối chiếu chồng thời gian với A/B.

**Khung code:**

```python
"""Dấu hiệu C: nhóm tài khoản có hành vi giống nhau (docs/review-angles-va-attack.md mục 2.4 C)."""
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import combinations
from app.services.attack.signals import Signal, mask_name
from app.services.attack.similar import _vn_hour

MANY_POSTS, MANY_POSTS_WINDOW = 5, timedelta(hours=24)
PAIR_POSTS, PAIR_GAP, PAIR_JACCARD = 3, timedelta(minutes=10), 0.5
MULTI_CLUSTER, STRONG_ACCOUNTS = 2, 5
MAX_PER_POST = 400  # bài quá nhiều bình luận: chỉ xét chừng này bình luận gần nhất để cặp không bùng nổ

def _at(row): return datetime.fromisoformat(row["posted_at"])

def _many_posts(times_by_post: dict[str, list[datetime]]) -> bool:
    """L1: có cửa sổ 24 giờ chứa >= MANY_POSTS bài khác nhau (lấy lần bình luận đầu ở mỗi bài)."""
    firsts = sorted(min(ts) for ts in times_by_post.values())
    j = 0
    for i, t in enumerate(firsts):
        while t - firsts[j] > MANY_POSTS_WINDOW: j += 1
        if i - j + 1 >= MANY_POSTS: return True
    return False

def account_groups(rows, clusters):
    rows = [r for r in rows if r.get("author_id") and r.get("posted_at")]
    by_acc: dict[str, dict[str, list[datetime]]] = defaultdict(lambda: defaultdict(list))
    by_post: dict[str, list[tuple[datetime, str]]] = defaultdict(list)
    for r in rows:
        by_acc[r["author_id"]][r["post_id"]].append(_at(r))
        by_post[r["post_id"]].append((_at(r), r["author_id"]))
    rule = defaultdict(set)                                  # acc -> {"many_posts","pair","multi_cluster"}
    for acc, posts in by_acc.items():
        if _many_posts(posts): rule[acc].add("many_posts")
    # L2: cặp gần nhau dưới cùng bài (cửa sổ trượt 10 phút trên bình luận đã sắp theo giờ)
    shared: dict[tuple[str, str], set[str]] = defaultdict(set)
    for post_id, items in by_post.items():
        items = sorted(items)[-MAX_PER_POST:]
        j = 0
        for i, (t, a) in enumerate(items):
            while t - items[j][0] > PAIR_GAP: j += 1
            for _, b in items[j:i]:
                if a != b: shared[tuple(sorted((a, b)))].add(post_id)
    pairs = []
    for (a, b), posts in shared.items():
        union = set(by_acc[a]) | set(by_acc[b])
        if len(posts) >= PAIR_POSTS and len(posts) / len(union) >= PAIR_JACCARD:
            pairs.append((a, b)); rule[a].add("pair"); rule[b].add("pair")
    # L3
    author_of = {r["id"]: r["author_id"] for r in rows}
    clusters_of = defaultdict(set)
    for ci, c in enumerate(clusters):
        for cid in c.get("member_ids", []):
            if cid in author_of: clusters_of[author_of[cid]].add(ci)
    for acc, cs in clusters_of.items():
        if len(cs) >= MULTI_CLUSTER: rule[acc].add("multi_cluster")
    # ghép nhóm: union-find trên pairs + cùng cụm (với tài khoản thoả L3) - xem similar.py để chép find/union
    ...
    # mỗi nhóm: accounts, qualifying (len(rule) >= 2), posts, comments, platforms, window, members (tên che bớt), rules_count
    return groups  # sắp theo số tài khoản đạt giảm dần
```

- `combinations` không cần nếu dùng cửa sổ trượt như trên; giữ `MAX_PER_POST` để bài viral 5.000 bình luận không sinh hàng
  triệu cặp.
- `account_signal(rows, clusters) -> Signal(key="accounts", fired, strong, score=<số tài khoản đạt của nhóm lớn nhất>,
  window, evidence=<10 bình luận của nhóm đó, tên mask_name>, detail={"groups": [...5 nhóm đầu, bỏ id thật]})`.
- **Không lưu `author_id` thật** trong `detail`/`evidence` (bảng `attack_signals` hiện lên trang): chỉ tên đã che.

**Test (`tests/test_attack.py`):**
- 5 tài khoản cùng bình luận dưới 3 bài, mỗi bài cách nhau 3–5 phút, cộng mỗi tài khoản có ở ≥ 2 cụm giả → `strong`.
- 5 tài khoản cùng 3 bài nhưng cách nhau 3 giờ → không có cặp L2 → không `strong`.
- 1 tài khoản chê ở 6 bài trong 10 giờ → thoả L1 nhưng chỉ 1 luật → không đạt.
- Bình luận thiếu `author_id` bị bỏ qua.

### A2. Dấu hiệu D — luận điểm lan rộng (`app/services/attack/spread.py`)

**Không bao giờ `strong`** — chỉ thêm ngữ cảnh cho người đọc.

Cho 5 cụm B lớn nhất (`clusters[:5]`), dùng `member_ids` để lấy lại các dòng bình luận:

| Số đo | Cách tính |
|---|---|
| `posts` | số `post_id` khác nhau |
| `channels` | số kênh khác nhau: `channel_of_post({"url": row["post_url"], "platform": row["platform"], "author": row["post_author"]})` (`app/services/ai_jobs/channels.py`); `None` thì dùng `post_id` làm kênh |
| `platforms` | số nền tảng |
| `minutes_to_10_posts` | từ bình luận đầu tiên của cụm tới lúc cụm xuất hiện ở bài thứ 10 (lần đầu ở mỗi bài, sắp theo giờ); < 10 bài thì `None` |
| `top_post_share` | số bình luận dưới bài nhiều nhất / cỡ cụm |
| `common_source` | `top_post_share >= 0.6` → "một làn sóng từ nguồn chung" |

- `fired` khi có cụm `channels >= 3` hoặc `platforms >= 2`. `score` = số kênh lớn nhất.
- `window` = window của cụm đó (đã có trong cụm B).
- `evidence`: một bình luận đầu tiên ở mỗi kênh (tối đa 10).
- `detail = {"clusters": [{"sample", "posts", "channels", "platforms", "minutes_to_10_posts", "top_post_share", "common_source"}]}`.

**Test:** cụm 10 bình luận dưới cùng 1 bài → `common_source = True`, không fired; cụm trải 4 kênh 2 nền tảng → fired, không strong.

### A3. Dấu hiệu E — khẳng định cần kiểm tra (`app/services/attack/claims.py`)

**Không bao giờ `strong`.** Chỉ ghi "cần kiểm tra"; chỉ ghi "sai" khi đội truyền thông nhập nguồn đối chiếu (để sau).

**Bản không AI (làm trước, không tốn tiền):** dò bình luận chê 48 giờ theo nhóm cụm từ. So khớp trên chữ đã bỏ dấu
(`similar.normalize`), theo ranh giới từ.

```python
CLAIM_PATTERNS = {
    "dao_nhai":   ("Đạo nhái / sao chép", r"\b(dao nhai|dao phim|copy|sao chep|nhai lai)\b"),
    "seeding":    ("Seeding / review thuê / tài khoản ảo", r"\b(seeding|seed|review thue|thue review|acc clone|tai khoan ao|bot)\b"),
    "doanh_thu":  ("Doanh thu / vé ảo, lỗ vốn", r"\b(doanh thu ao|ve ao|bom doanh thu|mua ve ao|lo von|lo nang)\b"),
    "diem_ao":    ("Điểm / rating ảo", r"\b(diem ao|rating ao|buff diem|cay diem)\b"),
    "kiem_duyet": ("Bị cấm / cắt cảnh / ngừng chiếu", r"\b(bi cam|cam chieu|cat canh|kiem duyet|rut khoi rap|ngung chieu)\b"),
    "ep_suat":    ("Ép suất chiếu", r"\b(ep suat|gianh suat|khong cho chieu|doc quyen suat)\b"),
}
MIN_COMMENTS, MIN_ACCOUNTS = 5, 3
```

- Mỗi nhóm: số bình luận, số tài khoản khác nhau, 5 câu tiêu biểu (nhiều like nhất, tên đã che).
- `fired` khi có nhóm ≥ `MIN_COMMENTS` bình luận từ ≥ `MIN_ACCOUNTS` tài khoản. `score` = số bình luận của nhóm lớn nhất.
- Từ "bot", "copy" dễ trúng nhầm: xem kết quả backtest (A7) rồi bỏ/siết.

**Bản có AI (để sau, đang tạm dừng ngân sách AI):**
- Chỉ chạy khi có cụm B `strong`; lấy 3 cụm lớn nhất × 10 câu.
- `call_llm_json(system_prompt=CLAIMS_SYSTEM, user_prompt=..., max_tokens=800, task="attack_claims")`
  (`app/ai/runtime.py`). Kiểm `task` mới có cần khai báo trong cấu hình AI không trước khi gọi.
- Prompt: "Tách các KHẲNG ĐỊNH VỀ SỰ THẬT có thể kiểm chứng (vd 'phim đạo nhái phim X', 'rạp ép suất'); bỏ cảm nhận ('phim dở').
  Trả JSON [{claim, quotes: [nguyên văn], count}]". Kiểm `quotes` có thật trong đầu vào (giống `parse_batch` của Review Angles).
- Bọc sau cờ (env `ATTACK_CLAIMS_AI=1` hoặc trường trong Settings), mặc định tắt. Gộp kết quả vào cùng `detail.claims`.

### A4. Quy tắc trạng thái (`verdict.py`)

- Logic giữ nguyên: `strong` chỉ có thể đến từ A, B, C. Kiểm `spread_signal` và `claims_signal` luôn trả `strong=False`.
- `coordinated` = ≥ 2 dấu hiệu strong có window chồng nhau → giờ gồm cả "đợt tăng A + nhóm tài khoản C" hay "cụm B + nhóm C".
- Thêm câu trong `_reason`:

```python
if signal.key == "accounts":
    g = (signal.detail.get("groups") or [{}])[0]
    return f"Nhóm {int(signal.score)} tài khoản có hành vi giống nhau (cùng bình luận dưới {g.get('posts', 0)} bài, cách nhau dưới 10 phút)"
if signal.key == "spread":
    c = (signal.detail.get("clusters") or [{}])[0]
    return ("Một luận điểm lan qua {} kênh / {} nền tảng".format(c.get("channels"), c.get("platforms"))
            + (" - phần lớn từ một bài gốc" if c.get("common_source") else ""))
if signal.key == "claims":
    return f"{int(signal.score)} bình luận nêu khẳng định cần kiểm tra (không phải kết luận là sai)"
```

- Cập nhật bảng trạng thái ở docstring đầu file.

**Test:** C strong + B strong chồng giờ → `coordinated`; không chồng → `suspicious`; chỉ D/E fired → `clear` kèm lý do.

### A5. Mốc sự kiện nhập tay (mục 3.4 — tránh báo nhầm quanh trailer, họp báo, drama)

Hiện `data.load_events` chỉ lấy ngày chiếu sớm + ngày khởi chiếu.

1. **Bảng D1 (SCRAPER_DB)** — thêm vào `ensureMoviePlanTables` (cinemark, `ensureAppTables.ts`), cùng kiểu `movie_phases`:

```sql
CREATE TABLE IF NOT EXISTS movie_events (
  id TEXT PRIMARY KEY, movie_id TEXT NOT NULL, kind TEXT NOT NULL,   -- trailer | hop_bao | suat_som | drama | khac
  name TEXT NOT NULL, at TEXT NOT NULL,                               -- ISO UTC
  created_by TEXT, updated_at TEXT);
CREATE INDEX IF NOT EXISTS movie_events_movie ON movie_events (movie_id);
```

2. **Worker:** `listEvents / saveEvent / deleteEvent` trong `measurePlan.ts` (chép mẫu `listPhases / savePhase / deletePhase`),
   route cạnh route giai đoạn hiện có, cùng quyền.
3. **Giao diện:** trong `MeasurePlanCard.tsx` thêm khối "Mốc sự kiện" (thêm/sửa/xoá: loại, tên, ngày giờ). Có thể hiện mốc trên
   biểu đồ chê theo giờ của Attack (`AttackSignalCard` → `NegativeSeries`: vạch dọc `ReferenceLine`).
4. **cinemark-api:** `app/repositories/d1/tables.py` thêm `movie_events = Table(...)`; `data.load_events` đọc thêm bảng này,
   đổi `at` sang giờ VN `YYYY-MM-DDTHH` (`vn_hour`). Bảng chưa có (Worker chưa tạo) thì bỏ qua lỗi "no such table".
   - Khác mốc ngày (đang để `T00`), mốc tay có giờ thật nên `EVENT_WINDOW = ±6 giờ` của `spikes.py` sẽ chính xác hơn.

**Test:** `find_spikes` với mốc tay lúc 20:00 → đợt tăng 21:00 không strong.

### A6. Giao diện cho C, D, E

1. **Contracts `attackDetection.ts`:**

```ts
export interface AttackAccountGroup { accounts: number; qualifying: number; posts: number; comments: number; platforms: string[];
  window: [string, string] | null; rules: { many_posts: number; pair: number; multi_cluster: number };
  members: { author: string; posts: number; comments: number; rules: string[] }[] }
export interface AttackSpread { sample: string; posts: number; channels: number; platforms: number;
  minutes_to_10_posts: number | null; top_post_share: number; common_source: boolean }
export interface AttackClaim { key: string; label: string; comments: number; accounts: number; samples: AttackEvidence[] }
// AttackSignal.key thêm 'accounts' | 'spread' | 'claims'; detail thêm groups?, spread clusters (đặt tên spreadClusters? hoặc dùng chung clusters),
// claims?
```

   Lưu ý `detail.clusters` đang là kiểu `AttackCluster` của B. Với D đặt tên khác (`detail.spread`) để không lẫn kiểu.

2. **`attackFormat.ts::SIGNAL_META`:**
   - `accounts`: "Dấu hiệu C - Tài khoản hành vi giống nhau" / "Nhóm tài khoản bình luận cùng bài, cùng lúc, lặp lại."
   - `spread`: "Dấu hiệu D - Luận điểm lan rộng" / "Một nội dung chê xuất hiện ở nhiều kênh, nhiều nền tảng." + chú thích "Chỉ là ngữ cảnh".
   - `claims`: "Dấu hiệu E - Khẳng định cần kiểm tra" / "Bình luận nêu thông tin cần đối chiếu nguồn, không phải kết luận là sai."
3. **`AttackSignalCard.tsx`:** icon `Users` (C), `Share2` (D), `HelpCircle` (E) từ lucide-react; thêm 3 khối giống khối `similar`:
   - C: danh sách nhóm (n tài khoản đạt / n tài khoản, số bài, khoảng giờ), xổ ra bảng thành viên (tên đã che, số bài, luật).
   - D: mỗi cụm: câu mẫu, "x bài · y kênh · z nền tảng", "lan tới 10 bài sau n phút", nhãn "Nguồn chung" nếu `common_source`.
   - E: mỗi nhóm khẳng định: nhãn, số bình luận / tài khoản, 3–5 câu tiêu biểu, nhãn xám "Cần kiểm tra".
4. Thứ tự thẻ trên trang: A, B, C, D, E (đúng thứ tự trong tài liệu).

### A7. Thử ngưỡng trên dữ liệu cũ (bắt buộc trước khi giao đội truyền thông)

- Có sẵn CLI: `python -m app.services.ai_jobs.attack_detect --movie-id <id> --backfill-days 30 --step-hours 6 --dry-run`.
  Thêm ghi kết quả ra `data/attack_backtest/<movieId>.jsonl` (một dòng mỗi mốc: status, reasons, score từng dấu hiệu).
- Phim nên chạy: phim từng bị chê nhiều (Án Mạng Xém Hoàn Hảo, Bò Sữa Bay) + 2–3 phim bình thường để đo báo nhầm.
- Cần embedding của bình luận cũ trên máy chạy (`data/comment_embeddings.sqlite`) để B/C/D có dữ liệu.
- Đếm: số mốc `suspicious` / `coordinated` của phim bình thường phải gần 0. Chỉnh `MANY_POSTS`, `PAIR_POSTS`, `STRONG_ACCOUNTS`,
  cụm từ của E. Ghi ngưỡng cuối + lý do vào docstring `verdict.py`.
- Đội truyền thông duyệt 10–20 trường hợp trước khi bật cảnh báo Telegram cho `coordinated`.

### A8. Còn lại của mục 3 (để sau)

- **So với phim tương đồng (3.4):** khi phim có < 7 ngày dữ liệu, mức nền của A lấy trung vị cùng giờ của các phim cùng thể loại,
  ra rạp trong ±60 ngày (cần trường thể loại cho phim).
- **Đưa Attack vào báo cáo gửi nhà sản xuất (3.7):** thêm mục trong báo cáo Dejaview: trạng thái + 3 bằng chứng + câu lưu ý.

---

## Checklist xong việc

- [ ] cinemark-api `pytest -q` xanh; test mới cho accounts/spread/claims/verdict/load_events.
- [ ] cinemark `pnpm typecheck && pnpm test` xanh; test `anglePeriods`, `compareFilmReviewAngles`.
- [ ] Dry-run Attack cho 2 phim đang chiếu, đọc tay kết quả C/D/E.
- [ ] Script bù `postedAt` đã chạy (sao lưu state trước).
- [ ] Push cinemark-api `main` → chờ deploy → push cinemark `dev`.
- [ ] Mở Social Topic: tab Review Angles có mũi tên tăng/giảm + chế độ so sánh nhiều phim; tab Attack có đủ 5 thẻ.
