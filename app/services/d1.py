"""Tầng truy cập Cloudflare D1 - cho service chạy trên VPS này đọc/ghi một database D1
mà không cần Cloudflare Worker (binding D1 chỉ có bên trong Worker; từ một tiến trình
thường, REST query API của D1 là cửa duy nhất). Nói chuyện với cùng database D1 mà
Worker của cinemark-scraper sở hữu, dùng các bảng
movies/keywords/posts/post_engagement_snapshots có sẵn của nó (xem
cinemark-scraper/src/db/schema.ts) - không phải bảng riêng của mình, nên một lượt
crawl kích hoạt từ đây (get_enabled_keywords/get_keyword) và bài nó sinh ra
(persist_post) dùng chung đúng một không gian movie_id/keyword_id, không cần tầng ánh
xạ ID nào.

Tầng truyền tải HTTP-hay-local (d1_query) giờ nằm ở app/clients/d1.py, còn query
riêng của posts/comments nằm ở app/repositories/d1/{posts,comments}.py - cả hai đều
được re-export bên dưới để các chỗ gọi `from app.services.d1 import persist_post`
v.v. hiện có không phải sửa. Module này giữ logic không phụ thuộc tầng truyền tải cho
mọi bảng khác (movies, keywords, social_topic_reports) cộng với các hàm chuyển tiếp
sang stats_summary.py.

Không phụ thuộc nền tảng: mọi hàm ở đây làm việc dựa trên registered_platforms() của
app.services.platforms, không gán cứng chữ "facebook" - xem docstring của module đó
để biết thêm một nền tảng mới cần những gì.

Cố gắng hết mức có thể ở mọi chỗ: mọi hàm ở đây trả về None/[]/False và ghi log khi
lỗi (thiếu cấu hình, lỗi mạng, lỗi HTTP) thay vì raise - một lần ghi D1 thất bại
tuyệt đối không được làm sập việc ingest từ Kafka, vốn là bảo đảm giao nhận bền vững
duy nhất mà service này có."""

from __future__ import annotations

import json
import re
import unicodedata
import uuid
from datetime import datetime, timezone
from typing import Any

from app.clients.d1 import _configured, d1_query
from app.clients.redis import REDIS_KEY_PREFIX, get_redis_client
from app.core.logging import get_logger
from app.repositories.d1.comments import (
    CommentRepository,
    comment_repo,
    list_all_comments,
    list_comments,
    persist_comment,
)
from app.repositories.d1.posts import (
    ENGAGEMENT_FIELDS,
    MIN_CONTENT_LENGTH,
    RELEVANT_POST_SQL,
    PostRepository,
    contains_keyword,
    get_post,
    get_post_by_external_id,
    list_posts,
    list_posts_needing_comments,
    movie_hashtag_present,
    persist_post,
    post_mentions_movie,
    post_repo,
    reputable_authors,
)

logger = get_logger(__name__)

# Re-export cho các chỗ gọi `from app.services.d1 import X` hiện có - xem docstring
# module. Tham chiếu chúng ở đây (không chỉ import) để linter không báo import thừa.
__all_reexports__ = (
    d1_query,
    _configured,
    CommentRepository,
    comment_repo,
    list_all_comments,
    list_comments,
    persist_comment,
    PostRepository,
    post_repo,
    ENGAGEMENT_FIELDS,
    MIN_CONTENT_LENGTH,
    contains_keyword,
    get_post,
    get_post_by_external_id,
    list_posts,
    list_posts_needing_comments,
    movie_hashtag_present,
    persist_post,
    post_mentions_movie,
    reputable_authors,
)


async def _related_hashtags_for_keywords(keyword_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    """Các tag TikTok xuất hiện cùng mà spider-hub đã lưu sau một lượt crawl (Redis
    tiktok:related_hashtags:{keyword_id}). Thiếu Redis hoặc key rỗng chỉ có nghĩa là
    dashboard chưa có chip nào để duyệt."""
    if not keyword_ids:
        return {}
    try:
        client = get_redis_client()
        keys = [f"{REDIS_KEY_PREFIX}tiktok:related_hashtags:{kid}" for kid in keyword_ids]
        values = await client.mget(keys)
    except Exception as exc:
        logger.warning("related_hashtags_redis_failed", error=str(exc))
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for kid, raw in zip(keyword_ids, values or []):
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except TypeError, json.JSONDecodeError:
            continue
        if not isinstance(parsed, list):
            continue
        cleaned: list[dict[str, Any]] = []
        for item in parsed:
            if not isinstance(item, dict) or not item.get("title"):
                continue
            cleaned.append(
                {
                    "id": str(item.get("id") or ""),
                    "title": str(item["title"]).lstrip("#"),
                    "count": int(item.get("count") or 0),
                    "bfs_depth": int(item.get("bfs_depth") or 1),
                }
            )
        if cleaned:
            out[kid] = cleaned
    return out


async def get_post_counts_by_platform() -> list[dict[str, Any]]:
    """Tổng số bài đã ingest theo nền tảng, cộng thời điểm crawl gần nhất và số ingest hôm
    nay so với hôm qua. Đọc bảng tổng hợp sẵn stats_platform_daily (xem
    app/services/stats_summary.py) - COUNT(*) toàn bộ posts qua D1 HTTP quá chậm cho
    trang Overview."""
    from app.services.stats_summary import get_post_counts_by_platform as _from_summary

    return await _from_summary()


async def get_post_timeseries(days: int) -> list[dict[str, Any]]:
    """Số bài theo ngày của từng nền tảng trong `days` ngày gần nhất - từ bảng tổng hợp
    stats_platform_daily."""
    from app.services.stats_summary import get_post_timeseries as _from_summary

    return await _from_summary(days)


async def get_comment_counts_by_platform() -> list[dict[str, Any]]:
    """Tổng số comment đã ingest theo nền tảng - cùng dạng với get_post_counts_by_platform,
    từ stats_platform_daily."""
    from app.services.stats_summary import get_comment_counts_by_platform as _from_summary

    return await _from_summary()


async def get_comment_timeseries(days: int) -> list[dict[str, Any]]:
    """Số comment theo ngày của từng nền tảng trong `days` ngày gần nhất."""
    from app.services.stats_summary import get_comment_timeseries as _from_summary

    return await _from_summary(days)


async def get_keyword_volume(platform: str | None = None) -> list[dict[str, Any]]:
    """Tổng số bài/comment theo từng từ khoá tìm kiếm cộng số ingest hôm nay so với hôm qua
    - từ bảng tổng hợp stats_keyword_daily."""
    from app.services.stats_summary import get_keyword_volume as _from_summary

    return await _from_summary(platform)


_MOVIE_COLUMNS = "id, title, slug, released_at, poster_url, description, director, `cast`, distributor"


def movie_slug(title: str) -> str:
    """Slug URL gần như ASCII từ một tiêu đề (đã bỏ dấu tiếng Việt)."""
    folded = title.strip().replace("đ", "d").replace("Đ", "D")
    normalized = unicodedata.normalize("NFKD", folded)
    ascii_ish = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_ish.lower()).strip("-")
    return slug or "movie"


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


async def _unique_movie_slug(base: str, exclude_id: str | None = None) -> str | None:
    slug = base
    suffix = 2
    while True:
        rows = await d1_query("SELECT id FROM movies WHERE slug = ?", [slug])
        if rows is None:
            return None
        clash = next((row for row in rows if row["id"] != exclude_id), None)
        if clash is None:
            return slug
        slug = f"{base}-{suffix}"
        suffix += 1


async def list_movies() -> list[dict[str, Any]]:
    """Mọi phim đang bật - cấp dữ liệu cho ô chọn "từ khoá mới này thuộc phim nào" khi tạo
    từ khoá ngay trong form kích hoạt crawl, và bảng chi tiết phim của dashboard. Cũng
    được scripts/generate_social_topic_reports.py dùng để duyệt qua mọi phim cần report
    (các trường thừa ở đây chỗ gọi đó đơn giản là không dùng, không làm hỏng gì). `cast`
    cần dấu backtick - nó là từ khoá SQL (hàm CAST()) trong ngữ pháp của chính SQLite,
    không chỉ của Python."""
    rows = await d1_query(f"SELECT {_MOVIE_COLUMNS} FROM movies WHERE enabled = 1 ORDER BY title ASC")
    return rows or []


async def get_movie(movie_id: str) -> dict[str, Any] | None:
    rows = await d1_query(f"SELECT {_MOVIE_COLUMNS} FROM movies WHERE id = ? AND enabled = 1", [movie_id])
    if rows is None:
        return None
    return rows[0] if rows else None


async def create_movie(fields: dict[str, Any]) -> dict[str, Any] | None:
    """Tạo phim từ trang Movies của dashboard - nhân viên gõ tên và các trường
    release/cast tuỳ chọn; slug được tự suy ra trừ khi họ truyền vào."""
    title = (fields.get("title") or "").strip()
    if not title:
        return None
    requested = _blank_to_none(fields.get("slug"))
    slug = await _unique_movie_slug(movie_slug(requested or title))
    if slug is None:
        return None
    now = datetime.now(tz=timezone.utc).isoformat()
    movie_id = f"movie_{uuid.uuid4()}"
    inserted = await d1_query(
        f"""
        INSERT INTO movies (
            id, title, slug, enabled, created_at, updated_at,
            released_at, poster_url, description, director, `cast`, distributor
        ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            movie_id,
            title,
            slug,
            now,
            now,
            _blank_to_none(fields.get("released_at")),
            _blank_to_none(fields.get("poster_url")),
            _blank_to_none(fields.get("description")),
            _blank_to_none(fields.get("director")),
            _blank_to_none(fields.get("cast")),
            _blank_to_none(fields.get("distributor")),
        ],
    )
    if inserted is None:
        return None
    return await get_movie(movie_id)


async def update_movie(movie_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
    existing = await get_movie(movie_id)
    if not existing:
        return None

    title = existing["title"]
    if "title" in fields and fields["title"] is not None:
        title = fields["title"].strip() or existing["title"]

    slug = existing["slug"]
    if "slug" in fields:
        requested = _blank_to_none(fields.get("slug"))
        if requested:
            unique = await _unique_movie_slug(movie_slug(requested), exclude_id=movie_id)
            if unique is None:
                return None
            slug = unique

    now = datetime.now(tz=timezone.utc).isoformat()

    def pick(key: str) -> str | None:
        if key not in fields:
            return existing.get(key)
        return _blank_to_none(fields.get(key))

    updated = await d1_query(
        f"""
        UPDATE movies SET
            title = ?, slug = ?, updated_at = ?,
            released_at = ?, poster_url = ?, description = ?,
            director = ?, `cast` = ?, distributor = ?
        WHERE id = ? AND enabled = 1
        """,
        [
            title,
            slug,
            now,
            pick("released_at"),
            pick("poster_url"),
            pick("description"),
            pick("director"),
            pick("cast"),
            pick("distributor"),
            movie_id,
        ],
    )
    if updated is None:
        return None
    return await get_movie(movie_id)


async def disable_movie(movie_id: str) -> bool | None:
    """Xoá mềm khỏi danh sách trên dashboard (keywords/posts vẫn giữ khoá ngoại)."""
    existing = await get_movie(movie_id)
    if existing is None:
        rows = await d1_query("SELECT id FROM movies WHERE id = ?", [movie_id])
        if rows is None:
            return None
        return False
    now = datetime.now(tz=timezone.utc).isoformat()
    result = await d1_query(
        "UPDATE movies SET enabled = 0, updated_at = ? WHERE id = ? AND enabled = 1",
        [now, movie_id],
    )
    if result is None:
        return None
    return True


# Dưới số comment đã phân loại này, scripts/generate_social_topic_reports.py bỏ qua hẳn
# một phim (quá ít tín hiệu để gom topic có ý nghĩa) - chỉnh thoải mái, không suy ra
# từ đâu cả.
MIN_COMMENTS_FOR_REPORT = 15

# Trần số comment đưa vào một lời gọi Kira gom topic - giữ prompt (và lượng token suy
# luận của model) có giới hạn bất kể số comment của phim lớn tới đâu. Xếp hạng theo
# tương tác trước, nên nếu phim có nhiều comment đã phân loại hơn mức này thì những
# comment có tín hiệu cao nhất là những comment được giữ lại.
REPORT_COMMENT_SAMPLE_SIZE = 400


_SENTIMENT_BUCKETS = ("positive", "negative", "neutral")


def _normalize_comment_text(message: str) -> str:
    """Thu gọn một comment thành key khử trùng - chữ thường, gộp khoảng trắng. Bắt được các
    bản copy-paste giống hệt/gần giống hệt (trại spam, nhóm bot đăng lại cùng một câu
    dưới nhiều bài) mà không tốn chi phí/độ phức tạp của so khớp mờ thật sự - xem
    docstring của get_comment_sample_for_movie để biết vì sao điều này quan trọng với một
    mẫu mà LLM coi là đại diện."""
    return " ".join(message.split()).casefold()


def _stratified_sample(by_sentiment: dict[str, list[dict[str, Any]]], limit: int) -> list[dict[str, Any]]:
    """Chọn `limit` dòng từ by_sentiment (trong mỗi nhóm đã xếp hạng theo tương tác sẵn),
    giữ đúng tỉ lệ thật của từng nhóm trong tập ứng viên, không chỉ lấy nhóm nào tình cờ
    có comment ồn ào/nhiều like nhất. Xem docstring của get_comment_sample_for_movie để
    biết vì sao mẫu thuần top-N theo tương tác từng làm lệch lời gọi gom topic/narrative."""
    total = sum(len(rows) for rows in by_sentiment.values())
    if total <= limit:
        combined = [row for rows in by_sentiment.values() for row in rows]
        combined.sort(key=lambda r: r.get("reactions_count") or 0, reverse=True)
        return combined

    # Phương pháp phần dư lớn nhất: hạn mức theo đúng tỉ lệ thật hiếm khi là số nguyên,
    # nên làm tròn xuống phần của mỗi nhóm, rồi chia vài chỗ còn dư cho các nhóm có phần
    # lẻ lớn nhất - giữ sum(quotas) == limit chính xác.
    raw_quotas = {k: (len(v) / total) * limit for k, v in by_sentiment.items()}
    quotas = {k: int(q) for k, q in raw_quotas.items()}
    remainder = limit - sum(quotas.values())
    for k in sorted(by_sentiment, key=lambda k: raw_quotas[k] - quotas[k], reverse=True)[:remainder]:
        quotas[k] += 1

    selected: list[dict[str, Any]] = []
    shortfall = 0
    for k, rows in by_sentiment.items():
        take = min(quotas[k], len(rows))
        selected.extend(rows[:take])
        shortfall += quotas[k] - take

    if shortfall > 0:
        # Một nhóm không đủ hạn mức (quá ít tín hiệu thật ở cảm xúc đó) - bù bằng các ứng viên
        # chưa được chọn, vẫn xếp theo tương tác, để mẫu vẫn dài đúng `limit` mỗi khi tổng các
        # nhóm còn đủ ứng viên.
        taken_ids = {row["id"] for row in selected}
        leftover = [row for rows in by_sentiment.values() for row in rows if row["id"] not in taken_ids]
        leftover.sort(key=lambda r: r.get("reactions_count") or 0, reverse=True)
        selected.extend(leftover[:shortfall])

    selected.sort(key=lambda r: r.get("reactions_count") or 0, reverse=True)
    return selected


async def get_comment_sample_for_movie(movie_id: str, limit: int = REPORT_COMMENT_SAMPLE_SIZE) -> list[dict[str, Any]]:
    """Mẫu comment đã phân loại của phim này, phân tầng theo cảm xúc và đã khử trùng, dùng
    cho lời gọi Bee/Kira gom topic trong scripts/generate_social_topic_reports.py -
    KHÔNG dùng cho tỉ lệ cảm xúc tổng thể (xem get_movie_sentiment_counts, hàm đếm mọi
    comment đã phân loại, không chỉ mẫu có giới hạn này).

    Chỉ lấy comment dưới bài vừa có relevance_label='related' VỪA qua được
    movie_hashtag_present (xem bộ lọc top 100 trong app/repositories/d1/posts.py, cùng
    cổng hai tín hiệu, cùng lý do: riêng relevance_label chỉ là bản sao phán quyết của AI
    lúc ingest, không phải bằng chứng củng cố độc lập - xem docstring của persist_post).
    Không có thêm movie_hashtag_present, một bài khớp từ khoá nhưng thực ra không nói về
    phim sẽ để các comment lạc đề của nó làm loãng việc gom topic và tỉ lệ cảm xúc y như
    từng làm loãng danh sách top 100. Đã xác nhận thực tế trước khi có bản sửa chỉ dùng
    relevance_label: có phim 25-66% "comment đã phân loại" nằm dưới những bài như vậy;
    đã xác nhận thực tế 2026-09-24 rằng riêng relevance_label vẫn chưa đủ (report "Huyết
    Thống" phải xoá và tạo lại sau khi thêm cổng thứ hai này - xem docstring của
    movie_hashtag_present để biết đúng sự cố).

    Thêm hai lỗ hổng độ chính xác được vá ngày 2026-09-25, sau những điều trên: một mẫu
    thuần top-N theo tương tác (i) để các đoạn text trùng hệt/gần trùng hệt (nhóm bot,
    chuỗi spam copy-paste - thường có số like bị thổi phồng hoặc phối hợp) chiếm nhiều
    chỗ như thể là các ý kiến độc lập, và (ii) có thể bị một cảm xúc viral chiếm trọn
    (ví dụ một chuỗi tích cực rất nhiều like), đẩy mất các comment tiêu cực/trung lập vốn
    có thật về tỉ lệ nhưng từng cái ít like hơn. Giờ: khử trùng theo text đã chuẩn hoá
    trước (giữ bản có tương tác cao nhất, vì các dòng vốn đã tới theo thứ tự tương tác),
    rồi lấy mẫu từng nhóm cảm xúc theo đúng tỉ lệ thật của nó trong tập ứng viên đã khử
    trùng (xem _stratified_sample), không chỉ lấy nhóm nào có comment ồn ào nhất.

    Lấy dư khá nhiều so với `limit` (movie_hashtag_present + khử trùng đều chạy bằng
    Python, sau khi lấy dữ liệu, và còn thu hẹp tập ứng viên thêm) - cùng dạng với
    list_posts(sort="engagement"), chỉ là biên rộng hơn vì giờ có hai bộ lọc nằm giữa
    lần lấy thô và mẫu cuối cùng thay vì một."""
    movie_rows = await d1_query("SELECT title FROM movies WHERE id = ?", [movie_id])
    movie_title = movie_rows[0]["title"] if movie_rows else None

    overfetch = max(limit * 6, limit + 1000)
    rows = await d1_query(
        f"""
        SELECT c.id, c.post_id, c.message, c.reactions_count, c.sentiment,
               c.author_name, c.author_url, c.author_profile_picture,
               p.url AS post_url, p.content AS post_content, p.author AS post_author, p.platform,
               k.keyword AS post_keyword
        FROM comments c
        JOIN posts p ON p.id = c.post_id
        LEFT JOIN keywords k ON k.id = p.keyword_id
        WHERE p.movie_id = ? AND {RELEVANT_POST_SQL}
          AND c.sentiment IS NOT NULL AND c.message IS NOT NULL
        ORDER BY c.reactions_count DESC, c.scraped_at DESC
        LIMIT ?
        """,
        [movie_id, overfetch],
    )
    reputable = await reputable_authors()
    seen_texts: set[str] = set()
    by_sentiment: dict[str, list[dict[str, Any]]] = {k: [] for k in _SENTIMENT_BUCKETS}
    for row in rows or []:
        is_reputable = (row.get("platform"), row.get("post_author")) in reputable
        if not movie_hashtag_present(
            row.get("post_content"), movie_title, row.pop("post_keyword", None), is_reputable_author=is_reputable
        ):
            continue
        sentiment = row.get("sentiment")
        if sentiment not in by_sentiment:
            continue
        normalized = _normalize_comment_text(row.get("message") or "")
        if not normalized or normalized in seen_texts:
            continue
        seen_texts.add(normalized)
        by_sentiment[sentiment].append(row)

    return _stratified_sample(by_sentiment, limit)


async def get_movie_sentiment_counts(movie_id: str) -> dict[str, int]:
    """Số lượng mọi comment đã phân loại của phim này, nhóm theo nhãn cảm xúc - sự thật gốc
    cho tỉ lệ overall_sentiment của report (chỗ gọi tính bằng phép chia đơn giản, không
    để LLM ước lượng), trên TOÀN BỘ tập comment, không chỉ mẫu có giới hạn đưa vào lời
    gọi gom topic.

    Cùng cổng relevance_label='related' VÀ movie_hashtag_present như
    get_comment_sample_for_movie ở trên, cùng lý do - tỉ lệ phải lấy từ đúng tập comment
    đúng chủ đề mà mẫu được rút ra, không phải từ một tập lớn hơn vẫn chứa comment của
    bài lạc đề. Riêng relevance_label không phải bằng chứng củng cố độc lập (xem
    docstring của hàm kia); hàm này từng bỏ qua cổng thứ hai, và đó chính là lý do "Huyết
    Thống" (một tên phim là từ vựng thông thường) vẫn làm bẩn tỉ lệ cảm xúc của chính nó
    ngay cả sau khi danh sách bài và mẫu comment đã được sửa để lọc bỏ - phần đếm ở đây
    chạy thẳng trên nhãn thô, không kiểm tra lại. Lấy mọi comment đã phân loại (không chỉ
    một mẫu có giới hạn, khác với get_comment_sample_for_movie) vì cần đếm đúng toàn bộ,
    không phải mẫu đại diện - sau đó movie_hashtag_present vẫn chạy bằng Python trên từng
    dòng."""
    movie_rows = await d1_query("SELECT title FROM movies WHERE id = ?", [movie_id])
    movie_title = movie_rows[0]["title"] if movie_rows else None

    rows = await d1_query(
        f"""
        SELECT c.sentiment, p.content AS post_content, p.author AS post_author, p.platform,
               k.keyword AS post_keyword
        FROM comments c
        JOIN posts p ON p.id = c.post_id
        LEFT JOIN keywords k ON k.id = p.keyword_id
        WHERE p.movie_id = ? AND {RELEVANT_POST_SQL} AND c.sentiment IS NOT NULL
        """,
        [movie_id],
    )
    reputable = await reputable_authors()
    counts: dict[str, int] = {}
    for row in rows or []:
        is_reputable = (row.get("platform"), row.get("post_author")) in reputable
        if not movie_hashtag_present(
            row.get("post_content"), movie_title, row.get("post_keyword"), is_reputable_author=is_reputable
        ):
            continue
        counts[row["sentiment"]] = counts.get(row["sentiment"], 0) + 1
    return counts


async def upsert_social_topic_report(
    *, movie_id: str, dashboard_data_json: str, comment_count: int, post_count: int, kira_model: str | None
) -> bool:
    """Upsert theo movie_id vào social_topic_reports - mỗi phim một dòng, bị ghi đè mỗi lần
    scripts/generate_social_topic_reports.py chạy (không giữ lịch sử; không có gì đọc
    report cũ)."""
    if not _configured():
        return False

    generated_at = datetime.now(tz=timezone.utc).isoformat()
    existing_rows = await d1_query("SELECT id FROM social_topic_reports WHERE movie_id = ?", [movie_id])
    if existing_rows:
        updated = await d1_query(
            """
            UPDATE social_topic_reports SET
                dashboard_data_json = ?, comment_count = ?, post_count = ?, kira_model = ?, generated_at = ?
            WHERE id = ?
            """,
            [dashboard_data_json, comment_count, post_count, kira_model, generated_at, existing_rows[0]["id"]],
        )
        return updated is not None

    inserted = await d1_query(
        """
        INSERT INTO social_topic_reports (id, movie_id, dashboard_data_json, comment_count, post_count, kira_model, generated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [f"str_{uuid.uuid4()}", movie_id, dashboard_data_json, comment_count, post_count, kira_model, generated_at],
    )
    return inserted is not None


async def get_or_create_keyword(movie_id: str, platform: str, keyword: str) -> dict[str, Any] | None:
    """Dùng cho luồng "gõ từ khoá mới" ngay trên dashboard (form kích hoạt crawl) - tìm
    dòng (movie_id, platform, keyword) đã có trước (bộ ba đó có unique index - xem
    src/db/schema.ts của cinemark-scraper) để gửi lặp lại hoặc đua với một tab khác chỉ
    trả về cùng dòng đó thay vì lỗi ràng buộc."""
    existing = await d1_query(
        """
        SELECT k.id, k.movie_id, m.title AS movie_title, k.keyword
        FROM keywords k JOIN movies m ON m.id = k.movie_id
        WHERE k.movie_id = ? AND k.platform = ? AND k.keyword = ?
        """,
        [movie_id, platform, keyword],
    )
    if existing:
        return existing[0]

    movie_rows = await d1_query("SELECT title FROM movies WHERE id = ? AND enabled = 1", [movie_id])
    if not movie_rows:
        return None
    movie_title = movie_rows[0]["title"]

    keyword_id = f"kw_{uuid.uuid4()}"
    created_at = datetime.now(tz=timezone.utc).isoformat()
    inserted = await d1_query(
        "INSERT INTO keywords (id, movie_id, platform, keyword, enabled, created_at) VALUES (?, ?, ?, ?, 1, ?)",
        [keyword_id, movie_id, platform, keyword, created_at],
    )
    if inserted is None:
        return None
    return {"id": keyword_id, "movie_id": movie_id, "movie_title": movie_title, "keyword": keyword}


async def list_keywords(platform: str) -> list[dict[str, Any]]:
    """Mọi từ khoá đang bật của nền tảng này, kèm tên phim - cấp dữ liệu cho ô chọn từ khoá
    trên dashboard (GET /<platform>/keywords) để một lần kích hoạt crawl tay có thể nhắm
    vào một từ khoá thay vì "mọi từ khoá đang bật của nền tảng này" (xem
    get_enabled_keywords bên dưới, vẫn dùng cho trường hợp chạy hàng loạt đó)."""
    rows = await d1_query(
        """
        SELECT k.id, k.movie_id, m.title AS movie_title, k.keyword
        FROM keywords k JOIN movies m ON m.id = k.movie_id
        WHERE k.platform = ? AND k.enabled = 1 AND m.enabled = 1
        ORDER BY m.title ASC, k.keyword ASC
        """,
        [platform],
    )
    return rows or []


async def get_keyword(keyword_id: str, platform: str) -> dict[str, Any] | None:
    """Một từ khoá đang bật theo id, trên nền tảng đã cho, join với cờ enabled của phim -
    giống những gì KeywordRepository.get() + tra phim của Postgres (đã xoá) từng làm cho
    nút kích hoạt "chạy một từ khoá". Tham số platform là bắt buộc, không phải tình cờ:
    mỗi nền tảng có router riêng (xem app/api/routes/facebook.py + platform_scraper.py)
    chỉ muốn từ khoá của chính nó - một keyword_id thuộc nền tảng khác không được âm thầm
    khớp ở đây."""
    rows = await d1_query(
        """
        SELECT k.id, k.movie_id, k.platform, k.keyword,
               m.title AS movie_title, m.director AS movie_director,
               m.`cast` AS movie_cast, m.distributor AS movie_distributor
        FROM keywords k JOIN movies m ON m.id = k.movie_id
        WHERE k.id = ? AND k.platform = ? AND k.enabled = 1 AND m.enabled = 1
        """,
        [keyword_id, platform],
    )
    return rows[0] if rows else None


async def get_enabled_keywords(platform: str, movie_id: str | None = None) -> list[dict[str, Any]]:
    """Mọi từ khoá đang bật trên nền tảng đã cho (có thể giới hạn trong một phim) mà phim
    của nó cũng đang bật - dùng cho cả nút "chạy mọi từ khoá của một phim" lẫn lời gọi
    cron hằng ngày của một nền tảng ("chạy mọi thứ của nền tảng này")."""
    conditions = ["k.platform = ?", "k.enabled = 1", "m.enabled = 1"]
    params: list[Any] = [platform]
    if movie_id:
        conditions.insert(0, "k.movie_id = ?")
        params.insert(0, movie_id)
    rows = await d1_query(
        f"""
        SELECT k.id, k.movie_id, k.platform, k.keyword
        FROM keywords k JOIN movies m ON m.id = k.movie_id
        WHERE {" AND ".join(conditions)}
        """,
        params,
    )
    return rows or []


async def set_keyword_enabled(platform: str, keyword_id: str, enabled: bool) -> dict[str, Any] | None:
    """Bật/tắt một từ khoá - cho người vận hành tạm dừng một từ khoá cũ/dùng một lần (hoặc
    một lô vừa thêm để thử) mà không xoá, để lịch hằng ngày của nền tảng
    (get_enabled_keywords ở trên) lấy đúng tập mong muốn. platform là phạm vi phòng thủ,
    không tự nó là key tra cứu - keyword_id vốn đã duy nhất - để một platform không khớp
    trên URL không thể âm thầm bật/tắt dòng của nền tảng khác. Trả về dòng đã cập nhật,
    hoặc None nếu id không tồn tại dưới nền tảng đó, hoặc việc ghi thất bại."""
    updated = await d1_query(
        "UPDATE keywords SET enabled = ? WHERE id = ? AND platform = ?",
        [1 if enabled else 0, keyword_id, platform],
    )
    if updated is None:
        logger.warning("d1_set_keyword_enabled_failed", platform=platform, keyword_id=keyword_id, enabled=enabled)
        return None
    rows = await d1_query(
        """
        SELECT k.id, k.movie_id, m.title AS movie_title, k.keyword, k.enabled
        FROM keywords k JOIN movies m ON m.id = k.movie_id
        WHERE k.id = ? AND k.platform = ?
        """,
        [keyword_id, platform],
    )
    return rows[0] if rows else None
