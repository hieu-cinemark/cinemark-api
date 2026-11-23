"""Mọi thứ đọc/ghi bảng `posts` của D1 (+ bảng anh em post_engagement_snapshots) -
chính các dòng mà Worker của cinemark-scraper sở hữu (xem api/schema/scraper.ts bên
đó). Tách ra từ khối d1.py lớn cũ trong app/services để các mẫu query, phân trang và
index của bảng này nằm một chỗ thay vì trộn lẫn với movies/keywords/comments.

app/repositories/d1/store.py vẫn re-export mọi tên bên dưới (persist_post, list_posts, ...)
để các chỗ gọi `from app.repositories.d1.store import persist_post` hiện có (ingest_consumer,
scripts, tests) không phải sửa - chỉ code mới mới cần dùng thẳng `post_repo`.

Bảng `dropped_posts` cũ từng nằm trong repo này (xem lịch sử git) để lưu payload
Kafka thô bị loại lúc ingest trước khi tới persist_post (mapper chưa đăng ký,
keyword_id thiếu/không biết). Giờ ingest_consumer ghi các bài bị loại đó lên topic
Kafka `ingest_decisions` (xem app/clients/kafka.py:publish_ingest_decision), và lake
writer (app/workers/lake_writer/main.py) lưu toàn bộ luồng đó lên R2 dưới
bronze/entity=decisions/. Muốn phát lại một bài bị loại trong quá khứ thì lấy từ đó,
không phải từ D1."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import and_, func, insert, or_, select, update

from app.clients.d1 import _configured
from app.core.logging import get_logger
from app.repositories.d1.sql import d1_exec
from app.repositories.d1.tables import ENGAGEMENT_COLUMNS, comments, keywords, movies, post_engagement_snapshots, posts
from app.services.crawl.platforms import PostDraft, registered_platforms

logger = get_logger(__name__)

from app.repositories.d1.author_reputation import reputable_authors
from app.repositories.d1.post_filters import ENGAGEMENT_FIELDS, MIN_CONTENT_LENGTH, engagement_score, relevant_post
from app.repositories.d1.post_matching import contains_keyword, movie_hashtag_present
from app.repositories.d1.post_raw import _hydrate_post_rows, trim_post_raw
from app.repositories.d1.post_schema import _ensure_post_indexes, ensure_post_voice_columns

_POST_REF = (posts.c.id, posts.c.platform, posts.c.external_id, posts.c.url)

# Danh sách bài cho dashboard: bài + từ khoá + tên phim.
_P, _K, _M = posts.alias("p"), keywords.alias("k"), movies.alias("m")
_FEED_FROM = _P.outerjoin(_K, _K.c.id == _P.c.keyword_id).outerjoin(_M, _M.c.id == _P.c.movie_id)
_FEED_COLUMNS = (
    *(_P.c[name] for name in ("id", "platform", "external_id", "url", "author", "content", "media_json")),
    func.json_extract(_P.c.raw_json, "$.cover_url").label("raw_cover_url"),
    *(_P.c[name] for name in ENGAGEMENT_COLUMNS),
    _P.c.posted_at,
    _P.c.scraped_at,
    _P.c.keyword_match,
    _K.c.keyword,
    _M.c.title.label("movie_title"),
)


def _feed_filters(
    platform: str | None, keyword_id: str | None, movie_id: str | None, keyword_match: bool | None
) -> list:
    """Bộ lọc không bắt buộc, cộng dồn, của list_posts / list_posts_cursor."""
    filters = []
    if platform:
        filters.append(_P.c.platform == platform)
    if keyword_id:
        filters.append(_P.c.keyword_id == keyword_id)
    if movie_id:
        filters.append(_P.c.movie_id == movie_id)
    if keyword_match is not None:
        filters.append(_P.c.keyword_match == (1 if keyword_match else 0))
    return filters


async def _snapshot(post_id: str, recorded_at: str, engagement: dict[str, int]) -> None:
    """Một dòng lịch sử tương tác của bài (ghi khi số đổi)."""
    await d1_exec(
        insert(post_engagement_snapshots).values(
            id=f"eng_{uuid.uuid4()}", post_id=post_id, recorded_at=recorded_at, **engagement
        )
    )


class PostRepository:
    """Giữ mọi query trên `posts`. Một instance cho cả tiến trình (`post_repo` bên dưới) -
    không có trạng thái theo request, nên đây chỉ là namespace cho các query cộng với cờ
    chặn tạo index lười ở trên."""

    async def list_posts(
        self,
        *,
        platform: str | None = None,
        keyword_id: str | None = None,
        movie_id: str | None = None,
        keyword_match: bool | None = None,
        sort: str = "recent",
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """Feed bài phân trang, join với phim/từ khoá để hiển thị - phục vụ tab "xem lại bài"
        của dashboard. Mọi bộ lọc đều không bắt buộc và cộng dồn; không truyền gì thì trả về
        trang mới nhất của cả bảng trên mọi nền tảng/phim. Trả về (rows, total_count) để chỗ
        gọi hiển thị phân trang mà không cần gọi thêm lượt nữa.

        Phân trang offset, không phải keyset: xem list_posts_cursor bên dưới để biết vì sao
        hàm này hiện vẫn là API đang dùng dù đã có method kia.

        sort="recent" (mặc định): crawl gần nhất trước, như trước giờ. sort="engagement":
        tương tác cao nhất trước (xem ENGAGEMENT_SCORE_SQL) - ví dụ keyword_id +
        sort="engagement" + limit=100 là màn hình "top 100 bài của từ khoá này" trên
        dashboard. Màn hình đó đòi relevance_label='related' (đặt lúc ingest, xem
        app/ai/tasks/post_relevance.py) VÀ kiểm tra độc lập của movie_hashtag_present -
        riêng relevance_label không phải ý kiến thứ hai như vẻ ngoài: persist_post lưu
        keyword_match như bản sao y nguyên phán quyết của AI mỗi khi AI thực sự được gọi (xem
        docstring của nó), nên chỉ dựa vào relevance_label thực chất là tin AI một lần, không
        phải hai lần. Đã xác nhận thực tế các bài của một hãng snack Mexico đạt
        99%+ "related" với phim "Mẹ Mìn" chỉ vì hashtag của nó fold ra cùng một chuỗi ngắn
        với tên phim - xem docstring của movie_hashtag_present cho cách sửa. Lấy dư
        (_ENGAGEMENT_OVERFETCH bên dưới) vì bộ lọc thứ hai này chạy bằng Python, rồi cắt lại
        còn `limit` - bài chưa được AI gắn nhãn, hoặc không có hashtag/tên phim khớp nào, sẽ
        không chiếm chỗ dù tương tác cao."""
        await _ensure_post_indexes()
        filters = _feed_filters(platform, keyword_id, movie_id, keyword_match)
        if sort == "engagement":
            filters.append(relevant_post(_P))
            order = (engagement_score(_P).desc(),)
        else:
            order = (_P.c.scraped_at.desc(),)
        # movie_hashtag_present chạy bằng Python sau khi lấy dữ liệu và loại bỏ một số dòng mà
        # riêng relevance_label='related' lẽ ra đã cho qua - nên lấy nhiều hơn `limit` ngay từ
        # đầu để sau khi lọc rồi cắt lại thì trang không bị thiếu/rỗng.
        sql_limit = max(limit * 4, limit + 100) if sort == "engagement" else limit

        rows = await d1_exec(
            select(*_FEED_COLUMNS)
            .select_from(_FEED_FROM)
            .where(*filters)
            .order_by(*order)
            .limit(sql_limit)
            .offset(offset),
            timeout=20.0 if keyword_match is not None else 10.0,
        )
        hydrated = _hydrate_post_rows(rows)
        if sort == "engagement":
            reputable = await reputable_authors()
            hydrated = [
                row
                for row in hydrated
                if movie_hashtag_present(
                    row.get("content"),
                    row.get("movie_title"),
                    row.get("keyword"),
                    is_reputable_author=(row.get("platform"), row.get("author")) in reputable,
                )
            ][:limit]
            return hydrated, len(hydrated)

        # Tab có lọc: bỏ COUNT(*) - nó quét toàn bảng cho tới khi có index keyword_match, và
        # đã làm D1 timeout (~10s) mỗi lần bấm tab. Ước lượng "còn trang sau" từ kích thước
        # trang thay thế.
        if keyword_match is None:
            count_rows = await d1_exec(select(func.count().label("total")).select_from(_P).where(*filters))
            total = (count_rows[0]["total"] if count_rows else 0) or 0
        else:
            n = len(hydrated)
            total = offset + n + (limit if n == limit else 0)
        return hydrated, total

    async def list_posts_cursor(
        self,
        *,
        platform: str | None = None,
        keyword_id: str | None = None,
        movie_id: str | None = None,
        keyword_match: bool | None = None,
        sort: str = "recent",
        cursor: str | None = None,
        limit: int = 50,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Bản phân trang keyset tương đương list_posts, chỉ cho sort="recent" (biểu thức sắp
        xếp theo tương tác không dùng làm cột keyset được nếu không có một bản sao
        generated/có index của nó - chưa đáng thêm cho tới khi method này thực sự gánh tải).
        `cursor` là chuỗi "<scraped_at>|<id>" không cần hiểu bên trong của dòng cuối trang
        trước; None là bắt đầu từ đầu. Không có tổng số - phân trang keyset đánh đổi "trang
        N trên M" (và, không phải ngẫu nhiên, cả con số tổng ước lượng/đôi khi sai mà nhánh
        keyword_match của list_posts báo - xem comment của nó) lấy chi phí O(limit) mỗi trang
        bất kể đi sâu tới đâu, và đó chính là mục đích.

        Giờ đã nối vào GET /stats/posts (xem stats.py) - OFFSET của list_posts, đã xác nhận
        thực tế khi số bài vượt khoảng 100 nghìn, *chậm dần theo độ sâu* đặc biệt khi đi kèm
        LEFT JOIN keyword/movie (bỏ JOIN thì đều khoảng 0,1s bất kể offset, so với tới vài
        giây ở offset 90 nghìn khi có JOIN) - D1 không đẩy LIMIT/OFFSET xuống dưới phép join.
        Keyset né hoàn toàn chuyện này: điều kiện WHERE giới hạn phạm vi quét trước khi join
        chạy, nên `cursor` trỏ sâu tới đâu cũng không sao."""
        if sort != "recent":
            raise ValueError("list_posts_cursor only supports sort='recent'")
        await _ensure_post_indexes()

        filters = _feed_filters(platform, keyword_id, movie_id, keyword_match)
        if cursor:
            cursor_scraped_at, _, cursor_id = cursor.partition("|")
            filters.append(
                or_(
                    _P.c.scraped_at < cursor_scraped_at,
                    and_(_P.c.scraped_at == cursor_scraped_at, _P.c.id < cursor_id),
                )
            )
        rows = await d1_exec(
            select(*_FEED_COLUMNS)
            .select_from(_FEED_FROM)
            .where(*filters)
            .order_by(_P.c.scraped_at.desc(), _P.c.id.desc())
            .limit(limit),
            timeout=20.0 if keyword_match is not None else 10.0,
        )
        hydrated = _hydrate_post_rows(rows)
        next_cursor = f"{hydrated[-1]['scraped_at']}|{hydrated[-1]['id']}" if len(hydrated) == limit else None
        return hydrated, next_cursor

    async def list_posts_needing_comments(
        self, *, platform: str, keyword_id: str, top_n: int = 100
    ) -> list[dict[str, Any]]:
        """Trong top `top_n` bài theo tương tác của từ khoá này (đúng cùng cách xếp hạng với
        list_posts(sort="engagement") ở trên), những bài chưa có comment nào được lưu - phục
        vụ lượt quét "top comments" hằng ngày (xem _top_comments_tick trong scheduler.py).
        Xếp hạng trước, *rồi* mới lọc bài không có comment (một CTE, không phải một WHERE
        duy nhất) - lọc trước sẽ để bài hạng 101/150 không có comment chen mất chỗ của một
        bài thật sự thuộc top 100 chỉ vì nó đã có vài comment, như vậy không còn là "trong
        top 100, bài nào vẫn cần comment" nữa.

        Bài đã được quét ở một ngày trước không bị xếp hàng lại chỉ vì vẫn nằm trong top
        100 của từ khoá - chỉ những bài mới vào top (hoặc lần đầu chưa lấy được comment)
        mới được xếp hàng - nên một từ khoá có top 100 gần như không đổi qua các ngày sẽ
        không tốn lại pool tài khoản/proxy cho những bài đã lấy comment rồi. Cùng cổng
        relevance_label='related' + movie_hashtag_present như danh sách top 100 trên
        dashboard (list_posts sort="engagement") - xem docstring của method đó và của
        movie_hashtag_present để biết vì sao riêng relevance_label không phải bằng chứng
        củng cố độc lập; lưu ý về bài đã được AI gắn nhãn cũng áp dụng (bài chưa được lượt
        chạy theo lô của label_posts_relevance.py quét qua cũng không hiện ở đây)."""
        await _ensure_post_indexes()
        overfetch = max(top_n * 4, top_n + 100)
        counts = select(comments.c.post_id, func.count().label("n")).group_by(comments.c.post_id).subquery("c")
        rows = await d1_exec(
            select(
                *(_P.c[name] for name in ("id", "external_id", "url", "content", "author")),
                _K.c.keyword,
                _M.c.title.label("movie_title"),
                func.coalesce(counts.c.n, 0).label("comment_n"),
            )
            .select_from(_FEED_FROM.outerjoin(counts, counts.c.post_id == _P.c.id))
            .where(_P.c.platform == platform, _P.c.keyword_id == keyword_id, relevant_post(_P))
            .order_by(engagement_score(_P).desc())
            .limit(overfetch)
        )
        reputable = await reputable_authors()
        selected: list[dict[str, Any]] = []
        for row in rows or []:
            if row.get("comment_n"):
                continue
            if not movie_hashtag_present(
                row.get("content"),
                row.get("movie_title"),
                row.get("keyword"),
                is_reputable_author=(platform, row.get("author")) in reputable,
            ):
                continue
            selected.append({"id": row["id"], "external_id": row["external_id"], "url": row["url"]})
            if len(selected) >= top_n:
                break
        return selected

    async def update_post_views(self, platform: str, external_id: str, view_count: int) -> bool:
        """Chỉ cập nhật lượt xem của một bài đã có (message kind="metrics" của spider-hub - lượt xem reel Facebook từ tab
        Reels của trang). Bài chưa có thì không làm gì (không đủ dữ liệu để tạo). False khi câu lệnh lỗi."""
        views = int(view_count)
        result = await d1_exec(
            update(posts)
            .where(posts.c.platform == platform, posts.c.external_id == external_id, posts.c.view_count != views)
            .values(view_count=views),
            quiet=True,
        )
        return result is not None

    async def update_post_engagement(
        self,
        platform: str,
        external_id: str,
        *,
        like_count: int | None,
        reply_count: int | None = None,
        repost_count: int | None = None,
    ) -> bool:
        """Chỉ cập nhật số tương tác của một bài đã có (message kind="metrics" của job post_metrics bên spider-hub - đọc
        trang bài không đăng nhập). Số None = trang không có (reel không có share) -> giữ số cũ. Ghi snapshot khi số đổi,
        như persist_post. Bài chưa có thì không làm gì. False khi bài không có hoặc câu lệnh lỗi."""
        await _ensure_post_indexes()
        rows = await d1_exec(
            select(posts.c.id, posts.c.keyword_id, *(posts.c[f] for f in ENGAGEMENT_FIELDS)).where(
                posts.c.platform == platform, posts.c.external_id == external_id
            ),
            quiet=True,
        )
        if not rows:
            return False
        existing = rows[0]
        engagement = {field: existing.get(field) or 0 for field in ENGAGEMENT_FIELDS}
        for field, value in (("like_count", like_count), ("reply_count", reply_count), ("repost_count", repost_count)):
            if value is not None:
                engagement[field] = int(value)
        changed = any((existing.get(field) or 0) != engagement[field] for field in ENGAGEMENT_FIELDS)
        scraped_at = datetime.now(tz=UTC).isoformat()
        updated = await d1_exec(
            update(posts)
            .where(posts.c.id == existing["id"])
            .values(
                like_count=engagement["like_count"],
                reply_count=engagement["reply_count"],
                repost_count=engagement["repost_count"],
                scraped_at=scraped_at,
            ),
            quiet=True,
        )
        if updated is None:
            logger.warning("d1_post_engagement_update_failed", platform=platform, external_id=external_id)
            return False
        if changed:
            await _snapshot(existing["id"], scraped_at, engagement)
        from app.services.stats.stats_summary import record_post

        await record_post(platform=platform, keyword_id=existing.get("keyword_id"), scraped_at=scraped_at, is_new=False)
        return True

    async def get_post_by_external_id(self, platform: str, external_id: str) -> dict[str, Any] | None:
        """Một bài theo (platform, external_id) - id mà payload comment của spider-hub mang theo
        (xem handle_comment trong app/workers/ingest_consumer/main.py), khác với id nội bộ D1
        của get_post."""
        await _ensure_post_indexes()
        rows = await d1_exec(select(*_POST_REF).where(posts.c.platform == platform, posts.c.external_id == external_id))
        return rows[0] if rows else None

    async def get_post(self, post_id: str) -> dict[str, Any] | None:
        """Một bài theo id D1 (không phải external_id) - dùng cho nút kích hoạt "lấy comment
        cho bài này" (xem app/api/routes/facebook.py) để tra ra id bài của nền tảng + url mà
        bootstrap/spider của spider-hub cần, từ id D1 mà dashboard đang có (xem Post.id
        trong app/schemas/stats.py)."""
        rows = await d1_exec(select(*_POST_REF).where(posts.c.id == post_id))
        return rows[0] if rows else None

    async def persist_post(
        self,
        *,
        movie_id: str,
        keyword_id: str,
        keyword: str,
        platform: str,
        draft: PostDraft,
        ai_relevant: bool | None = None,
        relevance_label: str | None = None,
        relevance_confidence: float | None = None,
    ) -> bool:
        """Upsert một bài đã crawl (của bất kỳ nền tảng đã đăng ký nào) thẳng vào bảng `posts`
        của cinemark-scraper (+ một snapshot tương tác khi có thay đổi) - chuyển từ
        src/jobs/persist-post.ts bên đó để cả scraper riêng của Worker lẫn đường đi qua
        Kafka này ghi qua đúng cùng một logic. `draft` đã được mapper của nền tảng chuẩn hoá
        (xem app/services/crawl/platforms.py) - hàm này không tự biết trường nào riêng của nền
        tảng nào.

        `ai_relevant`, khi có (xem app/ai/tasks/post_relevance.py), là phán quyết "related"
        của AI và được dùng cho keyword_match thay cho kiểm tra chuỗi con chính xác
        contains_keyword bên dưới - chỗ gọi truyền None để quay về kiểm tra chuỗi con (AI
        không chắc, đang tắt, vượt hạn mức ngày, hoặc lời gọi thất bại) thay vì chặn việc
        ingest chỉ vì bộ phân loại trục trặc.

        `relevance_label`/`relevance_confidence`, khi có, là chính kết quả phân loại đó được
        lưu thẳng lên dòng lúc ingest (hệ 3 nhóm related/not_related/uncertain, không phải
        keyword_match kiểu boolean) - đặt ngay lúc đó, để bài hiện ra trong các màn hình lọc
        theo độ liên quan mà không phải chờ lượt chạy theo lô nào."""
        if not _configured() or platform not in registered_platforms():
            return False
        await _ensure_post_indexes()
        await ensure_post_voice_columns()

        external_id = draft.get("external_id")
        if not external_id:
            return False

        content = draft.get("content")

        # Lọc rác: nội dung quá ngắn (một reaction/emoji trơ trọi không có tín hiệu gì để phân
        # tích) - bỏ qua hẳn. KHÔNG lọc theo keyword_match: người comment thật viết tắt, viết
        # tiếng Việt không dấu, hoặc dùng tên tiếng Anh của phim sẽ không bao giờ chứa nguyên
        # văn cụm từ khoá đã cấu hình, nên chặn việc lưu theo kiểu khớp đó sẽ âm thầm làm mất
        # bài thật. keyword_match vẫn được tính và lưu bên dưới như một cờ để chỗ gọi tự lọc
        # nếu muốn, như trước - chỉ là nó không phải lý do để bỏ hẳn việc lưu bài.
        if not content or len(content.strip()) < MIN_CONTENT_LENGTH:
            logger.info("post_skipped_junk", platform=platform, external_id=external_id, reason="content_too_short")
            # Cố ý bỏ qua, không phải ghi lỗi - trả True để `if not ok` của handle_post không kích
            # hoạt bộ đếm cảnh báo bài bị loại (bộ đếm đó lấy dữ liệu từ luồng Kafka
            # ingest_decisions, không phải D1).
            return True
        is_keyword_match = ai_relevant if ai_relevant is not None else contains_keyword(content, keyword)

        scraped_at = datetime.now(tz=UTC).isoformat()
        media_json = json.dumps(draft.get("media") or {})
        kept_raw = trim_post_raw(draft.get("raw"))
        raw_json = json.dumps(kept_raw) if kept_raw else None
        engagement = {field: draft.get(field) or 0 for field in ENGAGEMENT_FIELDS}
        # D1 lưu boolean dưới dạng số nguyên SQLite (0/1) - truyền int, không truyền bool
        # JSON, để HTTP API bind đúng kiểu mà cột integer(..., {mode: "boolean"}) của Drizzle
        # cần. Có thể bằng 0 là hợp lệ - xem docstring của hàm này để biết vì sao bài không
        # khớp vẫn được lưu thay vì bị bỏ qua.
        keyword_match = int(is_keyword_match)

        existing_rows = await d1_exec(
            select(posts.c.id, *(posts.c[f] for f in ENGAGEMENT_FIELDS)).where(
                posts.c.platform == platform, posts.c.external_id == external_id
            )
        )
        existing = existing_rows[0] if existing_rows else None

        if existing is None:
            post_id = f"post_{uuid.uuid4()}"
            relevance_labeled_at = scraped_at if relevance_label is not None else None
            inserted = await d1_exec(
                insert(posts).values(
                    id=post_id,
                    movie_id=movie_id,
                    keyword_id=keyword_id,
                    platform=platform,
                    external_id=external_id,
                    url=draft.get("url"),
                    author=draft.get("author"),
                    content=draft.get("content"),
                    media_json=media_json,
                    **engagement,
                    first_seen_at=scraped_at,  # chỉ ghi lúc INSERT, các UPDATE không đụng tới
                    posted_at=draft.get("posted_at"),
                    scraped_at=scraped_at,
                    raw_json=raw_json,
                    keyword_match=keyword_match,
                    relevance_label=relevance_label,
                    relevance_confidence=relevance_confidence,
                    relevance_labeled_at=relevance_labeled_at,
                )
            )
            if inserted is None:
                # Insert thất bại (đua với một message khác cùng external_id, D1 sập, ...) - dòng bài
                # không tồn tại, nên một snapshot tham chiếu post_id ở đây sẽ thành mồ côi. Log rồi
                # dừng; lần crawl lại bài này sau sẽ thử lại toàn bộ upsert từ đầu.
                logger.warning("d1_post_insert_failed", platform=platform, external_id=external_id)
                return False
            await _snapshot(post_id, scraped_at, engagement)
            await _save_transcript(platform, external_id, draft)
            from app.services.stats.stats_summary import record_post

            await record_post(platform=platform, keyword_id=keyword_id, scraped_at=scraped_at, is_new=True)
            return True

        post_id = existing["id"]
        # Lượt xem không bao giờ về 0: Facebook chỉ có lượt xem reel từ tab Reels của trang (update_post_views) - lần
        # crawl lại từ tìm kiếm / bảng tin gửi 0 thì giữ số đã có thay vì xoá.
        if not engagement["view_count"] and existing.get("view_count"):
            engagement["view_count"] = existing["view_count"]
        changed = any(existing.get(field) != engagement[field] for field in ENGAGEMENT_FIELDS)

        # relevance_* dùng COALESCE(?, column) thay vì ghi đè thẳng: nhánh này chạy lại mỗi
        # lần crawl lại một bài đã có (chỉ cập nhật tương tác), và nội dung không đổi nghĩa là
        # đi lại đúng đường contains_keyword/phân loại như trước, mà đường đó có thể trả None
        # một cách hợp lệ ở đây (chỉ khớp chuỗi con đã quyết định, không có phán quyết của
        # AI) - ghi đè thẳng sẽ xoá mất một nhãn thật mà lượt quét theo lô (hoặc lần phân
        # loại lúc ingest trước đó) đã đặt.
        updated = await d1_exec(
            update(posts)
            .where(posts.c.id == post_id)
            .values(
                url=draft.get("url"),
                author=draft.get("author"),
                content=draft.get("content"),
                media_json=media_json,
                **engagement,
                posted_at=draft.get("posted_at"),
                scraped_at=scraped_at,
                raw_json=raw_json,
                keyword_match=keyword_match,
                relevance_label=func.coalesce(relevance_label, posts.c.relevance_label),
                relevance_confidence=func.coalesce(relevance_confidence, posts.c.relevance_confidence),
                relevance_labeled_at=func.coalesce(
                    scraped_at if relevance_label is not None else None, posts.c.relevance_labeled_at
                ),
            )
        )
        if updated is None:
            # UPDATE thất bại (D1 sập, ...) - post_id vẫn trỏ tới một dòng thật đã có từ trước
            # (khác với nhánh insert ở trên), nên không có gì bị mồ côi, nhưng số tương tác bên
            # dưới sẽ phản ánh payload của message này chứ không phải dữ liệu thực sự đang lưu.
            # Bỏ qua snapshot; lần crawl lại sau sẽ thử lại toàn bộ upsert.
            logger.warning("d1_post_update_failed", platform=platform, external_id=external_id, post_id=post_id)
            return False
        if changed:
            await _snapshot(post_id, scraped_at, engagement)
        await _save_transcript(platform, external_id, draft)
        from app.services.stats.stats_summary import record_post

        await record_post(platform=platform, keyword_id=keyword_id, scraped_at=scraped_at, is_new=False)
        return True


async def _save_transcript(platform: str, external_id: str, draft: PostDraft) -> None:
    """Phụ đề video đi kèm bài (YouTube, xem repositories/d1/transcripts.py). Ghi lỗi không làm hỏng việc lưu bài."""
    if draft.get("transcript"):
        from app.repositories.d1.transcripts import save_transcript

        await save_transcript(platform, external_id, draft["transcript"])


post_repo = PostRepository()


# --- các hàm tự do để tương thích ngược (xem docstring module) -----------


async def list_posts(**kwargs: Any) -> tuple[list[dict[str, Any]], int]:
    return await post_repo.list_posts(**kwargs)


async def list_posts_needing_comments(**kwargs: Any) -> list[dict[str, Any]]:
    return await post_repo.list_posts_needing_comments(**kwargs)


async def get_post_by_external_id(platform: str, external_id: str) -> dict[str, Any] | None:
    return await post_repo.get_post_by_external_id(platform, external_id)


async def update_post_views(platform: str, external_id: str, view_count: int) -> bool:
    return await post_repo.update_post_views(platform, external_id, view_count)


async def update_post_engagement(platform: str, external_id: str, **counts: int | None) -> bool:
    return await post_repo.update_post_engagement(platform, external_id, **counts)


async def get_post(post_id: str) -> dict[str, Any] | None:
    return await post_repo.get_post(post_id)


async def persist_post(**kwargs: Any) -> bool:
    return await post_repo.persist_post(**kwargs)
