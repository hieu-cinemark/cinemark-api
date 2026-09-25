"""Everything that reads/writes D1's `posts` table (+ its
post_engagement_snapshots/dropped_posts siblings) - the same rows
cinemark-scraper's Worker owns (see api/schema/scraper.ts there). Pulled out
of the old app/services/d1.py monolith so this table's query patterns,
pagination, and indexes live in one place instead of being mixed in with
movies/keywords/comments.

app/services/d1.py still re-exports every name below (persist_post,
list_posts, ...) so existing `from app.services.d1 import persist_post`
call sites (ingest_consumer, scripts, tests) don't need to change - only
new code needs to reach for `post_repo` directly."""

from __future__ import annotations

import asyncio
import json
import re
import time
import unicodedata
import uuid
from datetime import datetime, timezone
from typing import Any

from app.core.logging import get_logger
from app.services.d1_client import d1_query, _configured
from app.services.platforms import PostDraft, registered_platforms

logger = get_logger(__name__)

ENGAGEMENT_FIELDS = ("like_count", "reply_count", "repost_count", "quote_count", "reshare_count", "view_count")

# Below this many characters (after trimming), a post's content is treated
# as junk - a bare reaction/emoji/one-word comment with nothing to analyze.
# Tune freely; this is a judgment call, not derived from anything.
MIN_CONTENT_LENGTH = 10

#  Interactions only, deliberately excluding view_count: a view is a passive
# impression, not a "tương tác" (interaction) - a big view count would
# otherwise dominate the sort outright, since it's routinely 10-100x the
# size of like/reply/repost/quote/reshare counts combined and would make
# this look like a "most viewed" sort wearing an "engagement" label.
_ENGAGEMENT_SCORE_SQL = "(p.like_count + p.reply_count + p.repost_count + p.quote_count + p.reshare_count)"

# Spaces / punctuation stripped so "#Anh Hùng" and "#AnhHung" both match
# the movie title "Anh Hùng".
_HASHTAG_STRIP = re.compile(r"[\s._-]+")



# --- indexes -------------------------------------------------------------
# Every index below exists because a real query in this file (or
# ingest_consumer's persist_post upsert-check) filters/sorts on exactly
# those columns - see each comment for which one. None of this is
# speculative: the table had zero indexes beyond the `id` primary key until
# 2026-09-21 (verified via PRAGMA index_list against the local mirror),
# meaning every one of these was previously a full table scan.
_POST_INDEXES = (
    # /stats/posts with no platform filter (the dashboard's default "All
    # platforms" tab - see spider-hub-dashboard's PostsReview.tsx) sorts by
    # scraped_at with no WHERE at all - the (platform, scraped_at) index
    # below can't serve that (rows aren't globally scraped_at-ordered
    # unless platform is also pinned), so this plain one covers it.
    "CREATE INDEX IF NOT EXISTS idx_posts_scraped_at ON posts(scraped_at DESC)",
    # /stats/posts?platform=X (a platform tab) - list_posts' WHERE
    # platform = ? ORDER BY scraped_at DESC.
    "CREATE INDEX IF NOT EXISTS idx_posts_platform_scraped_at ON posts(platform, scraped_at DESC)",
    # Top posts by keyword (TopPostsModal / useTopPostsByKeyword) and the
    # daily top-comments sweep (list_posts_needing_comments) both filter on
    # keyword_id alone (a keyword already implies one platform, so this
    # single column is selective enough without a composite).
    "CREATE INDEX IF NOT EXISTS idx_posts_keyword_id ON posts(keyword_id)",
    # Top posts by movie (TopPostsModal / useTopPostsByMovie) filters on
    # movie_id alone, same reasoning.
    "CREATE INDEX IF NOT EXISTS idx_posts_movie_id ON posts(movie_id)",
    # persist_post's own upsert-check (SELECT ... WHERE platform = ? AND
    # external_id = ?) and get_post_by_external_id run on *every single*
    # ingested post - by far the hottest query against this table. Not
    # UNIQUE: the local mirror already has a handful of pre-existing
    # (platform, external_id) duplicates from before this index existed
    # (a persist_post race - see that function's own docstring assuming a
    # unique index that never actually existed) - adding UNIQUE now would
    # just fail to create. Deduping those is a separate, deliberate
    # decision, not something to fold into an index migration.
    "CREATE INDEX IF NOT EXISTS idx_posts_platform_external_id ON posts(platform, external_id)",
)

# Built once in the background (see ensure_tab_filter_indexes) - never
# from list_posts. CREATE INDEX on the live posts table via D1 HTTP can
# exceed the 10s query timeout and, while it runs, starve every other
# dashboard query (the tab-switch failures logged as d1_request_failed).
_TAB_FILTER_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_posts_keyword_match_scraped_at ON posts(keyword_match, scraped_at DESC)",
)

_post_indexes_ready = False
_post_indexes_lock = asyncio.Lock()


async def _ensure_post_indexes() -> None:
    global _post_indexes_ready
    if _post_indexes_ready:
        return
    async with _post_indexes_lock:
        if _post_indexes_ready:
            return
        for sql in _POST_INDEXES:
            await d1_query(sql, quiet=True)
        _post_indexes_ready = True


async def ensure_tab_filter_indexes() -> None:
    """CREATE INDEX for PostsReview related/unrelated tabs. Safe to call
    from app startup as a background task - IF NOT EXISTS, 90s timeout."""
    for sql in _TAB_FILTER_INDEXES:
        await d1_query(sql, quiet=True, timeout=90.0)


def post_mentions_movie(content: str | None, title: str | None) -> bool:
    """True when the post text contains the movie's full title, or a
    hashtag of that title with spaces stripped (#AnhHùng for "Anh Hùng")."""
    if not content or not title:
        return False
    text = content.casefold()
    name = title.strip().casefold()
    if len(name) < 2:
        return False
    if name in text:
        return True
    compact = _HASHTAG_STRIP.sub("", name)
    if len(compact) < 2:
        return False
    compact_text = _HASHTAG_STRIP.sub("", text)
    return f"#{compact}" in compact_text


def _fold_for_keyword_match(text: str) -> str:
    """Lowercase, strip Vietnamese diacritics, remove spaces - ported from
    cinemark-scraper's src/lib/keyword-match.ts foldForKeywordMatch() so a
    post ingested here agrees with how posts in the same table compute
    keyword_match, whichever platform scraped it."""
    decomposed = unicodedata.normalize("NFD", text)
    without_marks = "".join(c for c in decomposed if not unicodedata.combining(c))
    without_dd = without_marks.replace("đ", "d").replace("Đ", "D")
    return re.sub(r"\s+", "", without_dd.lower())


def _keyword_match_parts(keyword: str) -> list[str]:
    """Normal keyword -> one phrase that must appear in full; `+`-joined
    keyword -> every part must appear (any order) - mirrors
    keywordMatchParts() in keyword-match.ts."""
    if "+" in keyword:
        return [part.strip() for part in keyword.split("+") if part.strip()]
    trimmed = keyword.strip()
    return [trimmed] if trimmed else []


def contains_keyword(content: str | None, keyword: str | None) -> bool:
    """Exact-substring keyword_match check. Public (not just persist_post's
    own fallback) so callers - see app/workers/ingest_consumer/main.py's
    handle_post - can check this cheap/free match first and only spend a
    Kira call (see app/kira/relevance.py) on the posts it actually misses,
    instead of classifying every single post regardless of whether the
    free check already found a match."""
    if not content or not keyword:
        return False
    haystack = _fold_for_keyword_match(content)
    parts = _keyword_match_parts(keyword)
    if not parts:
        return False
    for part in parts:
        needle = _fold_for_keyword_match(part)
        if not needle or needle not in haystack:
            return False
    return True


_HASHTAG_TOKEN_RE = re.compile(r"#(\w+)", re.UNICODE)
_VIETNAMESE_COMBINING_MARKS = ("̛", "̣", "̉")  # horn (ư/ơ), dot-below, hook-above tones


def _looks_vietnamese(text: str) -> bool:
    """Cheap language signal, not a real detector: đ, plus the combining
    horn (ư/ơ) and dot-below/hook-above tone marks (NFD-decomposed) are, in
    practice, essentially unique to Vietnamese among the languages that
    actually turn up in this crawl data - grave/acute/tilde alone are
    shared with Spanish/French/Portuguese so aren't used here on their
    own. Used only as movie_hashtag_present's tie-breaker for its weaker
    signal (an exact but short/ambiguous hashtag token, no literal title
    anywhere) - a real Vietnamese sentence of any normal length almost
    always has at least one of these; genuinely unrelated foreign-language
    content sharing that same short hashtag by coincidence won't."""
    if "đ" in text.lower():
        return True
    decomposed = unicodedata.normalize("NFD", text)
    return any(mark in decomposed for mark in _VIETNAMESE_COMBINING_MARKS)


# --- author reputation ----------------------------------------------------
# Corroborates movie_hashtag_present's weakest signal (a bare literal title
# match with no hashtag backing it) for a movie whose title also happens to
# be ordinary vocabulary - see that function's own module docstring for the
# "Huyết Thống" incident this exists to close. An author's reputation is
# built by scripts/build_author_reputation.py re-checking their own past
# relevance_label='related' posts with movie_hashtag_present's STRONG
# signals only (is_reputable_author defaults to False - see that param) -
# never the weak signal this table is itself meant to corroborate, so there
# is no circularity: reputation can only be earned via hashtag evidence.

MIN_MOVIES_FOR_REPUTABLE_AUTHOR = 2
_REPUTABLE_AUTHORS_TTL_SECONDS = 300.0

_author_reputation_ready = False
_reputable_authors_cache: tuple[float, set[tuple[str, str]]] | None = None


async def ensure_author_reputation_table() -> None:
    global _author_reputation_ready
    if _author_reputation_ready:
        return
    await d1_query(
        """
        CREATE TABLE IF NOT EXISTS author_reputation (
            platform text NOT NULL,
            author text NOT NULL,
            distinct_movies integer NOT NULL DEFAULT 0,
            confirmed_posts integer NOT NULL DEFAULT 0,
            updated_at text NOT NULL,
            PRIMARY KEY (platform, author)
        )
        """,
        quiet=True,
    )
    _author_reputation_ready = True


async def reputable_authors() -> set[tuple[str, str]]:
    """{(platform, author)} confirmed across >= MIN_MOVIES_FOR_REPUTABLE_AUTHOR
    distinct movies - cached in-process for _REPUTABLE_AUTHORS_TTL_SECONDS
    (this table only changes when someone re-runs the build script, not
    request-to-request, so a short cache avoids one extra D1 round trip per
    call). Callers check `(platform, author) in reputable_authors()` and
    pass the result as movie_hashtag_present's is_reputable_author."""
    global _reputable_authors_cache
    now = time.monotonic()
    if _reputable_authors_cache is not None and now - _reputable_authors_cache[0] < _REPUTABLE_AUTHORS_TTL_SECONDS:
        return _reputable_authors_cache[1]
    await ensure_author_reputation_table()
    rows = await d1_query(
        "SELECT platform, author FROM author_reputation WHERE distinct_movies >= ?",
        [MIN_MOVIES_FOR_REPUTABLE_AUTHOR],
    )
    result = {(r["platform"], r["author"]) for r in (rows or [])}
    _reputable_authors_cache = (now, result)
    return result


def movie_hashtag_present(
    content: str | None, movie_title: str | None, keyword: str | None, *, is_reputable_author: bool = False
) -> bool:
    """Stricter companion to post_mentions_movie/contains_keyword, for
    callers that want to trust relevance_label='related' (an AI or
    substring verdict - see persist_post's own docstring: keyword_match is
    just a mirror of relevance_label whenever the AI was actually invoked,
    NOT independent corroborating evidence) only when the post text itself
    also, independently, names this movie. Three signals, strongest first:

    1. The movie title's space-stripped hashtag form (e.g. "#HoangHauCuoiCung"
       for "Hoàng Hậu Cuối Cùng") appearing verbatim - deliberate hashtagging
       of the WHOLE title, trusted regardless of who posted it.

    2. A whole hashtag TOKEN whose folded form (see _fold_for_keyword_match -
       diacritics/case/whitespace-insensitive) exactly equals the movie's own
       folded title or configured keyword. Whole-token equality on purpose,
       not substring containment like contains_keyword: a short/generic
       folded keyword (e.g. "Mẹ Mìn" folds to "memin") can coincidentally be
       a SUBSTRING of a completely unrelated longer hashtag (confirmed live
       2026-09-24: a Mexican snack brand's
       "#botanasmemin"/"#echatelabotanaconbotanasmemin" posts scored 99%+
       "related" purely off that coincidence). Still needs
       _looks_vietnamese(content) too: "#memin" is ALSO an existing
       nickname/cultural reference in Spanish (a classic Mexican comic
       character) sharing that exact token, not just a substring of it - see
       that function's own docstring.

    3. The bare literal title occurring anywhere in free-form prose, with no
       hashtag backing it either way - reliable for an invented movie title
       (essentially never occurs by coincidence), unreliable when the title
       is also ordinary vocabulary. Confirmed live 2026-09-25: "Huyết Thống"
       - literally "blood relation" in Vietnamese - matched a stranger's
       unrelated family-conflict post on Threads at 99.9% "related"
       confidence from BOTH the substring check and the AI classifier, since
       the bare phrase carries no movie-specific signal on its own. Trusted
       only when is_reputable_author is True (see reputable_authors) - the
       account has its own independent track record of genuine movie
       content across *other* titles too, via signals 1/2 above, never via
       this same weak signal (no circularity). A first-time/one-off account
       making this exact claim isn't enough evidence by itself.

    Trade-off, accepted on purpose (see callers' own docstrings): a
    genuinely related post that uses neither the literal title, a
    title-equal hashtag, nor comes from a reputable account - just an
    actor's name, a compound tag like "#PhimMeMin", or a nickname - won't
    pass this either. That's the point for the callers using this (a
    "top 100" list, the daily comments sweep, and report generation) -
    fewer, more precisely on-topic results over exhaustive recall."""
    if not content:
        return False
    text = content.casefold()
    name = (movie_title or "").strip().casefold()

    if len(name) >= 2:
        compact = _HASHTAG_STRIP.sub("", name)
        if len(compact) >= 2 and f"#{compact}" in _HASHTAG_STRIP.sub("", text):
            return True

    folded_targets = {_fold_for_keyword_match(t) for t in (movie_title, keyword) if t}
    folded_targets.discard("")
    if folded_targets:
        tags = {_fold_for_keyword_match(tag) for tag in _HASHTAG_TOKEN_RE.findall(content)}
        if (tags & folded_targets) and _looks_vietnamese(content):
            return True

    if len(name) >= 2 and name in text:
        return is_reputable_author

    return False


# Playable video / permalinks cannot go in an <img>. Older TikTok rows
# stored playAddr as media_url and left cover_url only on raw_json.
_VIDEO_URL_HINTS = (
    ".mp4",
    ".m3u8",
    "/video/tos/",
    "webapp-prime.tiktok.com",
    "facebook.com/reel/",
    "facebook.com/watch",
    "facebook.com/share/v",
    "facebook.com/video",
    "tiktok.com/@",
    "threads.com/@",
    "threads.net/@",
)
_IMAGE_URL_HINTS = (
    "fbcdn.net",
    "cdninstagram.com",
    "tiktokcdn",
    "byteicdn",
    "ibyteimg",
    "byteimg.com",
    "muscdn.com",
    "scontent",
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
)


def _is_preview_image_url(url: Any) -> bool:
    if not isinstance(url, str) or not url.startswith("http"):
        return False
    lower = url.lower()
    if any(hint in lower for hint in _VIDEO_URL_HINTS):
        return False
    return any(hint in lower for hint in _IMAGE_URL_HINTS)


def _hydrate_post_rows(rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    out = rows or []
    for row in out:
        media = json.loads(row.pop("media_json") or "{}")
        raw_cover = row.pop("raw_cover_url", None)
        row["media_type"] = media.get("media_type")
        candidates = (
            media.get("cover_url"),
            raw_cover,
            media.get("thumbnail_url"),
            media.get("media_url"),
        )
        row["media_url"] = next((url for url in candidates if _is_preview_image_url(url)), None)
        quoted = media.get("quoted") if isinstance(media.get("quoted"), dict) else None
        row["quoted"] = (
            {
                "author": quoted.get("author"),
                "content": quoted.get("content"),
                "url": quoted.get("url"),
                "media_url": quoted.get("media_url") if _is_preview_image_url(quoted.get("media_url")) else None,
            }
            if quoted
            else None
        )
    return out


class PostRepository:
    """Owns every query against `posts`. One process-wide instance
    (`post_repo` below) - no per-request state, so this is just a
    namespace for the queries plus the lazy index-creation guard above."""

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
        """Paginated post feed, joined to its movie/keyword for display -
        backs the dashboard's "review posts" tab. Every filter is optional
        and additive; passing none returns the whole table's most recent
        page across every platform/movie. Returns (rows, total_count) so
        the caller can render pagination without a second round trip.

        Offset pagination, not keyset: see list_posts_cursor below for why
        this stays the live API for now despite that method existing.

        sort="recent" (default): most recently scraped first, as always.
        sort="engagement": highest-interaction first (see
        _ENGAGEMENT_SCORE_SQL) - e.g. keyword_id + sort="engagement" +
        limit=100 is the dashboard's "top 100 posts for this keyword" view.
        That view requires relevance_label='related' (see
        phobert-classifier/label_posts_relevance.py) AND
        movie_hashtag_present's own, independent check - relevance_label
        alone isn't a second opinion the way it looks: persist_post stores
        keyword_match as a straight mirror of the AI verdict whenever the
        AI was actually invoked (see its own docstring), so relying on
        relevance_label alone was really trusting the AI once, not twice.
        Confirmed live 2026-09-24: a Mexican snack brand's posts scored
        99%+ "related" to the movie "Mẹ Mìn" purely because its own hashtag
        folds to the same short string as the movie's - see
        movie_hashtag_present's own docstring for the fix. Over-fetches
        (_ENGAGEMENT_OVERFETCH below) since this second filter runs in
        Python, then trims back to `limit` - a post not yet AI-labeled, or
        with no matching hashtag/title mention at all, won't occupy a slot
        even at high engagement."""
        await _ensure_post_indexes()
        where = []
        params: list[Any] = []
        if platform:
            where.append("p.platform = ?")
            params.append(platform)
        if keyword_id:
            where.append("p.keyword_id = ?")
            params.append(keyword_id)
        if movie_id:
            where.append("p.movie_id = ?")
            params.append(movie_id)
        if keyword_match is not None:
            where.append("p.keyword_match = ?")
            params.append(1 if keyword_match else 0)
        if sort == "engagement":
            where.append("p.relevance_label = 'related'")
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        order_sql = f"{_ENGAGEMENT_SCORE_SQL} DESC" if sort == "engagement" else "p.scraped_at DESC"
        # movie_hashtag_present runs in Python after the fetch and rejects
        # some rows relevance_label='related' alone would have let through
        # - ask for more than `limit` up front so trimming back down to it
        # after filtering doesn't leave a short/empty page.
        sql_limit = max(limit * 4, limit + 100) if sort == "engagement" else limit

        rows = await d1_query(
            f"""
            SELECT
                p.id, p.platform, p.external_id, p.url, p.author, p.content, p.media_json,
                json_extract(p.raw_json, '$.cover_url') AS raw_cover_url,
                p.like_count, p.reply_count, p.repost_count, p.quote_count, p.reshare_count, p.view_count,
                p.posted_at, p.scraped_at, p.keyword_match,
                k.keyword, m.title AS movie_title
            FROM posts p
            LEFT JOIN keywords k ON k.id = p.keyword_id
            LEFT JOIN movies m ON m.id = p.movie_id
            {where_sql}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?
            """,
            [*params, sql_limit, offset],
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

        # Filtered tabs: skip COUNT(*) - it's a full scan until the
        # keyword_match index exists, and it was timing out D1 (~10s) on
        # every tab click. Approximate "there's another page" from the
        # page size instead.
        if keyword_match is None:
            count_rows = await d1_query(f"SELECT COUNT(*) AS total FROM posts p {where_sql}", params)
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
        """Keyset-paginated equivalent of list_posts, for sort="recent"
        only (engagement sort's expression can't be used as a keyset
        column without a generated/indexed copy of it - not worth adding
        until this method is actually load-bearing). `cursor` is the
        opaque "<scraped_at>|<id>" of the last row from the previous page;
        None starts from the top. No total count - keyset pagination
        trades "page N of M" (and, not incidentally, the approximate/
        sometimes-wrong total list_posts' keyword_match branch reports -
        see its own comment) for O(limit) cost per page regardless of how
        deep you go, which is the whole point.

        Now wired into GET /stats/posts (see stats.py) - list_posts' own
        OFFSET, confirmed live once posts crossed ~100k, gets *slower with
        depth* specifically when combined with the keyword/movie LEFT
        JOINs (flat ~0.1s regardless of offset with the JOINs removed,
        vs. up to several seconds at a 90k offset with them) - D1 isn't
        pushing the LIMIT/OFFSET below the join. Keyset sidesteps this
        entirely: the WHERE seek bounds the scan before the join ever
        runs, so it doesn't matter how deep `cursor` points."""
        if sort != "recent":
            raise ValueError("list_posts_cursor only supports sort='recent'")
        await _ensure_post_indexes()

        where = []
        params: list[Any] = []
        if platform:
            where.append("p.platform = ?")
            params.append(platform)
        if keyword_id:
            where.append("p.keyword_id = ?")
            params.append(keyword_id)
        if movie_id:
            where.append("p.movie_id = ?")
            params.append(movie_id)
        if keyword_match is not None:
            where.append("p.keyword_match = ?")
            params.append(1 if keyword_match else 0)
        if cursor:
            cursor_scraped_at, _, cursor_id = cursor.partition("|")
            where.append("(p.scraped_at < ? OR (p.scraped_at = ? AND p.id < ?))")
            params += [cursor_scraped_at, cursor_scraped_at, cursor_id]
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        rows = await d1_query(
            f"""
            SELECT
                p.id, p.platform, p.external_id, p.url, p.author, p.content, p.media_json,
                json_extract(p.raw_json, '$.cover_url') AS raw_cover_url,
                p.like_count, p.reply_count, p.repost_count, p.quote_count, p.reshare_count, p.view_count,
                p.posted_at, p.scraped_at, p.keyword_match,
                k.keyword, m.title AS movie_title
            FROM posts p
            LEFT JOIN keywords k ON k.id = p.keyword_id
            LEFT JOIN movies m ON m.id = p.movie_id
            {where_sql}
            ORDER BY p.scraped_at DESC, p.id DESC
            LIMIT ?
            """,
            [*params, limit],
            timeout=20.0 if keyword_match is not None else 10.0,
        )
        hydrated = _hydrate_post_rows(rows)
        next_cursor = f"{hydrated[-1]['scraped_at']}|{hydrated[-1]['id']}" if len(hydrated) == limit else None
        return hydrated, next_cursor

    async def list_posts_needing_comments(
        self, *, platform: str, keyword_id: str, top_n: int = 100
    ) -> list[dict[str, Any]]:
        """Of this keyword's own top `top_n` posts by engagement (the exact
        same ranking as list_posts(sort="engagement") above), the ones with
        zero comments stored yet - backs the daily "top comments" sweep (see
        scheduler.py's _top_comments_tick). Ranks first, *then* filters for
        zero comments (a CTE, not a single WHERE) - filtering first would let a
        101st/150th-ranked post with no comments crowd out an actual top-100
        post that merely already has some, which isn't "of the top 100, which
        still need comments" any more.

        A post already swept on some earlier day isn't queued again just
        because it's still sitting in the keyword's top 100 - only ones that
        are new to it (or never got comments the first time) are - so a
        keyword whose top 100 barely reshuffles day to day doesn't keep
        re-spending the account/proxy pool on posts it already fetched
        comments for. Same relevance_label='related' + movie_hashtag_present
        gate as the dashboard's top-100 list (list_posts sort="engagement")
        - see that method's own docstring and movie_hashtag_present's for
        why relevance_label alone isn't independent corroboration; same
        AI-labeled-post caveat applies (a post not yet swept by
        label_posts_relevance.py's batch run is invisible here too)."""
        await _ensure_post_indexes()
        overfetch = max(top_n * 4, top_n + 100)
        rows = await d1_query(
            f"""
            SELECT p.id, p.external_id, p.url, p.content, p.author, k.keyword, m.title AS movie_title,
                   COALESCE(c.n, 0) AS comment_n
            FROM posts p
            LEFT JOIN keywords k ON k.id = p.keyword_id
            LEFT JOIN movies m ON m.id = p.movie_id
            LEFT JOIN (SELECT post_id, COUNT(*) AS n FROM comments GROUP BY post_id) c ON c.post_id = p.id
            WHERE p.platform = ? AND p.keyword_id = ? AND p.relevance_label = 'related'
            ORDER BY {_ENGAGEMENT_SCORE_SQL} DESC
            LIMIT ?
            """,
            [platform, keyword_id, overfetch],
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

    async def get_post_by_external_id(self, platform: str, external_id: str) -> dict[str, Any] | None:
        """One post by (platform, external_id) - the id spider-hub's comment
        payloads carry (see app/workers/ingest_consumer/main.py's
        handle_comment), as opposed to get_post's D1-internal id."""
        await _ensure_post_indexes()
        rows = await d1_query(
            "SELECT id, platform, external_id, url FROM posts WHERE platform = ? AND external_id = ?",
            [platform, external_id],
        )
        return rows[0] if rows else None

    async def get_post(self, post_id: str) -> dict[str, Any] | None:
        """One post by its D1 id (not external_id) - used by the "fetch
        comments for this post" trigger (see app/api/routes/facebook.py) to
        resolve the platform's own post id + url spider-hub's bootstrap/spider
        needs, from the D1 id the dashboard actually has on hand (see
        app/schemas/stats.py's Post.id)."""
        rows = await d1_query("SELECT id, platform, external_id, url FROM posts WHERE id = ?", [post_id])
        return rows[0] if rows else None

    async def persist_dropped_post(
        self, *, platform: str, reason: str, payload: dict[str, Any], keyword_id: str | None = None
    ) -> None:
        """Archives a raw Kafka payload that the ingest consumer dropped before
        it ever reached persist_post (unregistered platform mapper, missing/
        unknown keyword_id - see app/workers/ingest_consumer/main.py's
        handle_post). persist_post's own `raw_json` column already covers
        "reprocess after fixing a mapper bug" for posts that DID get persisted;
        this covers the posts that never got that far at all - once the
        underlying issue is fixed (mapper registered, keyword corrected),
        these rows are the only way to recover that data without re-scraping
        it, which may not even be possible later (the post could be deleted by
        then, or the crawl window long gone). Best-effort like every other
        write here - a failure must not mask the drop itself, already logged/
        alerted by the caller regardless of whether this archive succeeds."""
        if not _configured():
            return
        await d1_query(
            "INSERT INTO dropped_posts (id, platform, reason, keyword_id, raw_json, dropped_at) VALUES (?, ?, ?, ?, ?, ?)",
            [
                f"dropped_{uuid.uuid4()}",
                platform,
                reason,
                keyword_id,
                json.dumps(payload),
                datetime.now(tz=timezone.utc).isoformat(),
            ],
        )

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
        """Upsert one scraped post (any registered platform) straight into
        cinemark-scraper's own `posts` table (+ an engagement snapshot on
        change) - ported from its src/jobs/persist-post.ts so both the
        Worker's own scrapers and this Kafka-fed path write through the exact
        same logic. `draft` is already normalized by the platform's mapper
        (see app/services/platforms.py) - this function has no
        platform-specific field knowledge of its own.

        `ai_relevant`, when given (see app/services/relevance_phobert.py),
        is the PhoBERT model's confident (non-"uncertain") verdict and is
        used for keyword_match instead of the exact-substring
        contains_keyword check below - callers pass None to fall back to
        the substring check (PhoBERT not configured, or the call failed)
        rather than blocking ingestion on a classifier hiccup.

        `relevance_label`/`relevance_confidence`, when given, are that same
        classification stored straight onto the row at ingest time (the
        3-bucket related/not_related/uncertain scheme, not the boolean
        keyword_match) - previously only ever set later by a batch sweep
        (phobert-classifier/label_posts_relevance.py), leaving every post
        NULL until the next run; a post classified live no longer needs
        that batch pass to show up in relevance-filtered views."""
        if not _configured() or platform not in registered_platforms():
            return False
        await _ensure_post_indexes()

        external_id = draft.get("external_id")
        if not external_id:
            return False

        content = draft.get("content")

        # Junk filter: too-short content (a bare reaction/emoji has no real
        # signal to analyze) - skip entirely. Does NOT filter on keyword_match:
        # a real commenter writing an abbreviation, an unaccented Vietnamese
        # spelling, or the movie's English name would never literally contain
        # the configured keyword phrase, so gating storage on that match would
        # silently drop real posts. keyword_match is still computed and stored
        # below as a flag for callers to filter on if they choose to, same as
        # before - it's just not a reason to skip storing the post outright.
        if not content or len(content.strip()) < MIN_CONTENT_LENGTH:
            logger.info("post_skipped_junk", platform=platform, external_id=external_id, reason="content_too_short")
            # Intentional skip, not a write failure - True so handle_post's
            # `if not ok` doesn't archive this to dropped_posts as
            # "d1_write_failed" and count it toward the drop-alert threshold.
            return True
        is_keyword_match = ai_relevant if ai_relevant is not None else contains_keyword(content, keyword)

        scraped_at = datetime.now(tz=timezone.utc).isoformat()
        media_json = json.dumps(draft.get("media") or {})
        raw_json = json.dumps(draft.get("raw")) if draft.get("raw") is not None else None
        engagement = {field: draft.get(field) or 0 for field in ENGAGEMENT_FIELDS}
        # D1 stores booleans as SQLite integers (0/1) - pass an int, not a JSON
        # bool, so the HTTP API binds it as the same type Drizzle's
        # integer(..., {mode: "boolean"}) column expects. Can legitimately be 0
        # - see this function's own docstring for why a non-match still gets
        # stored instead of skipped.
        keyword_match = int(is_keyword_match)

        existing_rows = await d1_query(
            "SELECT id, like_count, reply_count, repost_count, quote_count, reshare_count, view_count "
            "FROM posts WHERE platform = ? AND external_id = ?",
            [platform, external_id],
        )
        existing = existing_rows[0] if existing_rows else None

        if existing is None:
            post_id = f"post_{uuid.uuid4()}"
            relevance_labeled_at = scraped_at if relevance_label is not None else None
            inserted = await d1_query(
                """
                INSERT INTO posts (
                    id, movie_id, keyword_id, platform, external_id, url, author, content, media_json,
                    like_count, reply_count, repost_count, quote_count, reshare_count, view_count,
                    posted_at, scraped_at, raw_json, keyword_match,
                    relevance_label, relevance_confidence, relevance_labeled_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    post_id,
                    movie_id,
                    keyword_id,
                    platform,
                    external_id,
                    draft.get("url"),
                    draft.get("author"),
                    draft.get("content"),
                    media_json,
                    engagement["like_count"],
                    engagement["reply_count"],
                    engagement["repost_count"],
                    engagement["quote_count"],
                    engagement["reshare_count"],
                    engagement["view_count"],
                    draft.get("posted_at"),
                    scraped_at,
                    raw_json,
                    keyword_match,
                    relevance_label,
                    relevance_confidence,
                    relevance_labeled_at,
                ],
            )
            if inserted is None:
                # Insert failed (race with another message for the same
                # external_id, D1 outage, ...) - the post row doesn't exist,
                # so a snapshot referencing post_id here would be an orphan.
                # Log and stop; the next re-scrape of this post will retry
                # the whole upsert from scratch.
                logger.warning("d1_post_insert_failed", platform=platform, external_id=external_id)
                return False
            await d1_query(
                "INSERT INTO post_engagement_snapshots "
                "(id, post_id, recorded_at, like_count, reply_count, repost_count, quote_count, reshare_count, view_count) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [f"eng_{uuid.uuid4()}", post_id, scraped_at, *engagement.values()],
            )
            from app.services.stats_summary import record_post

            await record_post(platform=platform, keyword_id=keyword_id, scraped_at=scraped_at, is_new=True)
            return True

        post_id = existing["id"]
        changed = any(existing.get(field) != engagement[field] for field in ENGAGEMENT_FIELDS)

        # relevance_* use COALESCE(?, column) rather than a plain overwrite:
        # this branch re-runs on every re-scrape of an already-existing post
        # (engagement-only updates), and content unchanged means the same
        # contains_keyword/classify path as before, which can legitimately
        # be None here (substring match alone decided it, no PhoBERT call
        # made) - a plain overwrite would null out a real label a batch
        # sweep (or an earlier ingest classification) already set.
        updated = await d1_query(
            """
            UPDATE posts SET
                url = ?, author = ?, content = ?, media_json = ?,
                like_count = ?, reply_count = ?, repost_count = ?, quote_count = ?, reshare_count = ?, view_count = ?,
                posted_at = ?, scraped_at = ?, raw_json = ?, keyword_match = ?,
                relevance_label = COALESCE(?, relevance_label),
                relevance_confidence = COALESCE(?, relevance_confidence),
                relevance_labeled_at = COALESCE(?, relevance_labeled_at)
            WHERE id = ?
            """,
            [
                draft.get("url"),
                draft.get("author"),
                draft.get("content"),
                media_json,
                engagement["like_count"],
                engagement["reply_count"],
                engagement["repost_count"],
                engagement["quote_count"],
                engagement["reshare_count"],
                engagement["view_count"],
                draft.get("posted_at"),
                scraped_at,
                raw_json,
                keyword_match,
                relevance_label,
                relevance_confidence,
                scraped_at if relevance_label is not None else None,
                post_id,
            ],
        )
        if updated is None:
            # UPDATE failed (D1 outage, ...) - post_id still refers to a real,
            # pre-existing row (unlike the insert branch above), so nothing's
            # orphaned, but the engagement numbers below would reflect this
            # message's payload, not what's actually stored. Skip the snapshot;
            # the next re-scrape retries the whole upsert.
            logger.warning("d1_post_update_failed", platform=platform, external_id=external_id, post_id=post_id)
            return False
        if changed:
            await d1_query(
                "INSERT INTO post_engagement_snapshots "
                "(id, post_id, recorded_at, like_count, reply_count, repost_count, quote_count, reshare_count, view_count) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [f"eng_{uuid.uuid4()}", post_id, scraped_at, *engagement.values()],
            )
        from app.services.stats_summary import record_post

        await record_post(platform=platform, keyword_id=keyword_id, scraped_at=scraped_at, is_new=False)
        return True


post_repo = PostRepository()


# --- backward-compatible free functions (see module docstring) -----------


async def list_posts(**kwargs: Any) -> tuple[list[dict[str, Any]], int]:
    return await post_repo.list_posts(**kwargs)


async def list_posts_needing_comments(**kwargs: Any) -> list[dict[str, Any]]:
    return await post_repo.list_posts_needing_comments(**kwargs)


async def get_post_by_external_id(platform: str, external_id: str) -> dict[str, Any] | None:
    return await post_repo.get_post_by_external_id(platform, external_id)


async def get_post(post_id: str) -> dict[str, Any] | None:
    return await post_repo.get_post(post_id)


async def persist_dropped_post(**kwargs: Any) -> None:
    await post_repo.persist_dropped_post(**kwargs)


async def persist_post(**kwargs: Any) -> bool:
    return await post_repo.persist_post(**kwargs)
