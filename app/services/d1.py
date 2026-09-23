"""Cloudflare D1 access layer - lets this VPS-hosted service read/write a
D1 database without needing a Cloudflare Worker (D1 bindings only exist
inside Workers; from a plain process, D1's REST query API is the only door
in). Talks to the same D1 database cinemark-scraper's Worker owns, using its
existing movies/keywords/posts/post_engagement_snapshots tables (see
cinemark-scraper/src/db/schema.ts) - not a separate table of our own, so a
crawl triggered from here (get_enabled_keywords/get_keyword) and the post it
produces (persist_post) share the exact same movie_id/keyword_id space, no
ID-mapping layer needed.

The HTTP-vs-local transport (d1_query) now lives in app/services/d1_client.py,
and posts/comments' own queries live in app/repositories/d1/{posts,comments}.py
- both re-exported below so existing `from app.services.d1 import
persist_post` etc. call sites don't need to change. This module keeps the
transport-agnostic logic for every other table (movies, keywords,
social_topic_reports) plus the stats_summary.py passthroughs.

Platform-agnostic: every function here works off app.services.platforms'
registered_platforms(), not a hardcoded "facebook" literal - see that
module's docstring for what adding a new platform requires.

Best-effort throughout: every function here returns None/[]/False and logs
on failure (missing config, network error, HTTP error) instead of raising -
a D1 write that fails must never take down Kafka ingestion, which is the
only durable delivery guarantee this service has."""

from __future__ import annotations

import json
import re
import unicodedata
import uuid
from datetime import datetime, timezone
from typing import Any

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
    PostRepository,
    contains_keyword,
    get_post,
    get_post_by_external_id,
    list_posts,
    list_posts_needing_comments,
    persist_dropped_post,
    persist_post,
    post_mentions_movie,
    post_repo,
)
from app.services.d1_client import _configured, d1_query
from app.services.redis import REDIS_KEY_PREFIX, get_redis_client

logger = get_logger(__name__)

# Re-exported for existing `from app.services.d1 import X` call sites - see
# module docstring. Referencing them here (not just importing) keeps linters
# from flagging the import as unused.
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
    persist_dropped_post,
    persist_post,
    post_mentions_movie,
)


async def _related_hashtags_for_keywords(keyword_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    """TikTok co-occurring tags spider-hub stored after a crawl (Redis
    tiktok:related_hashtags:{keyword_id}). Missing Redis or empty keys
    just mean the dashboard shows no review chips yet."""
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
        except (TypeError, json.JSONDecodeError):
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
    """Total posts ingested per platform, plus the most recent scrape and
    today's vs yesterday's ingest counts. Reads pre-aggregated
    stats_platform_daily (see app/services/stats_summary.py) - a full
    COUNT(*) over posts via D1 HTTP is too slow for the Overview page."""
    from app.services.stats_summary import get_post_counts_by_platform as _from_summary

    return await _from_summary()


async def get_post_timeseries(days: int) -> list[dict[str, Any]]:
    """Daily post counts per platform for the last `days` days - from
    stats_platform_daily rollups."""
    from app.services.stats_summary import get_post_timeseries as _from_summary

    return await _from_summary(days)


async def get_comment_counts_by_platform() -> list[dict[str, Any]]:
    """Total comments ingested per platform - same shape as
    get_post_counts_by_platform, from stats_platform_daily."""
    from app.services.stats_summary import get_comment_counts_by_platform as _from_summary

    return await _from_summary()


async def get_comment_timeseries(days: int) -> list[dict[str, Any]]:
    """Daily comment counts per platform for the last `days` days."""
    from app.services.stats_summary import get_comment_timeseries as _from_summary

    return await _from_summary(days)


async def get_keyword_volume(platform: str | None = None) -> list[dict[str, Any]]:
    """Per-search-keyword post/comment totals plus today's vs yesterday's
    ingest - from stats_keyword_daily rollups."""
    from app.services.stats_summary import get_keyword_volume as _from_summary

    return await _from_summary(platform)


_MOVIE_COLUMNS = "id, title, slug, released_at, poster_url, description, director, `cast`, distributor"


def movie_slug(title: str) -> str:
    """ASCII-ish URL slug from a title (Vietnamese diacritics stripped)."""
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
    """Every enabled movie - feeds the dashboard's "which movie does this
    new keyword belong to" picker when creating a keyword inline from the
    crawl-trigger form, and the dashboard's own movie-detail table. Also
    used by scripts/generate_social_topic_reports.py to iterate every movie
    that should get a report (extra fields here are simply unused by that
    caller, not a breaking change for it). `cast` needs backticks - it's a
    SQL keyword (the CAST() function) in SQLite's own grammar, not just a
    Python one."""
    rows = await d1_query(f"SELECT {_MOVIE_COLUMNS} FROM movies WHERE enabled = 1 ORDER BY title ASC")
    return rows or []


async def get_movie(movie_id: str) -> dict[str, Any] | None:
    rows = await d1_query(f"SELECT {_MOVIE_COLUMNS} FROM movies WHERE id = ? AND enabled = 1", [movie_id])
    if rows is None:
        return None
    return rows[0] if rows else None


async def create_movie(fields: dict[str, Any]) -> dict[str, Any] | None:
    """Dashboard Movies page create - staff type a title and optional
    release/cast fields; slug is derived unless they pass one."""
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
    """Soft-delete from the dashboard list (keywords/posts keep the FK)."""
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


# Below this many classified comments, scripts/generate_social_topic_reports.py
# skips a movie entirely (too little signal for a meaningful topic cluster) -
# tune freely, not derived from anything.
MIN_COMMENTS_FOR_REPORT = 15

# Cap on how many comments feed one topic-clustering Kira call - keeps the
# prompt (and the model's reasoning-token spend) bounded regardless of how
# large a movie's comment volume gets. Ranked by engagement first, so the
# highest-signal comments are the ones that get dropped if a movie has more
# than this many classified comments.
REPORT_COMMENT_SAMPLE_SIZE = 400


async def get_comment_sample_for_movie(movie_id: str, limit: int = REPORT_COMMENT_SAMPLE_SIZE) -> list[dict[str, Any]]:
    """Engagement-ranked sample of this movie's already-sentiment-classified
    comments, for the topic-clustering Bee call in
    scripts/generate_social_topic_reports.py - NOT used for the overall
    sentiment percentages (see get_movie_sentiment_counts, which counts
    every classified comment, not just this capped sample).

    Only comments under a post the PhoBERT model confidently marked
    relevance_label='related' - a keyword-matched post that isn't actually
    about the movie (see app/repositories/d1/posts.py's own top-100 filter,
    same predicate) would otherwise let its off-topic comments dilute the
    topic clustering and the sentiment split just as much as it used to
    dilute the top-100 list. Confirmed live before this fix: some movies
    had 25-66% of their "classified comments" sitting under such posts."""
    rows = await d1_query(
        """
        SELECT c.id, c.post_id, c.message, c.reactions_count, c.sentiment,
               c.author_name, c.author_url, c.author_profile_picture,
               p.url AS post_url, p.content AS post_content, p.author AS post_author, p.platform
        FROM comments c
        JOIN posts p ON p.id = c.post_id
        WHERE p.movie_id = ? AND p.relevance_label = 'related'
          AND c.sentiment IS NOT NULL AND c.message IS NOT NULL
        ORDER BY c.reactions_count DESC, c.scraped_at DESC
        LIMIT ?
        """,
        [movie_id, limit],
    )
    return rows or []


async def get_movie_sentiment_counts(movie_id: str) -> dict[str, int]:
    """Count of every classified comment for this movie, grouped by
    sentiment label - the ground truth for the report's overall_sentiment
    percentages (computed by the caller via plain division, not estimated
    by an LLM), over the FULL population, not just the capped sample fed
    to the topic-clustering call.

    Same p.relevance_label='related' gate as get_comment_sample_for_movie
    above, for the same reason - the percentages must come from the same
    on-topic population the sample was drawn from, not a larger one that
    still includes off-topic posts' comments."""
    rows = await d1_query(
        """
        SELECT c.sentiment, COUNT(*) AS count
        FROM comments c
        JOIN posts p ON p.id = c.post_id
        WHERE p.movie_id = ? AND p.relevance_label = 'related' AND c.sentiment IS NOT NULL
        GROUP BY c.sentiment
        """,
        [movie_id],
    )
    return {row["sentiment"]: row["count"] for row in (rows or [])}


async def upsert_social_topic_report(
    *, movie_id: str, dashboard_data_json: str, comment_count: int, post_count: int, kira_model: str | None
) -> bool:
    """Upsert-by-movie_id into social_topic_reports - one row per movie,
    overwritten on each scripts/generate_social_topic_reports.py run (no
    history kept; nothing reads past reports)."""
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
    """Used by the dashboard's inline "type a new keyword" flow (crawl
    trigger form) - looks up an existing (movie_id, platform, keyword) row
    first (that triple has a unique index - see cinemark-scraper's
    src/db/schema.ts) so a repeat submission or a race with another tab
    just returns the same row instead of erroring on the constraint."""
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
    """Every enabled keyword for this platform, with its movie's title -
    feeds the dashboard's keyword picker (GET /<platform>/keywords) so a
    manual crawl trigger can target one keyword instead of "every enabled
    keyword for this platform" (see get_enabled_keywords below, still used
    for that fan-out case)."""
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
    """One enabled keyword by id, on the given platform, joined to its
    movie's enabled flag - mirrors what the deleted Postgres
    KeywordRepository.get() + movie lookup used to do for the manual "run
    one keyword" trigger. The platform argument is required, not
    incidental: each platform gets its own router (see
    app/api/routes/facebook.py + platform_scraper.py) that only ever wants
    keywords for itself - a keyword_id belonging to a different platform
    must not silently match here."""
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
    """Every enabled keyword on the given platform (optionally scoped to
    one movie) whose movie is also enabled - used for both the "run all
    keywords for a movie" trigger and a platform's daily cron ("run
    everything for this platform") call."""
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
    """Toggles one keyword on/off - lets an operator pause a stale/one-off
    keyword (or a batch just added for testing) without deleting it, so a
    platform's daily schedule (get_enabled_keywords above) picks up exactly
    the intended set. platform is a defensive scope, not a lookup key on
    its own - keyword_id is already unique - so a mismatched platform in
    the URL can't silently toggle a different platform's row. Returns the
    updated row, or None if the id doesn't exist under that platform, or
    the write itself failed."""
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
