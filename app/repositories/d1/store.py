"""Truy cập D1 `cinemark-scraper` cho các bảng phim, từ khoá, report, thống kê và các truy vấn dùng chung.

Dùng chung bảng với Worker cinemark (cùng không gian `movie_id` / `keyword_id`). Truy vấn bài và bình luận nằm ở
`posts.py`, `comments.py` và được re-export ở đây cho chỗ gọi cũ. Mọi hàm fail open: lỗi thì log và trả None/[]/False
thay vì raise, để một lần ghi D1 hỏng không làm sập ingest từ Kafka.
"""

from __future__ import annotations

from app.clients.d1 import _configured, d1_query
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
    movie_hashtag_present,
    persist_post,
    post_mentions_movie,
    post_repo,
    reputable_authors,
    update_post_engagement,
    update_post_views,
)

logger = get_logger(__name__)

# Re-export cho các chỗ gọi `from app.repositories.d1.store import X` hiện có - xem docstring
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
    update_post_engagement,
    update_post_views,
)

# Các nhóm hàm đã tách ra module riêng (17/11/2026) - re-export để `from app.repositories.d1.store import X` cũ vẫn chạy.
from app.repositories.d1.keywords import (  # noqa: F401
    add_keyword,
    enable_keyword,
    get_enabled_keywords,
    get_keyword,
    get_or_create_keyword,
    list_keywords,
    movie_of_keyword,
    set_keyword_enabled,
)
from app.repositories.d1.movies import (  # noqa: F401
    _KEYWORD_JOIN,
    _KEYWORD_ROW,
    _MOVIE_COLUMNS,
    _MOVIE_FIELDS,
    MOVIE_CONTEXT_FIELDS,
    _blank_to_none,
    _unique_movie_slug,
    create_movie,
    disable_movie,
    get_movie,
    list_movies,
    movie_slug,
    update_movie,
)
from app.repositories.d1.overview import (  # noqa: F401
    SHORTVID_COOCCUR_KEY_TMPL,
    SHORTVID_RELATED_MIN_VIDEOS,
    SHORTVID_RELATED_PER_KEYWORD,
    SPIDER_RELATED_KEY_TMPL,
    get_comment_counts_by_platform,
    get_comment_timeseries,
    get_keyword_volume,
    get_post_counts_by_platform,
    get_post_timeseries,
    related_hashtags_for_keywords,
)
from app.repositories.d1.report_data import (  # noqa: F401
    _BATCH_CONCURRENCY,
    _COMMENT_ABOUT_MOVIE_SQL,
    _POST_BATCH,
    _POST_PAGE,
    _QUOTE_MIN_CHARS,
    _REPORT_QUERY_TIMEOUT,
    _SENTIMENT_BUCKETS,
    ASPECT_MIN_N,
    ASPECT_QUOTES_PER_SIDE,
    ASPECT_SLIP_POINTS,
    ASPECT_TREND_DAYS,
    CHANNEL_MIN_MOVIES,
    CHANNEL_MIN_POSTS,
    MIN_COMMENTS_FOR_REPORT,
    REPORT_ASPECT_RESERVE,
    REPORT_COMMENT_SAMPLE_SIZE,
    REPORT_COMMENTS_PER_POST,
    REPORT_POST_SAMPLE_SIZE,
    REPORT_POST_TEXT_CHARS,
    REPORT_SAMPLE_CANDIDATES,
    _aspect_tags,
    _channel_authors,
    _comments_of_posts,
    _count_between,
    _d1_rows,
    _normalize_comment_text,
    _pick_quotes,
    _positive_pct,
    _report_posts,
    _sample_rng,
    _stratified_sample,
    get_movie_aspect_stats,
    get_movie_sentiment_counts,
    get_report_sample_for_movie,
    upsert_social_topic_report,
)
