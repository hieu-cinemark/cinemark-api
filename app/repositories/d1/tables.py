"""Bảng D1 dùng chung để dựng query bằng SQLAlchemy Core (xem sql.py). Chỉ khai các cột mà code Python đọc/ghi; schema
thật ở cinemark (src/db/schema.ts + migrations). Bảng riêng của một module khai ngay trong module đó (topics.py,
press_reviews.py)."""

from __future__ import annotations

from sqlalchemy import Column, Float, Integer, Table, Text

from app.repositories.d1.sql import metadata


def _text(*names: str) -> list[Column]:
    return [Column(name, Text) for name in names]


def _int(*names: str) -> list[Column]:
    return [Column(name, Integer) for name in names]


ENGAGEMENT_COLUMNS = ("like_count", "reply_count", "repost_count", "quote_count", "reshare_count", "view_count")

movies = Table(
    "movies",
    metadata,
    Column("id", Text, primary_key=True),
    *_text("title", "slug"),
    *_int("enabled"),
    *_text("created_at", "updated_at", "released_at", "poster_url", "description", "director", "cast", "distributor"),
)

keywords = Table(
    "keywords",
    metadata,
    Column("id", Text, primary_key=True),
    *_text("movie_id", "platform", "keyword"),
    *_int("enabled"),
    *_text("created_at", "related_keywords"),
)

posts = Table(
    "posts",
    metadata,
    Column("id", Text, primary_key=True),
    *_text("movie_id", "keyword_id", "platform", "external_id", "url", "author", "content", "media_json"),
    *_int(*ENGAGEMENT_COLUMNS),
    *_text("posted_at", "scraped_at", "raw_json"),
    *_int("keyword_match"),
    Column("relevance_label", Text),
    Column("relevance_confidence", Float),
    *_text("relevance_labeled_at", "sentiment", "aspects", "audience_stage", "insights_classified_at"),
    # trạng thái quét comment (crawl/comment_planner.py)
    Column("comments_crawled_at", Text),
    *_int("comments_crawled_replies"),
    *_text("comments_crawl_reason", "aspects_source", "first_seen_at", "sentiment_classified_at"),
    Column("comments_queued_at", Text),
    *_int("comments_queued_replies"),
    Column("comments_queued_run_id", Text),
    *_int("trend_post"),
)

post_engagement_snapshots = Table(
    "post_engagement_snapshots",
    metadata,
    Column("id", Text, primary_key=True),
    *_text("post_id", "recorded_at"),
    *_int(*ENGAGEMENT_COLUMNS),
)

comments = Table(
    "comments",
    metadata,
    Column("id", Text, primary_key=True),
    *_text("post_id", "platform", "external_id", "message", "author_name", "author_id", "author_url"),
    *_text("author_profile_picture"),
    *_int("reactions_count", "replies_count"),
    *_text("posted_at", "scraped_at", "raw_json", "sentiment", "sentiment_classified_at", "parent_external_id"),
    Column("relevance_confidence", Float),
    *_text("relevance_labeled_at", "relevance_label", "aspects", "audience_stage", "insights_classified_at"),
    Column("sentiment_source", Text),
    Column("sentiment_confidence", Float),
    *_text("aspects_source", "first_seen_at"),
)

social_topic_reports = Table(
    "social_topic_reports",
    metadata,
    Column("id", Text, primary_key=True),
    *_text("movie_id", "dashboard_data_json"),
    *_int("comment_count", "post_count"),
    *_text("kira_model", "generated_at"),
)

# Hồ sơ kênh: follower (crawl/channel_profiles.py) + tệp khán giả (ai_jobs/channel_niches.py). Schema tạo ở
# crawl/channel_profiles.TABLE_SQL.
channel_profiles = Table(
    "channel_profiles",
    metadata,
    Column("platform", Text, primary_key=True),
    Column("handle", Text, primary_key=True),
    Column("name", Text),
    *_int("followers"),
    *_text("followers_at", "niche"),
    *_int("niche_confidence"),
    *_text("niche_reason", "niche_source", "niche_at", "custom_name", "custom_url", "followers_source"),
)

# Điểm "đáng cào bình luận" AI chấm cho từng bài (crawl/comment_value.py).
post_comment_value = Table(
    "post_comment_value",
    metadata,
    Column("post_id", Text, primary_key=True),
    Column("score", Integer, nullable=False),
    Column("kind", Text),
    Column("reason", Text),
    Column("judged_at", Text, nullable=False),
)

# Ảnh bài đã chấm (scripts/jobs/post_visuals.py).
post_visuals = Table(
    "post_visuals",
    metadata,
    Column("post_id", Text, primary_key=True),
    Column("movie_id", Text, nullable=False),
    Column("platform", Text),
    Column("status", Text, nullable=False),
    Column("label", Text),
    Column("confidence", Float),
    *_text("probs", "r2_key", "embedding", "model"),
    *_text("scored_at"),
)

# Cảnh gom từ ảnh "chụp màn hình rạp" (scripts/jobs/post_visuals.py, app/vision/scenes.py).
movie_scenes = Table(
    "movie_scenes",
    metadata,
    Column("movie_id", Text, primary_key=True),
    Column("rank", Integer, primary_key=True),
    Column("rep_post_id", Text, nullable=False),
    *_text("r2_key"),
    *_int("posts", "authors", "discussions", "views"),
    *_text("first_at", "last_at"),
    Column("cohesion", Float),
    *_text("thumbs", "samples"),
    *_text("computed_at"),
)

# Nhãn tên cảnh do người vận hành đặt tay (scripts/tools/name_scenes.py).
movie_scene_labels = Table(
    "movie_scene_labels",
    metadata,
    Column("movie_id", Text, primary_key=True),
    Column("rep_post_id", Text, primary_key=True),
    *_text("label", "description"),
    *_int("confident"),
    *_text("created_at"),
)

# Nhóm bình luận theo chủ đề (scripts/tools/publish_clusters.py ghi, scripts/jobs/social_topic_reports.py đọc).
comment_cluster_topics = Table(
    "comment_cluster_topics",
    metadata,
    Column("movie_id", Text, primary_key=True),
    Column("cluster", Integer, primary_key=True),
    *_text("name", "description", "kind", "merge_with"),
    *_int("comments"),
    Column("share_percent", Float),
    *_text("generated_at"),
)

# Thành viên từng nhóm (scripts/tools/publish_clusters.py ghi).
comment_cluster_members = Table(
    "comment_cluster_members",
    metadata,
    Column("movie_id", Text, primary_key=True),
    Column("cluster", Integer, primary_key=True),
    Column("comment_id", Text, primary_key=True),
)

# Bảng key/value cấu hình trên database "config" (cinemark-config-db).
config_settings = Table("settings", metadata, Column("key", Text, primary_key=True), Column("value", Text))

# Mốc đo của phim (ngày chiếu sớm...) - cinemark ghi, bên này chỉ đọc.
movie_measure_settings = Table(
    "movie_measure_settings", metadata, Column("movie_id", Text, primary_key=True), Column("premiere_date", Text)
)
