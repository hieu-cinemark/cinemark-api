"""Đường dẫn lake và ánh xạ trường bronze -> cột silver cho từng nền tảng (xem silver.py)."""

from app.core.config import settings

BRONZE = f"r2://{settings.lake_bucket}/bronze"
SILVER = f"r2://{settings.lake_bucket}/silver"
GOLD = f"r2://{settings.lake_bucket}/gold"
COLS = """{topic: 'VARCHAR', "partition": 'INTEGER', "offset": 'BIGINT', kafka_ts: 'BIGINT', payload: 'JSON'}"""

WRITABLE_PREFIXES = ("silver/", "gold/")

# Biểu thức giống nhau trên mọi nền tảng.
COMMON = {
    "keyword_id": "payload->>'keyword_id'",
    "url": "payload->>'url'",
    "author_id": "payload->>'author_id'",
    "author_name": "payload->>'author_name'",
    "scraped_at": "to_timestamp(kafka_ts / 1000)",
}

# Mỗi nền tảng phải ánh xạ đúng đủ các cột này - xem _check_mapping. Nếu không,
# một tên viết sai sẽ lọt qua UNION ALL BY NAME thành một cột NULL mà không ai biết.
POST_COLUMNS = {
    "post_id",
    "content",
    "posted_at",
    "author_username",
    "hashtags",
    "likes",
    "comments",
    "shares",
    "views",
}

POST_FIELDS = {
    "facebook": {
        "post_id": "payload->>'post_id'",
        "content": "payload->>'message'",
        "posted_at": "to_timestamp(CAST(payload->>'timestamp' AS BIGINT))",
        "author_username": "NULL::VARCHAR",
        # Spider lưu hashtag Facebook ở dạng URL-encoded (villah%E1%BB%99ian).
        "hashtags": "list_transform(CAST(payload->'hashtags' AS VARCHAR[]), tag -> url_decode(tag))",
        "likes": "CAST(payload->>'reactions_count' AS INT)",
        "comments": "CAST(payload->>'comments_count' AS INT)",
        "shares": "CAST(payload->>'shares_count' AS INT)",
        "views": "NULL::BIGINT",
    },
    "threads": {
        "post_id": "payload->>'post_id'",
        "content": "payload->>'message'",
        "posted_at": "to_timestamp(CAST(payload->>'timestamp' AS BIGINT))",
        "author_username": "payload->>'author_username'",
        "hashtags": "NULL::VARCHAR[]",
        "likes": "CAST(payload->>'like_count' AS INT)",
        "comments": "CAST(payload->>'reply_count' AS INT)",
        "shares": "CAST(payload->>'repost_count' AS INT)",
        "views": "NULL::BIGINT",
    },
    "tiktok": {
        "post_id": "payload->>'video_id'",
        "content": "payload->>'desc'",
        "posted_at": "to_timestamp(CAST(payload->>'create_time' AS BIGINT))",
        "author_username": "payload->>'author_username'",
        "hashtags": "CAST(payload->'hashtags' AS VARCHAR[])",
        "likes": "CAST(payload->>'like_count' AS INT)",
        "comments": "CAST(payload->>'comment_count' AS INT)",
        "shares": "CAST(payload->>'share_count' AS INT)",
        # BIGINT: lượt xem của video viral có thể vượt giới hạn 2,1 tỉ của INT.
        "views": "CAST(payload->>'play_count' AS BIGINT)",
    },
}

# Nếu một nền tảng có hơn tỉ lệ này số dòng thiếu content/posted_at thì gần như
# chắc chắn đang ánh xạ sai tên trường.
MAX_NULL_SHARE = 0.5

# Comment: chỉ scraped_at là chung - comment không mang keyword_id/url.
COMMENT_COMMON = {
    "scraped_at": "to_timestamp(kafka_ts / 1000)",
}

# Tên cột đầu ra giống nhau ở mọi nền tảng; chỉ biểu thức bên phải khác theo payload
# của từng spider (xem các mapper comment trong app/services/platforms.py).
COMMENT_COLUMNS = {
    "comment_id",
    "parent_comment_id",
    "post_id",
    "content",
    "posted_at",
    "author_id",
    "author_name",
    "likes",
    "replies",
}

COMMENT_FIELDS = {
    "facebook": {
        "comment_id": "payload->>'comment_id'",
        "parent_comment_id": "payload->>'parent_comment_id'",
        "post_id": "payload->>'post_id'",
        "content": "payload->>'message'",
        "posted_at": "to_timestamp(CAST(payload->>'timestamp' AS BIGINT))",
        "author_id": "payload->>'author_id'",
        "author_name": "payload->>'author_name'",
        "likes": "CAST(payload->>'reactions_count' AS INT)",
        "replies": "CAST(payload->>'replies_count' AS INT)",
    },
    "threads": {
        # Threads gọi comment là reply.
        "comment_id": "payload->>'reply_id'",
        "parent_comment_id": "payload->>'parent_reply_id'",
        "post_id": "payload->>'post_id'",
        "content": "payload->>'message'",
        "posted_at": "to_timestamp(CAST(payload->>'timestamp' AS BIGINT))",
        "author_id": "payload->>'author_id'",
        "author_name": "coalesce(payload->>'author_name', payload->>'author_username')",
        "likes": "CAST(payload->>'like_count' AS INT)",
        "replies": "CAST(payload->>'reply_count' AS INT)",
    },
    "tiktok": {
        # Spider comment TikTok gửi post_id (khác bài viết, vốn dùng video_id).
        "comment_id": "payload->>'comment_id'",
        "parent_comment_id": "payload->>'parent_comment_id'",
        "post_id": "payload->>'post_id'",
        "content": "payload->>'message'",
        "posted_at": "to_timestamp(CAST(payload->>'timestamp' AS BIGINT))",
        "author_id": "payload->>'author_id'",
        "author_name": "coalesce(payload->>'author_name', payload->>'author_username')",
        "likes": "CAST(payload->>'like_count' AS INT)",
        "replies": "CAST(payload->>'reply_count' AS INT)",
    },
}
